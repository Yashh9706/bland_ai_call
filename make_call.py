import logging
import time
from datetime import datetime, timedelta
from functools import wraps
from fastapi import FastAPI, HTTPException, Request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.events import EVENT_JOB_ERROR
from psycopg2.extras import RealDictCursor
import psycopg2
import requests
from typing import Tuple, List, Dict

# Retry decorator for database operations
def retry_on_db_error(max_retries=3, delay=1):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries == max_retries:
                        logging.error(f"Failed after {max_retries} retries: {str(e)}")
                        raise
                    logging.warning(f"Database operation failed, retrying ({retries}/{max_retries}): {str(e)}")
                    time.sleep(delay)
            return None
        return wrapper
    return decorator

app = FastAPI()

DATABASE_URL = 'postgresql://neondb_owner:npg_eWph9LyzAki7@ep-winter-sea-a6tcb5f1.us-west-2.aws.neon.tech/neondb?sslmode=require'
BLAND_API_KEY = "org_0301c6c09e6f2613b52b17fb221b1b211abaa4e88525251a05982c0ccc8c494fa529dbc5e54dcae8ef0869"
PATHWAY_ID = "aa324946-14c7-4e72-a68e-8ec4f44b7d88"
CALL_URL = "https://api.bland.ai/v1/calls"
WEBHOOK_URL = "https://94cd-103-241-232-74.ngrok-free.app/webhook"

# Scheduler setup with better error handling
def create_scheduler():
    try:
        jobstores = {
            'default': SQLAlchemyJobStore(
                url=DATABASE_URL,
                engine_options={
                    'pool_pre_ping': True,  # Enable connection health checks
                    'pool_recycle': 3600,   # Recycle connections after 1 hour
                    'pool_timeout': 30,     # Wait up to 30 seconds for a connection
                    'connect_args': {
                        'connect_timeout': 10  # PostgreSQL connection timeout
                    }
                }
            )
        }
        scheduler = BackgroundScheduler(
            jobstores=jobstores,
            job_defaults={
                'misfire_grace_time': 30,
                'coalesce': True,  # Combine missed executions
                'max_instances': 1  # Prevent concurrent executions of same job
            },
            timezone='UTC'
        )
        
        # Add error listener
        def job_error_listener(event):
            if event.exception:
                logging.error(f"Job failed: {event.job_id}")
                logging.error(f"Error: {str(event.exception)}", exc_info=event.exception)
                
        scheduler.add_listener(job_error_listener, EVENT_JOB_ERROR)
        scheduler.start()
        logging.info("Scheduler started successfully")
        return scheduler
    except Exception as e:
        logging.error(f"Failed to create scheduler: {e}")
        raise

scheduler = create_scheduler()

# Database connection helpers
@retry_on_db_error(max_retries=3, delay=1)
def get_database_connection():
    try:
        conn = psycopg2.connect(
            DATABASE_URL,
            connect_timeout=10,  # Wait up to 10 seconds for connection
            keepalives=1,        # Enable keepalive
            keepalives_idle=30,  # Send keepalive after 30 seconds of idle
            keepalives_interval=10,  # Retry keepalive every 10 seconds
            keepalives_count=3    # Retry 3 times before giving up
        )
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        raise

def execute_update(query, params):
    conn = get_database_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    try:
        cursor.execute(query, params)
        conn.commit()
        return True
    except Exception as e:
        logging.error(f"DB update error: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

# DB update functions
def store_call_id(phone_number, user_id, call_id):
    return execute_update(
        "UPDATE person_details_dummy SET call_id = %s WHERE sms_phone_numbers_used = %s AND id = %s",
        (call_id, phone_number, user_id)
    )

def store_intent_and_summary(call_id, intent, summary):
    return execute_update(
        "UPDATE person_details_dummy SET intent = %s, summary = %s WHERE call_id = %s",
        (intent, summary, call_id)
    )

def update_call_schedule_time(user_id: str, schedule_time: datetime) -> bool:
    return execute_update(
        "UPDATE person_details_dummy SET call_scheduled_at = %s WHERE id = %s",
        (schedule_time, user_id)
    )

# Fetch person data
def fetch_all_person_data():
    conn = get_database_connection()
    if not conn:
        logging.error("Database connection failed in fetch_all_person_data")
        return []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        # First, let's count total records to help with debugging
        cursor.execute("SELECT COUNT(*) as count FROM person_details_dummy")
        result = cursor.fetchone()
        total_count = result['count']
        logging.info(f"Total records in person_details_dummy: {total_count}")

        # Now fetch the actual data with less restrictive conditions
        cursor.execute("""
            WITH available_people AS (
                SELECT pd.id, pd.full_name, pd.sms_phone_numbers_used,
                       jd.job_title, jd.location, jd.estimated_pay,
                       pd.call_id, pd.call_scheduled_at
                FROM person_details_dummy pd
                LEFT JOIN uniti_med_job_data jd ON pd.url = jd.url
                WHERE pd.sms_phone_numbers_used IS NOT NULL 
                AND pd.sms_phone_numbers_used != ''
            )
            SELECT * FROM available_people
            WHERE call_id IS NULL  -- Include fresh records that haven't been called
            OR (call_id IS NOT NULL AND call_scheduled_at IS NULL)  -- Include failed immediate calls
            OR (call_scheduled_at IS NOT NULL AND call_scheduled_at <= NOW())  -- Include rescheduled calls whose time has come
        """)
        results = cursor.fetchall()
        
        # Add detailed logging
        logging.info(f"Found {len(results)} people available for calling")
        if len(results) == 0:
            # Log counts for each condition to help debug
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE call_id IS NULL) as no_call_count,
                    COUNT(*) FILTER (WHERE call_scheduled_at IS NULL) as no_schedule_count,
                    COUNT(*) FILTER (WHERE call_scheduled_at < NOW() - INTERVAL '24 hours') as old_call_count
                FROM person_details_dummy
                WHERE sms_phone_numbers_used IS NOT NULL 
                AND sms_phone_numbers_used != ''
            """)
            counts = cursor.fetchone()
            logging.info(f"Detailed counts: {counts}")
            
        return results
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        logging.exception("Full error details:")
        return []
    finally:
        cursor.close()
        conn.close()


def get_24h_call_count() -> int:
    """Get count of successful calls made in last 24 hours"""
    conn = get_database_connection()
    if not conn:
        return 0
    try:
        cursor = conn.cursor()
        # Count only successful calls (where call_id exists and was actually made)
        cursor.execute("""
            SELECT COUNT(*)
            FROM person_details_dummy
            WHERE call_id IS NOT NULL 
            AND call_id != '' 
            AND created_at >= NOW() - INTERVAL '24 hours'
            -- Don't check call_scheduled_at for counting as it's only for rescheduled calls
        """)
        count = cursor.fetchone()[0]
        return count
    except Exception as e:
        logging.error(f"Error counting 24h calls: {e}")
        return 0
    finally:
        cursor.close()
        conn.close()

def get_next_available_slot(current_time: datetime) -> datetime:
    """Get next available time slot based on 24h call count"""
    call_count = get_24h_call_count()
    if call_count >= 2000:
        # If daily limit reached, schedule for next day at same time
        return current_time + timedelta(days=1)
    return current_time

# Call analysis
def analyze_call_intent(call_id):
    try:
        response = requests.post(
            f"{CALL_URL}/{call_id}/analyze",
            json={
                "goal": "Analyze caller's response to job opportunity",
                "questions": [[
                    "Based on the caller's response, categorize their interest: Answer 'yes' if genuinely interested in the job, 'no' if not interested/declined, or 'later' if they said they're busy/call later/call back later/will call you back",
                    "string"
                ]]
            },
            headers={
                "Authorization": f"Bearer {BLAND_API_KEY}",
                "Content-Type": "application/json"
            }
        )
        if response.status_code == 200:
            answer = response.json().get('answers', [None])[0]
            intent = (answer.get('answer') if isinstance(answer, dict) else answer or '').strip().lower()
            return intent if intent in {"yes", "no", "later"} else "unknown"
        return "error"
    except Exception as e:
        logging.error(f"Exception analyzing call: {e}")
        return "error"

# Get call summary
def get_call_summary(call_id):
    try:
        response = requests.get(
            f"{CALL_URL}/{call_id}",
            headers={"Authorization": f"Bearer {BLAND_API_KEY}"}
        )
        return response.json().get("summary", "No summary available.") if response.status_code == 200 else "Error fetching summary."
    except Exception as e:
        logging.error(f"Exception while getting summary: {e}")
        return "Error fetching summary."

# Make individual call
def make_calls(person):
    try:
        logging.info(f"Starting call for person: {person.get('id')}")
        # Check if we've hit the 24-hour call limit
        call_count = get_24h_call_count()
        current_time = datetime.now()
        
        logging.info(f"Current 24h call count: {call_count}")
        if call_count >= 2000:
            # Calculate next day same time
            original_time = person.get('call_scheduled_at', current_time)
            next_time = original_time + timedelta(days=1)
            
            # Reschedule the call for next day at same time
            job_id = f"call_{person['id']}_{int(next_time.timestamp())}"
            scheduler.add_job(
                make_calls,
                trigger='date',
                run_date=next_time,
                args=[person],
                id=job_id
            )
            update_call_schedule_time(person['id'], next_time)
            logging.info(f"24-hour limit reached (2000 calls). Rescheduling call for {person['id']} to {next_time}")
            return

        phone_number = person['sms_phone_numbers_used'].strip()
        pay = str(person.get('estimated_pay', '')).replace('$', '').replace(',', '')
            
        logging.info(f"Attempting to make call to {phone_number}")
        response = requests.post(
            CALL_URL,
            json={
                "phone_number": phone_number,
                "pathway_id": PATHWAY_ID,
                "pronunciation_guide": {"$": "dollars"},
                "voice": "85a2c852-2238-4651-acf0-e5cbe02186f2",
                "wait_for_greeting": True,
                "noise_cancellation": True,
                "webhook": WEBHOOK_URL,
                "request_data": {
                    "full_name": person.get('full_name'),
                    "job_title": person.get('job_title'),
                    "location": person.get('location'),
                    "pay": pay,
                    "user_name": person.get('id')
                }
            },
            headers={
                "Authorization": f"Bearer {BLAND_API_KEY}",
                "Content-Type": "application/json"
            }
        )
        if response.status_code == 200:
            call_id = response.json().get('call_id')
            if call_id:
                store_call_id(phone_number, person.get('id'), call_id)
                # No call_scheduled_at update for immediate calls - we leave it NULL
            else:
                logging.error("Call API returned 200 but no call_id in response")
        else:
            logging.error(f"Call API error. Status: {response.status_code}, Response: {response.text}")
    except Exception as e:
        logging.error(f"Error making call: {str(e)}", exc_info=True)

# Endpoint to initiate all calls concurrently
@app.post("/initiate-calls", response_model=dict)
async def initiate_calls():
    try:
        people = fetch_all_person_data()
        if not people:
            return {"message": "No person details found in database"}

        scheduled_counts = {}
        total_scheduled = 0
        current_time = datetime.now()
        
        # Make immediate calls for each person
        for person in people:
            call_count = get_24h_call_count()
            if call_count >= 2000:
                # If we hit the limit, schedule for next day
                next_time = current_time + timedelta(days=1)
                job_id = f"call_{person['id']}_{int(next_time.timestamp())}"
                scheduler.add_job(
                    make_calls,
                    trigger='date',
                    run_date=next_time,
                    args=[person],
                    id=job_id
                )
                update_call_schedule_time(person['id'], next_time)
                schedule_str = next_time.strftime("%Y-%m-%d %H:%M:%S")
            else:
                # Make immediate call
                make_calls(person)
                schedule_str = current_time.strftime("%Y-%m-%d %H:%M:%S")
            
            # Track counts
            scheduled_counts[schedule_str] = scheduled_counts.get(schedule_str, 0) + 1
            total_scheduled += 1
            
            # Add small delay between schedules to spread out the load
            current_time = current_time + timedelta(seconds=1)

        # Prepare response message
        response_messages = [
            f"{count} calls scheduled for {slot_time}"
            for slot_time, count in scheduled_counts.items()
        ]

        return {
            "message": " and ".join(response_messages),
            "total_scheduled": total_scheduled
        }
    except Exception as e:
        logging.error(f"Error in initiate_calls: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


# Webhook endpoint to process individual call responses
@app.post("/webhook")
async def webhook(request: Request):
    try:
        data = await request.json()
        call_id = data.get('call_id')
        phone_number = data.get('to', 'Unknown')
        if call_id:
            intent = analyze_call_intent(call_id)
            summary = get_call_summary(call_id)
            store_intent_and_summary(call_id, intent, summary)
            return {
                "message": "Webhook processed successfully",
                "call_id": call_id,
                "phone_number": phone_number,
                "intent": intent,
                "summary": summary
            }
        return {"message": "No call_id found", "intent": "unknown", "summary": None}
    except Exception as e:
        logging.error(f"Webhook processing error: {e}")
        return {
            "message": "Error processing webhook",
            "error": str(e),
            "intent": "error",
            "summary": None
        }

# Database initialization
def initialize_database():
    """Initialize database schema with required columns"""
    conn = get_database_connection()
    if not conn:
        return False
    cursor = conn.cursor()
    try:
        # Add call_scheduled_at column if it doesn't exist
        cursor.execute("""
            DO $$ 
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 
                    FROM information_schema.columns 
                    WHERE table_name='person_details_dummy' 
                    AND column_name='call_scheduled_at'
                ) THEN
                    ALTER TABLE person_details_dummy ADD COLUMN call_scheduled_at TIMESTAMP;
                END IF;
            END $$;
        """)
        conn.commit()
        logging.info("Database schema initialized successfully")
        return True
    except Exception as e:
        logging.error(f"Error initializing database schema: {e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

# Main app runner
if __name__ == "__main__":
    import uvicorn
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('call_logs.log'),
            logging.StreamHandler()
        ]
    )
    
    # Initialize database schema before starting the server
    if not initialize_database():
        logging.error("Failed to initialize database schema")
        exit(1)
        
    uvicorn.run(app, host="127.0.0.1", port=8000)
