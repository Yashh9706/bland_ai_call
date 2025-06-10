import logging
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.events import EVENT_JOB_MISSED
from psycopg2.extras import RealDictCursor
import psycopg2
import requests

app = FastAPI()

DATABASE_URL = 'postgresql://neondb_owner:npg_eWph9LyzAki7@ep-winter-sea-a6tcb5f1.us-west-2.aws.neon.tech/neondb?sslmode=require'
BLAND_API_KEY = "org_0301c6c09e6f2613b52b17fb221b1b211abaa4e88525251a05982c0ccc8c494fa529dbc5e54dcae8ef0869"
PATHWAY_ID = "2bd6bfcc-1d5a-4129-b150-5ab9cab0ac2e"
CALL_URL = "https://api.bland.ai/v1/calls"
WEBHOOK_URL = "https://aca7-182-70-119-161.ngrok-free.app/webhook"

# Scheduler setup
jobstores = {'default': SQLAlchemyJobStore(url=DATABASE_URL)}
scheduler = BackgroundScheduler(jobstores=jobstores, job_defaults={'misfire_grace_time': 30})


def missed_job_listener(event):
    try:
        missed_job = scheduler.get_job(event.job_id)
        if missed_job:
            scheduler.add_job(
                missed_job.func,
                'date',
                run_date=datetime.now() + timedelta(seconds=5),
                args=missed_job.args,
                id=f"retry_{event.job_id}_{int(datetime.now().timestamp())}"
            )
    except Exception as e:
        logging.error(f"Error scheduling retry: {e}")


scheduler.add_listener(missed_job_listener, EVENT_JOB_MISSED)
scheduler.start()


def get_database_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        return None


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


def fetch_all_person_data():
    conn = get_database_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT pd.id, pd.full_name, pd.url, pd.sms_phone_numbers_used,
                   jd.job_title, jd.location, jd.estimated_pay
            FROM person_details_dummy pd
            LEFT JOIN uniti_med_job_data jd ON pd.url = jd.url
            WHERE pd.sms_phone_numbers_used IS NOT NULL AND pd.sms_phone_numbers_used != ''
        """)
        return cursor.fetchall()
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return []
    finally:
        cursor.close()
        conn.close()


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


def make_calls(person):
    try:
        phone_number = person['sms_phone_numbers_used'].strip()
        pay = str(person.get('estimated_pay', '')).replace('$', '').replace(',', '')
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
    except Exception as e:
        logging.error(f"Error making call: {e}")


@app.post("/initiate-calls", response_model=dict)
async def initiate_calls():
    try:
        people = fetch_all_person_data()
        if not people:
            return {"message": "No person details found in database"}

        now = datetime.now()
        for i, person in enumerate(people):
            scheduler.add_job(
                make_calls,
                'date',
                run_date=now + timedelta(seconds=i * 5),
                args=[person],
                id=f"call_{person['id']}_{int(now.timestamp()) + i * 5}"
            )
        return {"message": "Calls initiated successfully"}
    except Exception as e:
        logging.error(f"Error in initiate_calls: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")


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
    uvicorn.run(app, host="127.0.0.1", port=8000)