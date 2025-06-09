import psycopg2
import requests
import threading
import re
import time
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
import signal
import sys
from contextlib import contextmanager
from flask import Flask, request, jsonify

call_results_lock = threading.Lock()

app = Flask(__name__)
call_webhook_results = {}  # Store webhook results by call_id

# Load environment variables
load_dotenv(override=True)

# Configuration from environment variables
db_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "sslmode": os.getenv("DB_SSLMODE", "require")
}
API_KEY = os.getenv("BLAND_API_KEY")
PATHWAY_ID = os.getenv("BLAND_PATHWAY_ID")
TABLE_NAME = os.getenv("TABLE_NAME", "person_details_dummy")

# Scheduler setup
DATABASE_URL = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}?sslmode={db_params['sslmode']}"
scheduler = BackgroundScheduler(
    jobstores={'default': SQLAlchemyJobStore(url=DATABASE_URL)},
    job_defaults={'misfire_grace_time': 30, 'coalesce': True, 'max_instances': 1},
    executors={'default': {'type': 'threadpool', 'max_workers': 10}}
)
scheduler.start()

def log_message(level, message):
    """Centralized logging function with timestamp and emoji."""
    levels = {"INFO": "ℹ️", "ERROR": "❌", "WARNING": "⚠️", "SUCCESS": "✅"}
    emoji = levels.get(level, "ℹ️")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] {emoji} {message}")

def validate_env_variables():
    """Validates required environment variables."""
    required_vars = ["DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", "BLAND_API_KEY", "BLAND_PATHWAY_ID"]
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        log_message("ERROR", f"Missing environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

@contextmanager
def get_db_connection():
    """Context manager for database connections."""
    conn = psycopg2.connect(**db_params)
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def get_db_cursor(conn):
    """Context manager for database cursors."""
    cur = conn.cursor()
    try:
        yield cur
    finally:
        cur.close()

def signal_handler(sig, frame):
    """Handles shutdown signals."""
    log_message("INFO", "Received shutdown signal, stopping scheduler...")
    if scheduler.running:
        scheduler.shutdown(wait=True)
        log_message("SUCCESS", "Scheduler shut down successfully")
    log_message("INFO", "Script execution completed")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

@app.route('/webhook', methods=['POST'])
def handle_end_call_webhook():
    """Handle incoming webhook when a call ends."""
    try:
        webhook_data = request.get_json()
        call_id = webhook_data.get('call_id')
        completed = webhook_data.get('completed', False)
        call_ended_by = webhook_data.get('call_ended_by', 'UNKNOWN')
        summary = webhook_data.get('summary', 'No summary available')
        
        log_message("INFO", f"🔔 WEBHOOK RECEIVED - Call {call_id} ended by {call_ended_by}")
        
        # Store webhook result
        call_webhook_results[call_id] = {
            'completed': completed,
            'summary': summary,
            'call_ended_by': call_ended_by,
            'webhook_received': True
        }
        
        return jsonify({"status": "success", "message": "Webhook processed"}), 200
        
    except Exception as e:
        log_message("ERROR", f"Error processing webhook: {str(e)}")
        return jsonify({"error": "Failed to process webhook"}), 500

def run_webhook_server():
    """Run the Flask webhook server in background"""
    log_message("INFO", "🚀 Starting webhook server on http://localhost:5000")
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False, threaded=True)

def analyze_call_interest(call_id, full_name):
    """Analyzes call to determine intent using the provided API endpoint."""
    log_message("INFO", f"Starting intent analysis for {full_name} (Call ID: {call_id})")
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        response = session.post(
            f"https://api.bland.ai/v1/calls/{call_id}/analyze",
            headers={"authorization": API_KEY, "Content-Type": "application/json"},
            json={
                "goal": "Determine caller's interest in job opportunity",
                "questions": [
                    ["Is the caller interested in taking a job (e.g., said ‘interested,’ ‘apply,’ asked about job details, or expressed intent to work)?", "string"]
                ]
            },
            timeout=10
        )
        if response.status_code == 200 and response.json().get("answers"):
            answer = response.json()["answers"][0].lower().strip()
            call_intent = "no"
            if answer == "yes":
                call_intent = "yes"
            elif answer == "callback_later":
                call_intent = "later"
            log_message("SUCCESS", f"Intent analysis for {full_name}: {call_intent}")
            return {'call_intent': call_intent}
        log_message("ERROR", f"Analysis failed for {full_name}: Invalid response")
        return {'call_intent': "no"}
    except requests.exceptions.RequestException as e:
        log_message("ERROR", f"Error analyzing call for {full_name}: {str(e)}")
        return {'call_intent': "no"}
    except Exception as e:
        log_message("ERROR", f"Unexpected error in intent analysis for {full_name}: {str(e)}")
        return {'call_intent': "no"}

def extract_first_url(url_string):
    """Extracts the first URL from a string."""
    if not url_string:
        return None
    return url_string.split(';')[0].strip() or None

def get_job_details_from_url(url):
    """Fetches job details from database."""
    with get_db_connection() as conn:
        with get_db_cursor(conn) as cursor:
            try:
                cursor.execute("SELECT job_title, location, estimated_pay FROM uniti_med_job_data WHERE url = %s LIMIT 1", (url,))
                result = cursor.fetchone()
                return {'job_title': result[0], 'location': result[1], 'pay': result[2]} if result else None
            except Exception as e:
                log_message("ERROR", f"Failed to fetch job details for URL {url}: {str(e)}")
                return None

def extract_valid_phone(directdials):
    """Extracts the first valid phone number."""
    if not directdials or str(directdials).lower() in ['none', 'null', '""', '[]'] or len(str(directdials)) <= 5:
        return None
    phone_data = directdials
    if isinstance(directdials, str):
        if directdials.startswith('"') and directdials.endswith('"'):
            phone_data = directdials[1:-1]
        try:
            phone_data = json.loads(directdials)
        except:
            pass
    phone_string = str(phone_data) if isinstance(phone_data, (list, dict)) else phone_data
    numbers = re.split(r'[;,/\s]+', phone_string)
    for number in numbers:
        clean = re.sub(r'[^\d+]', '', number.strip())
        if clean:
            if not clean.startswith('+') and len(clean) == 10:
                clean = '+1' + clean
            elif not clean.startswith('+') and len(clean) == 11 and clean.startswith('1'):
                clean = '+' + clean
            elif not clean.startswith('+') and 8 <= len(clean) <= 15:
                clean = '+' + clean
            if 8 <= len(clean) <= 15 and clean.startswith('+'):
                return clean
    return None

def get_call_summary_directly(call_id, full_name):
    """Fetches call summary from API."""
    log_message("INFO", f"Fetching call summary for {full_name} (Call ID: {call_id})")
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        response = session.get(
            f"https://api.bland.ai/v1/calls/{call_id}",
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=10
        )
        if response.status_code == 200:
            result = response.json()
            return {'summary': result.get('summary', 'No summary available')}
        log_message("ERROR", f"Failed to fetch call summary for {full_name}: {response.status_code}")
        return {'summary': f'Failed to fetch summary - Status {response.status_code}'}
    except requests.exceptions.RequestException as e:
        log_message("ERROR", f"Error fetching call summary for {full_name}: {str(e)}")
        return {'summary': f'Error fetching summary: {str(e)}'}
    except Exception as e:
        log_message("ERROR", f"Unexpected error fetching summary for {full_name}: {str(e)}")
        return {'summary': f'Error fetching summary: {str(e)}'}

def handle_api_error(full_name, call_id, response, error_type, call_data=None):
    """Handles API errors with standardized result."""
    log_message("ERROR", f"API {error_type} for {full_name}: {response.status_code if response else 'No response'}")
    return {
        'name': full_name,
        'call_id': call_id or 'N/A',
        'call_intent': 'no',
        'summary': call_data.get('summary', f'{error_type.capitalize()} - unable to retrieve summary') if call_data else f'{error_type.capitalize()} - unable to retrieve summary'
    }

def fetch_call_details(call_id, full_name):
    """Wait for webhook notification instead of polling API."""
    log_message("INFO", f"Waiting for webhook notification: {full_name} (ID: {call_id})")
    
    max_wait_time = 300  # 5 minutes timeout
    start_time = time.time()
    
    while time.time() - start_time < max_wait_time:
        # Check if webhook has been received
        if call_id in call_webhook_results:
            webhook_data = call_webhook_results[call_id]
            log_message("SUCCESS", f"Webhook received for {full_name}, processing results...")
            
            # Get call intent analysis
            analysis_result = analyze_call_interest(call_id, full_name)
            
            # Use webhook summary or get fresh summary
            summary = webhook_data.get('summary', 'No summary available')
            if summary == 'No summary available':
                call_data = get_call_summary_directly(call_id, full_name)
                summary = call_data['summary']
            
            # Clean up webhook result
            del call_webhook_results[call_id]
            
            return {
                'name': full_name,
                'call_id': call_id,
                'call_intent': analysis_result['call_intent'],
                'summary': summary
            }
        
        time.sleep(2)  # Check every 2 seconds
    
    # Timeout - fallback to API call
    log_message("WARNING", f"Webhook timeout for {full_name}, falling back to API polling")
    call_data = get_call_summary_directly(call_id, full_name)
    analysis_result = analyze_call_interest(call_id, full_name)
    
    return {
        'name': full_name,
        'call_id': call_id,
        'call_intent': analysis_result['call_intent'],
        'summary': call_data['summary']
    }

def make_call(full_name, job_details, phone_number, results_list, user_id):
    """Initiates a call and returns analysis results."""
    job_title = job_details.get('job_title', 'Unknown Position')
    location = job_details.get('location', 'Unknown Location')
    pay = job_details.get('pay', 'Competitive Pay')
    pay_match = re.findall(r"\$?([\d,]+)", pay)
    if pay_match:
        numeric_values = [val.replace(',', '') for val in pay_match]
        pay = f"{numeric_values[0]} dollars"
        if len(numeric_values) == 2:
            pay = f"{numeric_values[0]} dollars to {numeric_values[1]} dollars"
    
    log_message("INFO", f"Initiating call for {full_name} (User ID: {user_id}, Job Title: {job_title}, Location: {location}, Pay: {pay}, Phone: {phone_number})")
    
    # UPDATE THIS DATA DICTIONARY - Add webhook URL
    data = {
        "phone_number": phone_number,
        "pathway_id": PATHWAY_ID,
        "pronunciation_guide": {"$": "dollars"},
        "voice": "85a2c852-2238-4651-acf0-e5cbe02186f2",
        "wait_for_greeting": True,
        "noise_cancellation": True,
        "webhook": "https://755b-103-241-232-74.ngrok-free.app/webhook",  # ADD THIS LINE
        "record": True,  # ADD THIS LINE to enable recording
        "request_data": {
            "full_name": full_name,
            "job_title": job_title,
            "location": location,
            "pay": pay,
            "user_name": full_name
        }
    }
    
    # Rest of the make_call function remains the same...
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        response = session.post(
            "https://api.bland.ai/v1/calls",
            headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
            json=data,
            timeout=10
        )
        result = response.json()
        if response.status_code == 200 and result.get("call_id"):
            call_id = result["call_id"]
            log_message("SUCCESS", f"Call initiated successfully for {full_name} (Call ID: {call_id})")
            call_result = fetch_call_details(call_id, full_name)
            with call_results_lock:
                call_result['user_id'] = user_id
                results_list.append(call_result)
                log_message("INFO", f"Added result for {full_name} to call_results: {call_result}")
            return call_result
        return handle_api_error(full_name, None, response, 'failed')
    except requests.exceptions.RequestException as e:
        log_message("ERROR", f"Error making call for {full_name}: {str(e)}")
        error_result = handle_api_error(full_name, None, None, 'error')
        with call_results_lock:
            error_result['user_id'] = user_id
            results_list.append(error_result)
            log_message("INFO", f"Added error result for {full_name} to call_results: {error_result}")
        return error_result
    except Exception as e:
        log_message("ERROR", f"Unexpected error making call for {full_name}: {str(e)}")
        error_result = handle_api_error(full_name, None, None, 'error')
        with call_results_lock:
            error_result['user_id'] = user_id
            results_list.append(error_result)
            log_message("INFO", f"Added error result for {full_name} to call_results: {error_result}")
        return error_result

def schedule_call(user_id, full_name, job_details, phone_number, results_list):
    """Schedules a call for a user."""
    run_time = datetime.now() + timedelta(minutes=1)
    job_id = f"call_{user_id}_{int(run_time.timestamp())}"
    log_message("INFO", f"Scheduling call for {full_name} (User ID: {user_id}) at {run_time}")
    try:
        job = scheduler.add_job(
            make_call,
            'date',
            run_date=run_time,
            args=[full_name, job_details, phone_number, results_list, user_id],
            id=job_id,
            misfire_grace_time=30
        )
        log_message("SUCCESS", f"Call scheduled for {full_name} (User ID: {user_id}, Job ID: {job_id})")
        return job_id
    except Exception as e:
        log_message("ERROR", f"Failed to schedule call for {full_name}: {str(e)}")
        return None

def print_call_summary(call_results):
    """Prints a summary of call results."""
    if not call_results:
        log_message("ERROR", "No call results to summarize.")
        return
    log_message("INFO", "CALL SUMMARY")
    print("="*50)
    for result in call_results:
        summary_template = f"""\
Call ID: {result.get('call_id', 'Unknown')}
Intent: {result.get('call_intent', 'no').upper()}
Summary: {result.get('summary', 'No summary available')}
"""
        print(summary_template)
        print("-"*50)
    print("="*50)

def display_call_preview(call_list):
    """Displays a preview of scheduled calls."""
    log_message("INFO", f"CALL PREVIEW - {len(call_list)} calls to be made:")
    print("="*80)
    for i, (_, name, job_details, phone) in enumerate(call_list, 1):
        job_title = job_details.get('job_title', 'Unknown Position')
        location = job_details.get('location', 'Unknown Location')
        pay = job_details.get('pay', 'Competitive Pay')
        preview_template = f"""\
CALL #{i}
👤 Full Name: {name}
📋 Job Title: {job_title}
📍 Location: {location}
💰 Pay: {pay}
📱 Phone: {phone}
"""
        print(preview_template)
        print("-"*80)
    print(f"\n🚀 Starting {len(call_list)} calls now...\n")

if __name__ == "__main__":
    try:
        validate_env_variables()
        
        # START WEBHOOK SERVER IN BACKGROUND THREAD
        webhook_thread = threading.Thread(target=run_webhook_server, daemon=True)
        webhook_thread.start()
        time.sleep(2)  # Wait for server to start
        
        log_message("INFO", "Starting call scheduling process...")
        with get_db_connection() as conn:
            with get_db_cursor(conn) as cursor:
                cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", ('person_details_dummy',))
                if not cursor.fetchone()[0]:
                    log_message("ERROR", f"Table {TABLE_NAME} does not exist!")
                    sys.exit(1)
                cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
                total_records = cursor.fetchone()[0]
                log_message("INFO", f"Total records in {TABLE_NAME}: {total_records}")
                cursor.execute(
                    f"SELECT id, full_name, url, directdials FROM {TABLE_NAME} WHERE full_name IS NOT NULL AND url IS NOT NULL AND directdials IS NOT NULL ORDER BY id LIMIT 5"
                )
                records = cursor.fetchall()
                log_message("INFO", f"Found {len(records)} records matching initial query conditions")
                call_list = []
                used_phone_numbers = set()
                for user_id, full_name, url_string, directdials in records:
                    log_message("INFO", f"Processing record: ID={user_id}, Name={full_name}, URL={url_string}, Directdials={directdials}")
                    first_url = extract_first_url(url_string)
                    if not first_url:
                        log_message("WARNING", f"Skipping {full_name}: No valid URL")
                        continue
                    job_details = get_job_details_from_url(first_url)
                    if not job_details or not job_details.get('job_title'):
                        log_message("WARNING", f"Skipping {full_name}: No valid job details")
                        continue
                    phone_number = extract_valid_phone(directdials)
                    if not phone_number:
                        log_message("WARNING", f"Skipping {full_name}: No valid phone number")
                        continue
                    if phone_number in used_phone_numbers:
                        log_message("WARNING", f"Skipping {full_name}: Phone number {phone_number} already scheduled")
                        continue
                    call_list.append((user_id, full_name, job_details, phone_number))
                    used_phone_numbers.add(phone_number)
                    log_message("SUCCESS", f"Added {full_name} to call list with phone {phone_number}")
                if not call_list:
                    log_message("ERROR", "No valid records found to call.")
                    sys.exit(1)
                display_call_preview(call_list)
                call_results = []
                scheduled_jobs = []
                log_message("INFO", f"Processing {len(call_list)} calls...")
                for user_id, full_name, job_details, phone_number in call_list:
                    for existing_job_id, existing_user_id, _ in scheduled_jobs[:]:
                        if existing_user_id == user_id:
                            scheduler.remove_job(existing_job_id)
                            scheduled_jobs.remove((existing_job_id, existing_user_id, full_name))
                            log_message("INFO", f"Removed existing job for {full_name} (User ID: {user_id})")
                    job_id = schedule_call(user_id, full_name, job_details, phone_number, call_results)
                    if job_id:
                        scheduled_jobs.append((job_id, user_id, full_name))
                conn.commit()
                if not scheduled_jobs:
                    log_message("ERROR", "No calls were successfully scheduled. Exiting...")
                    sys.exit(1)
                log_message("SUCCESS", f"Call scheduling complete. {len(scheduled_jobs)} calls scheduled.")
                log_message("INFO", "Scheduled calls will execute in background. Press Ctrl+C to stop.")
                # Wait for call results to be populated
                start_time = time.time()
                timeout = 600  # 10 minutes timeout for all calls to complete
                while len(call_results) < len(scheduled_jobs) and time.time() - start_time < timeout:
                    time.sleep(10)
                log_message("SUCCESS", "All call results collected or timeout reached.")
                print_call_summary(call_results)
    except psycopg2.Error as e:
        log_message("ERROR", f"Database error: {e}")
    except Exception as e:
        log_message("ERROR", f"Unexpected error: {e}")
    finally:
        if scheduler.running:
            try:
                scheduler.shutdown(wait=True)
                log_message("SUCCESS", "Scheduler shut down successfully")
            except Exception as e:
                log_message("ERROR", f"Error during scheduler shutdown: {e}")
        log_message("INFO", "Script execution completed")
