import psycopg2
import requests
import threading
import re
import time
import json
import os
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from datetime import datetime, timedelta
import threading
import time
import signal
import sys
call_results_lock = threading.Lock()


# Load environment variables from .env file
load_dotenv(override=True)

# PostgreSQL connection parameters - FROM ENV
db_params = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "sslmode": os.getenv("DB_SSLMODE", "require")
}

# Bland API config - FROM ENV
API_KEY = os.getenv("BLAND_API_KEY")
PATHWAY_ID = os.getenv("BLAND_PATHWAY_ID")
print(PATHWAY_ID)

# Scheduler setup
DATABASE_URL = f"postgresql+psycopg2://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['dbname']}?sslmode={db_params['sslmode']}"
jobstores = {
    'default': SQLAlchemyJobStore(url=DATABASE_URL)
}
scheduler = BackgroundScheduler(
    jobstores=jobstores,
    job_defaults={
        'misfire_grace_time': 30,  # Allow 30 seconds grace period for missed jobs
        'coalesce': True,          # Run missed jobs once instead of multiple times
        'max_instances': 1         # Limit to one instance per job
    },
    executors={
        'default': {'type': 'threadpool', 'max_workers': 10}  # Use thread pool with 10 workers
    }
)
scheduler.start()

# Table name - FROM ENV (with fallback)
TABLE_NAME = os.getenv("TABLE_NAME", "person_details_dummy")

def validate_env_variables():
    """Validates that all required environment variables are set"""
    required_vars = [
        "DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST", 
        "BLAND_API_KEY", "BLAND_PATHWAY_ID"
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        exit(1)

def log_scheduled_call(cursor, user_id, full_name, phone_number, job_id, run_time, call_id=None):
    """Logs scheduled call details to the person_details_dummy table, excluding status"""
    try:
        # Ensure required columns exist in person_details_dummy (excluding status)
        required_columns = ['job_id', 'run_time', 'call_id']
        existing_columns = set()
        cursor.execute("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_name = %s
        """, ('person_details_dummy',))
        for row in cursor.fetchall():
            existing_columns.add(row[0])

        for column in required_columns:
            if column not in existing_columns:
                if column == 'job_id':
                    cursor.execute(f"""
                        ALTER TABLE person_details_dummy
                        ADD COLUMN job_id VARCHAR(100)
                    """)
                elif column == 'run_time':
                    cursor.execute(f"""
                        ALTER TABLE person_details_dummy
                        ADD COLUMN run_time TIMESTAMP
                    """)
                elif column == 'call_id':
                    cursor.execute(f"""
                        ALTER TABLE person_details_dummy
                        ADD COLUMN call_id VARCHAR(50)
                    """)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 📝 Added column {column} to person_details_dummy table")
        
        # Update person_details_dummy with call schedule details, excluding status
        cursor.execute(f"""
            UPDATE person_details_dummy
            SET job_id = %s, run_time = %s, call_id = %s
            WHERE id = %s
        """, (job_id, run_time, call_id, user_id))
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 📝 Logged scheduled call for {full_name} (User ID: {user_id}) in person_details_dummy")
        return True
    except psycopg2.Error as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Error logging scheduled call for {full_name} (User ID: {user_id}): {e}")
        raise

def signal_handler(sig, frame):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🛑 Received shutdown signal, stopping scheduler...")
    if scheduler.running:
        scheduler.shutdown(wait=True)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🛑 Scheduler shut down successfully")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🛑 Script execution completed")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)  # Handle Ctrl+C
signal.signal(signal.SIGTERM, signal_handler)  # Handle termination signals

def log_missed_job(cursor, job_id, user_id, full_name):
    """Logs a missed job to the person_details_dummy table"""
    try:
        cursor.execute(f"""
            UPDATE person_details_dummy
            SET status = %s
            WHERE job_id = %s
        """, ('missed', job_id))
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Logged missed job for {full_name} (User ID: {user_id}, Job ID: {job_id})")
    except psycopg2.Error as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Error logging missed job for {full_name}: {e}")


def analyze_call_interest(call_id, full_name):
    """Analyzes the call to determine if the caller showed interest in the job"""
    try:
        response = requests.post(
            f"https://api.bland.ai/v1/calls/{call_id}/analyze",
            headers={
                "authorization": API_KEY,
                "Content-Type": "application/json"
            },
            json={
                "goal": "Analyze the caller's response to the job opportunity call and determine their interest level and intent",
                "questions": [
                    ["Based on the conversation, is the caller genuinely interested in the job opportunity (expressed interest, asked questions about the job, wanted more details, or showed positive engagement)?", "boolean"],
                    ["What is the caller's primary response category: 'interested' (wants the job/more info), 'not_interested' (clearly declined/not interested), 'callback_later' (wants to be contacted later/needs time), 'no_answer' (didn't answer or very brief interaction), or 'unclear' (mixed signals)?", "string"],
                    ["What specific concerns, questions, or requirements did the caller mention about the job opportunity?", "string"],
                    ["What was the main reason behind the caller's response? Be specific about what they said or their primary concern/interest.", "string"],
                    ["Did the caller ask for more information, request a callback, or show any form of engagement with the opportunity?", "boolean"]
                ]
            },
            timeout=30
        )

        if response.status_code == 200:
            response_json = response.json()
            print(f"🔍 Analysis response for {full_name}: {response_json}")
            
            if "answers" in response_json and response_json["answers"] is not None:
                answers = response_json["answers"]
                
                # Safely extract answers with proper None checks
                is_interested = answers[0] if len(answers) > 0 and answers[0] is not None else False
                primary_response = answers[1] if len(answers) > 1 and answers[1] is not None else "unclear"
                concerns = answers[2] if len(answers) > 2 and answers[2] is not None else "No specific concerns mentioned"
                reason = answers[3] if len(answers) > 3 and answers[3] is not None else "No specific reason provided"
                showed_engagement = answers[4] if len(answers) > 4 and answers[4] is not None else False
                
                # Convert to string and clean up
                primary_response = str(primary_response).lower().strip()
                concerns = str(concerns).strip() if concerns and str(concerns).lower() not in ['none', 'null', ''] else "No specific concerns mentioned"
                reason = str(reason).strip() if reason and str(reason).lower() not in ['none', 'null', ''] else "No specific reason provided"
                
                # Map to yes/no/later
                call_intent = "no"  # Default
                if primary_response == "interested" or is_interested or showed_engagement:
                    call_intent = "yes"
                elif primary_response == "callback_later" or "later" in primary_response:
                    call_intent = "later"
                elif primary_response in ["not_interested", "no_answer", "unclear"]:
                    call_intent = "no"
                
                return {
                    'call_intent': call_intent,
                    'is_interested': is_interested,
                    'concerns': concerns,
                    'reason': reason,
                    'showed_engagement': showed_engagement,
                    'raw_intent': primary_response
                }
            else:
                print(f"⚠️ No answers in analysis response for {full_name}")
                return {
                    'call_intent': "no",  # Default to 'no' for failed analysis
                    'is_interested': None,
                    'concerns': "Analysis failed - no answers received",
                    'reason': "Analysis failed - no answers received",
                    'showed_engagement': None,
                    'raw_intent': "unknown"
                }
        else:
            print(f"❌ Analysis API error for {full_name}: {response.status_code}")
            print(f"Response: {response.text}")
            return {
                'call_intent': "no",  # Default to 'no' for API errors
                'is_interested': None,
                'concerns': f"API Error - Status {response.status_code}",
                'reason': f"API Error - Status {response.status_code}",
                'showed_engagement': None,
                'raw_intent': "error"
            }

    except Exception as e:
        print(f"❌ Error analyzing call for {full_name}: {str(e)}")
        return {
            'call_intent': "no",  # Default to 'no' for exceptions
            'is_interested': None,
            'concerns': f"Exception occurred: {str(e)}",
            'reason': f"Exception occurred: {str(e)}",
            'showed_engagement': None,
            'raw_intent': "error"
        }

def extract_first_url(url_string):
    """Extracts the first URL from a string that may contain multiple URLs separated by ';'"""
    if not url_string:
        return None
    
    urls = url_string.split(';')
    first_url = urls[0].strip()
    return first_url if first_url else None

def get_job_details_from_url(cursor, url):
    """Fetches job_title, location, and estimated_pay from uniti_med_job_data table based on URL"""
    try:
        cursor.execute("""
            SELECT job_title, location, estimated_pay 
            FROM uniti_med_job_data 
            WHERE url = %s
            LIMIT 1
        """, (url,))
        
        result = cursor.fetchone()
        if result:
            job_title, location, estimated_pay = result
            return {
                'job_title': job_title,
                'location': location,
                'pay': estimated_pay
            }
        else:
            return None
            
    except Exception as e:
        return None

def extract_valid_phone(directdials):
    """Extracts the first valid phone number from JSON or string data"""
    if not directdials:
        return None
    
    # Handle different NULL representations
    if str(directdials).lower() in ['none', 'null', '', '""']:
        return None
    
    # Handle JSON string format - remove quotes if it's a JSON string
    phone_data = directdials
    if isinstance(directdials, str):
        # Remove outer quotes if it's a JSON string
        if directdials.startswith('"') and directdials.endswith('"'):
            phone_data = directdials[1:-1]
        
        # Try to parse as JSON
        try:
            import json
            phone_data = json.loads(directdials)
        except:
            # If JSON parsing fails, use the cleaned string
            pass
    
    # Convert to string for processing
    if isinstance(phone_data, (list, dict)):
        phone_string = str(phone_data)
    else:
        phone_string = str(phone_data)
    
    # Split by comma first, then by other separators
    numbers = re.split(r'[;,/\s]+', phone_string)

    for number in numbers:
        clean = re.sub(r'[^\d+]', '', number.strip())
        if clean:
            if not clean.startswith('+') and len(clean) == 10:
                clean = '+1' + clean
            elif not clean.startswith('+') and len(clean) == 11 and clean.startswith('1'):
                clean = '+' + clean
            elif clean.startswith('+') and 8 <= len(clean) <= 15:
                pass
            elif not clean.startswith('+') and 8 <= len(clean) <= 15:
                clean = '+' + clean
            else:
                continue
            if 8 <= len(clean) <= 15 and clean.startswith('+'):
                return clean
    return None

def get_call_summary_directly(call_id, full_name):
    """Directly fetch call summary from the API without analysis"""
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        
        response = session.get(
            f"https://api.bland.ai/v1/calls/{call_id}",
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=60  # Increased timeout
        )

        if response.status_code == 200:
            result = response.json()
            return {
                'summary': result.get('summary', 'No summary available'),
                'transcript': result.get('transcript', 'No transcript available'),
                'status': result.get('status', 'unknown'),
                'duration': result.get('corrected_duration', result.get('duration', 'Unknown')),
                'answered': result.get('answered', False),
                'created_at': result.get('created_at'),
                'queue_time': result.get('queue_time'),
                'ring_time': result.get('ring_time'),
                'completed': result.get('completed', False)
            }
        else:
            print(f"❌ Failed to fetch call summary for {full_name}: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error fetching call summary for {full_name}: {str(e)}")
        return None

def fetch_call_details(call_id, full_name):
    """Fetches call details and analyzes the call for interest with retries"""
    print(f"🔄 Waiting for call completion: {full_name} (ID: {call_id})")
    
    # Set up retry strategy
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    
    max_attempts = 45
    attempt = 0

    while attempt < max_attempts:
        try:
            response = session.get(
                f"https://api.bland.ai/v1/calls/{call_id}",
                headers={"Authorization": f"Bearer {API_KEY}"},
                timeout=60
            )

            if response.status_code == 200:
                result = response.json()
                status = result.get("status")
                completed = result.get("completed")

                if completed or status == "completed":
                    print(f"✅ Call completed for {full_name}, analyzing...")
                    
                    # Get enhanced analysis
                    analysis_result = analyze_call_interest(call_id, full_name)
                    
                    # Extract additional details
                    call_summary = result.get('summary', 'No summary available')
                    transcript = result.get('transcript', 'No transcript available')
                    
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': status,
                        'duration': result.get('corrected_duration', result.get('duration', 'Unknown')),
                        'call_intent': analysis_result['call_intent'],
                        'is_interested': analysis_result['is_interested'],
                        'concerns': analysis_result['concerns'],
                        'reason': analysis_result['reason'],
                        'showed_engagement': analysis_result['showed_engagement'],
                        'raw_intent': analysis_result['raw_intent'],
                        'summary': call_summary,
                        'transcript': transcript,
                        'created_at': result.get('created_at'),
                        'answered': result.get('answered', False),
                        'queue_time': result.get('queue_time'),
                        'ring_time': result.get('ring_time')
                    }
                elif status == "failed" or status == "error":
                    print(f"❌ Call failed for {full_name}: {status}")
                    call_summary = result.get('summary', 'Call failed - no summary available')
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': status,
                        'call_intent': 'no',  # Map failed/error to 'no'
                        'summary': call_summary,
                        'transcript': result.get('transcript', 'No transcript available'),
                        'error': result.get('error_message', 'Call failed'),
                        'answered': result.get('answered', False),
                        'duration': result.get('corrected_duration', result.get('duration', 'Unknown'))
                    }
                else:
                    print(f"⏳ Call in progress for {full_name}: {status}")
            else:
                print(f"❌ API error for {full_name}: {response.status_code}")
                call_data = get_call_summary_directly(call_id, full_name)
                if call_data:
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': call_data.get('status', 'error'),
                        'call_intent': 'no',  # Map API error to 'no'
                        'summary': call_data.get('summary', 'API error - no summary available'),
                        'transcript': call_data.get('transcript', 'No transcript available'),
                        'error': f"API error: {response.status_code}",
                        'answered': call_data.get('answered', False),
                        'duration': call_data.get('duration', 'Unknown')
                    }
                else:
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': 'error',
                        'call_intent': 'no',  # Map API error to 'no'
                        'summary': 'API error - no summary available',
                        'error': f"API error: {response.status_code}"
                    }

        except requests.exceptions.Timeout as e:
            print(f"⚠️ Timeout error for {full_name} (attempt {attempt + 1}/{max_attempts}): {str(e)}")
            if attempt == max_attempts - 1:
                print(f"🔍 Final attempt - trying to get summary directly for {full_name}")
                call_data = get_call_summary_directly(call_id, full_name)
                if call_data:
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': call_data.get('status', 'timeout'),
                        'call_intent': 'no',  # Map timeout to 'no'
                        'summary': call_data.get('summary', 'Timeout - but summary retrieved'),
                        'transcript': call_data.get('transcript', 'No transcript available'),
                        'error': 'Timeout during polling but call may have completed',
                        'answered': call_data.get('answered', False),
                        'duration': call_data.get('duration', 'Unknown')
                    }
                else:
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': 'timeout',
                        'call_intent': 'no',  # Map timeout to 'no'
                        'summary': 'Timeout - unable to retrieve summary',
                        'error': 'Timeout during polling'
                    }
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Network error for {full_name} (attempt {attempt + 1}/{max_attempts}): {str(e)}")
            if attempt == max_attempts - 1:
                call_data = get_call_summary_directly(call_id, full_name)
                if call_data:
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': call_data.get('status', 'error'),
                        'call_intent': 'no',  # Map network error to 'no'
                        'summary': call_data.get('summary', 'Network error but summary retrieved'),
                        'transcript': call_data.get('transcript', 'No transcript available'),
                        'error': f'Network error: {str(e)}',
                        'answered': call_data.get('answered', False),
                        'duration': call_data.get('duration', 'Unknown')
                    }
                else:
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': 'error',
                        'call_intent': 'no',  # Map network error to 'no'
                        'summary': 'Network error - unable to retrieve summary',
                        'error': str(e)
                    }

        attempt += 1
        time.sleep(10)

    print(f"❌ Max attempts reached for {full_name} - attempting final summary fetch")
    call_data = get_call_summary_directly(call_id, full_name)
    if call_data:
        return {
            'name': full_name,
            'call_id': call_id,
            'status': call_data.get('status', 'timeout'),
            'call_intent': 'no',  # Map max attempts to 'no'
            'summary': call_data.get('summary', 'Max attempts reached but summary retrieved'),
            'transcript': call_data.get('transcript', 'No transcript available'),
            'error': 'Max polling attempts reached but call may have completed',
            'answered': call_data.get('answered', False),
            'duration': call_data.get('duration', 'Unknown')
        }
    else:
        return {
            'name': full_name,
            'call_id': call_id,
            'status': 'timeout',
            'call_intent': 'no',  # Map max attempts to 'no'
            'summary': 'Max attempts reached - unable to retrieve summary',
            'error': 'Max polling attempts reached'
        }

import re

def make_call(full_name, job_details, phone_number, results_list=None, user_id=None):
    """Makes a call and returns call analysis results"""
    job_title = job_details.get('job_title', 'Unknown Position')
    location = job_details.get('location', 'Unknown Location')
    pay = job_details.get('pay', 'Competitive Pay')

    # Transform pay format from "$2,285 - $2,380" to "2285 dollars - 2380 dollars"
    pay_match = re.findall(r"\$?([\d,]+)", pay)
    if pay_match:
        numeric_values = [val.replace(',', '') for val in pay_match]
        if len(numeric_values) == 2:
            pay = f"{numeric_values[0]} dollars - {numeric_values[1]} dollars"
        elif len(numeric_values) == 1:
            pay = f"{numeric_values[0]} dollars"
        pay = pay.replace('-', 'to')

    job_id = f"call_{user_id}_{int(datetime.now().timestamp())}"
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 📞 Initiating call for {full_name} (User ID: {user_id}, Job ID: {job_id}):")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}]    📋 Job Title: {job_title}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}]    📍 Location: {location}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}]    💰 Pay: {pay}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}]    📱 Phone: {phone_number}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}]    👤 User Name: {full_name}")
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] " + "-" * 50)
    
    data = {
        "phone_number": phone_number,
        "pathway_id": PATHWAY_ID,
        "pronunciation_guide": {
            "$": "dollars"
        },
        "request_data": {
            "full_name": full_name,
            "job_title": job_title,
            "location": location,
            "pay": pay,
            "user_name": full_name
        }
    }

    try:
        # Create a session with retries
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🔌 Sending API request to Bland AI...")
        response = session.post(
            "https://api.bland.ai/v1/calls",
            headers={
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json"
            },
            json=data,
            timeout=30
        )

        result = response.json()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 📡 API Response: Status={response.status_code}, Content={result}")

        if response.status_code == 200:
            call_id = result.get("call_id")
            if call_id:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ✅ Call initiated successfully for {full_name} (Call ID: {call_id})")
                call_result = fetch_call_details(call_id, full_name)
                
                if results_list is not None and call_result:
                    with call_results_lock:
                        call_result['user_id'] = user_id
                        results_list.append(call_result)
                
                return call_result
            else:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Failed to get call ID for {full_name}")
                error_result = {
                    'name': full_name,
                    'call_id': 'N/A',
                    'status': 'failed',
                    'call_intent': 'no',
                    'summary': 'Failed to initiate call - no call ID returned',
                    'error': 'No call ID returned',
                    'user_id': user_id
                }
                if results_list is not None:
                    with call_results_lock:
                        results_list.append(error_result)
                return error_result
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Call failed for {full_name}. Status: {response.status_code}")
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] Response: {result}")
            error_result = {
                'name': full_name,
                'call_id': 'N/A',
                'status': 'failed',
                'call_intent': 'no',
                'summary': f'Call initiation failed with status {response.status_code}',
                'error': f'HTTP {response.status_code}: {result}',
                'user_id': user_id
            }
            if results_list is not None:
                with call_results_lock:
                    results_list.append(error_result)
            return error_result

    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Error making call for {full_name}: {str(e)}")
        error_result = {
            'name': full_name,
            'call_id': 'N/A',
            'status': 'error',
            'call_intent': 'no',
            'summary': f'Exception during call initiation: {str(e)}',
            'error': str(e),
            'user_id': user_id
        }
        if results_list is not None:
            with call_results_lock:
                results_list.append(error_result)
        return error_result
    
def handle_response(user_id, full_name, job_details, phone_number, results_list=None):
    """Schedules a call for a user"""
    run_time = datetime.now() + timedelta(minutes=1)
    job_id = f"call_{user_id}_{int(run_time.timestamp())}"  # Unique ID
    scheduler.add_job(
        make_call,
        'date',
        run_date=run_time,
        args=[full_name, job_details, phone_number, results_list, user_id],
        id=job_id
    )
    print(f"✅ Call scheduled for user {user_id} ({full_name}) at {run_time}")

def print_call_summary(call_results):
    """Prints a detailed summary of all call results"""
    if not call_results:
        print("\n❌ No call results to summarize.")
        return
    
    print("\n" + "="*100)
    print("📊 DETAILED CALL SUMMARY")
    print("="*100)
    
    yes_count = 0
    no_count = 0
    later_count = 0
    
    for result in call_results:
        # Safely get all values with proper None checks
        name = result.get('name', 'Unknown') if result else 'Unknown'
        call_intent = result.get('call_intent', 'no') if result else 'no'
        duration = result.get('duration', 'Unknown') if result else 'Unknown'
        call_id = result.get('call_id', 'Unknown') if result else 'Unknown'
        summary = result.get('summary', 'No summary available') if result else 'No summary available'
        concerns = result.get('concerns', 'None') if result else 'None'
        reason = result.get('reason', 'Not specified') if result else 'Not specified'
        answered = result.get('answered', 'Unknown') if result else 'Unknown'
        showed_engagement = result.get('showed_engagement', 'Unknown') if result else 'Unknown'
        
        # Ensure string conversion for safety
        concerns = str(concerns) if concerns is not None else 'None'
        reason = str(reason) if reason is not None else 'Not specified'
        summary = str(summary) if summary is not None else 'No summary available'
        
        # Count by intent
        if call_intent == 'yes':
            yes_count += 1
            status_emoji = "✅"
        elif call_intent == 'later':
            later_count += 1
            status_emoji = "⏳"
        else:  # 'no' or any unexpected value
            no_count += 1
            status_emoji = "❌"
        
        print(f"{status_emoji} {name}")
        print(f"   📞 Call ID: {call_id}")
        print(f"   🎯 Intent: {call_intent.upper()}")
        print(f"   ⏱️ Duration: {duration}s")
        print(f"   📞 Answered: {answered}")
        print(f"   🤝 Showed Engagement: {showed_engagement}")
        
        if call_intent in ['yes', 'later']:
            print(f"   💭 Reason: {reason}")
            print(f"   ⚠️ Concerns: {concerns}")
        
        print(f"   📝 Summary: {summary}")
        
        if result and result.get('error'):
            print(f"   ❌ Error: {result.get('error')}")
            
        print("-" * 100)
    
    print(f"\n📊 STATISTICS:")
    print(f"✅ Yes/Interested: {yes_count}")
    print(f"❌ No/Not Interested: {no_count}")
    print(f"⏳ Later/Callback Requested: {later_count}")
    print(f"📞 Total Calls: {len(call_results)}")
    
    if len(call_results) > 0:
        success_rate = (yes_count / len(call_results)) * 100
        callback_rate = (later_count / len(call_results)) * 100
        engagement_rate = ((yes_count + later_count) / len(call_results)) * 100
        print(f"📊 Positive Interest Rate: {success_rate:.1f}%")
        print(f"📊 Callback Request Rate: {callback_rate:.1f}%")
        print(f"📊 Overall Engagement Rate: {engagement_rate:.1f}%")
    
    print("="*100)  

def display_call_preview(call_list):
    """Display detailed preview of all calls to be made"""
    print(f"📞 CALL PREVIEW - {len(call_list)} calls to be made:")
    print("="*80)
    
    for i, (name, job_details, phone) in enumerate(call_list, 1):
        job_title = job_details.get('job_title', 'Unknown Position')
        location = job_details.get('location', 'Unknown Location')  
        pay = job_details.get('pay', 'Competitive Pay')
        
        print(f"CALL #{i}")
        print(f"👤 Full Name: {name}")
        print(f"📋 Job Title: {job_title}")
        print(f"📍 Location: {location}")
        print(f"💰 Pay: {pay}")
        print(f"📱 Phone: {phone}")
        print(f"👤 User Name: {name}")
        print("-" * 80)
    
    print(f"\n🚀 Starting {len(call_list)} calls now...\n")

# Main execution
if __name__ == "__main__":
    try:
        validate_env_variables()
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🚀 Starting call scheduling process...")
        
        # Update table name
        TABLE_NAME = "person_details_dummy"
        
        # Connect to PostgreSQL and fetch records
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        # Check if table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = %s
            )
        """, ('person_details_dummy',))
        table_exists = cur.fetchone()[0]
        if not table_exists:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Table person_details_dummy does not exist!")
            cur.close()
            conn.close()
            exit()

        # Count total records
        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        total_records = cur.fetchone()[0]
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 📊 Total records in {TABLE_NAME}: {total_records}")

        # Fetch records without status column
        cur.execute(f"""
            SELECT id, full_name, url, directdials
            FROM {TABLE_NAME}
            WHERE full_name IS NOT NULL 
            AND url IS NOT NULL 
            AND directdials IS NOT NULL
            ORDER BY id
            LIMIT 5
        """)
        records = cur.fetchall()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 📋 Found {len(records)} records matching initial query conditions")

        call_list = []
        used_phone_numbers = set()
        
        for user_id, full_name, url_string, directdials in records:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🔍 Processing record: ID={user_id}, Name={full_name}, URL={url_string}, Directdials={directdials}")
            
            # Check directdials format
            if str(directdials).lower() in ['null', '""', '[]'] or len(str(directdials)) <= 5:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ⚠️ Skipping {full_name}: Invalid directdials format ({directdials})")
                continue

            # Extract first URL
            first_url = extract_first_url(url_string)
            if not first_url:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ⚠️ Skipping {full_name}: No valid URL")
                continue

            # Get job details
            job_details = get_job_details_from_url(cur, first_url)
            if not job_details or not job_details.get('job_title'):
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ⚠️ Skipping {full_name}: No valid job details")
                continue

            # Extract valid phone number from directdials
            phone_number = extract_valid_phone(directdials)
            if phone_number:
                if phone_number in used_phone_numbers:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ⚠️ Skipping {full_name}: Phone number {phone_number} already scheduled")
                    continue
                call_list.append((user_id, full_name, job_details, phone_number))
                used_phone_numbers.add(phone_number)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ✅ Added {full_name} to call list with phone {phone_number}")
            else:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ⚠️ Skipping {full_name}: No valid phone number")

        if not call_list:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ No valid records found to call.")
            cur.close()
            conn.close()
            exit()

        # Display detailed preview of all calls
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 📋 Call Preview:")
        display_call_preview([(name, job_details, phone) for _, name, job_details, phone in call_list])

        # Store call results for summary
        call_results = []

        # Track scheduled jobs
        scheduled_jobs = []

        # Schedule all calls for one minute from now
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🚀 Processing {len(call_list)} calls...")
        for index, (user_id, full_name, job_details, phone_number) in enumerate(call_list):
            try:
                run_time = datetime.now() + timedelta(minutes=1)
                job_id = f"call_{user_id}_{int(run_time.timestamp())}"
                if log_scheduled_call(cur, user_id, full_name, phone_number, job_id, run_time):
                    # Remove any existing job for this user_id
                    for existing_job_id, existing_user_id, _ in scheduled_jobs[:]:
                        if existing_user_id == user_id:
                            scheduler.remove_job(existing_job_id)
                            scheduled_jobs.remove((existing_job_id, existing_user_id, full_name))
                            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🗑️ Removed existing job for {full_name} (User ID: {user_id})")
                    
                    job = scheduler.add_job(
                        make_call,
                        'date',
                        run_date=run_time,
                        args=[full_name, job_details, phone_number, call_results, user_id],
                        id=job_id,
                        misfire_grace_time=30
                    )
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ✅ Call scheduled for {full_name} (User ID: {user_id}) at {run_time}")
                    scheduled_jobs.append((job_id, user_id, full_name))
                else:
                    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ⚠️ Call not scheduled for {full_name} (User ID: {user_id}): Log failed")
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Failed to schedule call for {full_name}: {str(e)}")
                continue

        conn.commit()
        cur.close()
        conn.close()

        if not scheduled_jobs:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ No calls were successfully scheduled. Exiting...")
            exit()

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ✅ Call scheduling complete. {len(scheduled_jobs)} calls scheduled.")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ⏳ Scheduled calls will execute in background...")

        # Keep the script running to allow scheduled jobs to execute
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ⏳ Keeping scheduler alive. Press Ctrl+C to stop.")
        while scheduler.get_jobs():
            time.sleep(10)  # Check every 10 seconds if jobs are still pending
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ✅ All scheduled jobs completed.")

    except psycopg2.Error as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Database error: {e}")
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Unexpected error: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
        # Only shut down the scheduler if no jobs are pending
        if scheduler.running and not scheduler.get_jobs():
            try:
                scheduler.shutdown(wait=True)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🛑 Scheduler shut down successfully")
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Error during scheduler shutdown: {e}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🛑 Script execution completed")