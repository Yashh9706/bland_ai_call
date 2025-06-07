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
        'misfire_grace_time': 30,
        'coalesce': True,
        'max_instances': 1
    },
    executors={
        'default': {'type': 'threadpool', 'max_workers': 10}
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
    
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        print("❌ Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        exit(1)

def log_scheduled_call(cursor, user_id, full_name, phone_number, job_id, run_time, call_id=None):
    """Logs scheduled call details to the person_details_dummy table, excluding status"""
    try:
        required_columns = ['job_id', 'run_time', 'call_id']
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name = %s", ('person_details_dummy',))
        existing_columns = {row[0] for row in cursor.fetchall()}

        for column in required_columns:
            if column not in existing_columns:
                if column == 'job_id':
                    cursor.execute("ALTER TABLE person_details_dummy ADD COLUMN job_id VARCHAR(100)")
                elif column == 'run_time':
                    cursor.execute("ALTER TABLE person_details_dummy ADD COLUMN run_time TIMESTAMP")
                elif column == 'call_id':
                    cursor.execute("ALTER TABLE person_details_dummy ADD COLUMN call_id VARCHAR(50)")
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 📝 Added column {column} to person_details_dummy table")
        
        cursor.execute(
            "UPDATE person_details_dummy SET job_id = %s, run_time = %s, call_id = %s WHERE id = %s",
            (job_id, run_time, call_id, user_id)
        )
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

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def log_missed_job(cursor, job_id, user_id, full_name):
    """Logs a missed job to the person_details_dummy table"""
    try:
        cursor.execute("UPDATE person_details_dummy SET status = %s WHERE job_id = %s", ('missed', job_id))
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Logged missed job for {full_name} (User ID: {user_id}, Job ID: {job_id})")
    except psycopg2.Error as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Error logging missed job for {full_name}: {e}")

def analyze_call_interest(call_id, full_name):
    """Analyzes the call to determine if the caller showed interest in the job"""
    try:
        response = requests.post(
            f"https://api.bland.ai/v1/calls/{call_id}/analyze",
            headers={"authorization": API_KEY, "Content-Type": "application/json"},
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

        if response.status_code == 200 and "answers" in response.json() and response.json()["answers"]:
            answers = response.json()["answers"]
            print(f"🔍 Analysis response for {full_name}: {response.json()}")
            
            is_interested = answers[0] if len(answers) > 0 else False
            primary_response = answers[1].lower().strip() if len(answers) > 1 else "unclear"
            concerns = answers[2] if len(answers) > 2 and answers[2] else "No specific concerns mentioned"
            reason = answers[3] if len(answers) > 3 and answers[3] else "No specific reason provided"
            showed_engagement = answers[4] if len(answers) > 4 else False
            
            call_intent = "no"
            if primary_response == "interested" or is_interested or showed_engagement:
                call_intent = "yes"
            elif primary_response == "callback_later" or "later" in primary_response:
                call_intent = "later"
                
            return {
                'call_intent': call_intent,
                'is_interested': is_interested,
                'concerns': concerns,
                'reason': reason,
                'showed_engagement': showed_engagement,
                'raw_intent': primary_response
            }
        else:
            print(f"⚠️ Analysis failed for {full_name}: {response.status_code if response else 'No response'}")
            return {
                'call_intent': "no",
                'is_interested': None,
                'concerns': f"Analysis failed - Status {response.status_code if response else 'No response'}",
                'reason': f"Analysis failed - Status {response.status_code if response else 'No response'}",
                'showed_engagement': None,
                'raw_intent': "error"
            }
    except Exception as e:
        print(f"❌ Error analyzing call for {full_name}: {str(e)}")
        return {
            'call_intent': "no",
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
        cursor.execute("SELECT job_title, location, estimated_pay FROM uniti_med_job_data WHERE url = %s LIMIT 1", (url,))
        result = cursor.fetchone()
        return {'job_title': result[0], 'location': result[1], 'pay': result[2]} if result else None
    except Exception:
        return None

def extract_valid_phone(directdials):
    """Extracts the first valid phone number from JSON or string data"""
    if not directdials or str(directdials).lower() in ['none', 'null', '', '""']:
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
    """Directly fetch call summary from the API without analysis"""
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        
        response = session.get(
            f"https://api.bland.ai/v1/calls/{call_id}",
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=60
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
        print(f"❌ Failed to fetch call summary for {full_name}: {response.status_code}")
        return None
    except Exception as e:
        print(f"❌ Error fetching call summary for {full_name}: {str(e)}")
        return None

def create_error_result(full_name, call_id, status, error_message, call_data=None):
    """Helper function to create standardized error result"""
    return {
        'name': full_name,
        'call_id': call_id,
        'status': status,
        'call_intent': 'no',
        'summary': call_data.get('summary', f'{status.capitalize()} - unable to retrieve summary') if call_data else f'{status.capitalize()} - unable to retrieve summary',
        'transcript': call_data.get('transcript', 'No transcript available') if call_data else 'No transcript available',
        'error': error_message,
        'answered': call_data.get('answered', False) if call_data else False,
        'duration': call_data.get('duration', 'Unknown') if call_data else 'Unknown'
    }

def fetch_call_details(call_id, full_name):
    """Fetches call details and analyzes the call for interest with retries"""
    print(f"🔄 Waiting for call completion: {full_name} (ID: {call_id})")
    
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    
    max_attempts = 45
    for attempt in range(max_attempts):
        try:
            response = session.get(
                f"https://api.bland.ai/v1/calls/{call_id}",
                headers={"Authorization": f"Bearer {API_KEY}"},
                timeout=60
            )

            if response.status_code != 200:
                print(f"❌ API error for {full_name}: {response.status_code}")
                call_data = get_call_summary_directly(call_id, full_name)
                return create_error_result(full_name, call_id, 'error', f"API error: {response.status_code}", call_data)

            result = response.json()
            status = result.get("status")
            if result.get("completed") or status == "completed":
                print(f"✅ Call completed for {full_name}, analyzing...")
                analysis_result = analyze_call_interest(call_id, full_name)
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
                    'summary': result.get('summary', 'No summary available'),
                    'transcript': result.get('transcript', 'No transcript available'),
                    'created_at': result.get('created_at'),
                    'answered': result.get('answered', False),
                    'queue_time': result.get('queue_time'),
                    'ring_time': result.get('ring_time')
                }
            elif status in ["failed", "error"]:
                print(f"❌ Call failed for {full_name}: {status}")
                return create_error_result(full_name, call_id, status, result.get('error_message', 'Call failed'), result)
            
            print(f"⏳ Call in progress for {full_name}: {status}")
            time.sleep(10)
        
        except requests.exceptions.Timeout as e:
            print(f"⚠️ Timeout error for {full_name} (attempt {attempt + 1}/{max_attempts}): {str(e)}")
            if attempt == max_attempts - 1:
                call_data = get_call_summary_directly(call_id, full_name)
                return create_error_result(full_name, call_id, 'timeout', 'Timeout during polling', call_data)
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Network error for {full_name} (attempt {attempt + 1}/{max_attempts}): {str(e)}")
            if attempt == max_attempts - 1:
                call_data = get_call_summary_directly(call_id, full_name)
                return create_error_result(full_name, call_id, 'error', f"Network error: {str(e)}", call_data)

    print(f"❌ Max attempts reached for {full_name}")
    call_data = get_call_summary_directly(call_id, full_name)
    return create_error_result(full_name, call_id, 'timeout', 'Max polling attempts reached', call_data)

def make_call(full_name, job_details, phone_number, results_list=None, user_id=None):
    """Makes a call and returns call analysis results"""
    job_title = job_details.get('job_title', 'Unknown Position')
    location = job_details.get('location', 'Unknown Location')
    pay = job_details.get('pay', 'Competitive Pay')

    pay_match = re.findall(r"\$?([\d,]+)", pay)
    if pay_match:
        numeric_values = [val.replace(',', '') for val in pay_match]
        pay = f"{numeric_values[0]} dollars"
        if len(numeric_values) == 2:
            pay = f"{numeric_values[0]} dollars to {numeric_values[1]} dollars"

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
        "pronunciation_guide": {"$": "dollars"},
        "request_data": {
            "full_name": full_name,
            "job_title": job_title,
            "location": location,
            "pay": pay,
            "user_name": full_name
        }
    }

    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🔌 Sending API request to Bland AI...")
        response = session.post(
            "https://api.bland.ai/v1/calls",
            headers={"Authorization": f"Bearer {API_KEY}", "Content-Type": "application/json"},
            json=data,
            timeout=30
        )

        result = response.json()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 📡 API Response: Status={response.status_code}, Content={result}")

        if response.status_code == 200 and result.get("call_id"):
            call_id = result["call_id"]
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ✅ Call initiated successfully for {full_name} (Call ID: {call_id})")
            call_result = fetch_call_details(call_id, full_name)
            if results_list and call_result:
                with call_results_lock:
                    call_result['user_id'] = user_id
                    results_list.append(call_result)
            return call_result
        else:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Call failed for {full_name}. Status: {response.status_code}")
            error_result = {
                'name': full_name,
                'call_id': 'N/A',
                'status': 'failed',
                'call_intent': 'no',
                'summary': f'Call initiation failed with status {response.status_code}',
                'error': f'HTTP {response.status_code}: {result}',
                'user_id': user_id
            }
            if results_list:
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
        if results_list:
            with call_results_lock:
                results_list.append(error_result)
        return error_result

def handle_response(user_id, full_name, job_details, phone_number, results_list=None):
    """Schedules a call for a user"""
    run_time = datetime.now() + timedelta(minutes=1)
    job_id = f"call_{user_id}_{int(run_time.timestamp())}"
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
    
    yes_count = later_count = no_count = 0
    
    for result in call_results:
        name = result.get('name', 'Unknown')
        call_intent = result.get('call_intent', 'no')
        duration = result.get('duration', 'Unknown')
        call_id = result.get('call_id', 'Unknown')
        summary = str(result.get('summary', 'No summary available'))
        concerns = str(result.get('concerns', 'None'))
        reason = str(result.get('reason', 'Not specified'))
        answered = result.get('answered', 'Unknown')
        showed_engagement = result.get('showed_engagement', 'Unknown')
        
        if call_intent == 'yes':
            yes_count += 1
            status_emoji = "✅"
        elif call_intent == 'later':
            later_count += 1
            status_emoji = "⏳"
        else:
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
        if result.get('error'):
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

if __name__ == "__main__":
    try:
        validate_env_variables()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🚀 Starting call scheduling process...")
        
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", ('person_details_dummy',))
        if not cur.fetchone()[0]:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Table person_details_dummy does not exist!")
            cur.close()
            conn.close()
            exit()

        cur.execute(f"SELECT COUNT(*) FROM {TABLE_NAME}")
        total_records = cur.fetchone()[0]
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 📊 Total records in {TABLE_NAME}: {total_records}")

        cur.execute(
            f"SELECT id, full_name, url, directdials FROM {TABLE_NAME} WHERE full_name IS NOT NULL AND url IS NOT NULL AND directdials IS NOT NULL ORDER BY id LIMIT 5"
        )
        records = cur.fetchall()
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 📋 Found {len(records)} records matching initial query conditions")

        call_list = []
        used_phone_numbers = set()
        
        for user_id, full_name, url_string, directdials in records:
            print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🔍 Processing record: ID={user_id}, Name={full_name}, URL={url_string}, Directdials={directdials}")
            
            if str(directdials).lower() in ['null', '""', '[]'] or len(str(directdials)) <= 5:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ⚠️ Skipping {full_name}: Invalid directdials format ({directdials})")
                continue

            first_url = extract_first_url(url_string)
            if not first_url:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ⚠️ Skipping {full_name}: No valid URL")
                continue

            job_details = get_job_details_from_url(cur, first_url)
            if not job_details or not job_details.get('job_title'):
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ⚠️ Skipping {full_name}: No valid job details")
                continue

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

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 📋 Call Preview:")
        display_call_preview([(name, job_details, phone) for _, name, job_details, phone in call_list])

        call_results = []
        scheduled_jobs = []

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🚀 Processing {len(call_list)} calls...")
        for index, (user_id, full_name, job_details, phone_number) in enumerate(call_list):
            try:
                run_time = datetime.now() + timedelta(minutes=1)
                job_id = f"call_{user_id}_{int(run_time.timestamp())}"
                if log_scheduled_call(cur, user_id, full_name, phone_number, job_id, run_time):
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

        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ⏳ Keeping scheduler alive. Press Ctrl+C to stop.")
        while scheduler.get_jobs():
            time.sleep(10)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ✅ All scheduled jobs completed.")

    except psycopg2.Error as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Database error: {e}")
    except Exception as e:
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Unexpected error: {e}")
    finally:
        if 'conn' in locals() and conn:
            conn.close()
        if scheduler.running and not scheduler.get_jobs():
            try:
                scheduler.shutdown(wait=True)
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🛑 Scheduler shut down successfully")
            except Exception as e:
                print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] ❌ Error during scheduler shutdown: {e}")
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}] 🛑 Script execution completed")