import psycopg2
import requests
import threading
import re
import time
import json
import os
import logging
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
from typing import List, Dict, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Module-level lock and webhook results storage
call_results_lock = threading.Lock()
call_webhook_results: Dict[str, Dict] = {}

class DatabaseConfig:
    """Handles database configuration and connections."""
    
    def __init__(self):
        load_dotenv(override=True)
        self.db_params = {
            "dbname": os.getenv("DB_NAME"),
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT", 5432)),
            "sslmode": os.getenv("DB_SSLMODE", "require")
        }
        self.table_name = os.getenv("TABLE_NAME", "person_details_dummy")
        
    def validate(self):
        """Validates required environment variables."""
        required_vars = ["DB_NAME", "DB_USER", "DB_PASSWORD", "DB_HOST"]
        missing_vars = [var for var in required_vars if not os.getenv(var)]
        if missing_vars:
            logger.error(f"Missing environment variables: {', '.join(missing_vars)}")
            raise ValueError(f"Missing environment variables: {', '.join(missing_vars)}")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(**self.db_params)
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
    
    @contextmanager
    def get_cursor(self, conn):
        """Context manager for database cursors."""
        cur = None
        try:
            cur = conn.cursor()
            yield cur
        except psycopg2.Error as e:
            logger.error(f"Cursor error: {str(e)}")
            raise
        finally:
            if cur:
                cur.close()

class APIService:
    """Handles API interactions with retry logic."""
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        self.session.mount("https://", HTTPAdapter(max_retries=retries))
    
    def post(self, url: str, data: Dict, timeout: int = 10) -> Dict:
        """Makes a POST request with retry logic."""
        try:
            response = self.session.post(
                url,
                headers={"Authorization": f"Bearer {self.api_key}", "Content-Type": "application/json"},
                json=data,
                timeout=timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed: {str(e)}")
            raise
    
    def get(self, url: str, timeout: int = 10) -> Dict:
        """Makes a GET request with retry logic."""
        try:
            response = self.session.get(
                url,
                headers={"Authorization": f"Bearer {self.api_key}"},
                timeout=timeout
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            logger.error(f"API GET request failed: {str(e)}")
            raise

def make_call(full_name: str, job_details: Dict, phone_number: str, results_list: List[Dict], user_id: int, 
              api_service: APIService, db_config: DatabaseConfig, pathway_id: str) -> Dict:
    """Initiates a call and handles results."""
    def fetch_call_details(call_id: str, full_name: str) -> Dict:
        """Waits for webhook notification or falls back to API polling."""
        max_wait_time = 300
        start_time = time.time()
        
        while time.time() - start_time < max_wait_time:
            if call_id in call_webhook_results:
                webhook_data = call_webhook_results.pop(call_id)
                logger.info(f"Webhook received for {full_name}, processing results...")
                analysis_result = analyze_call_interest(call_id, full_name, api_service)
                summary = webhook_data.get('summary', 'No summary available')
                if summary == 'No summary available':
                    summary = get_call_summary(call_id, full_name, api_service)['summary']
                return {
                    'name': full_name,
                    'call_id': call_id,
                    'call_intent': analysis_result['call_intent'],
                    'summary': summary
                }
            time.sleep(2)
        
        logger.warning(f"Webhook timeout for {full_name}, falling back to API polling")
        call_data = get_call_summary(call_id, full_name, api_service)
        analysis_result = analyze_call_interest(call_id, full_name, api_service)
        return {
            'name': full_name,
            'call_id': call_id,
            'call_intent': analysis_result['call_intent'],
            'summary': call_data['summary']
        }

    def analyze_call_interest(call_id: str, full_name: str, api_service: APIService) -> Dict:
        """Analyzes call intent."""
        max_retries = 3
        base_timeout = 30
        
        for attempt in range(max_retries):
            try:
                response = api_service.post(
                    f"https://api.bland.ai/v1/calls/{call_id}/analyze",
                    {
                        "goal": "Analyze caller's response to job opportunity",
                        "questions": [
                            ["Based on the caller's response, categorize their interest: Answer 'yes' if genuinely interested in the job, 'no' if not interested/declined, or 'later' if they said they're busy/call later/call back later/will call you back", "string"]
                        ]
                    },
                    timeout=base_timeout + (attempt * 15)
                )
                if response.get("answers"):
                    answer = response["answers"][0].lower().strip()
                    call_intent = "no"
                    if answer in ["yes", "later"]:
                        call_intent = answer
                    logger.info(f"Intent analysis for {full_name}: {call_intent} (raw answer: '{answer}')")
                    return {'call_intent': call_intent}
                logger.warning(f"Analysis attempt {attempt + 1} failed for {full_name}")
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error on attempt {attempt + 1} for {full_name}: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"Analysis failed for {full_name} after all retries")
                    return {'call_intent': "no"}
                time.sleep(3)
            except Exception as e:
                logger.error(f"Unexpected error in intent analysis for {full_name}: {str(e)}")
                return {'call_intent': "no"}
        return {'call_intent': "no"}

    def get_call_summary(call_id: str, full_name: str, api_service: APIService) -> Dict:
        """Fetches call summary from API."""
        try:
            result = api_service.get(f"https://api.bland.ai/v1/calls/{call_id}")
            return {'summary': result.get('summary', 'No summary available')}
        except Exception as e:
            logger.error(f"Error fetching call summary for {full_name}: {str(e)}")
            return {'summary': f'Error fetching summary: {str(e)}'}

    def handle_api_error(full_name: str, call_id: Optional[str], response: Optional[requests.Response], error_type: str, error_msg: Optional[str] = None) -> Dict:
        """Handles API errors with standardized result."""
        error_msg = error_msg or (f"Status {response.status_code}" if response else "No response")
        logger.error(f"API {error_type} for {full_name}: {error_msg}")
        error_result = {
            'name': full_name,
            'call_id': call_id or 'N/A',
            'call_intent': 'no',
            'summary': f'{error_type.capitalize()} - unable to retrieve summary'
        }
        return error_result

    job_title = job_details.get('job_title', 'Unknown Position')
    location = job_details.get('location', 'Unknown Location')
    pay = job_details.get('pay', 'Competitive Pay')
    pay_match = re.findall(r"\$?([\d,]+)", pay)
    if pay_match:
        numeric_values = [val.replace(',', '') for val in pay_match]
        pay = f"{numeric_values[0]} dollars"
        if len(numeric_values) == 2:
            pay = f"{numeric_values[0]} dollars to {numeric_values[1]} dollars"
    
    logger.info(f"Initiating call for {full_name} (User ID: {user_id}, Job: {job_title}, Location: {location}, Pay: {pay})")
    
    data = {
        "phone_number": phone_number,
        "pathway_id": pathway_id,
        "pronunciation_guide": {"$": "dollars"},
        "voice": "85a2c852-2238-4651-acf0-e5cbe02186f2",
        "wait_for_greeting": True,
        "noise_cancellation": True,
        "webhook": "https://c186-103-241-232-74.ngrok-free.app/webhook",
        "request_data": {
            "full_name": full_name,
            "job_title": job_title,
            "location": location,
            "pay": pay,
            "user_name": full_name
        }
    }
    
    try:
        result = api_service.post("https://api.bland.ai/v1/calls", data)
        if result.get("call_id"):
            call_id = result["call_id"]
            logger.info(f"Call initiated successfully for {full_name} (Call ID: {call_id})")
            call_result = fetch_call_details(call_id, full_name)
            call_result['user_id'] = user_id
            with call_results_lock:
                results_list.append(call_result)
                try:
                    with db_config.get_connection() as conn:
                        with db_config.get_cursor(conn) as cursor:
                            cursor.execute(f"""
                                UPDATE {db_config.table_name} 
                                SET call_id = %s, summary = %s, intent = %s 
                                WHERE id = %s
                            """, (
                                call_result.get('call_id', 'N/A'),
                                call_result.get('summary', 'No summary available'),
                                call_result.get('call_intent', 'no'),
                                user_id
                            ))
                            if cursor.rowcount > 0:
                                conn.commit()
                                logger.info(f"Updated database for user ID {user_id}")
                            else:
                                logger.warning(f"No record found for user ID {user_id}")
                except Exception as e:
                    logger.error(f"Failed to update database for user ID {user_id}: {str(e)}")
            return call_result
        return handle_api_error(full_name, None, None, 'failed')
    except Exception as e:
        return handle_api_error(full_name, None, None, 'error', str(e))

class CallScheduler:
    """Manages call scheduling and execution."""
    
    def __init__(self, db_config: DatabaseConfig, api_key: str, pathway_id: str):
        self.db_config = db_config
        self.api_service = APIService(api_key)
        self.pathway_id = pathway_id
        self.scheduler = BackgroundScheduler(
            jobstores={'default': SQLAlchemyJobStore(url=self._get_db_url())},
            job_defaults={'misfire_grace_time': 30, 'coalesce': True, 'max_instances': 1},
            executors={'default': {'type': 'threadpool', 'max_workers': 10}}
        )
        self.scheduler.start()
    
    def _get_db_url(self) -> str:
        """Constructs database URL for scheduler."""
        return (f"postgresql+psycopg2://{self.db_config.db_params['user']}:"
                f"{self.db_config.db_params['password']}@"
                f"{self.db_config.db_params['host']}:{self.db_config.db_params['port']}/"
                f"{self.db_config.db_params['dbname']}?sslmode={self.db_config.db_params['sslmode']}")
    
    def setup_database(self):
        """Sets up database columns if they don't exist."""
        try:
            with self.db_config.get_connection() as conn:
                with self.db_config.get_cursor(conn) as cursor:
                    columns_to_add = [
                        ("call_id", "VARCHAR(255)"),
                        ("summary", "TEXT"),
                        ("intent", "VARCHAR(50)")
                    ]
                    for column_name, column_type in columns_to_add:
                        cursor.execute("""
                            SELECT column_name 
                            FROM information_schema.columns 
                            WHERE table_name = %s AND column_name = %s
                        """, (self.db_config.table_name, column_name))
                        if not cursor.fetchone():
                            cursor.execute(f"ALTER TABLE {self.db_config.table_name} ADD COLUMN {column_name} {column_type}")
                            logger.info(f"Added column '{column_name}' to table '{self.db_config.table_name}'")
                        else:
                            logger.info(f"Column '{column_name}' already exists in table '{self.db_config.table_name}'")
                    conn.commit()
                    logger.info("Database columns setup completed")
        except Exception as e:
            logger.error(f"Failed to create database columns: {str(e)}")
            raise
    
    def schedule_call(self, user_id: int, full_name: str, job_details: Dict, phone_number: str, results_list: List[Dict]) -> Optional[str]:
        """Schedules a call for a user."""
        run_time = datetime.now() + timedelta(minutes=1)
        job_id = f"call_{user_id}_{int(run_time.timestamp())}"
        logger.info(f"Scheduling call for {full_name} (User ID: {user_id}) at {run_time}")
        try:
            job = self.scheduler.add_job(
                make_call,
                'date',
                run_date=run_time,
                args=[full_name, job_details, phone_number, results_list, user_id,
                      self.api_service, self.db_config, self.pathway_id],
                id=job_id,
                misfire_grace_time=30
            )
            logger.info(f"Call scheduled for {full_name} (Job ID: {job_id})")
            return job_id
        except Exception as e:
            logger.error(f"Failed to schedule call for {full_name}: {str(e)}")
            return None

class WebhookServer:
    """Manages Flask webhook server."""
    
    def __init__(self):
        self.app = Flask(__name__)
        self._setup_routes()
    
    def _setup_routes(self):
        @self.app.route('/webhook', methods=['POST'])
        def handle_webhook():
            """Handles incoming webhook when a call ends."""
            try:
                webhook_data = request.get_json()
                call_id = webhook_data.get('call_id')
                completed = webhook_data.get('completed', False)
                call_ended_by = webhook_data.get('call_ended_by', 'UNKNOWN')
                summary = webhook_data.get('summary', 'No summary available')
                
                logger.info(f"Webhook received - Call {call_id} ended by {call_ended_by}")
                call_webhook_results[call_id] = {
                    'completed': completed,
                    'summary': summary,
                    'call_ended_by': call_ended_by,
                    'webhook_received': True
                }
                return jsonify({"status": "success", "message": "Webhook processed"}), 200
            except Exception as e:
                logger.error(f"Error processing webhook: {str(e)}")
                return jsonify({"error": "Failed to process webhook"}), 500
    
    def run(self):
        """Runs the Flask server in a background thread."""
        logger.info("Starting webhook server on http://localhost:5000")
        threading.Thread(target=self.app.run, kwargs={'host': '0.0.0.0', 'port': 5000, 'debug': False, 'use_reloader': False}, daemon=True).start()
        time.sleep(2)

class CallManager:
    """Main class to manage the call process."""
    
    def __init__(self):
        load_dotenv(override=True)
        self.db_config = DatabaseConfig()
        self.api_key = os.getenv("BLAND_API_KEY")
        self.pathway_id = os.getenv("BLAND_PATHWAY_ID")
        if not all([self.api_key, self.pathway_id]):
            logger.error("Missing API_KEY or PATHWAY_ID")
            raise ValueError("Missing API_KEY or PATHWAY_ID")
        self.scheduler = CallScheduler(self.db_config, self.api_key, self.pathway_id)
        self.webhook_server = WebhookServer()
    
    def extract_first_url(self, url_string: Optional[str]) -> Optional[str]:
        """Extracts the first URL from a string."""
        if not url_string:
            return None
        return url_string.split(';')[0].strip() or None
    
    def get_job_details(self, url: str) -> Optional[Dict]:
        """Fetches job details from database."""
        try:
            with self.db_config.get_connection() as conn:
                with self.db_config.get_cursor(conn) as cursor:
                    cursor.execute("SELECT job_title, location, estimated_pay FROM uniti_med_job_data WHERE url = %s LIMIT 1", (url,))
                    result = cursor.fetchone()
                    return {'job_title': result[0], 'location': result[1], 'pay': result[2]} if result else None
        except Exception as e:
            logger.error(f"Failed to fetch job details for URL {url}: {str(e)}")
            return None
    
    def extract_valid_phone(self, directdials: Optional[str]) -> Optional[str]:
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
    
    def display_call_preview(self, call_list: List[Tuple[int, str, Dict, str]]):
        """Displays a preview of scheduled calls."""
        logger.info(f"CALL PREVIEW - {len(call_list)} calls to be made:")
        print("="*80)
        for i, (_, name, job_details, phone) in enumerate(call_list, 1):
            job_title = job_details.get('job_title', 'Unknown Position')
            location = job_details.get('location', 'Unknown Location')
            pay = job_details.get('pay', 'Competitive Pay')
            print(f"""\
CALL #{i}
Full Name: {name}
Job Title: {job_title}
Location: {location}
Pay: {pay}
Phone: {phone}
{"-"*80}
""")
        print(f"\nStarting {len(call_list)} calls now...\n")
    
    def print_call_summary(self, call_results: List[Dict]):
        """Prints a summary of call results."""
        if not call_results:
            logger.error("No call results to summarize.")
            return
        logger.info("CALL SUMMARY")
        print("="*50)
        for result in call_results:
            print(f"""\
Call ID: {result.get('call_id', 'Unknown')}
Intent: {result.get('call_intent', 'no').upper()}
Summary: {result.get('summary', 'No summary available')}
{"-"*50}
""")
        print("="*50)
    
    def run(self):
        """Main execution method."""
        try:
            self.db_config.validate()
            self.scheduler.setup_database()
            self.webhook_server.run()
            
            with self.db_config.get_connection() as conn:
                with self.db_config.get_cursor(conn) as cursor:
                    cursor.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", ('person_details_dummy',))
                    if not cursor.fetchone()[0]:
                        logger.error(f"Table {self.db_config.table_name} does not exist!")
                        raise ValueError(f"Table {self.db_config.table_name} does not exist!")
                    
                    cursor.execute(f"SELECT COUNT(*) FROM {self.db_config.table_name}")
                    total_records = cursor.fetchone()[0]
                    logger.info(f"Total records in {self.db_config.table_name}: {total_records}")
                    
                    cursor.execute(
                        f"SELECT id, full_name, url, directdials FROM {self.db_config.table_name} "
                        f"WHERE full_name IS NOT NULL AND url IS NOT NULL AND directdials IS NOT NULL ORDER BY id LIMIT 5"
                    )
                    records = cursor.fetchall()
                    logger.info(f"Found {len(records)} records matching initial query conditions")
                    
                    call_list = []
                    used_phone_numbers = set()
                    for user_id, full_name, url_string, directdials in records:
                        logger.info(f"Processing record: ID={user_id}, Name={full_name}")
                        first_url = self.extract_first_url(url_string)
                        if not first_url:
                            logger.warning(f"Skipping {full_name}: No valid URL")
                            continue
                        job_details = self.get_job_details(first_url)
                        if not job_details or not job_details.get('job_title'):
                            logger.warning(f"Skipping {full_name}: No valid job details")
                            continue
                        phone_number = self.extract_valid_phone(directdials)
                        if not phone_number:
                            logger.warning(f"Skipping {full_name}: No valid phone number")
                            continue
                        if phone_number in used_phone_numbers:
                            logger.warning(f"Skipping {full_name}: Phone number {phone_number} already scheduled")
                            continue
                        call_list.append((user_id, full_name, job_details, phone_number))
                        used_phone_numbers.add(phone_number)
                        logger.info(f"Added {full_name} to call list with phone {phone_number}")
                    
                    if not call_list:
                        logger.error("No valid records found to call.")
                        raise ValueError("No valid records found to call.")
                    
                    self.display_call_preview(call_list)
                    call_results = []
                    scheduled_jobs = []
                    
                    for user_id, full_name, job_details, phone_number in call_list:
                        for existing_job_id, existing_user_id, _ in scheduled_jobs[:]:
                            if existing_user_id == user_id:
                                self.scheduler.scheduler.remove_job(existing_job_id)
                                scheduled_jobs.remove((existing_job_id, existing_user_id, full_name))
                                logger.info(f"Removed existing job for {full_name} (User ID: {user_id})")
                        job_id = self.scheduler.schedule_call(user_id, full_name, job_details, phone_number, call_results)
                        if job_id:
                            scheduled_jobs.append((job_id, user_id, full_name))
                    
                    conn.commit()
                    if not scheduled_jobs:
                        logger.error("No calls were successfully scheduled.")
                        raise ValueError("No calls were successfully scheduled.")
                    
                    logger.info(f"Call scheduling complete. {len(scheduled_jobs)} calls scheduled.")
                    start_time = time.time()
                    timeout = 600
                    while len(call_results) < len(scheduled_jobs) and time.time() - start_time < timeout:
                        time.sleep(10)
                    
                    logger.info("All call results collected or timeout reached.")
                    self.print_call_summary(call_results)
        
        except Exception as e:
            logger.error(f"Unexpected error: {str(e)}")
            raise
        finally:
            if self.scheduler.scheduler.running:
                try:
                    self.scheduler.scheduler.shutdown(wait=True)
                    logger.info("Scheduler shut down successfully")
                except Exception as e:
                    logger.error(f"Error during scheduler shutdown: {str(e)}")
            logger.info("Script execution completed")

def signal_handler(sig, frame):
    """Handles shutdown signals."""
    logger.info("Received shutdown signal, stopping...")
    sys.exit(0)

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    call_manager = CallManager()
    call_manager.run()