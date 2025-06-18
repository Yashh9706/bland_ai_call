import logging
import requests
from fastapi import FastAPI, Request
from psycopg2.extras import RealDictCursor
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# Constants
DATABASE_URL = os.getenv('DATABASE_URL')
BLAND_API_KEY = os.getenv('BLAND_API_KEY')
PATHWAY_ID = os.getenv('PATHWAY_ID')
CALL_URL = os.getenv('CALL_URL')
WEBHOOK_URL = os.getenv('WEBHOOK_URL')

# FastAPI app
app = FastAPI()

# Database connection helper
def get_database_connection():
    try:
        conn = psycopg2.connect(
            DATABASE_URL,
            connect_timeout=10
        )
        return conn
    except Exception as e:
        logging.error(f"Database connection error: {e}")
        raise

# Fetch data from the database
def fetch_person_data():
    conn = get_database_connection()
    if not conn:
        logging.error("Database connection failed")
        return []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        cursor.execute("""
            SELECT id, full_name, sms_phone_numbers_used, job_title, location, estimated_pay
            FROM person_details_dummy
            WHERE sms_phone_numbers_used IS NOT NULL AND sms_phone_numbers_used != ''
        """)
        results = cursor.fetchall()
        logging.info(f"Fetched {len(results)} records from the database")
        return results
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

# Make a call using Bland AI
def make_call(person):
    try:
        phone_number = person['sms_phone_numbers_used'].strip()
        full_name = person['full_name']
        job_title = person['job_title']
        location = person['location']
        pay = str(person.get('estimated_pay', '')).replace('$', '').replace(',', '')

        data = {
            "phone_number": phone_number,
            "pathway_id": PATHWAY_ID,
            "task": "test_bland_ai_call",
            "wait_for_greeting": True,
            "request_data": {
                "full_name": full_name,
                "job_title": job_title,
                "location": location,
                "pay": pay,
                "user_name": full_name
            }
        }

        response = requests.post(
            CALL_URL,
            json=data,
            headers={
                "Authorization": f"Bearer {BLAND_API_KEY}",
                "Content-Type": "application/json"
            },
            timeout=30
        )
        response.raise_for_status()
        if response.status_code == 200:
            call_id = response.json().get('call_id')
            logging.info(f"Call initiated successfully for {full_name} with call_id: {call_id}")
            return call_id
        else:
            logging.error(f"Failed to initiate call. Status: {response.status_code}, Response: {response.text}")
            return None
    except Exception as e:
        logging.error(f"Error making call: {e}")
        return None

# Analyze call intent
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

# Webhook to handle call responses
@app.post("/webhook")
async def webhook(request: Request):
    try:
        data = await request.json()
        call_id = data.get('call_id')
        phone_number = data.get('to', 'Unknown')
        if call_id:
            logging.info(f"Webhook received for call_id: {call_id}, phone_number: {phone_number}")
            # Analyze call intent and get summary
            intent = analyze_call_intent(call_id)
            summary = get_call_summary(call_id)
            return {
                "message": "Webhook processed successfully",
                "call_id": call_id,
                "phone_number": phone_number,
                "intent": intent,
                "summary": summary
            }
        return {"message": "No call_id found", "intent": "unknown", "summary": None}
    except Exception as e:
        logging.error(f"Error processing webhook: {e}")
        return {
            "message": "Error processing webhook",
            "error": str(e),
            "intent": "error",
            "summary": None
        }