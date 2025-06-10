import asyncio
import logging
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.events import EVENT_JOB_MISSED
import psycopg2
from psycopg2.extras import RealDictCursor
import requests
import json

app = FastAPI()

DATABASE_URL = 'postgresql://neondb_owner:npg_eWph9LyzAki7@ep-winter-sea-a6tcb5f1.us-west-2.aws.neon.tech/neondb?sslmode=require'
BLAND_API_KEY = "org_0301c6c09e6f2613b52b17fb221b1b211abaa4e88525251a05982c0ccc8c494fa529dbc5e54dcae8ef0869"
PATHWAY_ID = "2bd6bfcc-1d5a-4129-b150-5ab9cab0ac2e"
CALL_URL = "https://api.bland.ai/v1/calls"

jobstores = {'default': SQLAlchemyJobStore(url=DATABASE_URL)}
job_defaults = {'misfire_grace_time': 30}
scheduler = BackgroundScheduler(jobstores=jobstores, job_defaults=job_defaults)

def missed_job_listener(event):
    print(f"Job {event.job_id} was missed. Retrying in 5 seconds...")
    try:
        missed_job = scheduler.get_job(event.job_id)
        if missed_job:
            retry_job_id = f"retry_{event.job_id}_{int(datetime.now().timestamp())}"
            scheduler.add_job(
                missed_job.func,
                'date',
                run_date=datetime.now() + timedelta(seconds=5),
                args=missed_job.args,
                id=retry_job_id
            )
    except Exception as e:
        print(f"Error scheduling retry: {e}")

scheduler.add_listener(missed_job_listener, EVENT_JOB_MISSED)
scheduler.start()

class UserInput(BaseModel):
    phone_number: str

def get_database_connection():
    try:
        return psycopg2.connect(DATABASE_URL)
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def fetch_all_person_data():
    conn = get_database_connection()
    if not conn:
        return []
    try:
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        query = """
        SELECT 
            pd.id,
            pd.full_name,
            pd.url,
            pd.sms_phone_numbers_used,
            jd.job_title,
            jd.location,
            jd.estimated_pay
        FROM person_details_dummy pd
        LEFT JOIN uniti_med_job_data jd ON pd.url = jd.url
        WHERE pd.sms_phone_numbers_used IS NOT NULL AND pd.sms_phone_numbers_used != ''
        """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        print(f"Error fetching data: {e}")
        conn.close()
        return []

def analyze_call_intent(call_id):
    analyze_url = f"{CALL_URL}/{call_id}/analyze"
    data = {
        "goal": "Analyze caller's response to job opportunity",
        "questions": [
            [
                "Based on the caller's response, categorize their interest: Answer 'yes' if genuinely interested in the job, 'no' if not interested/declined, or 'later' if they said they're busy/call later/call back later/will call you back",
                "string"
            ]
        ]
    }
    headers = {
        "Authorization": f"Bearer {BLAND_API_KEY}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.post(analyze_url, json=data, headers=headers)
        if response.status_code == 200:
            result = response.json()
            answer = result.get('answers', [None])[0]
            if isinstance(answer, dict):
                intent = answer.get('answer', '').strip().lower()
            elif isinstance(answer, str):
                intent = answer.strip().lower()
            else:
                intent = "unknown"
            return intent if intent in ["yes", "no", "later"] else "unknown"
        else:
            print(f"API error: {response.status_code} {response.text}")
            return "error"
    except Exception as e:
        print(f"Exception analyzing call: {e}")
        return "error"
    
def get_call_summary(call_id):
    summary_url = f"{CALL_URL}/{call_id}"
    headers = {
        "Authorization": f"Bearer {BLAND_API_KEY}",
        "Content-Type": "application/json"
    }
    try:
        response = requests.get(summary_url, headers=headers)
        if response.status_code == 200:
            result = response.json()
            summary = result.get("summary", "No summary available.")
            return summary
        else:
            print(f"API error while fetching summary: {response.status_code} {response.text}")
            return "Error fetching summary."
    except Exception as e:
        print(f"Exception while getting summary: {e}")
        return "Error fetching summary."


def make_calls(person_data):
    try:
        phone_number = person_data['sms_phone_numbers_used'].strip()
        pay_amount = str(person_data.get('estimated_pay', '')).replace('$', '').replace(',', '')

        payload = {
            "phone_number": phone_number,
            "pathway_id": PATHWAY_ID,
            "pronunciation_guide": {"$": "dollars"},
            "voice": "85a2c852-2238-4651-acf0-e5cbe02186f2",
            "wait_for_greeting": True,
            "noise_cancellation": True,
            "webhook": "https://aca7-182-70-119-161.ngrok-free.app/webhook",
            "request_data": {
                "full_name": person_data.get('full_name'),
                "job_title": person_data.get('job_title'),
                "location": person_data.get('location'),
                "pay": pay_amount,
                "user_name": person_data.get('id')
            }
        }

        headers = {
            "Authorization": f"Bearer {BLAND_API_KEY}",
            "Content-Type": "application/json"
        }

        print(f"Calling {phone_number} for {person_data.get('full_name', 'Unknown')}")
        response = requests.post(CALL_URL, json=payload, headers=headers)
        print(response.json())
        if response.status_code == 200:
            print(f"Call initiated for {person_data.get('full_name', 'Unknown')}")
        else:
            print(f"Failed to initiate call for {person_data.get('full_name', 'Unknown')}")
    except Exception as e:
        print(f"Error making call for {person_data.get('full_name', 'Unknown')}: {e}")

@app.post("/initiate-calls", response_model=dict)
async def initiate_calls(user_input: UserInput):
    try:
        all_persons = fetch_all_person_data()
        if not all_persons:
            return {"message": "No person details found in database"}

        now = datetime.now()
        for i, person in enumerate(all_persons):
            run_time = now + timedelta(minutes=0, seconds=i * 5)
            job_id = f"call_{person['id']}_{int(run_time.timestamp())}"
            scheduler.add_job(make_calls, 'date', run_date=run_time, args=[person], id=job_id)
            print(f"Scheduled call for {person.get('full_name', 'Unknown')} at {run_time}")
        return {"message": "Calls initiated successfully"}
    except Exception as e:
        print(f"Error in initiate_calls: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/webhook")
async def webhook(request: Request):
    try:
        data = await request.json()
        call_id = data.get('call_id')
        phone_number = data.get('to', 'Unknown')
        print("-" * 50)
        print("Webhook received")
        print(f"Status: {data.get('status', 'Unknown')}")
        print(f"Call ID: {call_id}")
        print(f"Phone Number: {phone_number}")
        print("-" * 50)

        if call_id:
            intent = analyze_call_intent(call_id)
            summary = get_call_summary(call_id)
            print("Call Analysis Complete")
            print(f"Intent: {intent.upper()}")
            print(f"Summary: {summary}")
            print("-" * 50)
            return {
                "message": "Webhook processed successfully",
                "call_id": call_id,
                "phone_number": phone_number,
                "intent": intent,
                "summary": summary
            }
        else:
            print("No call_id in webhook data")
            return {"message": "No call_id found", "intent": "unknown", "summary": None}
    except Exception as e:
        print(f"Webhook processing error: {e}")
        return {
            "message": "Error processing webhook",
            "error": str(e),
            "intent": "error",
            "summary": None
        }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
