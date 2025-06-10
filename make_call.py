import asyncio
import logging
from fastapi import FastAPI, HTTPException
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from datetime import datetime, timedelta
from pydantic import BaseModel
import psycopg2
from psycopg2.extras import RealDictCursor
import json

app = FastAPI()

DATABASE_URL = 'postgresql://neondb_owner:npg_eWph9LyzAki7@ep-winter-sea-a6tcb5f1.us-west-2.aws.neon.tech/neondb?sslmode=require'

jobstores = {
    'default': SQLAlchemyJobStore(url=DATABASE_URL)
}

scheduler = BackgroundScheduler(jobstores=jobstores)
scheduler.start()

class UserInput(BaseModel):
    phone_number: str

BLAND_API_KEY = "org_0301c6c09e6f2613b52b17fb221b1b211abaa4e88525251a05982c0ccc8c494fa529dbc5e54dcae8ef0869"
PATHWAY_ID = "2bd6bfcc-1d5a-4129-b150-5ab9cab0ac2e"

url_base = "https://api.bland.ai/v1/calls"

def get_database_connection():
    """Create and return a database connection"""
    try:
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def fetch_all_person_data():
    """Fetch all person details with their job data"""
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
            pd.sms_phone_number,
            jd.job_title,
            jd.location,
            jd.estimated_pay
        FROM person_details_dummy pd
        LEFT JOIN uniti_med_job_data jd ON pd.url = jd.url
        WHERE pd.sms_phone_number IS NOT NULL AND pd.sms_phone_number != ''
        """
        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        return results
    except Exception as e:
        print(f"Error fetching data: {e}")
        if conn:
            conn.close()
        return []

def make_calls(person_data):
    """Make a call using Bland AI API with database data"""
    # Clean phone number
    phone_number = person_data['sms_phone_number'].strip()
    
    # Extract pay amount from database
    pay_amount = None
    if person_data.get('estimated_pay'):
        try:
            import re
            pay_str = str(person_data['estimated_pay']).replace('$', '').replace(',', '')
        except Exception as e:
            print(f"Error processing estimated_pay: {e}")
            pay_str = None

    data = {
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

    try:
        print(f"Making call to {phone_number} for {person_data.get('full_name', 'Unknown')}")
        response = requests.post(url_base, json=data, headers=headers)
        
        if response.status_code == 200:
            print(f"Call initiated successfully for {person_data.get('full_name', 'Unknown')}")
        else:
            print(f"Failed to initiate call for {person_data.get('full_name', 'Unknown')}")
            print(f"Status code: {response.status_code}")
            
    except Exception as e:
        print(f"Error making call for {person_data.get('full_name', 'Unknown')}: {e}")

@app.post("/initiate-calls", response_model=dict)
async def initiate_calls(user_input: UserInput):
    """Fetch all data from database and schedule calls for everyone"""
    try:
        # Fetch all person data with job details
        all_persons = fetch_all_person_data()
        
        if not all_persons:
            return {"message": "No person details found in database"}
        
        current_time = datetime.now()
        
        # Schedule calls for all persons
        for i, person in enumerate(all_persons):
            # Schedule each call 1 minute from now with 5-second intervals
            run_time = current_time + timedelta(minutes=1, seconds=i*5)
            job_id = f"call_{person['id']}_{int(run_time.timestamp())}"
            
            scheduler.add_job(
                make_calls, 
                'date', 
                run_date=run_time, 
                args=[person], 
                id=job_id
            )
            
            print(f"Scheduled call for {person.get('full_name', 'Unknown')} at {run_time}")
        
        return {"message": "Calls initiated successfully"}
        
    except Exception as e:
        print(f"Error in initiate_calls: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/webhook")
async def webhook(request: dict):
    print("="*50)
    print("🔔 WEBHOOK RECEIVED - CALL ENDED!")
    print("="*50)
    print(f"Call Status: {request.get('status', 'Unknown')}")
    print(f"Call ID: {request.get('call_id', 'Unknown')}")
    print(f"Duration: {request.get('call_length', 'Unknown')} seconds")
    print(f"Phone Number: {request.get('to', 'Unknown')}")
    print(f"From Number: {request.get('from', 'Unknown')}")
    print("Full webhook data:")
    print(json.dumps(request, indent=2))
    print("="*50)
    # Process the webhook data
    return {"message": "Webhook received successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

@app.post("/initiate-calls", response_model=dict)
async def initiate_calls(user_input: UserInput):
    """Fetch all data from database and schedule calls for everyone"""
    try:
        # Fetch all person data with job details
        all_persons = fetch_all_person_data()
        
        if not all_persons:
            return {"message": "No person details found in database"}
        
        current_time = datetime.now()
        
        # Schedule calls for all persons
        for i, person in enumerate(all_persons):
            # Schedule each call 1 minute from now with 5-second intervals
            run_time = current_time + timedelta(minutes=1, seconds=i*5)
            job_id = f"call_{person['id']}_{int(run_time.timestamp())}"
            
            scheduler.add_job(
                make_calls, 
                'date', 
                run_date=run_time, 
                args=[person], 
                id=job_id
            )
            
            print(f"Scheduled call for {person.get('full_name', 'Unknown')} at {run_time}")
        
        return {"message": "Calls initiated successfully"}
        
    except Exception as e:
        print(f"Error in initiate_calls: {e}")
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

@app.post("/webhook")
async def webhook(request: dict):
    print("="*50)
    print("🔔 WEBHOOK RECEIVED - CALL ENDED!")
    print("="*50)
    print(f"Call Status: {request.get('status', 'Unknown')}")
    print(f"Call ID: {request.get('call_id', 'Unknown')}")
    print(f"Duration: {request.get('call_length', 'Unknown')} seconds")
    print(f"Phone Number: {request.get('to', 'Unknown')}")
    print(f"From Number: {request.get('from', 'Unknown')}")
    print("Full webhook data:")
    print(json.dumps(request, indent=2))
    print("="*50)
    # Process the webhook data
    return {"message": "Webhook received successfully"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)