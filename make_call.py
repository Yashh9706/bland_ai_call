import os
import requests
import psycopg2
from dotenv import load_dotenv
import json

# Load environment variables from .env file
load_dotenv()

# Get API key from environment
API_KEY = os.getenv("BLAND_API_KEY")

if not API_KEY:
    raise ValueError("Missing API key. Please set BLAND_API_KEY in your .env file.")

# Database connection details
DB_CONFIG = {
    'dbname': 'neondb',
    'user': 'neondb_owner',
    'password': 'npg_eWph9LyzAki7',
    'host': 'ep-winter-sea-a6tcb5f1.us-west-2.aws.neon.tech',
    'port': '5432',
    'sslmode': 'require'
}

# Target phone number for all calls
TARGET_PHONE = "+918128397292"

def fetch_person_details():
    """Fetch first 10 records from person_details table"""
    try:
        # Connect to database
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Query to get first 10 records
        query = "SELECT full_name, directdials FROM person_details LIMIT 10"
        cursor.execute(query)
        
        records = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return records
        
    except Exception as e:
        print(f"❌ Database error: {e}")
        return []

def create_call_data():
    """Create call data from database records"""
    records = fetch_person_details()
    call_data = []
    
    print("📞 Processing records from database:")
    print("-" * 50)
    
    for record in records:
        full_name, directdials = record
        
        # Handle case where directdials might be None or empty
        if not directdials:
            print(f"⚠️ No phone number found for {full_name}")
            continue
            
        # Split multiple phone numbers if they exist (assuming comma-separated)
        phone_numbers = [num.strip() for num in str(directdials).split(',') if num.strip()]
        
        for phone_num in phone_numbers:
            # Print the found number but use target number for actual call
            print(f"👤 Full Name: {full_name}")
            print(f"📱 Found Number: {phone_num}")
            print(f"📞 Calling Number: {TARGET_PHONE}")
            print("-" * 30)
            
            # Add to call data with target phone number
            call_data.append({
                "phone_number": TARGET_PHONE,
                "business": full_name,
                "service": "Database Service",
                "date": "December 30th",
            })
    
    return call_data

# Headers
headers = {
    "authorization": API_KEY,
}

# Get call data from database
call_data = create_call_data()

if not call_data:
    print("❌ No call data found. Please check your database connection and data.")
    exit()

# Data payload
data = {
    "base_prompt": "You are calling {{business}} to renew their subscription to {{service}} before it expires on {{date}}.",
    "call_data": call_data,
    "label": "Database Renewal Reminder - Automated Calls",
    "voice_id": 0,
    "max_duration": 10,
    "reduce_latency": True,
    "wait_for_greeting": True,
}

print(f"\n🚀 Preparing to send {len(call_data)} calls...")
print("=" * 50)

# Send API request
response = requests.post("https://api.bland.ai/v1/batches", json=data, headers=headers)

# Handle response
if response.status_code == 200:
    print("✅ Batch created successfully:")
    print(json.dumps(response.json(), indent=2))
else:
    print(f"❌ Error {response.status_code}:")
    print(response.text)