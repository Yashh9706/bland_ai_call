import os
import requests
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Get API key from environment
API_KEY = os.getenv("BLAND_API_KEY")

if not API_KEY:
    raise ValueError("Missing API key. Please set BLAND_API_KEY in your .env file.")

# Headers
headers = {
    "authorization": API_KEY,
}

# Data payload
data = {
    "base_prompt": "You are calling {{business}} to renew their subscription to {{service}} before it expires on {{date}}.",
    "call_data": [
        {
            "phone_number": "+918487857756",
            "business": "XYZ inc.",
            "service": "Window Cleaning",
            "date": "December 20th",
        },
        {
            "phone_number": "+918128397292",
            "business": "ABC Ltd.",
            "service": "Pest Control",
            "date": "December 22nd",
        }
    ],
    "label": "Renewal Reminder - Wednesday Afternoon with female voice",
    "voice_id": 0,
    "max_duration": 10,
    "reduce_latency": True,
    "wait_for_greeting": True,
}

# Send API request
response = requests.post("https://api.bland.ai/v1/batches", json=data, headers=headers)

# Handle response
if response.status_code == 200:
    print("✅ Batch created successfully:")
    print(response.json())
else:
    print(f"❌ Error {response.status_code}:")
    print(response.text)
