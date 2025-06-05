import psycopg2
import requests
import threading
import re
import time
import json
import os
from dotenv import load_dotenv

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
                "goal": "Determine if the caller is interested in taking a job",
                "questions": [
                    ["Is the caller interested in taking a job (e.g., said 'interested,' 'apply,' asked about job details, or expressed intent to work)?", "boolean"]
                ]
            },
            timeout=30
        )

        if response.status_code == 200:
            response_json = response.json()
            if "answers" in response_json:
                is_interested = response_json["answers"][0]
                call_intent = "positive" if is_interested else "negative"
                return call_intent, is_interested
            else:
                return "unknown", None
        else:
            return "error", None

    except Exception as e:
        return "error", None

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

def fetch_call_details(call_id, full_name):
    """Fetches call details and analyzes the call for interest"""
    while True:
        try:
            response = requests.get(
                f"https://api.bland.ai/v1/calls/{call_id}",
                headers={"Authorization": f"Bearer {API_KEY}"},
                timeout=20
            )

            if response.status_code == 200:
                result = response.json()
                status = result.get("status")
                completed = result.get("completed")

                if completed or status == "completed":
                    call_intent, is_interested = analyze_call_interest(call_id, full_name)
                    
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': status,
                        'duration': result.get('corrected_duration'),
                        'call_intent': call_intent,
                        'is_interested': is_interested,
                        'summary': result.get('summary')
                    }
            else:
                return None

        except Exception as e:
            return None

        time.sleep(5)  # Check every 5 seconds

def make_call(full_name, job_details, phone_number, results_list=None):
    """Makes a call and returns call analysis results"""
    job_title = job_details.get('job_title', 'Unknown Position')
    location = job_details.get('location', 'Unknown Location')
    pay = job_details.get('pay', 'Competitive Pay')
    
    # Print detailed call initiation info
    print(f"📞 Initiating call for {full_name}:")
    print(f"   📋 Job Title: {job_title}")
    print(f"   📍 Location: {location}")
    print(f"   💰 Pay: {pay}")
    print(f"   📱 Phone: {phone_number}")
    print(f"   👤 User Name: {full_name}")
    print("-" * 50)
    
    data = {
        "phone_number": phone_number,
        "pathway_id": PATHWAY_ID,
        "request_data": {
            "full_name": full_name,
            "job_title": job_title,
            "location": location,
            "pay": pay,
            "user_name": full_name
        }
    }

    try:
        response = requests.post(
            "https://api.bland.ai/v1/calls",
            headers={
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json"
            },
            json=data,
            timeout=30
        )

        result = response.json()

        if response.status_code == 200:
            call_id = result.get("call_id")
            if call_id:
                print(f"✅ Call initiated successfully for {full_name} (Call ID: {call_id})")
                call_result = fetch_call_details(call_id, full_name)
                
                if results_list is not None and call_result:
                    results_list.append(call_result)
                
                return call_result
            else:
                print(f"❌ Failed to get call ID for {full_name}")
                return None
        else:
            print(f"❌ Call failed for {full_name}. Status: {response.status_code}")
            return None

    except Exception as e:
        print(f"❌ Error making call for {full_name}: {str(e)}")
        return None

def print_call_summary(call_results):
    """Prints a summary of all call results"""
    if not call_results:
        print("\n❌ No call results to summarize.")
        return
    
    print("\n" + "="*60)
    print("📊 DETAILED CALL SUMMARY")
    print("="*60)
    
    interested_count = 0
    not_interested_count = 0
    unknown_count = 0
    
    for result in call_results:
        name = result.get('name', 'Unknown')
        call_intent = result.get('call_intent', 'unknown')
        duration = result.get('duration', 'Unknown')
        call_id = result.get('call_id', 'Unknown')
        
        if call_intent == 'positive':
            interested_count += 1
            status_emoji = "✅"
        elif call_intent == 'negative':
            not_interested_count += 1
            status_emoji = "❌"
        else:
            unknown_count += 1
            status_emoji = "❓"
        
        print(f"{status_emoji} {name}")
        print(f"   📞 Call ID: {call_id}")
        print(f"   🎯 Intent: {call_intent.upper()}")
        print(f"   ⏱️  Duration: {duration}s")
        if result.get('summary'):
            print(f"   📝 Summary: {result.get('summary')[:100]}...")
        print("-" * 40)
    
    print(f"\n📊 STATISTICS:")
    print(f"✅ Interested: {interested_count}")
    print(f"❌ Not Interested: {not_interested_count}")
    print(f"❓ Unknown/Error: {unknown_count}")
    print(f"📞 Total Calls: {len(call_results)}")
    
    if len(call_results) > 0:
        success_rate = (interested_count / len(call_results)) * 100
        print(f"📊 Interest Rate: {success_rate:.1f}%")
    
    print("="*60)

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
        
        # Connect to PostgreSQL and fetch records
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        cur.execute(f"""
            SELECT full_name, url, directdials, id
            FROM {TABLE_NAME}
            WHERE full_name IS NOT NULL 
            AND url IS NOT NULL 
            AND status = 'Yes'
            AND directdials IS NOT NULL
            AND directdials::text != 'null'
            AND directdials::text != '""'
            AND directdials::text != '[]'
            AND LENGTH(directdials::text) > 5
            ORDER BY id
            LIMIT 5
        """)
        records = cur.fetchall()

        call_list = []
        
        for full_name, url_string, directdials, record_id in records:
            # Extract first URL
            first_url = extract_first_url(url_string)
            if not first_url:
                continue

            # Get job details
            job_details = get_job_details_from_url(cur, first_url)
            if not job_details or not job_details.get('job_title'):
                continue

            # Extract valid phone number from directdials
            phone_number = extract_valid_phone(directdials)
            if phone_number:
                call_list.append((full_name, job_details, phone_number))

        cur.close()
        conn.close()

        if not call_list:
            print("❌ No valid records found to call.")
            exit()

        # Display detailed preview of all calls
        display_call_preview(call_list)

        # Store call results for summary
        call_results = []

        # Make all calls with threading
        threads = []
        
        for full_name, job_details, phone_number in call_list:
            thread = threading.Thread(target=make_call, args=(full_name, job_details, phone_number, call_results))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

        print("\n✅ All calls completed!")
        print_call_summary(call_results)

    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")