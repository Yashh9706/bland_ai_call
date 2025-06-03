import psycopg2
import requests
import threading
import re
import time
import json

# PostgreSQL connection parameters - UPDATED
db_params = {
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_eWph9LyzAki7",
    "host": "ep-winter-sea-a6tcb5f1.us-west-2.aws.neon.tech",
    "port": 5432,
    "sslmode": "require"
}

# Bland API config
API_KEY = 'org_0301c6c09e6f2613b52b17fb221b1b211abaa4e88525251a05982c0ccc8c494fa529dbc5e54dcae8ef0869'
PATHWAY_ID = "0608afc1-5c2b-4ee7-8c9f-83a67fd0ff3b"


def analyze_call_interest(call_id, full_name):
    """Analyzes the call to determine if the caller showed interest in the job"""
    print(f"🔍 Analyzing call interest for {full_name}...")
    
    try:
        # Make the POST request to analyze the call
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

        # Try to parse and use the response
        if response.status_code == 200:
            response_json = response.json()
            print(f"📊 Analysis Response for {full_name}: {response_json}")

            if "answers" in response_json:
                is_interested = response_json["answers"][0]
                call_intent = "positive" if is_interested else "negative"
                
                print(f"🎯 Call Intent for {full_name}: {call_intent}")
                print(f"{'✅ INTERESTED' if is_interested else '❌ NOT INTERESTED'} - {full_name}")
                
                return call_intent, is_interested
            else:
                print(f"⚠️ Error: 'answers' not found in the API response for {full_name}")
                if "error" in response_json:
                    print(f"API Error: {response_json['error']}")
                if "message" in response_json:
                    print(f"Message: {response_json['message']}")
                return "unknown", None
        else:
            print(f"❌ Analysis API error for {full_name}: {response.status_code} - {response.text}")
            return "error", None

    except Exception as e:
        print(f"❌ Failed to analyze call for {full_name}: {e}")
        return "error", None


def debug_database_contents():
    """Debug function to check what's in the database"""
    try:
        conn = psycopg2.connect(**db_params)
        cur = conn.cursor()
        
        print("🔍 DEBUGGING DATABASE CONTENTS:")
        print("=" * 60)
        
        # Check total records
        cur.execute("SELECT COUNT(*) FROM person_details WHERE status = 'Yes'")
        total = cur.fetchone()[0]
        print(f"📊 Total records with status='Yes': {total}")
        
        # Check different NULL representations (handling JSON column)
        queries = [
            ("NULL directdials", "directdials IS NULL"),
            ("JSON null directdials", "directdials::text = 'null'"),
            ("Empty JSON string", "directdials::text = '\"\"'"),
            ("Has actual value", "directdials IS NOT NULL AND directdials::text != 'null' AND directdials::text != '\"\"' AND LENGTH(directdials::text) > 5")
        ]
        
        for desc, condition in queries:
            cur.execute(f"SELECT COUNT(*) FROM person_details WHERE status = 'Yes' AND {condition}")
            count = cur.fetchone()[0]
            print(f"📈 {desc}: {count}")
        
        # Show sample records with actual phone data (handling JSON)
        print("\n📋 Sample records with potential phone numbers:")
        cur.execute("""
            SELECT id, full_name, directdials::text, 
                   CASE 
                       WHEN directdials IS NULL THEN 'NULL'
                       WHEN directdials::text = 'null' THEN 'JSON_NULL'
                       WHEN directdials::text = '""' THEN 'EMPTY_JSON_STRING'
                       ELSE 'HAS_VALUE'
                   END as status
            FROM person_details 
            WHERE status = 'Yes'
            ORDER BY 
                CASE 
                    WHEN directdials IS NOT NULL AND directdials::text != 'null' AND directdials::text != '""' THEN 1
                    ELSE 2
                END,
                id
            LIMIT 10
        """)
        
        records = cur.fetchall()
        for record in records:
            print(f"  ID: {record[0]}, Name: {record[1]}, Phone: {record[2]}, Status: {record[3]}")
        
        cur.close()
        conn.close()
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Debug error: {e}")


def extract_first_url(url_string):
    """Extracts the first URL from a string that may contain multiple URLs separated by ';'"""
    if not url_string:
        return None
    
    urls = url_string.split(';')
    first_url = urls[0].strip()
    return first_url if first_url else None


def get_job_title_from_url(cursor, url):
    """Fetches job_title from uniti_med_job_data table based on URL"""
    try:
        cursor.execute("""
            SELECT job_title 
            FROM uniti_med_job_data 
            WHERE url = %s
            LIMIT 1
        """, (url,))
        
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            print(f"⚠️ No job_title found for URL: {url}")
            return None
            
    except Exception as e:
        print(f"❌ Error fetching job_title for URL {url}: {str(e)}")
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


def validate_phone_number(phone):
    """Validates and formats phone number"""
    if not phone:
        return None
    
    # Remove all non-digit and non-plus characters
    clean = re.sub(r'[^\d+]', '', phone.strip())
    
    if not clean:
        return None
    
    # Format the phone number
    if not clean.startswith('+') and len(clean) == 10:
        clean = '+1' + clean
    elif not clean.startswith('+') and len(clean) == 11 and clean.startswith('1'):
        clean = '+' + clean
    elif clean.startswith('+') and 8 <= len(clean) <= 15:
        pass
    elif not clean.startswith('+') and 8 <= len(clean) <= 15:
        clean = '+' + clean
    else:
        return None
    
    if 8 <= len(clean) <= 15 and clean.startswith('+'):
        return clean
    return None


def fetch_call_details(call_id, full_name):
    """Fetches call details and analyzes the call for interest"""
    print(f"⏳ Waiting for {full_name}'s call to complete...")

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
                    print(f"\n✅ Call completed for {full_name}!\n")

                    print("📝 Full Call Transcript:")
                    print(result.get("concatenated_transcript", "(No transcript available)"))

                    print("\n🗣️ Individual Exchanges:")
                    for t in result.get("transcripts", []):
                        print(f"{t['user']}: {t['text']}")

                    print("\n📊 Call Metadata:")
                    print(f"📞 From: {result.get('from')}")
                    print(f"📱 To: {result.get('to')}")
                    print(f"⏱️ Duration: {result.get('corrected_duration')} seconds")
                    print(f"📅 Started At: {result.get('started_at')}")
                    print(f"📝 Summary: {result.get('summary')}")
                    
                    # NEW: Analyze the call for interest
                    print(f"\n🔍 Analyzing call interest for {full_name}...")
                    call_intent, is_interested = analyze_call_interest(call_id, full_name)
                    
                    # Store results for summary
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
                    print(f"🔄 Waiting... Call status: {status}")
            else:
                print(f"❌ Error fetching details: {response.status_code} - {response.text}")
                return None

        except Exception as e:
            print(f"❌ Exception fetching call data: {str(e)}")
            return None

        time.sleep(5)  # Check every 5 seconds


def make_call(full_name, job_title, phone_number, results_list=None):
    """Makes a call and returns call analysis results"""
    print(f"📞 Initiating call for {full_name} ({phone_number}) - {job_title}")
    data = {
        "phone_number": phone_number,
        "pathway_id": PATHWAY_ID,
        "request_data": {
            "full_name": full_name,
            "job_title": job_title,
            "user_name": full_name  # Add this to replace {{user_name}} placeholder
        }
    }
    
    print(f"📤 Sending data to API: {json.dumps(data, indent=2)}")

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
        print(f"📨 API response: {result}")

        if response.status_code == 200:
            call_id = result.get("call_id")
            if call_id:
                print(f"✅ Call started for {full_name} - Call ID: {call_id}")
                call_result = fetch_call_details(call_id, full_name)
                
                # Add to results list if provided (for threading)
                if results_list is not None and call_result:
                    results_list.append(call_result)
                
                return call_result
            else:
                print(f"⚠️ Call started but no Call ID returned for {full_name}")
                return None
        else:
            print(f"❌ Error calling {full_name}: {response.status_code} - {response.text}")
            return None

    except Exception as e:
        print(f"❌ Exception calling {full_name}: {str(e)}")
        return None


def print_call_summary(call_results):
    """Prints a summary of all call results"""
    if not call_results:
        print("\n❌ No call results to summarize.")
        return
    
    print("\n" + "="*80)
    print("📊 CALL SUMMARY REPORT")
    print("="*80)
    
    interested_count = 0
    not_interested_count = 0
    unknown_count = 0
    
    for result in call_results:
        name = result.get('name', 'Unknown')
        call_intent = result.get('call_intent', 'unknown')
        is_interested = result.get('is_interested')
        duration = result.get('duration', 'Unknown')
        
        # Count results
        if call_intent == 'positive':
            interested_count += 1
            status_emoji = "✅"
        elif call_intent == 'negative':
            not_interested_count += 1
            status_emoji = "❌"
        else:
            unknown_count += 1
            status_emoji = "❓"
        
        print(f"{status_emoji} {name}: {call_intent.upper()} ({duration}s)")
    
    print("\n" + "-"*40)
    print("📈 SUMMARY STATISTICS:")
    print(f"✅ Interested: {interested_count}")
    print(f"❌ Not Interested: {not_interested_count}")
    print(f"❓ Unknown/Error: {unknown_count}")
    print(f"📞 Total Calls: {len(call_results)}")
    
    if len(call_results) > 0:
        success_rate = (interested_count / len(call_results)) * 100
        print(f"📊 Interest Rate: {success_rate:.1f}%")
    
    print("="*80)


# Main execution
try:
    # First, debug the database contents
    debug_database_contents()
    
    # Connect to PostgreSQL and fetch records
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    print("\n🔍 Fetching records with improved filtering...")
    
    # Updated query with JSON handling
    cur.execute("""
        SELECT full_name, url, directdials::text, id
        FROM person_details
        WHERE full_name IS NOT NULL 
        AND url IS NOT NULL 
        AND status = 'Yes'
        AND directdials IS NOT NULL
        AND directdials::text != 'null'
        AND directdials::text != '""'
        AND LENGTH(directdials::text) > 5
        ORDER BY id
        LIMIT 5
    """)
    records = cur.fetchall()

    call_list = []
    print(f"\n📋 Found {len(records)} potential records:\n")
    
    for full_name, url_string, directdials, record_id in records:

        print(f"🆔 ID: {record_id}")
        print(f"👤 Full Name: {full_name}")
        print(f"🔗 Raw URLs: {url_string}")
        print(f"📱 Raw Phone: {directdials}")

        # Extract first URL
        first_url = extract_first_url(url_string)
        if not first_url:
            print(f"⚠️ Skipping {full_name}: No valid URL found")
            print("-" * 50)
            continue

        print(f"🎯 First URL: {first_url}")

        # Get job_title from uniti_med_job_data table
        job_title = get_job_title_from_url(cur, first_url)
        if not job_title:
            print(f"⚠️ Skipping {full_name}: No job_title found for URL")
            print("-" * 50)
            continue

        print(f"💼 Job Title: {job_title}")

        # Extract valid phone number
        phone_number = extract_valid_phone(directdials)
        if phone_number:
            print(f"📞 Cleaned Phone: {phone_number}")
            call_list.append((full_name, job_title, phone_number))
        else:
            print(f"⚠️ Skipping {full_name}: Invalid phone number format")
        
        print("-" * 50)

    cur.close()
    conn.close()

    if not call_list:
        print("❌ No valid records found to call.")
        print("\n💡 Suggestions:")
        print("1. Check if phone numbers are stored in a different column")
        print("2. Verify the phone number format in your database")
        print("3. Check if directdials contains JSON that needs different parsing")
        exit()

    print(f"\n🎯 Ready to make {len(call_list)} calls")
    
    # NEW: Add testing phone number option
    print("\n" + "="*60)
    print("🧪 TESTING MODE OPTION")
    print("="*60)
    print("For testing purposes, you can replace all phone numbers with your test number.")
    print("This prevents unnecessary calls to real users during development/testing.")
    print()
    
    user_input = input("Enter your test phone number (or press ENTER to use original numbers): ").strip()
    
    test_phone = None
    if user_input:
        # User entered something, try to validate it as a phone number
        test_phone = validate_phone_number(user_input)
        
        if test_phone:
            print(f"✅ Test phone number validated: {test_phone}")
        else:
            print("❌ Invalid phone number format. Please try again.")
            print("   Examples: +1234567890, 1234567890, +918487857756")
            
            # Give another chance to enter correct number
            while True:
                retry_input = input("\n📱 Enter your test phone number (or press ENTER to skip): ").strip()
                if not retry_input:
                    print("⏭️ Skipping test mode - using original numbers")
                    break
                    
                test_phone = validate_phone_number(retry_input)
                if test_phone:
                    print(f"✅ Test phone number validated: {test_phone}")
                    break
                else:
                    print("❌ Invalid format. Try again or press ENTER to skip.")
    else:
        print("⏭️ Using original phone numbers from database")
    
    # Update call list with test number if provided
    if test_phone:
        print(f"\n🧪 TEST MODE: Making only ONE call to your test number: {test_phone}")
        # Use the first person's details but with test phone number
        first_person = call_list[0]
        call_list = [(first_person[0], first_person[1], test_phone)]
        print(f"✅ Test call will be made using: {first_person[0]}'s details")
    
    print(f"\n📞 Final call list:")
    for i, (name, job, phone) in enumerate(call_list, 1):
        print(f"  {i}. {name} ({job}) -> {phone}")
    
    if test_phone:
        print(f"\n🧪 TEST MODE: Only 1 call will be made to your test number")
    else:
        print(f"\n🔄 LIVE MODE: {len(call_list)} calls will be made to actual users")
    
    print("\n" + "="*60)
    input("Press ENTER to start the calls...")

    # NEW: Store call results for summary
    call_results = []

    if test_phone:
        # For test mode, make only one call (no threading needed)
        print("🧪 Making test call...")
        full_name, job_title, phone_number = call_list[0]
        result = make_call(full_name, job_title, phone_number)
        if result:
            call_results.append(result)
    else:
        # For live mode, make all calls with threading
        threads = []
        # Use a thread-safe list for collecting results
        import threading
        results_lock = threading.Lock()
        
        for full_name, job_title, phone_number in call_list:
            thread = threading.Thread(target=make_call, args=(full_name, job_title, phone_number, call_results))
            thread.start()
            threads.append(thread)

        for thread in threads:
            thread.join()

    print("\n✅ All calls completed!")
    
    # NEW: Print summary of all calls
    print_call_summary(call_results)

except psycopg2.Error as e:
    print(f"❌ Database error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")