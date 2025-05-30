import psycopg2
import requests
import threading
import re

# PostgreSQL connection parameters
db_params = {
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_O0EKCHYfAnI9",
    "host": "ep-square-leaf-a5xdwsdw.us-east-2.aws.neon.tech",
    "port": 5432,
    "sslmode": "require"
}

# Bland API key
API_KEY = 'org_0301c6c09e6f2613b52b17fb221b1b211abaa4e88525251a05982c0ccc8c494fa529dbc5e54dcae8ef0869'
PATHWAY_ID = "0608afc1-5c2b-4ee7-8c9f-83a67fd0ff3b"

def extract_valid_phone(directdials):
    """Extracts the first valid phone number (max 15 digits, includes +)"""
    if not directdials:
        return None
    
    # Handle multiple phone numbers separated by various delimiters
    numbers = re.split(r'[;,/\s]+', str(directdials))
    
    for number in numbers:
        # Clean the number but preserve + sign
        clean = re.sub(r'[^\d+]', '', number.strip())
        
        # Check if it's a valid phone number
        if clean:
            # If it doesn't start with +, and has 10 digits, assume US number
            if not clean.startswith('+') and len(clean) == 10:
                clean = '+1' + clean
            # If it starts with 1 and has 11 digits, add +
            elif not clean.startswith('+') and len(clean) == 11 and clean.startswith('1'):
                clean = '+' + clean
            # If it starts with + and has reasonable length
            elif clean.startswith('+') and 8 <= len(clean) <= 15:
                pass
            # If it's international without +
            elif not clean.startswith('+') and 8 <= len(clean) <= 15:
                clean = '+' + clean
            else:
                continue
                
            # Final validation
            if 8 <= len(clean) <= 15 and clean.startswith('+'):
                return clean
    
    return None

def make_call(full_name, job_title, phone_number):
    print(f"📞 Initiating call for {full_name} ({phone_number}) - {job_title}")
    data = {
        "phone_number": phone_number,
        "pathway_id": PATHWAY_ID,
        "request_data": {
            "full_name": full_name,
            "job_title": job_title
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

        if response.status_code == 200:
            print(f"✅ Call started for {full_name}")
            print(f"Response: {response.json()}")
        else:
            print(f"❌ Error calling {full_name}: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Exception calling {full_name}: {str(e)}")

# Connect to PostgreSQL and fetch records
try:
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

    # Get first 5 records ordered by ID
    print("🔍 Fetching first 5 records...")
    cur.execute("""
        SELECT full_name, job_title, directdials, id
        FROM job_nurse_match
        WHERE full_name IS NOT NULL AND job_title IS NOT NULL
        ORDER BY id
        LIMIT 5
    """)
    records = cur.fetchall()

    cur.close()
    conn.close()

    # Prepare cleaned call data
    call_list = []
    print("\n📋 Fetched Records:\n")
    
    for full_name, job_title, directdials, record_id in records:
        print(f"🆔 ID: {record_id}")
        print(f"👤 Name: {full_name}")
        print(f"💼 Job Title: {job_title}")
        print(f"📱 Raw Phone Data: {directdials}")
        
        phone_number = extract_valid_phone(directdials)
        if not phone_number:
            print(f"⚠️ Skipping {full_name}: Invalid phone number → {directdials}")
        else:
            print(f"📞 Cleaned Phone: {phone_number}")
            call_list.append((full_name, job_title, phone_number))
        
        print("-" * 50)

    if not call_list:
        print("❌ No valid records found to call.")
        exit()

    print(f"\n🎯 Ready to make {len(call_list)} calls (out of 5 records fetched)")
    input("Press ENTER to start the calls...")

    # Launch calls in separate threads
    threads = []
    for full_name, job_title, phone_number in call_list:
        thread = threading.Thread(target=make_call, args=(full_name, job_title, phone_number))
        thread.start()
        threads.append(thread)

    # Wait for all threads to finish
    for thread in threads:
        thread.join()

    print("\n✅ All calls completed!")

except psycopg2.Error as e:
    print(f"❌ Database error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")