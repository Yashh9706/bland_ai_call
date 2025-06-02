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


def fetch_call_details(call_id, full_name):
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
                    return

                else:
                    print(f"🔄 Waiting... Call status: {status}")
            else:
                print(f"❌ Error fetching details: {response.status_code} - {response.text}")
                return

        except Exception as e:
            print(f"❌ Exception fetching call data: {str(e)}")
            return

        time.sleep(5)  # Check every 5 seconds


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

        result = response.json()
        print(f"📨 API response: {result}")

        if response.status_code == 200:
            call_id = result.get("call_id")
            if call_id:
                print(f"✅ Call started for {full_name} - Call ID: {call_id}")
                fetch_call_details(call_id, full_name)
            else:
                print(f"⚠️ Call started but no Call ID returned for {full_name}")
        else:
            print(f"❌ Error calling {full_name}: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"❌ Exception calling {full_name}: {str(e)}")


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
    input("Press ENTER to start the calls...")

    threads = []
    for full_name, job_title, phone_number in call_list:
        thread = threading.Thread(target=make_call, args=(full_name, job_title, phone_number))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    print("\n✅ All calls completed!")

except psycopg2.Error as e:
    print(f"❌ Database error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")