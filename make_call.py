import psycopg2
import requests
import threading
import re
import time

# PostgreSQL connection parameters
db_params = {
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_O0EKCHYfAnI9",
    "host": "ep-square-leaf-a5xdwsdw.us-east-2.aws.neon.tech",
    "port": 5432,
    "sslmode": "require"
}

# Bland API config
API_KEY = 'org_0301c6c09e6f2613b52b17fb221b1b211abaa4e88525251a05982c0ccc8c494fa529dbc5e54dcae8ef0869'
PATHWAY_ID = "0608afc1-5c2b-4ee7-8c9f-83a67fd0ff3b"


def extract_valid_phone(directdials):
    """Extracts the first valid phone number (max 15 digits, includes +)"""
    if not directdials:
        return None

    numbers = re.split(r'[;,/\s]+', str(directdials))

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
            call_id = result.get("call_id")  # ✅ Corrected key
            if call_id:
                print(f"✅ Call started for {full_name} - Call ID: {call_id}")
                fetch_call_details(call_id, full_name)
            else:
                print(f"⚠️ Call started but no Call ID returned for {full_name}")
        else:
            print(f"❌ Error calling {full_name}: {response.status_code} - {response.text}")

    except Exception as e:
        print(f"❌ Exception calling {full_name}: {str(e)}")



# Connect to PostgreSQL and fetch records
try:
    conn = psycopg2.connect(**db_params)
    cur = conn.cursor()

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

    call_list = []
    print("\n📋 Fetched Records:\n")
    for full_name, job_title, directdials, record_id in records:
        if full_name.strip().lower() != "hirmi":
            continue  # ✅ Only call 'hirmi'

        print(f"🆔 ID: {record_id}")
        print(f"👤 Name: {full_name}")
        print(f"💼 Job Title: {job_title}")
        print(f"📱 Raw Phone: {directdials}")

        phone_number = extract_valid_phone(directdials)
        if phone_number:
            print(f"📞 Cleaned Phone: {phone_number}")
            call_list.append((full_name, job_title, phone_number))
        else:
            print(f"⚠️ Skipping {full_name}: Invalid phone number")
        print("-" * 50)

    if not call_list:
        print("❌ No valid records found to call.")
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
