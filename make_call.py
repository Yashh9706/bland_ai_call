import psycopg2
import requests
import threading
import re
import time
import json
import os
from dotenv import load_dotenv
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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
                "goal": "Analyze the caller's response to the job opportunity call and determine their interest level and intent",
                "questions": [
                    ["Based on the conversation, is the caller genuinely interested in the job opportunity (expressed interest, asked questions about the job, wanted more details, or showed positive engagement)?", "boolean"],
                    ["What is the caller's primary response category: 'interested' (wants the job/more info), 'not_interested' (clearly declined/not interested), 'callback_later' (wants to be contacted later/needs time), 'no_answer' (didn't answer or very brief interaction), or 'unclear' (mixed signals)?", "string"],
                    ["What specific concerns, questions, or requirements did the caller mention about the job opportunity?", "string"],
                    ["What was the main reason behind the caller's response? Be specific about what they said or their primary concern/interest.", "string"],
                    ["Did the caller ask for more information, request a callback, or show any form of engagement with the opportunity?", "boolean"]
                ]
            },
            timeout=30
        )

        if response.status_code == 200:
            response_json = response.json()
            print(f"🔍 Analysis response for {full_name}: {response_json}")
            
            if "answers" in response_json and response_json["answers"] is not None:
                answers = response_json["answers"]
                
                # Safely extract answers with proper None checks
                is_interested = answers[0] if len(answers) > 0 and answers[0] is not None else False
                primary_response = answers[1] if len(answers) > 1 and answers[1] is not None else "unclear"
                concerns = answers[2] if len(answers) > 2 and answers[2] is not None else "No specific concerns mentioned"
                reason = answers[3] if len(answers) > 3 and answers[3] is not None else "No specific reason provided"
                showed_engagement = answers[4] if len(answers) > 4 and answers[4] is not None else False
                
                # Convert to string and clean up
                primary_response = str(primary_response).lower().strip()
                concerns = str(concerns).strip() if concerns and str(concerns).lower() not in ['none', 'null', ''] else "No specific concerns mentioned"
                reason = str(reason).strip() if reason and str(reason).lower() not in ['none', 'null', ''] else "No specific reason provided"
                
                # Map to yes/no/later
                call_intent = "no"  # Default
                if primary_response == "interested" or is_interested or showed_engagement:
                    call_intent = "yes"
                elif primary_response == "callback_later" or "later" in primary_response:
                    call_intent = "later"
                elif primary_response in ["not_interested", "no_answer", "unclear"]:
                    call_intent = "no"
                
                return {
                    'call_intent': call_intent,
                    'is_interested': is_interested,
                    'concerns': concerns,
                    'reason': reason,
                    'showed_engagement': showed_engagement,
                    'raw_intent': primary_response
                }
            else:
                print(f"⚠️ No answers in analysis response for {full_name}")
                return {
                    'call_intent': "no",  # Default to 'no' for failed analysis
                    'is_interested': None,
                    'concerns': "Analysis failed - no answers received",
                    'reason': "Analysis failed - no answers received",
                    'showed_engagement': None,
                    'raw_intent': "unknown"
                }
        else:
            print(f"❌ Analysis API error for {full_name}: {response.status_code}")
            print(f"Response: {response.text}")
            return {
                'call_intent': "no",  # Default to 'no' for API errors
                'is_interested': None,
                'concerns': f"API Error - Status {response.status_code}",
                'reason': f"API Error - Status {response.status_code}",
                'showed_engagement': None,
                'raw_intent': "error"
            }

    except Exception as e:
        print(f"❌ Error analyzing call for {full_name}: {str(e)}")
        return {
            'call_intent': "no",  # Default to 'no' for exceptions
            'is_interested': None,
            'concerns': f"Exception occurred: {str(e)}",
            'reason': f"Exception occurred: {str(e)}",
            'showed_engagement': None,
            'raw_intent': "error"
        }

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

def get_call_summary_directly(call_id, full_name):
    """Directly fetch call summary from the API without analysis"""
    try:
        session = requests.Session()
        retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
        session.mount("https://", HTTPAdapter(max_retries=retries))
        
        response = session.get(
            f"https://api.bland.ai/v1/calls/{call_id}",
            headers={"Authorization": f"Bearer {API_KEY}"},
            timeout=60  # Increased timeout
        )

        if response.status_code == 200:
            result = response.json()
            return {
                'summary': result.get('summary', 'No summary available'),
                'transcript': result.get('transcript', 'No transcript available'),
                'status': result.get('status', 'unknown'),
                'duration': result.get('corrected_duration', result.get('duration', 'Unknown')),
                'answered': result.get('answered', False),
                'created_at': result.get('created_at'),
                'queue_time': result.get('queue_time'),
                'ring_time': result.get('ring_time'),
                'completed': result.get('completed', False)
            }
        else:
            print(f"❌ Failed to fetch call summary for {full_name}: {response.status_code}")
            return None
            
    except Exception as e:
        print(f"❌ Error fetching call summary for {full_name}: {str(e)}")
        return None

def fetch_call_details(call_id, full_name):
    """Fetches call details and analyzes the call for interest with retries"""
    print(f"🔄 Waiting for call completion: {full_name} (ID: {call_id})")
    
    # Set up retry strategy
    session = requests.Session()
    retries = Retry(total=5, backoff_factor=2, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))
    
    max_attempts = 45
    attempt = 0

    while attempt < max_attempts:
        try:
            response = session.get(
                f"https://api.bland.ai/v1/calls/{call_id}",
                headers={"Authorization": f"Bearer {API_KEY}"},
                timeout=60
            )

            if response.status_code == 200:
                result = response.json()
                status = result.get("status")
                completed = result.get("completed")

                if completed or status == "completed":
                    print(f"✅ Call completed for {full_name}, analyzing...")
                    
                    # Get enhanced analysis
                    analysis_result = analyze_call_interest(call_id, full_name)
                    
                    # Extract additional details
                    call_summary = result.get('summary', 'No summary available')
                    transcript = result.get('transcript', 'No transcript available')
                    
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': status,
                        'duration': result.get('corrected_duration', result.get('duration', 'Unknown')),
                        'call_intent': analysis_result['call_intent'],
                        'is_interested': analysis_result['is_interested'],
                        'concerns': analysis_result['concerns'],
                        'reason': analysis_result['reason'],
                        'showed_engagement': analysis_result['showed_engagement'],
                        'raw_intent': analysis_result['raw_intent'],
                        'summary': call_summary,
                        'transcript': transcript,
                        'created_at': result.get('created_at'),
                        'answered': result.get('answered', False),
                        'queue_time': result.get('queue_time'),
                        'ring_time': result.get('ring_time')
                    }
                elif status == "failed" or status == "error":
                    print(f"❌ Call failed for {full_name}: {status}")
                    call_summary = result.get('summary', 'Call failed - no summary available')
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': status,
                        'call_intent': 'no',  # Map failed/error to 'no'
                        'summary': call_summary,
                        'transcript': result.get('transcript', 'No transcript available'),
                        'error': result.get('error_message', 'Call failed'),
                        'answered': result.get('answered', False),
                        'duration': result.get('corrected_duration', result.get('duration', 'Unknown'))
                    }
                else:
                    print(f"⏳ Call in progress for {full_name}: {status}")
            else:
                print(f"❌ API error for {full_name}: {response.status_code}")
                call_data = get_call_summary_directly(call_id, full_name)
                if call_data:
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': call_data.get('status', 'error'),
                        'call_intent': 'no',  # Map API error to 'no'
                        'summary': call_data.get('summary', 'API error - no summary available'),
                        'transcript': call_data.get('transcript', 'No transcript available'),
                        'error': f"API error: {response.status_code}",
                        'answered': call_data.get('answered', False),
                        'duration': call_data.get('duration', 'Unknown')
                    }
                else:
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': 'error',
                        'call_intent': 'no',  # Map API error to 'no'
                        'summary': 'API error - no summary available',
                        'error': f"API error: {response.status_code}"
                    }

        except requests.exceptions.Timeout as e:
            print(f"⚠️ Timeout error for {full_name} (attempt {attempt + 1}/{max_attempts}): {str(e)}")
            if attempt == max_attempts - 1:
                print(f"🔍 Final attempt - trying to get summary directly for {full_name}")
                call_data = get_call_summary_directly(call_id, full_name)
                if call_data:
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': call_data.get('status', 'timeout'),
                        'call_intent': 'no',  # Map timeout to 'no'
                        'summary': call_data.get('summary', 'Timeout - but summary retrieved'),
                        'transcript': call_data.get('transcript', 'No transcript available'),
                        'error': 'Timeout during polling but call may have completed',
                        'answered': call_data.get('answered', False),
                        'duration': call_data.get('duration', 'Unknown')
                    }
                else:
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': 'timeout',
                        'call_intent': 'no',  # Map timeout to 'no'
                        'summary': 'Timeout - unable to retrieve summary',
                        'error': 'Timeout during polling'
                    }
        except requests.exceptions.RequestException as e:
            print(f"⚠️ Network error for {full_name} (attempt {attempt + 1}/{max_attempts}): {str(e)}")
            if attempt == max_attempts - 1:
                call_data = get_call_summary_directly(call_id, full_name)
                if call_data:
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': call_data.get('status', 'error'),
                        'call_intent': 'no',  # Map network error to 'no'
                        'summary': call_data.get('summary', 'Network error but summary retrieved'),
                        'transcript': call_data.get('transcript', 'No transcript available'),
                        'error': f'Network error: {str(e)}',
                        'answered': call_data.get('answered', False),
                        'duration': call_data.get('duration', 'Unknown')
                    }
                else:
                    return {
                        'name': full_name,
                        'call_id': call_id,
                        'status': 'error',
                        'call_intent': 'no',  # Map network error to 'no'
                        'summary': 'Network error - unable to retrieve summary',
                        'error': str(e)
                    }

        attempt += 1
        time.sleep(10)

    print(f"❌ Max attempts reached for {full_name} - attempting final summary fetch")
    call_data = get_call_summary_directly(call_id, full_name)
    if call_data:
        return {
            'name': full_name,
            'call_id': call_id,
            'status': call_data.get('status', 'timeout'),
            'call_intent': 'no',  # Map max attempts to 'no'
            'summary': call_data.get('summary', 'Max attempts reached but summary retrieved'),
            'transcript': call_data.get('transcript', 'No transcript available'),
            'error': 'Max polling attempts reached but call may have completed',
            'answered': call_data.get('answered', False),
            'duration': call_data.get('duration', 'Unknown')
        }
    else:
        return {
            'name': full_name,
            'call_id': call_id,
            'status': 'timeout',
            'call_intent': 'no',  # Map max attempts to 'no'
            'summary': 'Max attempts reached - unable to retrieve summary',
            'error': 'Max polling attempts reached'
        }

def make_call(full_name, job_details, phone_number, results_list=None):
    """Makes a call and returns call analysis results"""
    job_title = job_details.get('job_title', 'Unknown Position')
    location = job_details.get('location', 'Unknown Location')
    pay = job_details.get('pay', 'Competitive Pay')
    
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
                
                if results_list is not None:
                    if call_result:
                        results_list.append(call_result)
                    else:
                        results_list.append({
                            'name': full_name,
                            'call_id': call_id,
                            'status': 'unknown',
                            'call_intent': 'no',  # Map to 'no' for failed fetch
                            'summary': 'Failed to fetch call details',
                            'error': 'Failed to fetch call details'
                        })
                
                return call_result
            else:
                print(f"❌ Failed to get call ID for {full_name}")
                error_result = {
                    'name': full_name,
                    'call_id': 'N/A',
                    'status': 'failed',
                    'call_intent': 'no',  # Map to 'no' for no call ID
                    'summary': 'Failed to initiate call - no call ID returned',
                    'error': 'No call ID returned'
                }
                if results_list is not None:
                    results_list.append(error_result)
                return error_result
        else:
            print(f"❌ Call failed for {full_name}. Status: {response.status_code}")
            print(f"Response: {result}")
            error_result = {
                'name': full_name,
                'call_id': 'N/A',
                'status': 'failed',
                'call_intent': 'no',  # Map to 'no' for failed call
                'summary': f'Call initiation failed with status {response.status_code}',
                'error': f'HTTP {response.status_code}: {result}'
            }
            if results_list is not None:
                results_list.append(error_result)
            return error_result

    except Exception as e:
        print(f"❌ Error making call for {full_name}: {str(e)}")
        error_result = {
            'name': full_name,
            'call_id': 'N/A',
            'status': 'error',
            'call_intent': 'no',  # Map to 'no' for exceptions
            'summary': f'Exception during call initiation: {str(e)}',
            'error': str(e)
        }
        if results_list is not None:
            results_list.append(error_result)
        return error_result

def print_call_summary(call_results):
    """Prints a detailed summary of all call results"""
    if not call_results:
        print("\n❌ No call results to summarize.")
        return
    
    print("\n" + "="*100)
    print("📊 DETAILED CALL SUMMARY")
    print("="*100)
    
    yes_count = 0
    no_count = 0
    later_count = 0
    
    for result in call_results:
        # Safely get all values with proper None checks
        name = result.get('name', 'Unknown') if result else 'Unknown'
        call_intent = result.get('call_intent', 'no') if result else 'no'
        duration = result.get('duration', 'Unknown') if result else 'Unknown'
        call_id = result.get('call_id', 'Unknown') if result else 'Unknown'
        summary = result.get('summary', 'No summary available') if result else 'No summary available'
        concerns = result.get('concerns', 'None') if result else 'None'
        reason = result.get('reason', 'Not specified') if result else 'Not specified'
        answered = result.get('answered', 'Unknown') if result else 'Unknown'
        showed_engagement = result.get('showed_engagement', 'Unknown') if result else 'Unknown'
        
        # Ensure string conversion for safety
        concerns = str(concerns) if concerns is not None else 'None'
        reason = str(reason) if reason is not None else 'Not specified'
        summary = str(summary) if summary is not None else 'No summary available'
        
        # Count by intent
        if call_intent == 'yes':
            yes_count += 1
            status_emoji = "✅"
        elif call_intent == 'later':
            later_count += 1
            status_emoji = "⏳"
        else:  # 'no' or any unexpected value
            no_count += 1
            status_emoji = "❌"
        
        print(f"{status_emoji} {name}")
        print(f"   📞 Call ID: {call_id}")
        print(f"   🎯 Intent: {call_intent.upper()}")
        print(f"   ⏱️ Duration: {duration}s")
        print(f"   📞 Answered: {answered}")
        print(f"   🤝 Showed Engagement: {showed_engagement}")
        
        if call_intent in ['yes', 'later']:
            print(f"   💭 Reason: {reason}")
            print(f"   ⚠️ Concerns: {concerns}")
        
        print(f"   📝 Summary: {summary}")
        
        if result and result.get('error'):
            print(f"   ❌ Error: {result.get('error')}")
            
        print("-" * 100)
    
    print(f"\n📊 STATISTICS:")
    print(f"✅ Yes/Interested: {yes_count}")
    print(f"❌ No/Not Interested: {no_count}")
    print(f"⏳ Later/Callback Requested: {later_count}")
    print(f"📞 Total Calls: {len(call_results)}")
    
    if len(call_results) > 0:
        success_rate = (yes_count / len(call_results)) * 100
        callback_rate = (later_count / len(call_results)) * 100
        engagement_rate = ((yes_count + later_count) / len(call_results)) * 100
        print(f"📊 Positive Interest Rate: {success_rate:.1f}%")
        print(f"📊 Callback Request Rate: {callback_rate:.1f}%")
        print(f"📊 Overall Engagement Rate: {engagement_rate:.1f}%")
    
    print("="*100)  

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