import os
import logging
import msal
import requests
from schema import EmailSchema, JobApplication

logger = logging.getLogger(__name__)


def send_job_application_email(email: EmailSchema) -> dict:
    # Outlook/Microsoft Graph API configuration
    CLIENT_ID = os.environ.get("CLIENT_ID")
    CLIENT_SECRET = os.environ.get("CLIENT_SECRET")
    TENANT_ID = os.environ.get("TENANT_ID")
    
    sender_email = os.environ.get("FROM_EMAIL", "aiteam@carefast.ai")
    receiver_email = "meet.radadiya@bacancy.com"

    print(f"Sending email from: {sender_email}")
    print(f"Sending email to: {receiver_email}")

    if not all([CLIENT_ID, CLIENT_SECRET, TENANT_ID]):
        return {"error": "Email configuration is incomplete"}

    try:
        # Acquire access token
        AUTHORITY = f"https://login.microsoftonline.com/{TENANT_ID}"
        SCOPE = ["https://graph.microsoft.com/.default"]
        
        app = msal.ConfidentialClientApplication(
            client_id=CLIENT_ID,
            client_credential=CLIENT_SECRET,
            authority=AUTHORITY
        )
        result = app.acquire_token_for_client(scopes=SCOPE)
        
        if "access_token" not in result:
            return {"error": f"Token acquisition failed: {result.get('error_description')}"}
        
        access_token = result["access_token"]

        # Prepare email content
        body = f"""
Dear Hiring Manager,

A new job application has been received through the Nursefast.ai platform.

═══════════════════════════════════════════════════════════════════
                        APPLICANT INFORMATION
═══════════════════════════════════════════════════════════════════

👤 Full Name: {email.full_name}
📱 Phone Number: {email.phone_number}

═══════════════════════════════════════════════════════════════════
                        APPLICATION DETAILS
═══════════════════════════════════════════════════════════════════

🏥 Position Applied For: {email.job_title}
💰 Expected Salary: {email.pay}
📍 Preferred Location: {email.location}
⏰ Work Experience: {email.work_experience}
📞 Call ID: {email.call_id}
🎯 Candidate Intent: {email.intent}

═══════════════════════════════════════════════════════════════════

Best regards,
Nursefast.ai Team
Automated Application System
        """

        # Send email using Microsoft Graph API
        endpoint = f"https://graph.microsoft.com/v1.0/users/{sender_email}/sendMail"
        payload = {
            "message": {
                "subject": f"New Job Application: {email.job_title}",
                "body": {
                    "contentType": "Text",
                    "content": body
                },
                "toRecipients": [{"emailAddress": {"address": receiver_email}}]
            },
            "saveToSentItems": True
        }
        
        headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        
        response = requests.post(endpoint, headers=headers, json=payload)
        print(f"Graph API response: status={response.status_code}, text={response.text}")
        
        if response.status_code == 202:
            return {"message": "Email sent successfully", "response": response.text}
        else:
            return {"error": f"Failed to send email: {response.status_code} - {response.text}", "response": response.text}

    except Exception as e:
        return {"error": str(e)}


def process_job_application(application: JobApplication) -> dict:
    BLAND_API_KEY = os.environ.get("BLAND_API_KEY")
    PATHWAY_ID = os.environ.get("PATHWAY_ID")

    if not BLAND_API_KEY or not PATHWAY_ID:
        raise ValueError("Missing BLAND_API_KEY or PATHWAY_ID")

    url = "https://api.bland.ai/v1/calls"
    data = {
        "phone_number": application.phone_number,
        "pathway_id": PATHWAY_ID,
        "voice": "85a2c852-2238-4651-acf0-e5cbe02f6f2",
        "task": "test_bland_ai_call",
        "wait_for_greeting": True,
        "request_data": {
            "full_name": application.full_name,
            "phone_number": application.phone_number,
            "job_title": application.job_title,
            "location": application.location,
            "pay": application.pay,
            "work_experience": application.work_experience,
            "user_name": application.full_name,
        }
    }

    headers = {
        "Authorization": f"Bearer {BLAND_API_KEY}",
        "Content-Type": "application/json"
    }

    response = requests.post(url, json=data, headers=headers)

    if response.status_code in (200, 202):
        return {"message": "Call initiated", "response": response.json()}
    else:
        logger.error(f"Call failed: {response.status_code} - {response.text}")
        raise Exception(f"Call failed with status {response.status_code}")