import os
import logging
import smtplib
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from schema import EmailSchema, JobApplication

logger = logging.getLogger(__name__)


def send_job_application_email(email: EmailSchema) -> dict:
    sender_email = os.environ.get("EMAIL_USER")
    receiver_email = "admin@carefast.ai"
    email_password = os.environ.get("EMAIL_PASSWORD")

    if not all([sender_email, email_password]):
        logger.error("Missing EMAIL_USER or EMAIL_PASSWORD in environment")
        return {"error": "Email configuration is incomplete"}

    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = f"New Job Application: {email.job_title}"

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
    message.attach(MIMEText(body, "plain"))

    try:
        with smtplib.SMTP(os.environ.get("EMAIL_HOST", "smtp.gmail.com"),
                          int(os.environ.get("EMAIL_PORT", 587))) as server:
            server.starttls()
            server.login(sender_email, email_password)
            server.send_message(message)

        logger.info(f"Email sent successfully for call ID: {email.call_id}")
        return {"message": "Email sent successfully"}
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
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
        "voice": "85a2c852-2238-4651-acf0-e5cbe02186f2",
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
