from fastapi import FastAPI, Request, HTTPException
from pydantic import BaseModel, field_validator
from fastapi.responses import JSONResponse, HTMLResponse, FileResponse
from fastapi.exceptions import RequestValidationError
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv
import logging
import requests
from typing import Optional

# Load environment variables
load_dotenv(override=True)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Job Application API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class EmailSchema(BaseModel):
    full_name: str
    phone_number: str
    job_title: str
    pay: str
    location: str
    call_id: str
    intent: str
    work_experience: str

@app.post("/send_email")
async def send_email(email: EmailSchema):
    try:
        sender_email = os.getenv("EMAIL_USER")
        receiver_email = "meet.radadiya@bacancy.com"
        email_password = os.getenv("EMAIL_PASSWORD")
        
        if not all([sender_email, email_password]):
            logger.error("Email configuration is incomplete - missing EMAIL_USER or EMAIL_PASSWORD")
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

        This application was submitted through our automated system and the candidate 
        has expressed interest in the nursing position. Please review the details above 
        and follow up accordingly.

        Best regards,
        Nursefast.ai Team
        Automated Application System
        """
        message.attach(MIMEText(body, "plain"))

        logger.info(
            "Attempting to send email for job application:\n"
            f"  Job Title: {email.job_title}\n"
            f"  Call ID: {email.call_id}\n"
            f"  Location: {email.location}\n"
            f"  Recipient: {receiver_email}"
        )

        with smtplib.SMTP(os.getenv("EMAIL_HOST", "smtp.gmail.com"), int(os.getenv("EMAIL_PORT", "587"))) as server:
            server.starttls()
            server.login(sender_email, email_password)
            server.send_message(message)
            
        logger.info(
            "Email sent successfully:\n"
            f"  Job Title: {email.job_title}\n"
            f"  Call ID: {email.call_id}\n"
            f"  Sent to: {receiver_email}"
        )
        return {"message": "Email sent successfully"}
    except Exception as e:
        logger.error(
            "Failed to send email:\n"
            f"  Job Title: {email.job_title}\n"
            f"  Call ID: {email.call_id}\n"
            f"  Error: {str(e)}"
        )
        return {"error": str(e)}
    
@app.post("/not_interested")
async def not_interested(email: EmailSchema):
    logger.info(
        "User indicated not interested in job opportunity:\n"
        f"  Job Title: {email.job_title}\n"
        f"  Call ID: {email.call_id}\n"
        f"  Location: {email.location}\n"
        f"  Pay: {email.pay}\n"
        f"  Intent: {email.intent}\n"
        f"  Work Experience: {email.work_experience}"
    )
    return {"message": "User is not interested in the job."}

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors()}
    )

class JobApplication(BaseModel):
    full_name: str
    job_title: str
    pay: str
    location: str
    work_experience: str
    phone_number: str

def process_job_application(application_data: JobApplication) -> dict:
    """
    Process job application data and initiate Bland AI call.
    """
    BLAND_API_KEY = os.getenv("BLAND_API_KEY")
    PATHWAY_ID = os.getenv("PATHWAY_ID")

    url = "https://api.bland.ai/v1/calls"

    phone_number = application_data.phone_number
    full_name = application_data.full_name
    job_title = application_data.job_title
    location = application_data.location
    pay = application_data.pay

    data = {
        "phone_number": phone_number,
        "pathway_id": PATHWAY_ID,
        "voice": "85a2c852-2238-4651-acf0-e5cbe02186f2",
        "task": "test_bland_ai_call",
        "wait_for_greeting": True,
        "request_data": {
            "full_name": full_name,
            "phone_number": phone_number,
            "job_title": job_title,
            "location": location,
            "pay": pay,
            "work_experience": application_data.work_experience,
            "user_name": full_name
        }
    }

    headers = {
        "Authorization": f"Bearer {BLAND_API_KEY}",
        "Content-Type": "application/json"
    }

    logger.info(
        "Initiating Bland AI call for job application:\n"
        f"  Full Name: {full_name}\n"
        f"  Phone Number: {phone_number}\n"
        f"  Job Title: {job_title}\n"
        f"  Location: {location}\n"
        f"  Pay: {pay}\n"
        f"  Pathway ID: {PATHWAY_ID}"
    )
    
    response = requests.post(url, json=data, headers=headers)

    if response.status_code == 200:
        logger.info(
            "Bland AI call initiated successfully:\n"
            f"  Status Code: {response.status_code}\n"
            f"  Full Name: {full_name}\n"
            f"  Phone Number: {phone_number}\n"
            f"  Response: {response.json()}"
        )
    elif response.status_code == 202:
        logger.info(
            "Bland AI call request accepted:\n"
            f"  Status Code: {response.status_code}\n"
            f"  Full Name: {full_name}\n"
            f"  Phone Number: {phone_number}\n"
            f"  Response: {response.json()}"
        )
    else:
        logger.error(
            "Failed to initiate Bland AI call:\n"
            f"  Status Code: {response.status_code}\n"
            f"  Full Name: {full_name}\n"
            f"  Phone Number: {phone_number}\n"
            f"  Error Response: {response.text}"
        )
    return "success"

@app.get("/", response_class=HTMLResponse)
async def serve_html():
    """Serve the HTML form page"""
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        html_path = os.path.join(current_dir, "index.html")
        
        with open(html_path, "r", encoding="utf-8") as file:
            html_content = file.read()
        
        return HTMLResponse(content=html_content, status_code=200)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="HTML file not found")

@app.post("/submit-job-application")
async def submit_job_application(application: JobApplication):
    """Handle job application form submission"""
    try:
        result = process_job_application(application)
        return {
            "success": True,
            "data": result
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal server error: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, port=8000, reload=True)