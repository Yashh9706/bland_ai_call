# main.py

from fastapi import FastAPI, Request
from pydantic import BaseModel, field_validator
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

class EmailSchema(BaseModel):
    job_title: str
    pay: str
    location: str
    call_id: str
    intent: str
    work_experience: str
    
    @field_validator('*')
    @classmethod
    def check_empty_strings(cls, v: str) -> str:
        if not isinstance(v, str):
            raise ValueError('Field must be a string')
        if not v.strip():
            raise ValueError('Field cannot be empty')
        return v

@app.post("/send_email")
async def send_email(email: EmailSchema):
    try:
        sender_email = os.getenv("EMAIL_USER")
        receiver_email = "meet.radadiya@bacancy.com"
        email_password = os.getenv("EMAIL_PASSWORD")
        
        if not all([sender_email, email_password]):
            return {"error": "Email configuration is incomplete"}
        
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Subject"] = email.job_title
        
        body = f"""
Job Details:
------------
Job Title: {email.job_title}
Pay: {email.pay}
Location: {email.location}
Call ID: {email.call_id}
Work Experience: {email.work_experience}
Intent: {email.intent}
"""
        message.attach(MIMEText(body, "plain"))
        
        with smtplib.SMTP(os.getenv("EMAIL_HOST", "smtp.gmail.com"), int(os.getenv("EMAIL_PORT", "587"))) as server:
            server.starttls()
            server.login(sender_email, email_password)
            server.send_message(message)
            
        return {"message": "Email sent successfully"}
    except Exception as e:
        return {"error": str(e)}
    
@app.post("/not_interested")
async def not_interested(email: EmailSchema):
    print(f"User is not interested in the job: {email.job_title}, call ID: {email.call_id}, location: {email.location}, pay: {email.pay}, intent: {email.intent}, work experience: {email.work_experience}")
    return {"message": "User is not interested in the job."}

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors()}
    )