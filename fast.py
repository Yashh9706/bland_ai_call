# main.py

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel, field_validator
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv
import logging
import json

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

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

# Home route
@app.get("/receive_call")
def read_root():
    return {"message": "Welcome to FastAPI!"}

@app.post("/send_email")
async def send_email(request: Request, email: EmailSchema):
    try:
        # Log raw request data for debugging
        body = await request.body()
        logger.info(f"Raw request body: {body.decode()}")
        # Log the parsed data for debugging
        logger.info("Received data:")
        logger.info(json.dumps({
            "job_title": email.job_title,
            "pay": email.pay,
            "location": email.location,
            "call_id": email.call_id,
            "intent": email.intent,
            "work_experience": email.work_experience
        }, indent=2))
        # Log environment variables (without password)
        logger.info(f"Sender email: {os.getenv('EMAIL_USER')}")
        logger.info(f"SMTP Host: {os.getenv('EMAIL_HOST')}")
        logger.info(f"SMTP Port: {os.getenv('EMAIL_PORT')}")
        
        # Email configuration
        sender_email = os.getenv("EMAIL_USER")
        receiver_email = "meet.radadiya@bacancy.com"
        email_password = os.getenv("EMAIL_PASSWORD")
        
        if not all([sender_email, email_password]):
            logger.error("Missing email configuration")
            return {"error": "Email configuration is incomplete"}
        
        # Create the email message
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Subject"] = email.job_title
        
        # Construct and add email body
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
        
        logger.info("Attempting to connect to SMTP server...")
        # Create SMTP session
        with smtplib.SMTP(os.getenv("EMAIL_HOST", "smtp.gmail.com"), int(os.getenv("EMAIL_PORT", "587"))) as server:
            logger.info("Connected to SMTP server")
            server.starttls()  # Enable TLS
            logger.info("TLS enabled")
            server.login(sender_email, email_password)
            logger.info("Login successful")
            text = message.as_string()
            server.send_message(message)
            logger.info("Email sent successfully")
        return {"message": "Email sent successfully"}
    except Exception as e:
        logger.error(f"Error sending email: {str(e)}")
        return {"error": str(e)}
    
@app.post("/not_interested")
async def not_interested(email: EmailSchema):
    logger.info(f"User is not interested in the job: {email.job_title, email.pay, email.location,email.call_id}")
    logger.info(email.intent)
    logger.info(email.work_experience)
    return {"message": "User is not interested in the job."}

@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    # Log the raw request body
    body = await request.body()
    logger.error(f"Validation error on request body: {body.decode()}")
    logger.error(f"Validation errors: {exc.errors()}")
    return JSONResponse(
        status_code=422,
        content={"detail": exc.errors()}
    )