# main.py

from fastapi import FastAPI
from pydantic import BaseModel
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import os
from dotenv import load_dotenv
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

app = FastAPI()

class EmailSchema(BaseModel):
    subject: str
    body: str

# Home route
@app.get("/receive_call")
def read_root():
    return {"message": "Welcome to FastAPI!"}

@app.post("/send_email")
async def send_email(email: EmailSchema):
    try:
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
        message["Subject"] = email.subject
        
        # Add body to email
        message.attach(MIMEText(email.body, "plain"))
        
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
