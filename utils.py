import os
import logging
import msal
import requests
from schema import EmailSchema, JobApplication
import re
import json
import time
import base64
from typing import Dict, Any
from PIL import Image
from io import BytesIO
import fitz  # PyMuPDF
from langchain.schema import HumanMessage, SystemMessage
from config import SYSTEM_PROMPT
from langchain_openai import ChatOpenAI
from dotenv import load_dotenv

load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

logger = logging.getLogger(__name__)

llm = ChatOpenAI(
    api_key=OPENAI_API_KEY,
    temperature=0.7,
    model="gpt-4o-mini"
)
logger.info("LLM initialized with model: gpt-4o-mini")



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
    
def extract_json_from_content(content: str) -> Dict[str, Any]:
    """Extract JSON object from content string that might contain markdown code blocks."""
    logger.info("Extracting JSON from content")

    # Try to find JSON content in markdown code blocks first
    json_pattern = r"```(?:json)?\s*([\s\S]*?)```"
    matches = re.findall(json_pattern, content)

    if matches:
        logger.info(f"Found {len(matches)} potential JSON blocks in markdown")
        try:
            # Try the first JSON block found
            result = json.loads(matches[0])
            logger.info("Successfully parsed JSON from markdown code block")
            return result
        except json.JSONDecodeError as e:
            logger.warning(f"Failed to parse JSON from markdown block: {e}")

    # If no JSON blocks or they weren't valid, try to parse the entire content
    try:
        logger.info("Attempting to parse entire content as JSON")
        return json.loads(content)
    except json.JSONDecodeError as e:
        logger.warning(f"Failed to parse entire content as JSON: {e}")

        # If all fails, return the original content wrapped in a dict
        logger.info("Returning raw content wrapped in dictionary")
        return {"raw_content": content}

def process_pdf(pdf_path):
    logger.info(f"Processing PDF: {pdf_path}")
    start_time = time.time()

    try:
        # Open the PDF
        pdf_document = fitz.open(pdf_path)
        if len(pdf_document) == 0:
            logger.error("PDF contains no pages")
            return "Error: PDF contains no pages."

        # Process pages
        page_count = len(pdf_document)
        logger.info(f"Processing {page_count} pages")

        all_page_images = []
        for page_num in range(page_count):
            page = pdf_document[page_num]
            pix = page.get_pixmap(matrix=fitz.Matrix(1.5, 1.5))
            base64_image = base64.b64encode(pix.tobytes()).decode("utf-8")

            all_page_images.append({
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/png;base64,{base64_image}",
                    "detail": "auto"
                }
            })

            # Log progress for every 5 pages or the last page
            if page_num % 5 == 0 or page_num == page_count - 1:
                logger.info(f"Processed page {page_num + 1}/{page_count}")

        pdf_document.close()
        logger.info("PDF document processed and closed")

        # Create message and send to LLM
        logger.info("Sending request to OpenAI API")
        api_start_time = time.time()

        messages = [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=all_page_images)
        ]

        response = llm.invoke(messages, timeout=120)

        api_duration = time.time() - api_start_time
        logger.info(f"OpenAI API response received in {api_duration:.2f} seconds")

        # Validate response
        if not response or not hasattr(response, 'content'):
            logger.error("Invalid response from LLM")
            return "Error: Invalid response from LLM."
        if not response.content:
            logger.error("Empty response from LLM")
            return "Error: Empty response from LLM."
        if not isinstance(response.content, str):
            logger.error("Response is not a string")
            return "Error: Response is not a string."

        process_duration = time.time() - start_time
        logger.info(f"PDF processing completed in {process_duration:.2f} seconds")
        return response.content

    except Exception as e:
        logger.exception(f"Error processing PDF: {str(e)}")
        return f"Error processing PDF: {str(e)}"

def process_docx(docx_path):
    logger.info(f"Processing DOCX: {docx_path}")
    start_time = time.time()

    try:
        # Load DOCX
        doc = docx.Document(docx_path)
        logger.info("DOCX file loaded")

        all_messages = []

        # Extract text
        full_text = "\n".join([para.text for para in doc.paragraphs if para.text.strip()])
        if full_text:
            all_messages.append({
                "type": "text",
                "text": full_text
            })
            logger.info(f"Extracted text length: {len(full_text)} characters")
        else:
            logger.warning("No text found in DOCX")

        # Extract and encode images
        image_count = 0
        for rel in doc.part._rels:
            rel_obj = doc.part._rels[rel]
            if "image" in rel_obj.target_ref:
                image_data = rel_obj.target_part.blob
                image = Image.open(BytesIO(image_data))

                # Resize image if too large (optional)
                max_dim = 1024
                if image.width > max_dim or image.height > max_dim:
                    image.thumbnail((max_dim, max_dim))

                # Convert to base64
                buffer = BytesIO()
                image.save(buffer, format="PNG")
                encoded_image = base64.b64encode(buffer.getvalue()).decode("utf-8")

                all_messages.append({
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/png;base64,{encoded_image}",
                        "detail": "auto"
                    }
                })

                image_count += 1

        logger.info(f"Extracted {image_count} images from DOCX")

        if not all_messages:
            logger.warning("No content extracted from DOCX")
            return "Error: No content found in DOCX."

        # Create message and send to LLM
        logger.info("Sending request to OpenAI API")
        api_start_time = time.time()

        messages = [
            SystemMessage(content=SYSTEM_PROMPT),
            HumanMessage(content=all_messages)
        ]

        response = llm.invoke(messages, timeout=120)

        api_duration = time.time() - api_start_time
        logger.info(f"OpenAI API response received in {api_duration:.2f} seconds")

        # Validate response
        if not response or not hasattr(response, 'content'):
            logger.error("Invalid response from LLM")
            return "Error: Invalid response from LLM."
        if not response.content:
            logger.error("Empty response from LLM")
            return "Error: Empty response from LLM."
        if not isinstance(response.content, str):
            logger.error("Response is not a string")
            return "Error: Response is not a string."

        process_duration = time.time() - start_time
        logger.info(f"DOCX processing completed in {process_duration:.2f} seconds")
        return response.content

    except Exception as e:
        logger.exception(f"Error processing DOCX: {str(e)}")
        return f"Error processing DOCX: {str(e)}"

def normalize_keys(content_json):
    """Map old keys to new keys for CSV and display."""
    key_map = {
        "phone": "phone_number",
        "name": "full_name",
        "job title": "job_title",
        "job_title": "job_title",
        "location": "location"
    }
    new_json = {}
    for k, v in content_json.items():
        new_key = key_map.get(k.strip().lower(), k)
        new_json[new_key] = v
    return new_json

def process_single_file(file_path: str, filename: str) -> Dict[str, Any]:
    """Process a single file and return structured result"""
    logger.info(f"Processing single file: {filename}")

    try:
        # Determine file type and process accordingly
        if filename.lower().endswith('.pdf'):
            result = process_pdf(file_path)
        elif filename.lower().endswith('.docx'):
            result = process_docx(file_path)
        else:
            return {
                "filename": filename,
                "status": "error",
                "content": {},
                "error": f"Unsupported file type: {filename}"
            }

        # Check for processing errors
        if isinstance(result, str) and result.startswith("Error"):
            return {
                "filename": filename,
                "status": "error",
                "content": {},
                "error": result
            }

        # Extract JSON from result
        content_json = extract_json_from_content(result)
        # Normalize total_work_experience
        if "total_work_experience" in content_json:
            experience_value = str(content_json["total_work_experience"]).strip()
            if experience_value and not experience_value.lower().endswith("years"):
                content_json["total_work_experience"] = f"{experience_value} years"
        # Normalize keys
        content_json = normalize_keys(content_json)
        return {
            "filename": filename,
            "status": "success",
            "content": content_json,
            "error": None
        }

    except Exception as e:
        logger.exception(f"Error processing file {filename}: {str(e)}")
        return {
            "filename": filename,
            "status": "error",
            "content": {},
            "error": str(e)
        }