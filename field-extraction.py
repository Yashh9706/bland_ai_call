import tempfile
import os
import shutil
import json
import docx
import re
import logging
import time
import base64
from PIL import Image
from io import BytesIO
import pymupdf as fitz 
from datetime import datetime
from config import SYSTEM_PROMPT
from dotenv import load_dotenv
from pydantic import BaseModel
from typing import Optional, Dict, Any

# Import your existing processing code components
from langchain.schema import HumanMessage, SystemMessage
from langchain_openai import ChatOpenAI
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware

# Create logs directory if it doesn't exist
logs_dir = "logs"
if not os.path.exists(logs_dir):
    os.makedirs(logs_dir)
    print(f"Created logs directory: {logs_dir}")

# Set up logging with date-based log files
current_date = datetime.now().strftime("%Y-%m-%d")
log_filename = f"{logs_dir}/{current_date}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("pdf-processor")
logger.info(f"Logging initialized with log file: {log_filename}")

# Load environment variables
load_dotenv()
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
logger.info("Environment variables loaded")

logger.info("System prompt loaded from config")

# Initialize FastAPI app
app = FastAPI(
    title="PDF Processing API",
    description="API to process PDFs using LLM",
    version="1.0.0"
)
logger.info("FastAPI application initialized")

# Initialize the LLM
llm = ChatOpenAI(
    api_key=OPENAI_API_KEY,
    temperature=0.7,
    model="gpt-4o-mini"
)
logger.info("LLM initialized with model: gpt-4o-mini")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Or specify your frontend's URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Response model
class ProcessResponse(BaseModel):
    unique_id: str
    status: str
    content: Dict[str, Any]
    error: Optional[str] = None

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
    
def upsert_to_postgres(data: dict, table_name="person_details_CV"):
    import os
    import psycopg2
    from psycopg2 import sql

    db_config = {
        'dbname': os.environ['PGDATABASE'],
        'user': os.environ['PGUSER'],
        'password': os.environ['PGPASSWORD'],
        'host': os.environ['PGHOST'],
        'port': os.environ['PGPORT'],
        'sslmode': 'require'
    }

    try:
        conn = psycopg2.connect(**db_config)
        cur = conn.cursor()

        # Ensure table exists
        columns = list(data.keys())
        create_table_query = sql.SQL("""
            CREATE TABLE IF NOT EXISTS {table} (
                id SERIAL PRIMARY KEY,
                {fields}
            )
        """).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(
                sql.SQL(f"{col} TEXT") for col in columns
            )
        )
        cur.execute(create_table_query)

        # Ensure all columns exist
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name = %s", (table_name,))
        existing_cols = set(row[0] for row in cur.fetchall())

        for col in columns:
            if col not in existing_cols:
                cur.execute(sql.SQL("ALTER TABLE {table} ADD COLUMN {col} TEXT").format(
                    table=sql.Identifier(table_name),
                    col=sql.Identifier(col)
                ))

        # Insert data
        insert_query = sql.SQL("""
            INSERT INTO {table} ({fields}) VALUES ({placeholders})
        """).format(
            table=sql.Identifier(table_name),
            fields=sql.SQL(', ').join(map(sql.Identifier, columns)),
            placeholders=sql.SQL(', ').join(sql.Placeholder() * len(columns))
        )
        cur.execute(insert_query, list(data.values()))

        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"Data inserted into {table_name}")

    except Exception as e:
        logger.exception(f"Error inserting into PostgreSQL: {e}")

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
    
async def process_pdf_endpoint(
    file: UploadFile = File(...)
):
    logger.info(f"Received request for file: {file.filename}")
    
    # Validate file
    if not file.filename.lower().endswith(('.pdf', '.docx')):

        logger.warning(f"Invalid file type: {file.filename}")
        raise HTTPException(status_code=400, detail="File must be a PDF")
    
    # Create temporary file for PDF
    temp_path = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as temp_file:
            shutil.copyfileobj(file.file, temp_file)
            temp_path = temp_file.name
        logger.info(f"PDF saved to temporary file: {temp_path}")
        
        # Process the PDF
        start_time = time.time()
        if file.filename.lower().endswith('.pdf'):
            logger.info("Processing PDF file")
            result = process_pdf(temp_path)
        elif file.filename.lower().endswith('.docx'):
            logger.info("Processing DOCX file")
            result = process_docx(temp_path)
        else:
            logger.warning(f"Unsupported file type: {file.filename}")
            raise HTTPException(status_code=400, detail="Unsupported file type")
        logger.info(f"Processing completed for file: {file.filename}")
        process_duration = time.time() - start_time
        logger.info(f"Total processing time: {process_duration:.2f} seconds")
        print(f"Processing result: {result}")
        # Check for error
        if isinstance(result, str) and result.startswith("Error"):
            logger.error(f"Processing error: {result}")
            return JSONResponse(content={
                "status": "error",
                "content": {},
                "error": result
            })
        
        # Extract JSON from the result
        content_json = extract_json_from_content(result)

        # Normalize total_work_experience
        if "total_work_experience" in content_json:
            experience_value = str(content_json["total_work_experience"]).strip()
            if experience_value and not experience_value.lower().endswith("years"):
                content_json["total_work_experience"] = f"{experience_value} years"

        # Save to PostgreSQL
        # upsert_to_postgres(content_json)

        logger.info(f"Content extracted and ready to return to frontend")
        # Prepare response
        response = {
            "content": content_json,
            "error": None
        }
        
        logger.info(f"Completed successfully")
        return JSONResponse(content=response)
    
    except Exception as e:
        logger.exception(f"Unhandled exception: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "content": {},
                "error": str(e)
            }
        )
    finally:
        # Clean up the temporary file
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)
            logger.info("Temporary file removed")

# Also add an endpoint for resume processing specifically
@app.post("/process-resume/", response_model=ProcessResponse)
async def process_resume_endpoint(
    file: UploadFile = File(...)
):
    # This is just an alias for process-pdf to maintain backward compatibility
    logger.info(f"Resume processing request received")
    return await process_pdf_endpoint(file=file)

# Root endpoint
@app.get("/")
async def root():
    return {"message": "PDF Processing API is running. Use /process-resume/ endpoint to process PDFs."}

# Middleware for request logging (simplified)
@app.middleware("http")
async def log_requests(request, call_next):
    path = request.url.path
    method = request.method
    
    start_time = time.time()
    logger.info(f"Request started: {method} {path}")
    
    response = await call_next(request)
    
    process_time = time.time() - start_time
    logger.info(f"Request completed: {method} {path} - Status: {response.status_code} - Duration: {process_time:.2f}s")
    
    return response

# Function to create a new log file handler when the date changes
def get_log_handler():
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"{logs_dir}/resume_processor_{current_date}.log"
    
    # Remove existing FileHandlers
    for handler in logger.handlers[:]:
        if isinstance(handler, logging.FileHandler):
            logger.removeHandler(handler)
    
    # Add new FileHandler with updated filename
    file_handler = logging.FileHandler(log_filename)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(file_handler)
    logger.info(f"Log file rotated to: {log_filename}")
    return file_handler

# Add middleware to check if log file needs to be rotated
@app.middleware("http")
async def check_log_rotation(request, call_next):
    # Get current date and check if log file needs to be rotated
    current_date = datetime.now().strftime("%Y-%m-%d")
    log_filename = f"{logs_dir}/resume_processor_{current_date}.log"
    
    # If log file doesn't exist for current date, rotate logs
    if not os.path.exists(log_filename):
        get_log_handler()
    
    response = await call_next(request)
    return response

if __name__ == "__main__":
    import uvicorn
    logger.info("Starting FastAPI server")
    uvicorn.run(app, host="0.0.0.0", port=8000)