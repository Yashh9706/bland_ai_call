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
from typing import Optional, Dict, Any, List

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
    description="API to process PDFs and DOCX files using LLM - supports single and multiple files",
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

# Response models
class ProcessResponse(BaseModel):
    unique_id: str
    status: str
    content: Dict[str, Any]
    error: Optional[str] = None

class MultiProcessResponse(BaseModel):
    status: str
    results: List[Dict[str, Any]]
    total_files: int
    successful_files: int
    failed_files: int
    errors: List[str] = []

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

# SINGLE FILE PROCESSING ENDPOINTS
@app.post("/process-file/")
async def process_single_file_endpoint(
    file: UploadFile = File(...)
):
    """Process a single PDF or DOCX file"""
    logger.info(f"Received single file request: {file.filename}")
    
    # Validate file type
    if not file.filename.lower().endswith(('.pdf', '.docx')):
        logger.warning(f"Invalid file type: {file.filename}")
        raise HTTPException(status_code=400, detail="File must be a PDF or DOCX")
    
    temp_path = None
    try:
        # Create temporary file
        suffix = '.pdf' if file.filename.lower().endswith('.pdf') else '.docx'
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
            shutil.copyfileobj(file.file, temp_file)
            temp_path = temp_file.name
        
        logger.info(f"File saved to temporary path: {temp_path}")
        
        # Process the file
        result = process_single_file(temp_path, file.filename)
        
        # Save to PostgreSQL if successful
        # if result["status"] == "success":
        #     upsert_to_postgres(result["content"])
        
        logger.info(f"Single file processing completed: {file.filename}")
        return JSONResponse(content=result)
    
    except Exception as e:
        logger.exception(f"Unhandled exception in single file processing: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "filename": file.filename,
                "status": "error",
                "content": {},
                "error": str(e)
            }
        )
    finally:
        # Clean up temporary file
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)
            logger.info("Temporary file cleaned up")

# MULTIPLE FILES PROCESSING ENDPOINTS
@app.post("/process-multiple-files/", response_model=MultiProcessResponse)
async def process_multiple_files_endpoint(
    files: List[UploadFile] = File(...)
):
    """Process multiple PDF and DOCX files"""
    logger.info(f"Received multiple files request: {len(files)} files")
    
    if not files:
        raise HTTPException(status_code=400, detail="No files provided")
    
    if len(files) > 10:  # Limit to prevent abuse
        raise HTTPException(status_code=400, detail="Maximum 10 files allowed per request")
    
    results = []
    errors = []
    successful_files = 0
    failed_files = 0
    temp_paths = []
    
    try:
        # Process each file
        for i, file in enumerate(files):
            logger.info(f"Processing file {i+1}/{len(files)}: {file.filename}")
            
            # Validate file type
            if not file.filename.lower().endswith(('.pdf', '.docx')):
                error_msg = f"File {file.filename} has unsupported type. Only PDF and DOCX are allowed."
                logger.warning(error_msg)
                errors.append(error_msg)
                results.append({
                    "filename": file.filename,
                    "status": "error",
                    "content": {},
                    "error": error_msg
                })
                failed_files += 1
                continue
            
            temp_path = None
            try:
                # Create temporary file
                suffix = '.pdf' if file.filename.lower().endswith('.pdf') else '.docx'
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
                    shutil.copyfileobj(file.file, temp_file)
                    temp_path = temp_file.name
                temp_paths.append(temp_path)
                
                # Process the file
                result = process_single_file(temp_path, file.filename)
                results.append(result)
                
                if result["status"] == "success":
                    successful_files += 1
                    # Save to PostgreSQL if successful
                    # upsert_to_postgres(result["content"])
                else:
                    failed_files += 1
                    if result["error"]:
                        errors.append(f"{file.filename}: {result['error']}")
                
                logger.info(f"File {i+1}/{len(files)} processed: {file.filename} - Status: {result['status']}")
                
            except Exception as e:
                error_msg = f"Error processing {file.filename}: {str(e)}"
                logger.exception(error_msg)
                errors.append(error_msg)
                results.append({
                    "filename": file.filename,
                    "status": "error",
                    "content": {},
                    "error": str(e)
                })
                failed_files += 1
        
        # Prepare final response
        response = {
            "status": "completed",
            "results": results,
            "total_files": len(files),
            "successful_files": successful_files,
            "failed_files": failed_files,
            "errors": errors
        }
        
        logger.info(f"Multiple files processing completed. Success: {successful_files}, Failed: {failed_files}")
        return JSONResponse(content=response)
    
    except Exception as e:
        logger.exception(f"Unhandled exception in multiple files processing: {str(e)}")
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "results": results,
                "total_files": len(files),
                "successful_files": successful_files,
                "failed_files": failed_files,
                "errors": [str(e)]
            }
        )
    finally:
        # Clean up all temporary files
        for temp_path in temp_paths:
            if temp_path and os.path.exists(temp_path):
                try:
                    os.unlink(temp_path)
                except Exception as e:
                    logger.warning(f"Failed to delete temporary file {temp_path}: {e}")
        logger.info("All temporary files cleaned up")

# BACKWARD COMPATIBILITY ENDPOINTS
@app.post("/process-resume/", response_model=ProcessResponse)
async def process_resume_endpoint(
    file: UploadFile = File(...)
):
    """Legacy endpoint for single resume processing - maintained for backward compatibility"""
    logger.info(f"Resume processing request received (legacy endpoint)")
    result = await process_single_file_endpoint(file=file)
    
    # Convert to legacy format
    if isinstance(result, JSONResponse):
        content = result.body.decode() if hasattr(result, 'body') else '{}'
        try:
            parsed_content = json.loads(content)
        except:
            parsed_content = {"error": "Failed to parse response"}
    else:
        parsed_content = result
    
    # Convert to ProcessResponse format
    legacy_response = {
        "unique_id": f"resume_{int(time.time())}",
        "status": parsed_content.get("status", "error"),
        "content": parsed_content.get("content", {}),
        "error": parsed_content.get("error")
    }
    
    return JSONResponse(content=legacy_response)

# ROOT ENDPOINT
@app.get("/")
async def root():
    return {
        "message": "PDF and DOCX Processing API is running",
        "endpoints": {
            "single_file": "/process-file/ - Process a single PDF or DOCX file",
            "multiple_files": "/process-multiple-files/ - Process multiple PDF and DOCX files",
            "legacy_resume": "/process-resume/ - Legacy endpoint for single resume processing"
        },
        "supported_formats": ["PDF", "DOCX"],
        "max_files_per_request": 10
    }

# REQUEST LOGGING MIDDLEWARE
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

# LOG ROTATION FUNCTIONS
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
    logger.info("Starting FastAPI server with multi-file support")
    uvicorn.run(app, host="0.0.0.0", port=8000)