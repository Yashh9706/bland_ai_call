from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse, HTMLResponse, RedirectResponse, FileResponse
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi import FastAPI, File, UploadFile, HTTPException
from dotenv import load_dotenv
import os
import logging
import tempfile
import shutil
from typing import List
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

from schema import EmailSchema, JobApplication, MultiProcessResponse
from utils import send_job_application_email, process_job_application, process_single_file


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

templates = Jinja2Templates(directory=os.path.dirname(os.path.abspath(__file__)))


# Mount static files at /static and serve index.html at /index.html
app.mount("/static", StaticFiles(directory=os.path.dirname(os.path.abspath(__file__))), name="static")


@app.get("/index.html", include_in_schema=False)
async def serve_index():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(current_dir, "index.html")
    return FileResponse(html_path, media_type="text/html")


@app.get("/result.html", include_in_schema=False)
async def serve_result():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(current_dir, "result.html")
    return FileResponse(html_path, media_type="text/html")


@app.get("/choose.html", include_in_schema=False)
async def serve_choose():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(current_dir, "choose.html")
    return FileResponse(html_path, media_type="text/html")


@app.get("/make_call.html", include_in_schema=False)
async def serve_make_call():
    current_dir = os.path.dirname(os.path.abspath(__file__))
    html_path = os.path.join(current_dir, "make_call.html")
    return FileResponse(html_path, media_type="text/html")


@app.post("/send_email")
async def send_email(email: EmailSchema):
    return send_job_application_email(email)


@app.post("/not_interested")
async def not_interested(email: EmailSchema):
    logger.info(f"User not interested: {email.call_id}, {email.job_title}")
    return {"message": "User is not interested in the job."}


@app.post("/submit-job-application")
async def submit_job(application: JobApplication):
    logger.info(f"Received job application: {application}")
    try:
        result = process_job_application(application)
        return {"success": True, "data": result}
    except ValueError as e:
        logger.error(f"Validation error: {e}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Internal error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/", include_in_schema=False)
async def root_choose():
    return RedirectResponse(url="/choose.html")


@app.exception_handler(RequestValidationError)
async def validation_handler(request: Request, exc: RequestValidationError):
    return JSONResponse(status_code=422, content={"detail": exc.errors()})

@app.post("/process-multiple-files/", response_model=MultiProcessResponse)
async def process_multiple_files_endpoint(
    files: List[UploadFile] = File(...)
):
    """
    Process single or multiple PDF and DOCX files
    
    This endpoint can handle:
    - Single file: Upload 1 file
    - Multiple files: Upload up to 10 files at once
    
    Supported formats: PDF, DOCX
    """
    logger.info(f"Received files request: {len(files)} files")

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
            if not file.filename.lower().endswith((".pdf", ".docx")):
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
                suffix = ".pdf" if file.filename.lower().endswith(".pdf") else ".docx"
                with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as temp_file:
                    shutil.copyfileobj(file.file, temp_file)
                    temp_path = temp_file.name
                temp_paths.append(temp_path)

                # Process the file
                result = process_single_file(temp_path, file.filename)
                results.append(result)

                if result["status"] == "success":
                    successful_files += 1
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

        response = {
            "status": "completed",
            "results": results,
            "total_files": len(files),
            "successful_files": successful_files,
            "failed_files": failed_files,
            "errors": errors
        }
        logger.info(f"Files processing completed. Success: {successful_files}, Failed: {failed_files}")
        return JSONResponse(content=response)

    except Exception as e:
        logger.exception(f"Unhandled exception in files processing: {str(e)}")
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", port=5000, reload=True)
