from pydantic import BaseModel
from typing import List, Dict, Any


class EmailSchema(BaseModel):
    full_name: str
    phone_number: str
    job_title: str
    pay: str
    location: str
    call_id: str
    intent: str
    work_experience: str


class JobApplication(BaseModel):
    full_name: str
    job_title: str
    pay: str
    location: str
    work_experience: str
    phone_number: str

class MultiProcessResponse(BaseModel):
    status: str
    results: List[Dict[str, Any]]
    total_files: int
    successful_files: int
    failed_files: int
    errors: List[str] = []