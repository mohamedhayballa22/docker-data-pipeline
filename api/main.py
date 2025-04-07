from fastapi import FastAPI, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List
from models import JobItem, get_db, Job

app = FastAPI(title="Data Pipeline API")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/health")
def health_check():
    return {"status": "healthy"}

@app.get("/data", response_model=List[JobItem])
def get_data(db: Session = Depends(get_db)):
    try:
        # Query all jobs with their related skills
        jobs = db.query(Job).all()
        
        # JobItem will automatically convert from SQLAlchemy models thanks to from_attributes=True
        return jobs
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")