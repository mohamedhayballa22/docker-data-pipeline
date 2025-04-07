from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List
from models import JobItem, get_db, Job
import subprocess

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
    
@app.post("/trigger-job-pipeline")
async def trigger_job_pipeline(background_tasks: BackgroundTasks):
    """Trigger the scraper and loader pipeline as a background task."""
    background_tasks.add_task(run_job_pipeline)
    return {"message": "Job pipeline triggered. Check logs for progress."}

def run_job_pipeline():
    """Run the scraper and loader services in sequence."""
    try:
        # Run the scraper
        scraper_result = subprocess.run(
            ["docker", "compose", "run", "--rm", "scraper"],
            check=True,
            text=True,
            capture_output=True
        )
        
        # Check if scraper was successful
        if scraper_result.returncode != 0:
            print(f"Scraper failed: {scraper_result.stderr}")
            return
        
        print("Scraper completed successfully")
        
        # Run the loader
        loader_result = subprocess.run(
            ["docker", "compose", "run", "--rm", "loader"],
            check=True,
            text=True,
            capture_output=True
        )
        
        if loader_result.returncode != 0:
            print(f"Loader failed: {loader_result.stderr}")
            return
            
        print("Loader completed successfully")
        
    except Exception as e:
        print(f"Error running job pipeline: {str(e)}")