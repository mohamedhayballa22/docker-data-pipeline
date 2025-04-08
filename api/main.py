from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session
from typing import List
from api.models import JobItem, get_db, Job
from api.docker_sdk import get_docker_client, get_container_sdk

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
    client = get_docker_client()
    docker_status = "connected" if client else "error"
    return {"status": "healthy", "docker_client": docker_status}

@app.get("/data", response_model=List[JobItem])
def get_data(db: Session = Depends(get_db)):
    try:
        jobs = db.query(Job).all()
        return jobs
    except Exception as e:
        print(f"Database query error: {e}")
        raise HTTPException(status_code=500, detail="Failed to retrieve data from database.")

@app.post("/trigger-job-pipeline")
async def trigger_job_pipeline(background_tasks: BackgroundTasks):
    """Trigger the scraper and loader pipeline as a background task using Docker SDK."""
    print("Received request to trigger job pipeline.")
    background_tasks.add_task(run_job_pipeline_sdk)
    return {"message": "Job pipeline trigger request accepted."}

def run_job_pipeline_sdk():
    """Runs scraper and loader scripts inside their containers using docker exec via SDK."""
    print("Attempting to trigger job pipeline via Docker SDK...")
    client = get_docker_client()
    if not client:
        print("Error: Docker client is not available. Cannot run pipeline.")
        return

    scraper_container = get_container_sdk("scraper")
    loader_container = get_container_sdk("loader")

    if not scraper_container:
        print("Error: Could not find running scraper container. Pipeline aborted.")
        return
    if not loader_container:
        print("Error: Could not find running loader container. Pipeline aborted.")
        return

    scraper_script_path = "/app/scraper.py"
    loader_script_path = "/app/loader.py"

    try:
        # --- Run Scraper ---
        print(f"Triggering scraper script '{scraper_script_path}' in container: {scraper_container.name}...")
        # Use exec_run to execute the command inside the container
        scrape_exit_code, scrape_output = scraper_container.exec_run(
            cmd=f"python {scraper_script_path}",
            stream=False,
            demux=False,
            tty=False # Important: Don't allocate pseudo-TTY
        )

        # Decode output from bytes to string
        output_str = scrape_output.decode('utf-8', errors='replace') if scrape_output else "(No output)"
        print(f"--- Scraper Output (Exit Code: {scrape_exit_code}) ---")
        print(output_str)
        print("--- End Scraper Output ---")

        if scrape_exit_code != 0:
            # Raise an exception to stop the sequence and log the failure
            raise Exception(f"Scraper task failed with exit code {scrape_exit_code}")

        print("Scraper task completed successfully.")

        # --- Run Loader ---
        print(f"Triggering loader script '{loader_script_path}' in container: {loader_container.name}...")
        load_exit_code, load_output = loader_container.exec_run(
            cmd=f"python {loader_script_path}",
            stream=False,
            demux=False,
            tty=False
        )

        output_str = load_output.decode('utf-8', errors='replace') if load_output else "(No output)"
        print(f"--- Loader Output (Exit Code: {load_exit_code}) ---")
        print(output_str)
        print("--- End Loader Output ---")

        if load_exit_code != 0:
            raise Exception(f"Loader task failed with exit code {load_exit_code}")

        print("Loader task completed successfully.")
        print("Job pipeline finished successfully.")

    except Exception as e:
        print(f"An error occurred running the job pipeline: {str(e)}")
