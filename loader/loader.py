import json
import os
import sys
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Job, JobSkill
from logger.logger import get_logger

logger = get_logger("loader")

def parse_date(date_str):
    """Parse date string into a datetime.date object."""
    if not date_str:
        return None
    try:
        return datetime.strptime(date_str, '%Y-%m-%d').date()
    except ValueError:
        logger.warning(f"Could not parse date: {date_str}")
        return None

def load_transformed_data(file_path):
    """Load the transformed data from the JSON file."""
    try:
        with open(file_path, 'r') as file:
            return json.load(file)
    except Exception as e:
        logger.error(f"Failed to load transformed data: {e}")
        return []

def process_job_listing(session, job_data):
    """Process a single job listing and add it to the database."""
    # Check if job already exists (based on URL)
    existing_job = None
    if job_data.get('url'):
        existing_job = session.query(Job).filter_by(job_url=job_data['url']).first()
    
    if existing_job:
        logger.info(f"Job already exists: {job_data['title']} at {job_data['company']}")
        return False
    
    # Create new job
    new_job = Job(
        title=job_data['title'],
        company_name=job_data['company'],
        location=job_data['location'],
        job_url=job_data.get('url'),
        date_posted=parse_date(job_data.get('date_posted')),
        date_scraped=datetime.now(),
        progress="Haven't Applied"
    )
    
    # Add skills
    for skill in job_data.get('extracted_skills', []):
        new_job.skills.append(JobSkill(skill=skill))
    
    session.add(new_job)
    return True

def main():
    """Main function to process transformed data and load into database."""
    # Connect to database
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        logger.error("DATABASE_URL environment variable not set")
        sys.exit(1)
    
    logger.info(f"Connecting to database..")
    engine = create_engine(database_url)
    
    # Create session
    Session = sessionmaker(bind=engine)
    session = Session()
    
    # Find the latest transformed data file
    data_dir = '/app/data'
    transformed_files = [f for f in os.listdir(data_dir)]
    
    if not transformed_files:
        logger.error("No transformed data files found")
        sys.exit(1)
    
    # Sort files by modification time (newest first)
    latest_file = sorted(
        transformed_files, 
        key=lambda f: os.path.getmtime(os.path.join(data_dir, f)), 
        reverse=True
    )[0]
    
    file_path = os.path.join(data_dir, latest_file)
    logger.info(f"Processing transformed data from: {file_path}")
    
    # Load transformed data
    job_listings = load_transformed_data(file_path)
    
    # Process each job listing
    new_jobs_count = 0
    try:
        for job_data in job_listings:
            if process_job_listing(session, job_data):
                new_jobs_count += 1
                
        # Commit changes
        session.commit()
        logger.info(f"Successfully loaded {new_jobs_count} new jobs into the database")

        # Delete the JSON file after successful processing
        os.remove(file_path)
        logger.info(f"Deleted processed file: {file_path}")
        
    except Exception as e:
        logger.error(f"Error processing job listings: {e}")
        session.rollback()
        sys.exit(1)
    finally:
        session.close()

if __name__ == "__main__":
    main()