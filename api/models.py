from typing import List, Optional, Literal
from datetime import date, datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    Date,
    Text,
    MetaData,
    DateTime,
    ForeignKey,
    func,
    create_engine,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from pydantic import BaseModel, ConfigDict, Field
import os

# SQLAlchemy setup
Base = declarative_base()
metadata = MetaData(schema="core")

# Database connection
DB_URL = os.environ.get("DATABASE_URL")
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# SQLAlchemy models
class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = {"schema": "core"}

    job_id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255), nullable=False)
    company_name = Column(String(255))
    location = Column(Text)
    job_url = Column(Text, unique=True)
    date_posted = Column(Date)
    date_scraped = Column(DateTime, default=func.now())
    progress = Column(String(50))

    # Relationship with skills
    skills = relationship("JobSkill", back_populates="job")

class JobSkill(Base):
    __tablename__ = 'job_skills'
    __table_args__ = {'schema': 'core'}
    
    job_skill_id = Column(Integer, primary_key=True, autoincrement=True)
    job_id = Column(Integer, ForeignKey('core.jobs.job_id'))
    skill = Column(String(100))
    
    # Relationship with job
    job = relationship("Job", back_populates="skills")


# Pydantic models (for API responses)
class SkillItem(BaseModel):
    skill: str

    model_config = ConfigDict(from_attributes=True)


class JobItem(BaseModel):
    job_id: int
    title: str
    company_name: Optional[str] = None
    location: Optional[str] = None
    job_url: Optional[str] = None
    date_posted: Optional[date] = None
    date_scraped: Optional[datetime] = None
    progress: Optional[str] = None
    skills: List[SkillItem] = []

    model_config = ConfigDict(from_attributes=True)

class PipelineTriggerRequest(BaseModel):
    job_titles: str = Field(..., min_length=1, description="Comma-separated string of job titles to search for.")
    location: str = Field(..., min_length=1, description="Location for the job search (e.g., 'Paris', 'Bordeaux').")
    time_filter: Literal["24h", "1w", "1m"] = Field(..., description="Time filter for job postings ('24h', '1w', or '1m').")
    max_jobs: int = Field(..., gt=0, description="Maximum number of jobs to retrieve per job title.")

class ProgressUpdate(BaseModel):
    progress: str