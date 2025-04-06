from sqlalchemy import Column, Integer, String, Date, Text, ForeignKey, MetaData, DateTime, func
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

# Define base class
Base = declarative_base()
metadata = MetaData(schema="core")

class Job(Base):
    __tablename__ = 'jobs'
    __table_args__ = {'schema': 'core'}
    
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