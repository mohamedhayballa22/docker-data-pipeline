-- Create schema
CREATE SCHEMA IF NOT EXISTS core;

-- Jobs table - core data
CREATE TABLE core.jobs (
    job_id SERIAL PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    company_name VARCHAR(255),
    location TEXT,
    job_url TEXT UNIQUE,
    date_posted DATE,
    date_scraped TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    progress VARCHAR(50)
);

-- Job skills table
CREATE TABLE core.job_skills (
    job_skill_id SERIAL PRIMARY KEY,
    job_id INTEGER REFERENCES core.jobs(job_id),
    skill VARCHAR(100)
);

-- Indexes
CREATE INDEX idx_jobs_title ON core.jobs(title);
CREATE INDEX idx_jobs_company ON core.jobs(company_name);
CREATE INDEX idx_jobs_location ON core.jobs(location);
CREATE INDEX idx_jobs_date_posted ON core.jobs(date_posted);