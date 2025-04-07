import requests
from bs4 import BeautifulSoup
import time
import random
import google.generativeai as genai
import json
import os
import sys
import logging
from typing import List, Dict, Optional, Any
from dotenv import load_dotenv

load_dotenv()

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# --- Constants and Configuration ---
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
HEADERS = {'User-Agent': USER_AGENT}

OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/app/data")
OUTPUT_FILENAME = os.environ.get("OUTPUT_FILENAME", "jobs.json")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

# --- LLM Configuration ---
llm_enabled = False  # Default value

if not GOOGLE_API_KEY:
    logger.warning("GOOGLE_API_KEY environment variable not set. Skill extraction will be skipped.")
else:
    try:
        genai.configure(api_key=GOOGLE_API_KEY)
        model = genai.GenerativeModel('models/gemini-1.5-flash')
        generation_config = genai.GenerationConfig(temperature=0.2)
        safety_settings = [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
        ]
        logger.info("Google AI Model configured successfully.")
        llm_enabled = True
    except Exception as e:
        logger.error(f"Failed to configure Google AI: {e}")
        logger.error("Skill extraction will be disabled.")
        llm_enabled = False

def get_time_filter_param(filter_type: Optional[str]) -> str:
    if not filter_type:
        return ''
    filters = {
        '24h': 'f_TPR=r86400',
        '1w': 'f_TPR=r604800',
        '1m': 'f_TPR=r2592000'
    }
    return filters.get(filter_type.lower(), '')

def get_job_description(job_url: str) -> Optional[str]:
    if not job_url or not job_url.startswith('http'):
        logger.warning(f"Skipping description fetch for invalid URL: {job_url}")
        return None

    logger.info(f"Fetching description from: {job_url}")
    try:
        time.sleep(random.uniform(2.0, 5.0))
        response = requests.get(job_url, headers=HEADERS, timeout=25)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, 'html.parser')

        description_section = soup.find('section', class_='show-more-less-html')
        if description_section:
            description_div = description_section.find('div', class_='show-more-less-html__markup')
            if description_div:
                return description_div.get_text(separator=' ', strip=True)

        description_div_alt = soup.find('div', class_='description__text--rich')
        if description_div_alt:
             return description_div_alt.get_text(separator=' ', strip=True)

        logger.warning(f"Description content not found using known selectors for {job_url}")
        return None

    except requests.exceptions.Timeout:
        logger.error(f"Timeout fetching job description from {job_url}")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"HTTP error fetching job description from {job_url}: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error parsing job description from {job_url}: {e}")
        return None

def extract_skills_with_llm(description_text: Optional[str]) -> List[str]:
    if not llm_enabled or not model or not description_text:
        return []

    prompt = f"""
    Extract all technical skills (like programming languages, software, tools, frameworks, databases)
    and soft skills (like communication, teamwork, leadership, problem-solving)
    mentioned in the following job description.

    Return ONLY a JSON list of strings, where each string is a skill.
    Example: ["Python", "SQL", "Data Analysis", "Communication", "AWS", "Project Management"]
    If no skills are found, return an empty list [].

    Job Description (first 8000 chars):
    ---
    {description_text[:8000]}
    ---

    JSON Skill List:
    """

    logger.info("Calling LLM for skill extraction...")
    try:
        response = model.generate_content(
            prompt,
            generation_config=generation_config,
            safety_settings=safety_settings
            )

        response_text = response.text.strip()
        json_start = response_text.find('[')
        json_end = response_text.rfind(']') + 1

        if json_start != -1 and json_end != 0 and json_start < json_end:
            json_string = response_text[json_start:json_end]
            try:
                skills_list = json.loads(json_string)
                if isinstance(skills_list, list):
                    cleaned_skills = [str(skill).strip() for skill in skills_list if str(skill).strip()]
                    logger.info(f"Extracted skills: {cleaned_skills}")
                    return cleaned_skills
                else:
                    logger.warning(f"LLM response JSON was not a list: {skills_list}")
                    return []
            except json.JSONDecodeError as json_err:
                logger.error(f"Failed to decode JSON from LLM response: {json_err}")
                logger.error(f"LLM Response Text: {response_text}")
                return []
        else:
            logger.warning(f"Could not find valid JSON list '[]' in LLM response: {response_text}")
            return []

    except Exception as e:
        logger.error(f"Exception during LLM API call or processing: {e}")
        return []

def make_absolute_url(base_url: str, relative_url: Optional[str]) -> Optional[str]:
    if not relative_url:
        return None
    if relative_url.startswith('http'):
        return relative_url
    if relative_url.startswith('/'):
        from urllib.parse import urljoin
        return urljoin(base_url, relative_url)
    logger.warning(f"Could not make relative URL absolute: {relative_url}")
    return None

def scrape_linkedin_jobs(job_titles: List[str], location: str, time_filter: Optional[str], num_pages: int, max_jobs: Optional[int] = None) -> List[Dict[str, Any]]:
    all_jobs_data: List[Dict[str, Any]] = []
    time_param = get_time_filter_param(time_filter)
    base_search_url = "https://www.linkedin.com/jobs/search/"
    jobs_per_title = None

    if max_jobs is not None:
        jobs_per_title = max_jobs // len(job_titles)
        remainder = max_jobs % len(job_titles)
        logger.info(f"Maximum jobs per title: {jobs_per_title}")

    for i, job_title_query in enumerate(job_titles):
        jobs_scraped_for_title = 0 # Keep track of jobs scraped for current title
        max_jobs_for_title = jobs_per_title
        if max_jobs is not None and i < remainder:
           max_jobs_for_title = jobs_per_title + 1

        logger.info(f"\n--- Scraping jobs for: '{job_title_query}' in '{location}' ---")
        try:
            formatted_job = requests.utils.quote(job_title_query)
            formatted_location = requests.utils.quote(location)
        except Exception as e:
            logger.error(f"Could not URL-encode search terms: {e}. Skipping query '{job_title_query}'.")
            continue

        for page in range(num_pages):
            if max_jobs is not None and jobs_scraped_for_title >= max_jobs_for_title:
                logger.info(f"Reached maximum jobs limit ({max_jobs_for_title}) for title '{job_title_query}'. Skipping remaining pages.")
                break

            start = page * 25
            search_url = f"{base_search_url}?keywords={formatted_job}&location={formatted_location}&start={start}"
            if time_param:
                search_url += f"&{time_param}"

            logger.info(f"Requesting search page {page + 1}/{num_pages}: {search_url}")
            try:
                response = requests.get(search_url, headers=HEADERS, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                job_cards = soup.find_all('div', class_='base-card')

                if not job_cards:
                    logger.info(f"No job cards found on page {page + 1} for '{job_title_query}'. Stopping search for this title.")
                    break

                logger.info(f"Found {len(job_cards)} potential jobs on page {page + 1}. Processing...")

                for card in job_cards:
                    if max_jobs is not None and jobs_scraped_for_title >= max_jobs_for_title:
                        logger.info(f"Reached maximum jobs limit ({max_jobs_for_title}) for title '{job_title_query}'. Skipping remaining cards.")
                        break # Break inner loop

                    job_data: Dict[str, Any] = {
                        "search_query": job_title_query,
                        "title": None,
                        "company": None,
                        "location": None,
                        "date_posted": None,
                        "url": None,
                        "extracted_skills": []
                    }

                    title_element = card.find('h3', class_='base-search-card__title')
                    if title_element: job_data['title'] = title_element.text.strip()

                    company_element = card.find('h4', class_='base-search-card__subtitle')
                    if company_element: job_data['company'] = company_element.text.strip()

                    location_element = card.find('span', class_='job-search-card__location')
                    if location_element: job_data['location'] = location_element.text.strip()

                    link_element = card.find('a', class_='base-card__full-link')
                    if not link_element:
                         link_element = card.find('a', href=True)
                    relative_url = link_element.get('href') if link_element else None
                    job_data['url'] = make_absolute_url(base_search_url, relative_url)

                    date_element = card.find('time', class_='job-search-card__listdate')
                    if date_element: job_data['date_posted'] = date_element.get('datetime')
                    else:
                         date_element_alt = card.find('time', class_='job-search-card__listdate--new')
                         if date_element_alt: job_data['date_posted'] = date_element_alt.get('datetime')

                    if job_data['url']:
                        job_description = get_job_description(job_data['url'])
                        if job_description:
                             job_data['extracted_skills'] = extract_skills_with_llm(job_description)
                        else:
                             logger.info(f"Could not retrieve description for job: {job_data.get('title', 'N/A')}")
                    else:
                         logger.info(f"Skipping description/skills fetch due to missing/invalid URL for job: {job_data.get('title', 'N/A')}")

                    if job_data.get('title') and job_data.get('company'):
                        all_jobs_data.append(job_data)
                        jobs_scraped_for_title += 1
                        logger.info(f"Processed: {job_data['title']} at {job_data['company']}")
                    else:
                        logger.info("Skipping card - missing essential info (title or company).")

                logger.info(f"Finished processing page {page + 1} for '{job_title_query}'.")

            except requests.exceptions.Timeout:
                 logger.error(f"Timeout requesting search page {page + 1} for '{job_title_query}'. Stopping for this query.")
                 break
            except requests.exceptions.RequestException as e:
                logger.error(f"HTTP error scraping search page {page + 1} for '{job_title_query}': {e}. Stopping for this query.")
                break
            except Exception as e:
                 logger.error(f"An unexpected error occurred on search page {page + 1} for '{job_title_query}': {e}")
                 break

            if page < num_pages - 1:
                 sleep_time = random.uniform(4.0, 9.0)
                 logger.info(f"Sleeping for {sleep_time:.1f} seconds before next page...")
                 time.sleep(sleep_time)

        logger.info(f"Scraped a total of {jobs_scraped_for_title} jobs for title '{job_title_query}'.")

    return all_jobs_data

def save_to_json(data: List[Dict[str, Any]], filepath: str):
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"Successfully saved {len(data)} jobs to {filepath}")
    except IOError as e:
        logger.error(f"Could not write to file {filepath}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during JSON saving: {e}")

def main():
    # Retrieve configuration from environment variables
    job_titles_str = os.environ.get("JOB_TITLES")
    if not job_titles_str:
        logger.error("JOB_TITLES environment variable is required.")
        sys.exit(1)
    job_titles = job_titles_str.split()

    location = os.environ.get("LOCATION")
    if not location:
        logger.error("LOCATION environment variable is required.")
        sys.exit(1)

    time_filter = os.environ.get("TIME_FILTER")
    num_pages = int(os.environ.get("NUM_PAGES", "1"))  # Default to 1 if not set
    max_jobs = os.environ.get("MAX_JOBS")
    max_jobs = int(max_jobs) if max_jobs else None   # Convert to int or None

    if not GOOGLE_API_KEY and llm_enabled:
         logger.error("LLM was configured but GOOGLE_API_KEY is missing. Exiting.")
         sys.exit(1)

    logger.info("\n--- Starting LinkedIn Job Scraper ---")
    logger.info(f"Search Queries: {job_titles}")
    logger.info(f"Location: {location}")
    logger.info(f"Time Filter: {time_filter or 'None'}")
    logger.info(f"Pages per Query: {num_pages}")
    logger.info(f"Max Jobs: {max_jobs or 'Unlimited (within page limits)'}")
    logger.info(f"LLM Skill Extraction: {'Enabled' if llm_enabled else 'Disabled'}")
    logger.info(f"Output File: {OUTPUT_PATH}")
    logger.info("-" * 35)

    scraped_data = scrape_linkedin_jobs(
        job_titles=job_titles,
        location=location,
        time_filter=time_filter,
        num_pages=num_pages,
        max_jobs=max_jobs
    )

    logger.info("\n--- Scraping Complete ---")
    logger.info(f"Total jobs processed: {len(scraped_data)}")

    save_to_json(scraped_data, OUTPUT_PATH)

    logger.info("--- Script Finished ---")


if __name__ == "__main__":
    main()