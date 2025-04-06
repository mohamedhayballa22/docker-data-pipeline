import requests
from bs4 import BeautifulSoup
import time
import random
import google.generativeai as genai
import json
import os
import argparse
import sys
from typing import List, Dict, Optional, Any
from dotenv import load_dotenv

load_dotenv()

# --- Constants and Configuration ---
GOOGLE_API_KEY = os.environ.get("GOOGLE_API_KEY")
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36'
HEADERS = {'User-Agent': USER_AGENT}

OUTPUT_DIR = "/app/data"
OUTPUT_FILENAME = "jobs.json"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_FILENAME)

# --- LLM Configuration ---
if not GOOGLE_API_KEY:
    print("WARNING: GOOGLE_API_KEY environment variable not set. Skill extraction will be skipped.")
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
        print("✅ Google AI Model configured successfully.")
        llm_enabled = True
    except Exception as e:
        print(f"ERROR: Failed to configure Google AI: {e}")
        print("Skill extraction will be disabled.")
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
        print(f"   Skipping description fetch for invalid URL: {job_url}")
        return None

    print(f"   Fetching description from: {job_url}")
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

        print(f"   WARN: Description content not found using known selectors for {job_url}")
        return None

    except requests.exceptions.Timeout:
        print(f"   ERROR: Timeout fetching job description from {job_url}")
        return None
    except requests.exceptions.RequestException as e:
        print(f"   ERROR: HTTP error fetching job description from {job_url}: {e}")
        return None
    except Exception as e:
        print(f"   ERROR: Unexpected error parsing job description from {job_url}: {e}")
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

    print("   Calling LLM for skill extraction...")
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
                    print(f"   Extracted skills: {cleaned_skills}")
                    return cleaned_skills
                else:
                    print(f"   WARN: LLM response JSON was not a list: {skills_list}")
                    return []
            except json.JSONDecodeError as json_err:
                print(f"   ERROR: Failed to decode JSON from LLM response: {json_err}")
                print(f"   LLM Response Text: {response_text}")
                return []
        else:
            print(f"   WARN: Could not find valid JSON list '[]' in LLM response: {response_text}")
            return []

    except Exception as e:
        print(f"   ERROR: Exception during LLM API call or processing: {e}")
        return []

def make_absolute_url(base_url: str, relative_url: Optional[str]) -> Optional[str]:
    if not relative_url:
        return None
    if relative_url.startswith('http'):
        return relative_url
    if relative_url.startswith('/'):
        from urllib.parse import urljoin
        return urljoin(base_url, relative_url)
    print(f"   WARN: Could not make relative URL absolute: {relative_url}")
    return None

def scrape_linkedin_jobs(job_titles: List[str], location: str, time_filter: Optional[str], num_pages: int, max_jobs: Optional[int] = None) -> List[Dict[str, Any]]:
    all_jobs_data: List[Dict[str, Any]] = []
    time_param = get_time_filter_param(time_filter)
    base_search_url = "https://www.linkedin.com/jobs/search/"
    jobs_per_title = None

    if max_jobs is not None:
        jobs_per_title = max_jobs // len(job_titles)
        remainder = max_jobs % len(job_titles)
        print(f"Maximum jobs per title: {jobs_per_title}")

    for i, job_title_query in enumerate(job_titles):
        jobs_scraped_for_title = 0 # Keep track of jobs scraped for current title
        max_jobs_for_title = jobs_per_title
        if max_jobs is not None and i < remainder:
           max_jobs_for_title = jobs_per_title + 1

        print(f"\n--- Scraping jobs for: '{job_title_query}' in '{location}' ---")
        try:
            formatted_job = requests.utils.quote(job_title_query)
            formatted_location = requests.utils.quote(location)
        except Exception as e:
            print(f"ERROR: Could not URL-encode search terms: {e}. Skipping query '{job_title_query}'.")
            continue

        for page in range(num_pages):
            if max_jobs is not None and jobs_scraped_for_title >= max_jobs_for_title:
                print(f"Reached maximum jobs limit ({max_jobs_for_title}) for title '{job_title_query}'. Skipping remaining pages.")
                break

            start = page * 25
            search_url = f"{base_search_url}?keywords={formatted_job}&location={formatted_location}&start={start}"
            if time_param:
                search_url += f"&{time_param}"

            print(f"Requesting search page {page + 1}/{num_pages}: {search_url}")
            try:
                response = requests.get(search_url, headers=HEADERS, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                job_cards = soup.find_all('div', class_='base-card')

                if not job_cards:
                    print(f"No job cards found on page {page + 1} for '{job_title_query}'. Stopping search for this title.")
                    break

                print(f"Found {len(job_cards)} potential jobs on page {page + 1}. Processing...")

                for card in job_cards:
                    if max_jobs is not None and jobs_scraped_for_title >= max_jobs_for_title:
                        print(f"Reached maximum jobs limit ({max_jobs_for_title}) for title '{job_title_query}'. Skipping remaining cards.")
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
                             print(f"   Could not retrieve description for job: {job_data.get('title', 'N/A')}")
                    else:
                         print(f"   Skipping description/skills fetch due to missing/invalid URL for job: {job_data.get('title', 'N/A')}")

                    if job_data.get('title') and job_data.get('company'):
                        all_jobs_data.append(job_data)
                        jobs_scraped_for_title += 1
                        print(f"   Processed: {job_data['title']} at {job_data['company']}")
                    else:
                        print("   Skipping card - missing essential info (title or company).")

                print(f"Finished processing page {page + 1} for '{job_title_query}'.")

            except requests.exceptions.Timeout:
                 print(f"ERROR: Timeout requesting search page {page + 1} for '{job_title_query}'. Stopping for this query.")
                 break
            except requests.exceptions.RequestException as e:
                print(f"ERROR: HTTP error scraping search page {page + 1} for '{job_title_query}': {e}. Stopping for this query.")
                break
            except Exception as e:
                 print(f"ERROR: An unexpected error occurred on search page {page + 1} for '{job_title_query}': {e}")
                 break

            if page < num_pages - 1:
                 sleep_time = random.uniform(4.0, 9.0)
                 print(f"Sleeping for {sleep_time:.1f} seconds before next page...")
                 time.sleep(sleep_time)

        print(f"Scraped a total of {jobs_scraped_for_title} jobs for title '{job_title_query}'.")

    return all_jobs_data

def save_to_json(data: List[Dict[str, Any]], filepath: str):
    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"✅ Successfully saved {len(data)} jobs to {filepath}")
    except IOError as e:
        print(f"ERROR: Could not write to file {filepath}: {e}")
    except Exception as e:
        print(f"ERROR: An unexpected error occurred during JSON saving: {e}")

def main():
    parser = argparse.ArgumentParser(description="Scrape LinkedIn job postings.")
    parser.add_argument(
        "--job-titles",
        nargs='+',
        required=True,
        help="List of job titles to search for (e.g., 'data scientist' 'software engineer')."
    )
    parser.add_argument(
        "--location",
        type=str,
        required=True,
        help="Geographic location for the job search (e.g., 'France', 'New York City')."
    )
    parser.add_argument(
        "--time-filter",
        type=str,
        choices=['24h', '1w', '1m'],
        default=None,
        help="Filter jobs posted within the last '24h', '1w', or '1m'."
    )
    parser.add_argument(
        "--num-pages",
        type=int,
        default=1,
        help="Number of search result pages to scrape for *each* job title."
    )
    parser.add_argument(
        "--max-jobs",
        type=int,
        default=None,
        help="Maximum number of jobs to scrape in total (divided evenly across job titles). If not specified, scrapes all within page limit."
    )

    args = parser.parse_args()

    if not GOOGLE_API_KEY and llm_enabled:
         print("ERROR: LLM was configured but GOOGLE_API_KEY is missing. Exiting.")
         sys.exit(1)

    print("\n--- Starting LinkedIn Job Scraper ---")
    print(f"Search Queries: {args.job_titles}")
    print(f"Location: {args.location}")
    print(f"Time Filter: {args.time_filter or 'None'}")
    print(f"Pages per Query: {args.num_pages}")
    print(f"Max Jobs: {args.max_jobs or 'Unlimited (within page limits)'}")
    print(f"LLM Skill Extraction: {'Enabled' if llm_enabled else 'Disabled'}")
    print(f"Output File: {OUTPUT_PATH}")
    print("-" * 35)

    scraped_data = scrape_linkedin_jobs(
        job_titles=args.job_titles,
        location=args.location,
        time_filter=args.time_filter,
        num_pages=args.num_pages,
        max_jobs=args.max_jobs
    )

    print(f"\n--- Scraping Complete ---")
    print(f"Total jobs processed: {len(scraped_data)}")

    save_to_json(scraped_data, OUTPUT_PATH)

    print("--- Script Finished ---")


if __name__ == "__main__":
    main()