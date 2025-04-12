import requests
from bs4 import BeautifulSoup
import time
import random
import google.generativeai as genai
import json
import os
import sys
from typing import List, Dict, Optional, Any, Callable
from logger.logger import get_logger
from kafka_client import get_kafka_producer, get_kafka_consumer, send_kafka_message

logger = get_logger("scraper")

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}

KAFKA_SCRAPING_TOPIC = "scraping-jobs"
KAFKA_STATUS_TOPIC = "job-status-updates"
KAFKA_PROCESSING_TOPIC = "data-processing"
KAFKA_NOTIFICATIONS_TOPIC = "system-notifications"
KAFKA_CONSUMER_GROUP_ID = "scraper-group"

def get_time_filter_param(filter_type: Optional[str]) -> str:
    if not filter_type:
        return ""
    filters = {"24h": "f_TPR=r86400", "1w": "f_TPR=r604800", "1m": "f_TPR=r2592000"}
    return filters.get(filter_type.lower(), "")

def get_job_description(job_url: str) -> Optional[str]:
    if not job_url or not job_url.startswith("http"):
        logger.warning(f"Skipping description fetch for invalid URL: {job_url}")
        return None

    logger.info(f"Fetching description from: {job_url}")
    try:
        time.sleep(random.uniform(2.0, 5.0))
        response = requests.get(job_url, headers=HEADERS, timeout=25)
        response.raise_for_status()
        soup = BeautifulSoup(response.content, "html.parser")

        description_section = soup.find("section", class_="show-more-less-html")
        if description_section:
            description_div = description_section.find(
                "div", class_="show-more-less-html__markup"
            )
            if description_div:
                return description_div.get_text(separator=" ", strip=True)

        description_div_alt = soup.find("div", class_="description__text--rich")
        if description_div_alt:
            return description_div_alt.get_text(separator=" ", strip=True)

        logger.warning(
            f"Description content not found using known selectors for {job_url}"
        )
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


def extract_skills_with_llm(description_text: Optional[str], model: Optional[genai.GenerativeModel]) -> List[str]:
    """Extracts skills using the provided LLM model instance."""
    if not model or not description_text:
        logger.info("LLM model not available or no description text, skipping skill extraction.")
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
        generation_config = genai.GenerationConfig(temperature=0.2)
        safety_settings = [
            {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
            {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
            {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
            {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_MEDIUM_AND_ABOVE"},
        ]

        response = model.generate_content(
            prompt, generation_config=generation_config, safety_settings=safety_settings
        )

        response_text = response.text.strip()
        json_start = response_text.find("[")
        json_end = response_text.rfind("]") + 1

        if json_start != -1 and json_end != 0 and json_start < json_end:
            json_string = response_text[json_start:json_end]
            try:
                skills_list = json.loads(json_string)
                if isinstance(skills_list, list):
                    cleaned_skills = [
                        str(skill).strip()
                        for skill in skills_list
                        if str(skill).strip()
                    ]
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
            logger.warning(
                f"Could not find valid JSON list '[]' in LLM response: {response_text}"
            )
            return []

    except Exception as e:
        logger.error(f"Exception during LLM API call or processing: {e}")
        return []


def make_absolute_url(base_url: str, relative_url: Optional[str]) -> Optional[str]:
    if not relative_url:
        return None
    if relative_url.startswith("http"):
        return relative_url
    if relative_url.startswith("/"):
        from urllib.parse import urljoin
        return urljoin(base_url, relative_url)
    logger.warning(f"Could not make relative URL absolute: {relative_url}")
    return None


def scrape_linkedin_jobs(
    job_id: str,
    job_titles: List[str],
    location: str,
    time_filter: Optional[str],
    num_pages: int,
    max_jobs: Optional[int],
    llm_model: Optional[genai.GenerativeModel],
    progress_callback: Callable[[float], None]
) -> List[Dict[str, Any]]:
    """
    Scrapes LinkedIn jobs based on parameters, reports progress, and uses LLM if provided.
    (Function implementation remains unchanged from previous corrected version)
    """
    all_jobs_data: List[Dict[str, Any]] = []
    time_param = get_time_filter_param(time_filter)
    base_search_url = "https://www.linkedin.com/jobs/search/"
    total_jobs_to_scrape = max_jobs
    jobs_scraped_count = 0
    jobs_processed_count = 0
    estimated_total_cards_to_process = len(job_titles) * num_pages * 25

    def report_progress():
        nonlocal jobs_scraped_count, jobs_processed_count
        percentage = 0.0
        if total_jobs_to_scrape and total_jobs_to_scrape > 0:
            percentage = min(100.0, (jobs_scraped_count / total_jobs_to_scrape) * 100)
        elif estimated_total_cards_to_process > 0:
             percentage = min(100.0, (jobs_processed_count / estimated_total_cards_to_process) * 100)

        progress_callback(percentage)


    logger.info(f"[Job {job_id}] Starting scrape for {len(job_titles)} titles, location='{location}', pages={num_pages}, max_jobs={max_jobs}")
    report_progress()

    for job_title_query in job_titles:
        if max_jobs is not None and jobs_scraped_count >= max_jobs:
                logger.info(f"[Job {job_id}] Reached overall max_jobs limit ({max_jobs}). Stopping search.")
                break

        logger.info(f"[Job {job_id}] --- Scraping for: '{job_title_query}' ---")
        try:
            formatted_job = requests.utils.quote(job_title_query)
            formatted_location = requests.utils.quote(location)
        except Exception as e:
            logger.error(
                f"[Job {job_id}] Could not URL-encode search terms: {e}. Skipping query '{job_title_query}'."
            )
            continue

        for page in range(num_pages):
            if max_jobs is not None and jobs_scraped_count >= max_jobs:
                logger.info(f"[Job {job_id}] Reached max_jobs limit during page processing. Breaking inner loop.")
                break

            start = page * 25
            search_url = f"{base_search_url}?keywords={formatted_job}&location={formatted_location}&start={start}"
            if time_param:
                search_url += f"&{time_param}"

            logger.info(f"[Job {job_id}] Requesting search page {page + 1}/{num_pages}: {search_url}")
            try:
                response = requests.get(search_url, headers=HEADERS, timeout=30)
                response.raise_for_status()
                soup = BeautifulSoup(response.content, "html.parser")
                job_cards = soup.find_all("div", class_="base-card")

                if not job_cards:
                    logger.info(
                        f"[Job {job_id}] No job cards found on page {page + 1} for '{job_title_query}'. Stopping search for this title."
                    )
                    break

                logger.info(
                    f"[Job {job_id}] Found {len(job_cards)} potential jobs on page {page + 1}. Processing..."
                )

                for card in job_cards:
                    jobs_processed_count += 1
                    if max_jobs is not None and jobs_scraped_count >= max_jobs:
                        break

                    job_data: Dict[str, Any] = {
                        "search_query": job_title_query,
                        "title": None,
                        "company": None,
                        "location": None,
                        "date_posted": None,
                        "url": None,
                        "extracted_skills": [],
                    }

                    title_element = card.find("h3", class_="base-search-card__title")
                    if title_element:
                        job_data["title"] = title_element.text.strip()

                    company_element = card.find("h4", class_="base-search-card__subtitle")
                    if company_element:
                        job_data["company"] = company_element.text.strip()

                    location_element = card.find("span", class_="job-search-card__location")
                    if location_element:
                        job_data["location"] = location_element.text.strip()

                    link_element = card.find("a", class_="base-card__full-link")
                    if not link_element:
                        link_element = card.find("a", href=True)
                    relative_url = link_element.get("href") if link_element else None
                    job_data["url"] = make_absolute_url(base_search_url, relative_url)

                    date_element = card.find("time", class_="job-search-card__listdate")
                    if date_element:
                        job_data["date_posted"] = date_element.get("datetime")
                    else:
                        date_element_alt = card.find("time", class_="job-search-card__listdate--new")
                        if date_element_alt:
                            job_data["date_posted"] = date_element_alt.get("datetime")

                    if job_data["url"]:
                        job_description = get_job_description(job_data["url"])
                        if job_description and llm_model:
                            job_data["extracted_skills"] = extract_skills_with_llm(job_description, llm_model)
                        elif not job_description:
                            logger.info(f"[Job {job_id}] Could not retrieve description for job: {job_data.get('title', 'N/A')}")
                    else:
                         logger.info(f"[Job {job_id}] Skipping description/skills fetch due to missing/invalid URL for job: {job_data.get('title', 'N/A')}")

                    if job_data.get("title") and job_data.get("company"):
                        all_jobs_data.append(job_data)
                        jobs_scraped_count += 1
                        logger.info(f"[Job {job_id}] Processed ({jobs_scraped_count}/{max_jobs or 'inf'}): {job_data['title']} at {job_data['company']}")
                        report_progress()
                    else:
                        logger.info(f"[Job {job_id}] Skipping card - missing essential info (title or company).")

                logger.info(f"[Job {job_id}] Finished processing page {page + 1} for '{job_title_query}'.")

            except requests.exceptions.Timeout:
                logger.error(f"[Job {job_id}] Timeout requesting search page {page + 1} for '{job_title_query}'. Stopping for this query.")
                break
            except requests.exceptions.RequestException as e:
                logger.error(f"[Job {job_id}] HTTP error scraping search page {page + 1} for '{job_title_query}': {e}. Stopping for this query.")
                break
            except Exception as e:
                logger.error(f"[Job {job_id}] An unexpected error occurred on search page {page + 1} for '{job_title_query}': {e}")

            if page < num_pages - 1:
                sleep_time = random.uniform(4.0, 9.0)
                logger.info(f"[Job {job_id}] Sleeping for {sleep_time:.1f} seconds before next page...")
                time.sleep(sleep_time)

    logger.info(f"[Job {job_id}] Scraping finished. Total jobs found: {len(all_jobs_data)}")
    return all_jobs_data

def save_to_json(data: List[Dict[str, Any]], filepath: str, job_id: str):
    """Saves data to a JSON file."""
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        logger.info(f"[Job {job_id}] Successfully saved {len(data)} jobs to {filepath}")
    except IOError as e:
        logger.error(f"[Job {job_id}] Could not write to file {filepath}: {e}")
        raise
    except Exception as e:
        logger.error(f"[Job {job_id}] An unexpected error occurred during JSON saving: {e}")
        raise


def process_scraping_job(job_data: Dict[str, Any], producer):
    """
    Handles a single scraping job received from Kafka.
    Sends status updates to KAFKA_STATUS_TOPIC.
    Sends loading requests to KAFKA_PROCESSING_TOPIC.
    Sends errors to KAFKA_NOTIFICATIONS_TOPIC.
    """
    job_id = job_data.get("job_id")
    parameters = job_data.get("parameters", {})

    if not job_id or not parameters:
        logger.error(f"Received invalid job data (missing job_id or parameters): {job_data}")
        return

    logger.info(f"[Job {job_id}] Received job request. Parameters: {parameters}")

    google_api_key = parameters.get("GOOGLE_API_KEY")
    job_titles_str = parameters.get("JOB_TITLES")
    location = parameters.get("LOCATION")
    time_filter = parameters.get("TIME_FILTER")
    num_pages_str = parameters.get("NUM_PAGES", "1")
    max_jobs_str = parameters.get("MAX_JOBS")
    output_dir = parameters.get("OUTPUT_DIR", "/app/data")

    if not job_titles_str or not location:
        error_msg = "Missing required parameters: JOB_TITLES and/or LOCATION."
        logger.error(f"[Job {job_id}] {error_msg}")
        send_kafka_message(producer, KAFKA_NOTIFICATIONS_TOPIC, {
            "job_id": job_id,
            "event_type": "job_failed",
            "source": "scraper",
            "error_details": error_msg
        }, job_id)
        return

    try:
        job_titles = [title.strip() for title in job_titles_str.split(',') if title.strip()]
        num_pages = int(num_pages_str)
        max_jobs = int(max_jobs_str) if max_jobs_str else None
        output_filename = f"{job_id}_jobs.json"
        output_path = os.path.join(output_dir, output_filename)
    except ValueError as e:
        error_msg = f"Invalid parameter format (NUM_PAGES or MAX_JOBS must be integers): {e}"
        logger.error(f"[Job {job_id}] {error_msg}")
        send_kafka_message(producer, KAFKA_NOTIFICATIONS_TOPIC, {
            "job_id": job_id,
            "event_type": "job_failed",
            "source": "scraper",
            "error_details": error_msg
        }, job_id)
        return

    llm_model = None
    llm_enabled = False
    if google_api_key:
        try:
            logger.info(f"[Job {job_id}] Configuring Google AI...")
            genai.configure(api_key=google_api_key)
            llm_model = genai.GenerativeModel("models/gemini-1.5-flash")
            logger.info(f"[Job {job_id}] Google AI Model configured successfully.")
            llm_enabled = True
        except Exception as e:
            logger.error(f"[Job {job_id}] Failed to configure Google AI: {e}. Skill extraction will be disabled for this job.")
            llm_enabled = False
            llm_model = None
    else:
        logger.info(f"[Job {job_id}] GOOGLE_API_KEY not provided in parameters. Skill extraction disabled.")

    def kafka_progress_reporter(percentage: float):
        logger.debug(f"[Job {job_id}] Progress update: {percentage:.2f}%")
        send_kafka_message(producer, KAFKA_STATUS_TOPIC, {
            "job_id": job_id,
            "event_type": "job_progress",
            "source": "scraper",
            "percentage": round(percentage, 2)
        }, job_id)

    # Execute Scraping
    try:
        logger.info(f"[Job {job_id}] Starting scrape process...")
        send_kafka_message(producer, KAFKA_STATUS_TOPIC, {
            "job_id": job_id,
            "event_type": "job_started",
            "source": "scraper"
        }, job_id)

        scraped_data = scrape_linkedin_jobs(
            job_id=job_id,
            job_titles=job_titles,
            location=location,
            time_filter=time_filter,
            num_pages=num_pages,
            max_jobs=max_jobs,
            llm_model=llm_model,
            progress_callback=kafka_progress_reporter
        )

        logger.info(f"[Job {job_id}] Scraping completed. Found {len(scraped_data)} jobs. Saving results...")

        save_to_json(scraped_data, output_path, job_id)

        logger.info(f"[Job {job_id}] Job completed successfully. Sending loading request.")

        # Send completion message to data-processing topic
        send_kafka_message(producer, KAFKA_PROCESSING_TOPIC, {
            "job_id": job_id,
            "event_type": "loading_requested",
            "source": "scraper"
        }, job_id)

    except Exception as e:
        error_msg = f"Scraping job failed: {type(e).__name__} - {e}"
        logger.exception(f"[Job {job_id}] {error_msg}")
        send_kafka_message(producer, KAFKA_NOTIFICATIONS_TOPIC, {
            "job_id": job_id,
            "event_type": "job_failed",
            "source": "scraper",
            "error_details": error_msg
        }, job_id)


# Main Execution Loop (Consumer)
def main_consumer_loop():
    logger.info("Initializing Kafka Producer...")
    producer = get_kafka_producer()
    if not producer:
        logger.error("Failed to initialize Kafka Producer. Exiting.")
        sys.exit(1)

    # Consumer *only* listens to the KAFKA_SCRAPING_TOPIC for job requests
    logger.info(f"Initializing Kafka Consumer for topic '{KAFKA_SCRAPING_TOPIC}' with group '{KAFKA_CONSUMER_GROUP_ID}'...")
    consumer = get_kafka_consumer(KAFKA_SCRAPING_TOPIC, KAFKA_CONSUMER_GROUP_ID)
    if not consumer:
        logger.error("Failed to initialize Kafka Consumer. Exiting.")
        if producer:
             producer.close()
        sys.exit(1)

    logger.info("Scraper service started. Waiting for job requests on Kafka topic...")

    try:
        for message in consumer:
            try:
                logger.info(f"Received message: Topic='{message.topic}', Partition={message.partition}, Offset={message.offset}")
                job_data = message.value

                if isinstance(job_data, dict) and job_data.get("event_type") == "job_requested":
                     process_scraping_job(job_data, producer)
                else:
                    logger.warning(f"Skipping unexpected message type on {KAFKA_SCRAPING_TOPIC}: {job_data}")

            except json.JSONDecodeError:
                logger.error(f"Failed to decode JSON message: {message.value}")
            except Exception as e:
                logger.error(f"Error processing Kafka message from {KAFKA_SCRAPING_TOPIC}: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.info("Consumer loop interrupted by user (KeyboardInterrupt). Shutting down.")
    except Exception as e:
        logger.error(f"Critical error in Kafka consumer loop: {e}", exc_info=True)
    finally:
        logger.info("Closing Kafka Consumer and Producer...")
        if consumer:
            consumer.close()
        if producer:
            producer.close(timeout=10)
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    main_consumer_loop()