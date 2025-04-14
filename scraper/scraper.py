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
from urllib.parse import urljoin

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
        time.sleep(random.uniform(1.5, 4.0))
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
        main_content = soup.find("main") or soup.find("body")
        if main_content:
            text = main_content.get_text(separator=" ", strip=True)
            if len(text) > 200:
                logger.info(f"Using fallback text extraction for {job_url}")
                return text
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

    Job Description (first 10000 chars):
    ---
    {description_text[:10000]}
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
            prompt,
            generation_config=generation_config,
            safety_settings=safety_settings,
            request_options={'timeout': 60}
        )

        response_text = response.text.strip()
        if response_text.startswith("```json"):
            response_text = response_text[7:]
        if response_text.endswith("```"):
            response_text = response_text[:-3]
        response_text = response_text.strip()

        json_start = response_text.find("[")
        json_end = response_text.rfind("]")

        if json_start != -1 and json_end != -1 and json_start < json_end:
            json_string = response_text[json_start : json_end + 1]
            try:
                skills_list = json.loads(json_string)
                if isinstance(skills_list, list):
                    cleaned_skills = sorted(list(set([
                        str(skill).strip()
                        for skill in skills_list
                        if str(skill).strip() and len(str(skill).strip()) > 1
                    ])))
                    logger.info(f"Extracted {len(cleaned_skills)} unique skills.")
                    return cleaned_skills
                else:
                    logger.warning(f"LLM response JSON was not a list: {json_string}")
                    return []
            except json.JSONDecodeError as json_err:
                logger.error(f"Failed to decode JSON from LLM response: {json_err}")
                logger.error(f"Attempted JSON String: {json_string}")
                logger.error(f"Original LLM Response Text: {response.text.strip()}")
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
    if '?' in relative_url:
        relative_url = relative_url.split('?')[0]
    if relative_url.startswith("http"):
        return relative_url
    if relative_url.startswith("/"):
        return urljoin(base_url, relative_url)
    logger.warning(f"Could not make relative URL absolute: {relative_url} with base {base_url}")
    return None

# Main Scraping Function
def scrape_linkedin_jobs(
    job_id: str,
    job_titles: List[str],
    location: str,
    time_filter: Optional[str],
    num_pages: int,
    max_jobs: int,
    llm_model: Optional[genai.GenerativeModel],
    progress_callback: Callable[[float, str], None]
) -> List[Dict[str, Any]]:
    """
    Scrapes LinkedIn jobs, reports descriptive progress, and uses LLM if provided.
    """
    all_jobs_data: List[Dict[str, Any]] = []
    time_param = get_time_filter_param(time_filter)
    base_search_url = "https://www.linkedin.com/jobs/search/"
    jobs_scraped_count = 0
    total_jobs_to_scrape = max_jobs

    logger.info(f"[Job {job_id}] Starting scrape for {len(job_titles)} titles, location='{location}', pages={num_pages}, max_jobs={max_jobs}")
    progress_callback(5.0, "Searching for jobs...")

    search_interrupted = False

    for job_title_query in job_titles:
        if search_interrupted:
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
            if jobs_scraped_count >= total_jobs_to_scrape:
                logger.info(f"[Job {job_id}] Reached max_jobs limit ({total_jobs_to_scrape}). Stopping search.")
                search_interrupted = True
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

                for card_index, card in enumerate(job_cards):
                    if jobs_scraped_count >= total_jobs_to_scrape:
                        logger.info(f"[Job {job_id}] Reached max_jobs limit during card processing.")
                        search_interrupted = True
                        break

                    job_data: Dict[str, Any] = {
                        "search_query": job_title_query,
                        "title": None,
                        "company": None,
                        "location": None,
                        "date_posted": None,
                        "url": None,
                        "description": None,
                        "extracted_skills": [],
                    }

                    # Extract basic info
                    title_element = card.find("h3", class_="base-search-card__title")
                    job_data["title"] = title_element.text.strip() if title_element else "N/A"

                    company_element = card.find("h4", class_="base-search-card__subtitle")
                    job_data["company"] = company_element.text.strip() if company_element else "N/A"

                    location_element = card.find("span", class_="job-search-card__location")
                    job_data["location"] = location_element.text.strip() if location_element else None

                    link_element = card.find("a", class_="base-card__full-link")
                    if not link_element:
                         link_element = card.find("a", href=True)
                    relative_url = link_element.get("href") if link_element else None
                    job_data["url"] = make_absolute_url(base_search_url, relative_url)

                    date_element = card.find("time", class_="job-search-card__listdate")
                    if date_element:
                        job_data["date_posted"] = date_element.get("datetime") or date_element.text.strip()
                    else:
                        date_element_alt = card.find("time")
                        if date_element_alt:
                            job_data["date_posted"] = date_element_alt.get("datetime") or date_element_alt.text.strip()

                    # Check if essential data was found before adding and counting
                    if job_data.get("title") != "N/A" and job_data.get("company") != "N/A" and job_data.get("url"):
                        all_jobs_data.append(job_data)
                        jobs_scraped_count += 1

                        # Calculate progress percentage (scaled between 5% and 90%)
                        percentage = 5.0 + min(85.0, (jobs_scraped_count / total_jobs_to_scrape) * 85.0)
                        percentage = round(percentage, 2)

                        # Create descriptive message
                        progress_message = f"Processing job {jobs_scraped_count}/{total_jobs_to_scrape}: {job_data['title']}"
                        logger.info(f"[Job {job_id}] {progress_message} ({percentage}%)")

                        progress_callback(percentage, progress_message)
                    else:
                        logger.warning(f"[Job {job_id}] Skipping card - missing essential info (Title='{job_data['title']}', Company='{job_data['company']}', URL='{job_data['url']}').")

                    # Fetch Description and Skills
                    if job_data["url"]:
                        description_text = get_job_description(job_data["url"])
                        job_data["description"] = description_text
                        if description_text and llm_model:
                            job_data["extracted_skills"] = extract_skills_with_llm(description_text, llm_model)
                        elif not description_text:
                             logger.info(f"[Job {job_id}] Could not retrieve description for job: {job_data['title']} at {job_data['company']}")
                        time.sleep(random.uniform(0.5, 1.5))
                    else:
                         logger.info(f"[Job {job_id}] Skipping description/skills fetch due to missing/invalid URL for job: {job_data['title']} at {job_data['company']}")

                logger.info(f"[Job {job_id}] Finished processing page {page + 1} for '{job_title_query}'.")

            except requests.exceptions.Timeout:
                logger.error(f"[Job {job_id}] Timeout requesting search page {page + 1} for '{job_title_query}'. Stopping for this query.")
                break
            except requests.exceptions.RequestException as e:
                logger.error(f"[Job {job_id}] HTTP error scraping search page {page + 1} for '{job_title_query}': {e}. Stopping for this query.")
                break
            except Exception as e:
                logger.error(f"[Job {job_id}] An unexpected error occurred on search page {page + 1} for '{job_title_query}': {e}", exc_info=True)

            if page < num_pages - 1 and not search_interrupted:
                sleep_time = random.uniform(3.0, 7.0)
                logger.info(f"[Job {job_id}] Sleeping for {sleep_time:.1f} seconds before next page...")
                time.sleep(sleep_time)

        if search_interrupted:
              break

    logger.info(f"[Job {job_id}] Scraping finished. Total jobs collected: {len(all_jobs_data)}")
    progress_callback(90.0, f"Finished data scraping.")

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
    Handles a single scraping job, sends detailed progress updates.
    """
    job_id = job_data.get("job_id")
    parameters = job_data.get("parameters", {})

    if not job_id or not parameters:
        logger.error(f"Received invalid job data (missing job_id or parameters): {job_data}")
        return

    logger.info(f"[Job {job_id}] Received job request. Parameters: {parameters}")

    # Kafka Progress Reporter
    def kafka_progress_reporter(percentage: float, description: str):
        logger.debug(f"[Job {job_id}] Progress update: {percentage:.2f}% - {description}")
        send_kafka_message(producer, KAFKA_STATUS_TOPIC, {
            "job_id": job_id,
            "event_type": "job_progress",
            "source": "scraper",
            "percentage": round(percentage, 2),
            "description": description
        }, job_id)

    # Send initial progress message
    kafka_progress_reporter(0.0, "Initializing...")

    # Extract parameters
    google_api_key = parameters.get("GOOGLE_API_KEY")
    job_titles_str = parameters.get("JOB_TITLES")
    location = parameters.get("LOCATION")
    time_filter = parameters.get("TIME_FILTER")
    num_pages_str = parameters.get("NUM_PAGES", "1")
    max_jobs_str = parameters.get("MAX_JOBS")
    output_dir = parameters.get("OUTPUT_DIR", "/app/data")

    # Parameter Validation
    if not job_titles_str or not location or not max_jobs_str:
        error_msg = "Missing required parameters: JOB_TITLES, LOCATION, and MAX_JOBS are required."
        logger.error(f"[Job {job_id}] {error_msg}")
        send_kafka_message(producer, KAFKA_NOTIFICATIONS_TOPIC, {
            "job_id": job_id, "event_type": "job_failed", "source": "scraper",
            "error_details": error_msg
        }, job_id)
        kafka_progress_reporter(0.0, f"Failed: {error_msg}")
        return

    try:
        job_titles = [title.strip() for title in job_titles_str.split(',') if title.strip()]
        num_pages = int(num_pages_str)
        max_jobs = int(max_jobs_str)
        if max_jobs <= 0:
            raise ValueError("MAX_JOBS must be a positive integer.")
        output_filename = f"{job_id}_jobs.json"
        output_path = os.path.join(output_dir, output_filename)
    except ValueError as e:
        error_msg = f"Invalid parameter format (NUM_PAGES/MAX_JOBS must be positive integers): {e}"
        logger.error(f"[Job {job_id}] {error_msg}")
        send_kafka_message(producer, KAFKA_NOTIFICATIONS_TOPIC, {
            "job_id": job_id, "event_type": "job_failed", "source": "scraper",
            "error_details": error_msg
        }, job_id)
        kafka_progress_reporter(0.0, f"Failed: {error_msg}")
        return

    # Configure LLM
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
            logger.warning(f"[Job {job_id}] Failed to configure Google AI: {e}. Skill extraction will be disabled.")
            llm_enabled = False
            llm_model = None
    else:
        logger.info(f"[Job {job_id}] GOOGLE_API_KEY not provided. Skill extraction disabled.")


    # Execute Scraping
    try:
        logger.info(f"[Job {job_id}] Starting scrape process...")
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

        logger.info(f"[Job {job_id}] Scraping process function finished. Saving results...")
        save_to_json(scraped_data, output_path, job_id)
        logger.info(f"[Job {job_id}] Scraper job phase completed. Sending loading request.")

        # Send request to data-processing topic
        send_kafka_message(producer, KAFKA_PROCESSING_TOPIC, {
            "job_id": job_id,
            "event_type": "loading_requested",
            "source": "scraper",
            "data_path": output_path
        }, job_id)

    except Exception as e:
        error_msg = f"Scraping job failed during execution: {type(e).__name__} - {e}"
        logger.exception(f"[Job {job_id}] {error_msg}")
        # Send failure notification
        send_kafka_message(producer, KAFKA_NOTIFICATIONS_TOPIC, {
            "job_id": job_id,
            "event_type": "job_failed",
            "source": "scraper",
            "error_details": error_msg
        }, job_id)
        kafka_progress_reporter(0.0, f"Failed: {error_msg}")


# Main Consumer Loop
def main_consumer_loop():
    logger.info("Initializing Kafka Producer...")
    producer = get_kafka_producer()
    if not producer:
        logger.error("Failed to initialize Kafka Producer. Exiting.")
        sys.exit(1)

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
                    logger.warning(f"Skipping unexpected message format or event_type on {KAFKA_SCRAPING_TOPIC}: Type={type(job_data)}, Data='{str(job_data)[:200]}...'") # Log type and snippet

            except Exception as e:
                job_id_in_error = "unknown"
                if isinstance(message.value, dict):
                    job_id_in_error = message.value.get("job_id", "unknown")
                logger.error(f"Error processing Kafka message (Job ID: {job_id_in_error}) from {KAFKA_SCRAPING_TOPIC}: {e}", exc_info=True)


    except KeyboardInterrupt:
        logger.info("Consumer loop interrupted by user (KeyboardInterrupt). Shutting down.")
    except Exception as e:
        logger.error(f"Critical error in Kafka consumer loop: {e}", exc_info=True)
    finally:
        logger.info("Closing Kafka Consumer and Producer...")
        if consumer:
            try:
                consumer.close()
            except Exception as ce:
                logger.error(f"Error closing Kafka consumer: {ce}")
        if producer:
            try:
                producer.flush(timeout=10)
                producer.close(timeout=10)
            except Exception as pe:
                 logger.error(f"Error closing Kafka producer: {pe}")
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    main_consumer_loop()