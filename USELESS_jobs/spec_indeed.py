import re
import urllib.parse
from crawl4ai import AsyncWebCrawler
from crawl4ai.models import CrawlResult
import asyncio
import json
import uuid
from bs4 import BeautifulSoup

async def parse_user_prompt(prompt):
    """Parse user prompt to extract job title, location, and job type."""
    job_title_match = re.search(r"(developer|engineer|programmer)", prompt, re.IGNORECASE)
    location_match = re.search(r"in\s+([A-Za-z\s]+?)(?:\s+in\s+|$)", prompt, re.IGNORECASE)
    job_type_match = re.search(r"(internship|full-time|part-time|contract)", prompt, re.IGNORECASE)

    job_title = job_title_match.group(1).lower() if job_title_match else "developer"
    location = location_match.group(1).strip() if location_match else "Paris"
    job_type = job_type_match.group(1).lower() if job_type_match else "internship"

    return job_title, location, job_type

async def construct_search_url(job_title, location, job_type):
    """Construct Indeed search URL based on parameters."""
    base_url = "https://www.indeed.fr/jobs"  # Use .fr for Paris; adjust for other regions
    query_params = {
        "q": job_title,
        "l": location,
        "jt": job_type
    }
    encoded_params = urllib.parse.urlencode(query_params)
    return f"{base_url}?{encoded_params}"

async def extract_job_details(crawler, job_url):
    """Extract details from a single job listing page."""
    result = await crawler.arun(url=job_url, css_selector=".jobsearch-JobComponent")
    if not result.success or not result.html:
        print(f"Failed to fetch job page: {job_url}")
        return None

    try:
        # Parse HTML with BeautifulSoup
        soup = BeautifulSoup(result.html, "html.parser")
        title = soup.select_one("h1.jobsearch-JobInfoHeader-title").text.strip() if soup.select_one("h1.jobsearch-JobInfoHeader-title") else ""
        company = soup.select_one("div[data-testid='inlinecompanyname']").text.strip() if soup.select_one("div[data-testid='inlinecompanyname']") else ""
        location = soup.select_one("div[data-testid='job-location']").text.strip() if soup.select_one("div[data-testid='job-location']") else ""
        description = soup.select_one("div#jobDescriptionText").text.strip() if soup.select_one("div#jobDescriptionText") else ""
        posted_date = soup.select_one("span[data-testid='posted-date']").text.strip() if soup.select_one("span[data-testid='posted-date']") else ""

        return {
            "job_id": str(uuid.uuid4()),
            "title": title,
            "company": company,
            "location": location,
            "description": description,
            "posted_date": posted_date,
            "url": job_url
        }
    except Exception as e:
        print(f"Error extracting job details from {job_url}: {e}")
        return None

async def scrape_indeed_jobs(prompt):
    """Main function to scrape Indeed jobs based on user prompt."""
    # Initialize Crawl4AI crawler with verbose output
    crawler = AsyncWebCrawler(verbose=True)
    
    # Parse user prompt
    job_title, location, job_type = await parse_user_prompt(prompt)
    print(f"Parsed prompt: Job Title={job_title}, Location={location}, Job Type={job_type}")

    # Construct initial search URL
    search_url = await construct_search_url(job_title, location, job_type)
    print(f"Starting crawl at: {search_url}")

    # Store all job listings
    all_jobs = []
    page = 0
    max_pages = 10  # Limit for demo; adjust as needed

    while page < max_pages:
        # Add pagination parameter
        paginated_url = f"{search_url}&start={page * 10}"
        result = await crawler.arun(url=paginated_url, css_selector=".jobsearch-ResultsList")

        if not result.success or not result.html:
            print(f"Failed to crawl page {page + 1}")
            break

        # Parse search results page
        soup = BeautifulSoup(result.html, "html.parser")
        job_cards = soup.select("a[data-testid='job-title']")
        if not job_cards:
            print("No more jobs found; exiting pagination.")
            break

        for job_card in job_cards:
            job_url = job_card.get("href")
            if job_url:
                # Ensure full URL
                if job_url.startswith("/"):
                    job_url = f"https://www.indeed.fr{job_url}"
                
                # Scrape job details
                job_details = await extract_job_details(crawler, job_url)
                if job_details:
                    all_jobs.append(job_details)
                    print(f"Scraped job: {job_details['title']} at {job_details['company']}")

        page += 1
        # Respectful crawling: delay between pages
        await asyncio.sleep(2)

    return all_jobs

async def main():
    prompt = "I am looking for developer job offers in Paris in Internship"
    jobs = await scrape_indeed_jobs(prompt)
    
    # Save results to JSON
    with open(r"C:\Users\victo\Desktop\crawl\indeed_jobs.json", "w", encoding="utf-8") as f:
        json.dump(jobs, f, indent=2, ensure_ascii=False)
    
    print(f"Scraped {len(jobs)} jobs. Results saved to indeed_jobs.json")

if __name__ == "__main__":
    asyncio.run(main())