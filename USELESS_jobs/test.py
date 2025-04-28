import asyncio
import os
import json
import re
import logging
from urllib.parse import urljoin, urlparse
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig

# Set up logging
logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configuration
TARGET_URL = "https://fr.indeed.com/jobs?q=d%C3%A9veloppeur&l=Paris"
OUTPUT_DIR = "indeed_jobs_data"
OUTPUT_JSON = os.path.join(OUTPUT_DIR, "jobs.json")
DEBUG_MARKDOWN_DIR = os.path.join(OUTPUT_DIR, "debug_markdown")
RESPECT_ROBOTS = True
USE_JAVASCRIPT = True
MAX_PAGES = 50
REQUEST_DELAY = 5.0  # Increased to avoid bot detection
RELEVANT_PATHS = ('/jobs', '/viewjob', '/rc/clk')  # Added /rc/clk for job links

async def save_debug_markdown(url, markdown_content):
    """Save markdown content for debugging."""
    os.makedirs(DEBUG_MARKDOWN_DIR, exist_ok=True)
    filename = re.sub(r'[<>:"/\\|?*]', '_', urlparse(url).path or "index") + ".md"
    filepath = os.path.join(DEBUG_MARKDOWN_DIR, filename)
    try:
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(f"# Source URL: {url}\n\n{markdown_content}\n")
        logger.info(f"Saved debug markdown to {filepath}")
    except OSError as e:
        logger.error(f"Failed to save debug markdown {filepath}: {e}")

async def extract_job_data(markdown, url):
    """Extract job details from markdown content."""
    job_data = {}
    try:
        # Extract job title
        title_match = re.search(r'#+\s*(.+?)(?:\n|$)', markdown, re.I)
        job_data['title'] = title_match.group(1).strip() if title_match else "Unknown"

        # Extract company name
        company_match = re.search(r'(?:Company|Entreprise|Posted by):?\s*([^\n]+)', markdown, re.I)
        job_data['company'] = company_match.group(1).strip() if company_match else "Unknown"

        # Extract location
        location_match = re.search(r'(?:Location|Lieu):?\s*([^\n]+)', markdown, re.I)
        job_data['location'] = location_match.group(1).strip() if location_match else "Unknown"

        # Extract job description
        description_match = re.search(r'(?:Description|Job description|Mission)[^\n]*\n([\s\S]+?)(?=(?:#+\s|\Z))', markdown, re.I)
        job_data['description'] = description_match.group(1).strip() if description_match else ""

        # Add source URL
        job_data['url'] = url

        # Clean up description
        job_data['description'] = re.sub(r'\n{2,}', '\n', job_data['description']).strip()

        return job_data if job_data['title'] != "Unknown" else None
    except Exception as e:
        logger.error(f"Error extracting job data from {url}: {e}")
        return None

async def crawl_website(url, output_dir, use_js, respect_robots_txt, relevant_paths, max_pages, delay):
    """Crawl website and extract job data into JSON."""
    os.makedirs(output_dir, exist_ok=True)
    jobs = []
    visited_urls = set()
    queue = asyncio.Queue()
    await queue.put(url)
    visited_urls.add(url)
    base_domain = urlparse(url).netloc
    pages_crawled = 0

    # Crawler configuration with anti-bot headers
    run_config = CrawlerRunConfig(
        cache_mode="BYPASS",
        exclude_external_links=True,
        exclude_social_media_links=True,
        user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        extra_headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Referer": "https://fr.indeed.com/"
        },
        playwright_options={
            "timeout": 60000,  # 60 seconds timeout
            "wait_until": "networkidle"  # Wait for network to be idle
        }
    )

    while not queue.empty() and pages_crawled < max_pages:
        current_url = await queue.get()
        pages_crawled += 1
        logger.info(f"Processing URL {pages_crawled}/{max_pages}: {current_url}")

        # Polite delay
        if pages_crawled > 1:
            await asyncio.sleep(delay)

        async with AsyncWebCrawler(
            verbose=True,
            respect_robots_txt=respect_robots_txt,
            use_playwright=use_js
        ) as crawler:
            try:
                result = await crawler.arun(url=current_url, config=run_config)

                if result.success and result.markdown:
                    markdown_content = result.markdown.raw_markdown
                    logger.info(f"Markdown length: {len(markdown_content)} chars for {current_url}")

                    # Save markdown for debugging
                    await save_debug_markdown(current_url, markdown_content)

                    # Extract job data if it's a job detail page
                    if '/viewjob' in current_url or '/rc/clk' in current_url:
                        job_data = await extract_job_data(markdown_content, current_url)
                        if job_data:
                            jobs.append(job_data)
                            logger.info(f"Extracted job: {job_data['title']} from {current_url}")

                    # Log and queue internal links
                    internal_links = result.links.get("internal", [])
                    logger.debug(f"Found {len(internal_links)} internal links on {current_url}")
                    for link_data in internal_links:
                        href = link_data.get("href") if isinstance(link_data, dict) else str(link_data)
                        if not href:
                            continue

                        absolute_url = urljoin(current_url, href)
                        absolute_url = urlparse(absolute_url)._replace(fragment="").geturl()
                        parsed_link = urlparse(absolute_url)

                        # Allow /jobs links with query parameters (e.g., pagination)
                        is_valid = (
                            parsed_link.scheme in ['http', 'https'] and
                            parsed_link.netloc.endswith(base_domain) and
                            (
                                any(parsed_link.path.startswith(prefix) for prefix in relevant_paths) or
                                (parsed_link.path.startswith('/jobs') and parsed_link.query)  # Include pagination
                            )
                        )

                        logger.debug(f"Link: {absolute_url} | Valid: {is_valid} | Path: {parsed_link.path} | Query: {parsed_link.query}")
                        if is_valid and absolute_url not in visited_urls:
                            await queue.put(absolute_url)
                            visited_urls.add(absolute_url)
                            logger.info(f"Queued: {absolute_url}")

                elif result.error_message:
                    logger.error(f"Failed to crawl {current_url}: {result.error_message}")

            except Exception as e:
                logger.exception(f"Exception during crawling {current_url}: {e}")

        queue.task_done()

    # Save jobs to JSON
    try:
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(jobs, f, indent=2, ensure_ascii=False)
        logger.info(f"Saved {len(jobs)} jobs to {OUTPUT_JSON}")
    except OSError as e:
        logger.error(f"Failed to save JSON file {OUTPUT_JSON}: {e}")

    logger.info(f"Crawl completed. Processed {pages_crawled} pages, extracted {len(jobs)} jobs.")

async def main():
    await crawl_website(
        url=TARGET_URL,
        output_dir=OUTPUT_DIR,
        use_js=USE_JAVASCRIPT,
        respect_robots_txt=RESPECT_ROBOTS,
        relevant_paths=RELEVANT_PATHS,
        max_pages=MAX_PAGES,
        delay=REQUEST_DELAY
    )

if __name__ == "__main__":
    asyncio.run(main())