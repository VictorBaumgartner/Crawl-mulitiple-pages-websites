import asyncio
import logging
import os
import re
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse

import httpx
from bs4 import BeautifulSoup
import html2text # For HTML to Markdown conversion

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl, Field

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Advanced FastAPI Web Crawler",
    description="An API to crawl websites, retrieve markdown for every page, and save them to a specified folder.",
    version="1.0.0"
)

# --- Output Directory Configuration ---
# WARNING: Writing to /crawl_output2/ directly usually requires elevated permissions (sudo).
# It is highly recommended to use a user-writable directory like:
# output_base_dir = Path.home() / "crawl_output2" # In user's home directory
# OR
# output_base_dir = Path(__file__).parent / "crawl_output2" # Relative to script location
output_base_dir = Path("/crawl_output2") # As specifically requested by the user

class CrawlRequest(BaseModel):
    """
    Request model for the web crawling endpoint.
    """
    url: HttpUrl = Field(..., example="https://books.toscrape.com/") # The starting URL for the crawl
    max_pages: int = Field(3000, ge=1, example=100)  # Maximum number of pages to crawl.
    concurrency_limit: int = Field(8, ge=1, le=50, example=8) # Number of parallel requests

class CrawlResponse(BaseModel):
    """
    Response model for the web crawling endpoint.
    """
    success: bool
    message: str
    total_pages_crawled: int
    saved_files: List[str]
    errors: List[str] = []


class CustomWebCrawler:
    def __init__(self, start_url: str, max_pages: int, concurrency_limit: int, output_dir: Path):
        self.start_url = start_url
        self.max_pages = max_pages
        self.concurrency_limit = concurrency_limit
        self.output_dir = output_dir
        self.visited_urls: Set[str] = set()
        self.to_visit_queue: deque[str] = deque([start_url])
        self.semaphore = asyncio.Semaphore(self.concurrency_limit)
        self.crawled_results: List[Dict[str, str]] = [] # Stores {'url': 'markdown'}
        self.errors: List[str] = []
        self.html_to_markdown = html2text.HTML2Text()
        self.html_to_markdown.ignore_links = False
        self.html_to_markdown.ignore_images = False


    async def _fetch_page_content(self, client: httpx.AsyncClient, url: str) -> Optional[str]:
        try:
            async with self.semaphore:
                logger.info(f"Fetching: {url}")
                response = await client.get(url, follow_redirects=True, timeout=30)
                response.raise_for_status()
                self.visited_urls.add(url) # Add to visited after successful fetch
                return response.text
        except httpx.RequestError as exc:
            error_msg = f"HTTPX Request Error for {url}: {exc}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return None
        except Exception as exc:
            error_msg = f"Unexpected error fetching {url}: {exc}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return None

    def _extract_links(self, html_content: str, base_url: str) -> Set[str]:
        soup = BeautifulSoup(html_content, 'html.parser')
        links = set()
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            # Resolve relative URLs to absolute URLs
            abs_url = urljoin(base_url, href)
            # Only consider links within the same domain as the start_url
            if urlparse(abs_url).netloc == urlparse(self.start_url).netloc:
                # Basic sanitization to remove fragments
                parsed_abs_url = urlparse(abs_url)
                cleaned_url = parsed_abs_url._replace(fragment="").geturl()
                links.add(cleaned_url)
        return links

    def _html_to_markdown(self, html_content: str) -> str:
        # html2text converts HTML to Markdown
        return self.html_to_markdown.handle(html_content)

    def _sanitize_filename(self, url: str) -> str:
        """
        Sanitizes a URL to create a valid filename.
        Replaces invalid characters with underscores.
        """
        # Remove scheme (http/https) and common prefixes like www.
        cleaned_url = re.sub(r'^(https?://)?(www\.)?', '', url)
        # Replace invalid characters with underscores
        sanitized_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', cleaned_url)
        # Ensure it's not too long for filesystem limits (common limit ~255 chars)
        max_filename_len = 200
        if len(sanitized_name) > max_filename_len:
            hash_suffix = str(abs(hash(url)))[:8] # Use a short hash for uniqueness
            sanitized_name = sanitized_name[:max_filename_len - len(hash_suffix) - 1] + "_" + hash_suffix
        return sanitized_name + ".md"


    async def crawl(self) -> Tuple[int, List[str]]:
        saved_files_paths: List[str] = []

        # Ensure output directory exists
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Ensured output directory exists: {self.output_dir}")
        except OSError as e:
            error_msg = f"Failed to create output directory {self.output_dir}: {e}"
            logger.error(error_msg, exc_info=True)
            self.errors.append(error_msg)
            return 0, [] # Return 0 pages crawled and empty list if directory creation fails

        # Use httpx.AsyncClient for session management and connection pooling
        async with httpx.AsyncClient() as client:
            while self.to_visit_queue and len(self.crawled_results) < self.max_pages:
                current_url = self.to_visit_queue.popleft()

                # Skip if already visited or if it's already added to queue for processing
                if current_url in self.visited_urls:
                    continue

                # Fetch content
                html_content = await self._fetch_page_content(client, current_url)
                if html_content is None:
                    continue

                # Convert to markdown and store
                markdown_content = self._html_to_markdown(html_content)
                
                # Save markdown to file
                try:
                    filename = self._sanitize_filename(current_url)
                    file_path = self.output_dir / filename
                    with open(file_path, "w", encoding="utf-8") as f:
                        f.write(markdown_content)
                    saved_files_paths.append(str(file_path))
                    self.crawled_results.append({'url': current_url, 'markdown': markdown_content})
                    logger.info(f"Saved markdown for {current_url} to {file_path}")
                except IOError as e:
                    error_msg = f"Failed to save markdown for {current_url} to {file_path}: {e}"
                    logger.error(error_msg, exc_info=True)
                    self.errors.append(error_msg)
                except Exception as e:
                    error_msg = f"Unexpected error saving markdown for {current_url}: {e}"
                    logger.error(error_msg, exc_info=True)
                    self.errors.append(error_msg)

                # Extract new links if we haven't reached max_pages
                if len(self.crawled_results) < self.max_pages:
                    new_links = self._extract_links(html_content, current_url)
                    for link in new_links:
                        if link not in self.visited_urls and link not in self.to_visit_queue:
                            self.to_visit_queue.append(link)
        
        # Return count of successfully crawled pages and paths of saved files
        return len(self.crawled_results), saved_files_paths


@app.post("/crawl_all_markdowns", response_model=CrawlResponse)
async def crawl_all_markdowns(request: CrawlRequest):
    """
    Crawls a website, retrieves markdown content for every page (up to max_pages),
    and saves each page's markdown to the specified output directory.
    """
    logger.info(f"Received request to crawl all markdowns for URL: {request.url}, max_pages: {request.max_pages}")

    # Initialize the custom web crawler
    crawler = CustomWebCrawler(
        start_url=str(request.url),
        max_pages=request.max_pages,
        concurrency_limit=request.concurrency_limit,
        output_dir=output_base_dir
    )

    try:
        total_pages_crawled, saved_files_paths = await crawler.crawl()

        if not crawler.errors and total_pages_crawled > 0:
            message = f"Successfully crawled {total_pages_crawled} pages. Markdown files saved."
            logger.info(message)
            return CrawlResponse(
                success=True,
                message=message,
                total_pages_crawled=total_pages_crawled,
                saved_files=saved_files_paths,
                errors=crawler.errors
            )
        else:
            message = f"Crawl completed with {total_pages_crawled} pages crawled. Some errors occurred or no pages were saved."
            if crawler.errors:
                message += f" See errors for details."
            logger.warning(message)
            raise HTTPException(
                status_code=500,
                detail=CrawlResponse(
                    success=False,
                    message=message,
                    total_pages_crawled=total_pages_crawled,
                    saved_files=saved_files_paths,
                    errors=crawler.errors
                ).model_dump() # Use model_dump for Pydantic v2
            )

    except Exception as e:
        logger.exception(f"An unexpected error occurred during the crawl for {request.url}")
        raise HTTPException(
            status_code=500,
            detail=f"An internal server error occurred during crawl setup: {str(e)}"
        )

# --- How to Run This Application ---
# 1. Save the code: Save the code above as `main.py` in your project directory.
# 2. Install dependencies:
#    pip install "fastapi[all]" uvicorn httpx beautifulsoup4 html2text
# 3. Run the FastAPI application using Uvicorn:
#    uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
#    (To potentially avoid PermissionError for /crawl_output2/, you might need to run:
#     sudo uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
#     However, using a user-writable directory (e.g., `Path.home() / "crawl_output2"`)
#     is strongly preferred for security and ease of use.)

# --- Example Usage with curl (from another terminal) ---
# To crawl a website (e.g., a documentation site or blog) and save all page markdowns:
# curl -X POST "http://localhost:8000/crawl_all_markdowns" \
#      -H "Content-Type: application/json" \
#      -d '{"url": "https://www.datacamp.com/blog/", "max_pages": 50}'

# For a local example:
# curl -X POST "http://localhost:8000/crawl_all_markdowns" \
#      -H "Content-Type: application/json" \
#      -d '{"url": "http://quotes.toscrape.com/", "max_pages": 20}'

# --- API Documentation ---
# Once the server is running, you can access the interactive API documentation
# (Swagger UI) in your web browser at: http://localhost:8000/docs