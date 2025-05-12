import asyncio
import logging
import os
import re
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse
import json

import httpx
from bs4 import BeautifulSoup
import html2text

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

# --- Markdown Cleaning Function ---
# Numba is not suitable for direct acceleration of this function
# because it heavily relies on Python's 're' module for regular expressions.
# Numba's @jit decorator does not natively support the 're' module and
# would compile this function in "object mode," providing no significant
# performance benefit and potentially adding overhead.
def clean_markdown(md_text: str) -> str:
    """
    Cleans Markdown content by removing inline links, footnotes, URLs, images,
    bold/italic formatting, blockquotes, empty headings, and compacting whitespace.
    """
    # Remove inline links (e.g., [text](url)) and keep only the text
    md_text = re.sub(r'\[([^\]]+)\]\((http[s]?://[^\)]+)\)', r'\1', md_text)
    
    # Remove standalone URLs (not part of a link)
    md_text = re.sub(r'http[s]?://\S+', '', md_text)
    
    # Remove images (e.g., ![alt text](url))
    md_text = re.sub(r'!\[([^\]]*)\]\((http[s]?://[^\)]+)\)', '', md_text)
    
    # Remove footnote references (e.g., [^1])
    md_text = re.sub(r'\[\^?\d+\]', '', md_text)
    
    # Remove footnote definitions (e.g., [^1]: some text) at the start of a line
    md_text = re.sub(r'^\s*\[\^?\d+\]:\s?.*$', '', md_text, flags=re.MULTILINE)
    
    # Remove blockquotes (lines starting with > )
    md_text = re.sub(r'^\s{0,3}>\s?', '', md_text, flags=re.MULTILINE)
    
    # Remove bold/italic formatting (e.g., **text** or _text_)
    md_text = re.sub(r'(\*\*|__)(.*?)\1', r'\2', md_text) # Bold
    md_text = re.sub(r'(\*|_)(.*?)\1', r'\2', md_text)    # Italic
    
    # Remove empty headings (e.g., lines that are just '##' or '###')
    md_text = re.sub(r'^\s*#+\s*$', '', md_text, flags=re.MULTILINE)
    
    # Remove empty parentheses sometimes left after URL removal
    md_text = re.sub(r'\(\)', '', md_text)
    
    # Compact multiple newlines into at most two newlines (single blank line)
    md_text = re.sub(r'\n\s*\n+', '\n\n', md_text)
    
    # Compact multiple spaces into single spaces (useful after removing formatting)
    md_text = re.sub(r'[ \t]+', ' ', md_text)
    
    # Strip leading/trailing whitespace from the entire text
    return md_text.strip()


class CrawlRequest(BaseModel):
    """
    Request model for the web crawling endpoint.
    """
    url: HttpUrl = Field(..., example="https://books.toscrape.com/") # The starting URL for the crawl
    max_pages: int = Field(3000, ge=1, example=100)  # Maximum number of pages to crawl.
    concurrency_limit: int = Field(8, ge=1, le=50, example=8) # Number of parallel requests
    # New: Optional output directory in the payload
    output_directory: Optional[str] = Field(
        None,
        example="my_crawl_results",
        description="Optional. The path to the directory where markdown files will be saved. "
                    "Can be absolute (e.g., /Users/victor/my_output) or relative (e.g., my_results_folder). "
                    "If not provided, defaults to 'crawl_output2' within the script's directory."
    )

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
        self.crawled_results: List[Dict[str, str]] = []
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
                self.visited_urls.add(url)
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
            abs_url = urljoin(base_url, href)
            if urlparse(abs_url).netloc == urlparse(self.start_url).netloc:
                parsed_abs_url = urlparse(abs_url)
                cleaned_url = parsed_abs_url._replace(fragment="").geturl()
                links.add(cleaned_url)
        return links

    def _html_to_markdown(self, html_content: str) -> str:
        # This method uses html2text, which is an external library.
        # Numba cannot accelerate calls to external libraries.
        return self.html_to_markdown.handle(html_content)

    def _sanitize_filename(self, url: str) -> str:
        """
        Sanitizes a URL to create a valid filename.
        Replaces invalid characters with underscores.
        """
        # This function uses re.sub, making it unsuitable for Numba acceleration.
        cleaned_url = re.sub(r'^(https?://)?(www\.)?', '', url)
        sanitized_name = re.sub(r'[^a-zA-Z0-9_.-]', '_', cleaned_url)
        max_filename_len = 200
        if len(sanitized_name) > max_filename_len:
            hash_suffix = str(abs(hash(url)))[:8]
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
            return 0, []

        async with httpx.AsyncClient() as client:
            tasks = deque()
            
            # Pre-seed the queue with the initial URL
            initial_url = str(self.start_url) # Ensure it's a string
            if initial_url not in self.visited_urls:
                self.to_visit_queue.append(initial_url)


            async def _worker_task_logic():
                while self.to_visit_queue and len(self.crawled_results) < self.max_pages:
                    current_url = None
                    try:
                        current_url = self.to_visit_queue.popleft()
                    except IndexError: # Queue is empty
                        break

                    if current_url in self.visited_urls:
                        continue
                    
                    html_content = await self._fetch_page_content(client, current_url)
                    if html_content is None:
                        continue

                    markdown_content = self._html_to_markdown(html_content)
                    cleaned_markdown_content = clean_markdown(markdown_content) # Apply cleaning here!
                    
                    # Save markdown to file
                    try:
                        filename = self._sanitize_filename(current_url)
                        file_path = self.output_dir / filename
                        with open(file_path, "w", encoding="utf-8") as f:
                            f.write(cleaned_markdown_content)
                        saved_files_paths.append(str(file_path))
                        self.crawled_results.append({'url': current_url, 'markdown': cleaned_markdown_content})
                        logger.info(f"Saved cleaned markdown for {current_url} to {file_path}")
                    except IOError as e:
                        error_msg = f"Failed to save markdown for {current_url} to {file_path}: {e}"
                        logger.error(error_msg, exc_info=True)
                        self.errors.append(error_msg)
                    except Exception as e:
                        error_msg = f"Unexpected error saving markdown for {current_url}: {e}"
                        logger.error(error_msg, exc_info=True)
                        self.errors.append(error_msg)

                    # Extract new links if we haven't reached max_pages after current page
                    if len(self.crawled_results) < self.max_pages:
                        new_links = self._extract_links(html_content, current_url)
                        for link in new_links:
                            if link not in self.visited_urls and link not in self.to_visit_queue:
                                self.to_visit_queue.append(link)
            
            # This is a more robust way to manage worker tasks for a crawler.
            # It ensures that new tasks are continuously spawned as long as
            # there are URLs to visit and the max_pages limit hasn't been reached.
            active_workers = [
                asyncio.create_task(_worker_task_logic()) 
                for _ in range(self.concurrency_limit)
            ]

            # Keep adding new workers if necessary and manage existing ones
            while self.to_visit_queue and len(self.crawled_results) < self.max_pages:
                # Clean up finished workers
                active_workers = [worker for worker in active_workers if not worker.done()]
                
                # Spawn new workers up to the concurrency limit
                while len(active_workers) < self.concurrency_limit and self.to_visit_queue:
                    active_workers.append(asyncio.create_task(_worker_task_logic()))
                
                if not active_workers and self.to_visit_queue: # All workers done, but queue still has items (shouldn't happen often)
                    break # Exit to prevent infinite loop if workers get stuck

                await asyncio.sleep(0.1) # Give time for tasks to run

            # Wait for any remaining active tasks to complete gracefully
            await asyncio.gather(*active_workers, return_exceptions=True)

        # Save metadata.json with crawled URLs
        try:
            metadata = {"crawled_urls": [result['url'] for result in self.crawled_results]}
            metadata_file_path = self.output_dir / "metadata.json"
            with open(metadata_file_path, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            logger.info(f"Saved metadata to {metadata_file_path}")
        except IOError as e:
            error_msg = f"Failed to save metadata.json to {metadata_file_path}: {e}"
            logger.error(error_msg, exc_info=True)
            self.errors.append(error_msg)
        except Exception as e:
            error_msg = f"Unexpected error saving metadata.json to {metadata_file_path}: {e}"
            logger.error(error_msg, exc_info=True)
            self.errors.append(error_msg)

        return len(self.crawled_results), saved_files_paths


@app.post("/crawl_all_markdowns", response_model=CrawlResponse)
async def crawl_all_markdowns(request: CrawlRequest):
    """
    Crawls a website, retrieves markdown content for every page (up to max_pages),
    and saves each page's CLEANED markdown to the specified output directory.

    The output directory can be specified in the payload. If not, it defaults
    to a 'crawl_output2' folder in the same directory as the script.
    """
    logger.info(f"Received request to crawl all markdowns for URL: {request.url}, max_pages: {request.max_pages}")

    # Determine the output directory based on the request payload
    if request.output_directory:
        # User provided a path, use it directly. Convert to Path object and resolve to absolute path.
        # This handles both absolute and relative paths provided by the user.
        final_output_dir = Path(request.output_directory).resolve()
    else:
        # No output directory specified in payload, use the script's parent directory + default name
        final_output_dir = Path(__file__).parent / "crawl_output2"

    logger.info(f"Output directory for this crawl: {final_output_dir}")

    # Initialize the custom web crawler with the determined output_dir
    crawler = CustomWebCrawler(
        start_url=str(request.url),
        max_pages=request.max_pages,
        concurrency_limit=request.concurrency_limit,
        output_dir=final_output_dir # Pass the resolved output directory
    )

    try:
        total_pages_crawled, saved_files_paths = await crawler.crawl()

        if not crawler.errors and total_pages_crawled > 0:
            message = f"Successfully crawled {total_pages_crawled} pages. Cleaned markdown files saved."
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
            # Raise HTTPException with the structured response detail for 500 errors
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