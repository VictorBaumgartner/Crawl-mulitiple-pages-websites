import asyncio
import logging
import os
import re
import json
from collections import deque
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse
import numpy as np
import numba
from bs4 import BeautifulSoup
import html2text
import httpx
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl, Field

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="FastAPI Web Crawler",
    description="An API to crawl static websites, convert HTML to markdown, and save results. Optimized with Numba for faster processing.",
    version="1.0.0"
)

# --- Numba-Optimized Helper Functions ---
@numba.jit(nopython=True, cache=True)
def compact_whitespace(text: np.ndarray, max_newlines: int = 2) -> np.ndarray:
    """
    Compacts multiple newlines and spaces in a character array to at most max_newlines consecutive newlines
    and single spaces, returning a new array.
    """
    result = np.zeros(len(text) + 1, dtype=np.uint8)  # +1 for safety
    result_idx = 0
    newline_count = 0
    space_count = 0
    i = 0

    while i < len(text):
        char = text[i]
        
        if char == ord('\n'):
            if newline_count < max_newlines:
                result[result_idx] = char
                result_idx += 1
            newline_count += 1
            space_count = 0
        elif char == ord(' ') or char == ord('\t'):
            if space_count == 0 and result_idx > 0 and result[result_idx - 1] != ord('\n'):
                result[result_idx] = ord(' ')
                result_idx += 1
            space_count += 1
            newline_count = 0
        else:
            result[result_idx] = char
            result_idx += 1
            newline_count = 0
            space_count = 0
        i += 1

    return result[:result_idx]

@numba.jit(nopython=True, cache=True)
def sanitize_chars(text: np.ndarray, valid_chars: np.ndarray) -> np.ndarray:
    """
    Replaces invalid characters in a text array with '_', keeping only valid_chars.
    """
    result = np.copy(text)
    for i in range(len(result)):
        is_valid = False
        for valid_char in valid_chars:
            if result[i] == valid_char:
                is_valid = True
                break
        if not is_valid:
            result[i] = ord('_')
    return result

# --- Markdown Cleaning Function ---
def clean_markdown(md_text: str) -> str:
    """
    Cleans Markdown content by removing inline links, footnotes, URLs, images,
    bold/italic formatting, blockquotes, empty headings, and compacting whitespace.
    Uses Numba for whitespace compaction and character processing.
    """
    # Regex-based removals (Numba doesn't support regex)
    md_text = re.sub(r'\[([^\]]+)\]\((http[s]?://[^\)]+)\)', r'\1', md_text)  # Inline links
    md_text = re.sub(r'http[s]?://\S+', '', md_text)  # Standalone URLs
    md_text = re.sub(r'!\[([^\]]*)\]\((http[s]?://[^\)]+)\)', '', md_text)  # Images
    md_text = re.sub(r'\[\^?\d+\]', '', md_text)  # Footnote references
    md_text = re.sub(r'^\s*\[\^?\d+\]:\s?.*$', '', md_text, flags=re.MULTILINE)  # Footnote defs
    md_text = re.sub(r'^\s{0,3}>\s?', '', md_text, flags=re.MULTILINE)  # Blockquotes
    md_text = re.sub(r'(\*\*|__)(.*?)\1', r'\2', md_text)  # Bold
    md_text = re.sub(r'(\*|_)(.*?)\1', r'\2', md_text)  # Italic
    md_text = re.sub(r'^\s*#+\s*$', '', md_text, flags=re.MULTILINE)  # Empty headings
    md_text = re.sub(r'\(\)', '', md_text)  # Empty parentheses

    # Convert to NumPy array for Numba processing
    text_array = np.frombuffer(md_text.encode('utf-8'), dtype=np.uint8)
    
    # Compact whitespace using Numba
    compacted_array = compact_whitespace(text_array)
    
    # Convert back to string and strip
    result = compacted_array.tobytes().decode('utf-8', errors='ignore').strip()
    
    return result


class CrawlRequest(BaseModel):
    """
    Request model for the web crawling endpoint.
    """
    url: HttpUrl = Field(..., example="http://quotes.toscrape.com/")  # Starting URL
    max_pages: int = Field(3000, ge=1, example=100)  # Max pages to crawl
    concurrency_limit: int = Field(4, ge=1, le=20, example=4)  # Parallel requests
    output_directory: Optional[str] = Field(
        None,
        example="my_crawl_results",
        description="Optional. Path to save markdown files and metadata.json. "
                    "Can be absolute or relative. Defaults to 'crawl_output2'."
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
        self.http_client = httpx.AsyncClient(follow_redirects=True, timeout=10.0)
        # Valid characters for filenames (used in Numba)
        self.valid_filename_chars = np.array([ord(c) for c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-'], dtype=np.uint8)

    async def close_client(self):
        """Close the HTTP client."""
        await self.http_client.aclose()
        logger.info("Closed HTTP client")

    async def _fetch_page_content(self, url: str) -> Optional[str]:
        """Fetch page content using httpx."""
        try:
            async with self.semaphore:
                logger.info(f"Fetching: {url}")
                response = await self.http_client.get(url)
                if response.status_code in [403, 429]:
                    error_msg = f"Access denied at {url}: HTTP {response.status_code}"
                    logger.warning(error_msg)
                    self.errors.append(error_msg)
                    return None
                response.raise_for_status()
                self.visited_urls.add(url)
                return response.text
        except httpx.TimeoutException as exc:
            error_msg = f"Timeout error fetching {url}: {exc}"
            logger.error(error_msg)
            self.errors.append(error_msg)
            return None
        except httpx.HTTPStatusError as exc:
            error_msg = f"HTTP error fetching {url}: {exc}"
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
        return self.html_to_markdown.handle(html_content)

    def _sanitize_filename(self, url: str) -> str:
        """
        Sanitizes a URL to create a valid filename using Numba for character replacement.
        """
        # Initial cleaning with regex
        cleaned_url = re.sub(r'^(https?://)?(www\.)?', '', url)
        
        # Convert to NumPy array for Numba
        url_array = np.frombuffer(cleaned_url.encode('utf-8'), dtype=np.uint8)
        
        # Sanitize characters using Numba
        sanitized_array = sanitize_chars(url_array, self.valid_filename_chars)
        sanitized_name = sanitized_array.tobytes().decode('utf-8', errors='ignore')
        
        # Truncate and append hash if too long
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

        try:
            async def _worker_task_logic():
                while self.to_visit_queue and len(self.crawled_results) < self.max_pages:
                    try:
                        current_url = self.to_visit_queue.popleft()
                    except IndexError:  # Queue is empty
                        break

                    if current_url in self.visited_urls:
                        continue

                    html_content = await self._fetch_page_content(current_url)
                    if html_content is None:
                        continue

                    markdown_content = self._html_to_markdown(html_content)
                    cleaned_markdown_content = clean_markdown(markdown_content)

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

                    # Extract new links if we haven't reached max_pages
                    if len(self.crawled_results) < self.max_pages:
                        new_links = self._extract_links(html_content, current_url)
                        for link in new_links:
                            if link not in self.visited_urls and link not in self.to_visit_queue:
                                self.to_visit_queue.append(link)

            # Manage worker tasks
            active_workers = [
                asyncio.create_task(_worker_task_logic())
                for _ in range(self.concurrency_limit)
            ]

            while self.to_visit_queue and len(self.crawled_results) < self.max_pages:
                active_workers = [worker for worker in active_workers if not worker.done()]
                while len(active_workers) < self.concurrency_limit and self.to_visit_queue:
                    active_workers.append(asyncio.create_task(_worker_task_logic()))
                if not active_workers and self.to_visit_queue:
                    break
                await asyncio.sleep(0.1)

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

        finally:
            await self.close_client()

        return len(self.crawled_results), saved_files_paths


@app.post("/crawl_all_markdowns", response_model=CrawlResponse)
async def crawl_all_markdowns(request: CrawlRequest):
    """
    Crawls a website, retrieves markdown content for every page (up to max_pages),
    and saves each page's CLEANED markdown to the specified output directory.
    Uses httpx for static content fetching and Numba for optimized processing.
    """
    logger.info(f"Received request to crawl all markdowns for URL: {request.url}, max_pages: {request.max_pages}")

    # Determine the output directory
    if request.output_directory:
        final_output_dir = Path(request.output_directory).resolve()
    else:
        final_output_dir = Path(__file__).parent / "crawl_output2"

    logger.info(f"Output directory for this crawl: {final_output_dir}")

    # Initialize the crawler
    crawler = CustomWebCrawler(
        start_url=str(request.url),
        max_pages=request.max_pages,
        concurrency_limit=request.concurrency_limit,
        output_dir=final_output_dir
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
            raise HTTPException(
                status_code=500,
                detail=CrawlResponse(
                    success=False,
                    message=message,
                    total_pages_crawled=total_pages_crawled,
                    saved_files=saved_files_paths,
                    errors=crawler.errors
                ).model_dump()
            )

    except Exception as e:
        logger.exception(f"An unexpected error occurred during the crawl for {request.url}")
        raise HTTPException(
            status_code=500,
            detail=f"An internal server error occurred during crawl setup: {str(e)}"
        )