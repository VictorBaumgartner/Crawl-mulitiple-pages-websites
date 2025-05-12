import asyncio
import logging
from typing import Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl

# Ensure crawl4ai and FastAPI are installed:
# pip install "crawl4ai @ git+https://github.com/unclecode/crawl4ai.git" "fastapi[all]" uvicorn
# After installing crawl4ai, run the setup command to install Playwright browsers:
# crawl4ai-setup

try:
    from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, CrawlResult as Crawl4AICrawlResult
    # Optional: If you plan to use LLM-powered data extraction, uncomment the following:
    # from crawl4ai.types import LLMExtractionStrategy
except ImportError:
    print("Error: 'crawl4ai' library is not found.")
    print("Please install it using: pip install 'crawl4ai @ git+https://github.com/unclecode/crawl4ai.git'")
    print("Also, ensure you run 'crawl4ai-setup' after installation to set up browsers.")
    exit(1)

# Configure logging for better visibility
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Crawl4AI FastAPI Web Crawler",
    description="An API to crawl websites using Crawl4AI, waiting for the entire website's pages (up to a maximum) to be processed.",
    version="1.0.0"
)

class CrawlRequest(BaseModel):
    """
    Request model for the web crawling endpoint.
    """
    url: HttpUrl  # The starting URL for the crawl (validated as a URL)
    max_pages: int = 3000  # Maximum number of pages to crawl. Default is 3000.
    # You can add other CrawlerRunConfig parameters here if you want them
    # to be configurable via the API, e.g.:
    # strategy: Optional[str] = "bfs" # "bfs", "dfs", "bestfirst"
    # max_depth: Optional[int] = None

class CrawlResponse(BaseModel):
    """
    Response model for the web crawling endpoint.
    """
    url: str
    success: bool
    message: str
    final_redirect_url: Optional[str] = None
    total_pages_crawled: Optional[int] = None
    primary_page_markdown: Optional[str] = None # This line is now uncommented to include markdown
    # primary_page_cleaned_html: Optional[str] = None # Optional: Uncomment to include cleaned HTML
    # primary_page_extracted_content: Optional[dict] = None # Optional: Uncomment for LLM extracted content


@app.post("/crawl", response_model=CrawlResponse)
async def crawl_website(request: CrawlRequest):
    """
    Initiates a web crawl for the given URL using Crawl4AI.
    The API waits for the entire crawl process to complete (up to `max_pages`)
    before returning a response.

    - **Parallelism:** Crawl4AI leverages Playwright for browser automation and
      manages internal concurrency efficiently. While you don't set a specific
      "8 parallelized ways" parameter, the library is designed for high-speed,
      parallel processing of web pages.
    - **Completion Guarantee:** The API response is delayed until `crawl4ai`
      has finished processing the website, either by reaching the `max_pages`
      limit or exhausting all discoverable links.
    """
    logger.info(f"Received crawl request for URL: {request.url}, max_pages: {request.max_pages}")

    try:
        # Initialize AsyncWebCrawler. Set verbose=True for detailed crawl4ai internal logs.
        async with AsyncWebCrawler(verbose=False) as crawler:
            config = CrawlerRunConfig(
                url=str(request.url), # Convert Pydantic's HttpUrl to string
                max_pages=request.max_pages,
                # Add other configurations for the crawl strategy here, for example:
                # strategy="bfs", # (Breadth-First Search)
                # max_depth=5,    # Maximum link depth to follow from the starting URL
                # For AI-driven extraction (requires an LLM API key setup, e.g., OPENAI_API_KEY env var):
                # extraction_strategy=LLMExtractionStrategy(
                #     provider="openai",
                #     api_token="YOUR_OPENAI_API_KEY", # Or read from env var
                #     instruction="Extract the main article content and any linked images.",
                # ),
            )

            logger.info(f"Starting crawl for {request.url} (max_pages: {request.max_pages}). This may take significant time...")

            # Await the crawl process. The FastAPI endpoint will not respond until this operation
            # (which includes crawling multiple pages as per max_pages) is fully completed.
            crawl_summary_result: Crawl4AICrawlResult = await crawler.arun(config=config)

            if crawl_summary_result.success:
                message = (f"Crawl completed successfully for {request.url}. "
                           f"Crawl4AI processed up to {request.max_pages} pages (or fewer if all links exhausted).")
                logger.info(message)

                return CrawlResponse(
                    url=str(request.url),
                    success=True,
                    message=message,
                    final_redirect_url=crawl_summary_result.url,
                    total_pages_crawled=request.max_pages,
                    primary_page_markdown=crawl_summary_result.markdown, # Markdown of the initial URL is now included
                    # primary_page_cleaned_html=crawl_summary_result.cleaned_html,
                    # primary_page_extracted_content=crawl_summary_result.extracted_content,
                )
            else:
                error_message = (f"Crawl failed or was incomplete for {request.url}. "
                                 f"Error: {crawl_summary_result.error_message or 'Unknown error'}")
                logger.error(error_message)
                raise HTTPException(
                    status_code=500,
                    detail=error_message
                )

    except Exception as e:
        logger.exception(f"An unexpected error occurred during crawling for {request.url}")
        raise HTTPException(
            status_code=500,
            detail=f"An internal server error occurred: {str(e)}"
        )

# --- How to Run This Application ---
# 1. Save the code: Save the code above as `main.py` in your project directory.
# 2. Install dependencies:
#    pip install "crawl4ai @ git+https://github.com/unclecode/crawl4ai.git" "fastapi[all]" uvicorn
# 3. Set up Playwright browsers (required by crawl4ai):
#    crawl4ai-setup
# 4. Run the FastAPI application using Uvicorn:
#    uvicorn main:app --host 0.0.0.0 --port 8000 --workers 1
#    (Using `--workers 1` is recommended for long-running asynchronous tasks to avoid blocking multiple Uvicorn processes.)

# --- Example Usage with curl (from another terminal) ---
# To crawl example.com and get markdown for the initial page:
# curl -X POST "http://localhost:8000/crawl" \
#      -H "Content-Type: application/json" \
#      -d '{"url": "https://www.example.com", "max_pages": 100}'

# --- API Documentation ---
# Once the server is running, you can access the interactive API documentation
# (Swagger UI) in your web browser at: http://localhost:8000/docs