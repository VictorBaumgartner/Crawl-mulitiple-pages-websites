import asyncio
import os
import json
import re
from urllib.parse import urljoin, urlparse
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
import logging # Import logging

# Configure basic logging
# Logs will appear in your terminal when you run the FastAPI app with uvicorn
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI()

class ScrapingRequest(BaseModel):
    url: str
    folder_name: str


def clean_markdown(md_text):
    """
    Cleans Markdown content by:
    - Removing inline links, footnotes, raw URLs, images
    - Removing bold, italic, blockquotes
    - Removing empty headings
    - Compacting whitespace
    """
    # Remove Markdown links but keep the text
    md_text = re.sub(r'\[([^\]]+)\]\((http[s]?://[^\)]+)\)', r'\1', md_text)

    # Remove raw URLs
    md_text = re.sub(r'http[s]?://\S+', '', md_text)

    # Remove images
    md_text = re.sub(r'!\[([^\]]*)\]\((http[s]?://[^\)]+)\)', '', md_text)

    # Remove footnotes
    md_text = re.sub(r'\[\^?\d+\]', '', md_text)
    md_text = re.sub(r'^\[\^?\d+\]:\s?.*$', '', md_text, flags=re.MULTILINE)

    # Remove blockquotes
    md_text = re.sub(r'^\s{0,3}>\s?', '', md_text, flags=re.MULTILINE)

    # Remove bold and italic formatting (**bold**, *italic*, __underline__, _italic_)
    md_text = re.sub(r'(\*\*|__)(.*?)\1', r'\2', md_text)
    md_text = re.sub(r'(\*|_)(.*?)\1', r'\2', md_text)

    # Remove empty headings (lines that are only '#', '##', etc.)
    md_text = re.sub(r'^\s*#+\s*$', '', md_text, flags=re.MULTILINE)

    # Remove leftover empty parentheses - risky, but keeping based on original code intent
    md_text = re.sub(r'\(\)', '', md_text)

    # Compact multiple empty lines into one
    md_text = re.sub(r'\n\s*\n+', '\n\n', md_text)

    # Clean extra spaces
    md_text = re.sub(r'[ \t]+', ' ', md_text)

    return md_text.strip()


async def crawl_website(start_url, output_dir, max_concurrency=8):
    """
    Crawl a website deeply and save each page as a cleaned Markdown file, with parallelization.
    """
    os.makedirs(output_dir, exist_ok=True)
    md_generator = DefaultMarkdownGenerator(
        options={
            "ignore_links": True,  # Links are extracted separately, no need for them in Markdown
            "escape_html": True,
            "body_width": 0
        }
    )

    config = CrawlerRunConfig(
        markdown_generator=md_generator,
        cache_mode="BYPASS",
        exclude_external_links=True, # Important for staying within the domain
        exclude_social_media_links=True,
    )

    visited_urls = set()
    # queued_urls helps prevent adding the same URL multiple times before it's visited
    queued_urls = set()

    crawl_queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(max_concurrency)

    def sanitize_filename(url):
        parsed = urlparse(url)
        # Use netloc and path to create a unique filename
        netloc = parsed.netloc.replace(".", "_").replace(":", "_")
        path = parsed.path.strip("/").replace("/", "_")
        query = parsed.query.replace("=", "_").replace("&", "_")
        fragment = parsed.fragment.replace("=", "_") # Often ignored for content, but can differentiate

        filename_parts = [netloc]
        if path:
            filename_parts.append(path)
        if query:
             # Add a separator before query part
             filename_parts.append(f"query_{query}")
        if fragment:
             # Add a separator before fragment part
             filename_parts.append(f"fragment_{fragment}")

        filename = "_".join(filename_parts) or "index" # Default to index if parts are empty

        # Clean any remaining invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)

        # Ensure filename doesn't start or end with _ from sanitization
        filename = filename.strip('_') or "index"

        # Add .md extension
        filename = f"{filename}.md"

        # Truncate to a reasonable length to avoid OS limits
        return filename[:250]


    async def crawl_page(current_url):
        if current_url in visited_urls:
            logger.info(f"Skipping visited: {current_url}")
            return

        # Add to visited *before* crawling to prevent other workers from adding it
        visited_urls.add(current_url)
        logger.info(f"Crawling ({len(visited_urls)} visited): {current_url}")

        # Acquire semaphore before fetching/processing the page content
        async with semaphore:
            try:
                async with AsyncWebCrawler(verbose=True) as crawler: # verbose=True will add crawler logs
                    result = await crawler.arun(url=current_url, config=config)

                if result.success:
                    markdown_content = result.markdown.raw_markdown

                    # Clean the Markdown before saving
                    cleaned_markdown = clean_markdown(markdown_content)

                    filename = sanitize_filename(current_url)
                    output_path = os.path.join(output_dir, filename)

                    try:
                        with open(output_path, "w", encoding="utf-8") as f:
                            # Add original URL as a header for context
                            f.write(f"# {current_url}\n\n{cleaned_markdown}\n")
                        logger.info(f"Saved cleaned Markdown to: {output_path}")
                    except IOError as e:
                        logger.error(f"Failed to save file {output_path}: {e}")


                    internal_links = result.links.get("internal", [])
                    start_netloc = urlparse(start_url).netloc # Get netloc once for comparison

                    for link in internal_links:
                        href = link.get("href")
                        if not href:
                            continue # Skip links with no href

                        # Construct absolute URL safely
                        absolute_url = urljoin(current_url, href)

                        # Parse the absolute URL once
                        parsed_absolute_url = urlparse(absolute_url)

                        # Simple checks to avoid common non-content links or fragments
                        if parsed_absolute_url.scheme not in ['http', 'https']:
                             logger.debug(f"Skipping non-http/https link: {absolute_url}")
                             continue
                        # Skip fragment-only links on the same page, they don't add new content
                        if parsed_absolute_url.fragment and not parsed_absolute_url.path and urlparse(current_url).path == parsed_absolute_url.path:
                             logger.debug(f"Skipping fragment-only link on same page: {absolute_url}")
                             continue

                        # Normalize URL: remove fragment for queuing, keep path/query
                        normalized_url = urljoin(absolute_url, parsed_absolute_url.path)
                        if parsed_absolute_url.query:
                            normalized_url += "?" + parsed_absolute_url.query


                        # Check if it's on the same domain as the start URL
                        if parsed_absolute_url.netloc == start_netloc:
                            # Check if already visited or queued *before* adding
                            if normalized_url not in visited_urls and normalized_url not in queued_urls:
                                logger.debug(f"Queueing new link: {normalized_url}")
                                # Add to queued_urls set and put in queue
                                queued_urls.add(normalized_url)
                                # Use await put - blocks if queue maxsize is reached, but default is infinite
                                await crawl_queue.put(normalized_url)
                            #else:
                                #logger.debug(f"Already visited or queued: {normalized_url}")


                        else:
                            logger.debug(f"Skipping external link: {absolute_url} (domain: {parsed_absolute_url.netloc})")

                else:
                    logger.error(f"Failed to crawl {current_url}: {result.error_message}")

            except Exception as e:
                # Catch any unexpected errors during page processing
                logger.error(f"An error occurred while processing {current_url}: {e}", exc_info=True)


    async def worker():
        """Worker function to process URLs from the queue."""
        while True: # Keep the worker running indefinitely until cancelled
            try:
                # Get an item from the queue. This will block if the queue is empty.
                # The timeout is optional, but can help prevent workers from hanging forever
                # in edge cases if the queue logic somehow gets stuck.
                # For standard crawls, removing timeout is fine; join() handles completion.
                current_url = await crawl_queue.get() # Removed timeout to rely solely on queue.join()

                # Process the URL
                await crawl_page(current_url)

            except asyncio.CancelledError:
                # Task was cancelled, break the loop
                logger.info(f"Worker {asyncio.current_task().get_name()} received cancellation signal. Exiting.")
                break
            except Exception as e:
                 # Catch unexpected errors in the worker loop itself
                 logger.error(f"Unexpected error in worker {asyncio.current_task().get_name()}: {e}", exc_info=True)
                 # Decide if you want the worker to stop on unexpected errors.
                 # Breaking might prevent infinite error loops.
                 break
            finally:
                # This is crucial: Indicate that the currently processed task is done.
                # This is how queue.join() knows when all tasks are finished.
                crawl_queue.task_done()


    # --- Main crawling orchestration ---

    # Add the starting URL to the queue and the set
    # Ensure it's not already queued/visited from a previous run if cache was used (not in this config)
    # or if the same URL was somehow added via another path immediately.
    normalized_start_url = urljoin(start_url, urlparse(start_url).path)
    if urlparse(start_url).query:
         normalized_start_url += "?" + urlparse(start_url).query

    if normalized_start_url not in queued_urls and normalized_start_url not in visited_urls:
         logger.info(f"Adding initial URL to queue: {normalized_start_url}")
         await crawl_queue.put(normalized_start_url)
         queued_urls.add(normalized_start_url)
    else:
         logger.info(f"Initial URL already queued or visited: {normalized_start_url}")


    # Create and start worker tasks
    tasks = []
    for i in range(max_concurrency):
        # Give tasks names for easier debugging in logs
        task = asyncio.create_task(worker(), name=f"worker-{i}")
        tasks.append(task)
        logger.info(f"Started worker task {task.get_name()}")


    # Wait for the queue to be empty and all tasks to call task_done()
    # This will block until all initially added URLs and all subsequently added URLs
    # by the workers have been retrieved from the queue and task_done() has been called.
    logger.info("Waiting for crawl queue to finish...")
    await crawl_queue.join()
    logger.info("Crawl queue is empty and all tasks are done.")

    # Cancel worker tasks as they are likely blocked on queue.get()
    logger.info("Cancelling worker tasks...")
    for task in tasks:
        task.cancel()

    # Wait for the cancelled tasks to finish their cleanup (enter the except CancelledError block)
    # Using gather with return_exceptions=True handles potential errors during cancellation
    # and prevents the program from stopping if a task raises an unhandled exception during cancellation.
    try:
        await asyncio.gather(*tasks, return_exceptions=True)
        logger.info("All worker tasks confirmed cancelled.")
    except Exception as e:
         # This might catch exceptions from gather itself if return_exceptions=False,
         # but with return_exceptions=True, it mostly ensures we don't crash here.
         logger.error(f"Error while gathering tasks after cancellation: {e}")


    # Save metadata
    metadata_path = os.path.join(output_dir, "metadata.json")
    try:
        with open(metadata_path, "w", encoding="utf-8") as f:
            # Save both visited and queued URLs for potential future reference
            # queued_urls_at_end should ideally be empty if join() completed successfully
            json.dump({"crawled_urls": list(visited_urls), "queued_urls_at_end": list(queued_urls)}, f, indent=2)
        logger.info(f"Metadata saved to {metadata_path}")
    except IOError as e:
         logger.error(f"Failed to save metadata file {metadata_path}: {e}")


@app.post("/scrape")
async def scrape_website(request: ScrapingRequest):
    """
    FastAPI endpoint to trigger the website scraping process.
    Takes a URL and a folder name for output.
    """
    logger.info(f"Received scraping request for URL: {request.url}, folder: {request.folder_name}")
    try:
        target_url = request.url
        folder_name = request.folder_name
        # Using 'crawl_output' as a base directory relative to where the script runs
        output_dir = os.path.join("crawl_output", folder_name) # Changed base folder name back to 'crawl_output' as per initial structure

        # Basic URL validation
        parsed_url = urlparse(target_url)
        if not parsed_url.scheme or not parsed_url.netloc:
             logger.warning(f"Invalid URL format received: {target_url}")
             raise HTTPException(status_code=400, detail=f"Invalid URL format: {target_url}. Please provide a full URL including scheme (http/https).")

        # Start the scraping process
        # The await here means the API endpoint will wait for the entire crawl to finish
        await crawl_website(start_url=target_url, output_dir=output_dir)

        logger.info(f"Scraping completed successfully for {target_url}")
        return {"status": "success", "message": f"Scraping completed for {target_url}. Output saved to {output_dir}"}
    except HTTPException as http_exc:
        # Re-raise HTTP exceptions (like the 400 for invalid URL)
        raise http_exc
    except Exception as e:
        # Catch any other unexpected errors, log them, and return a 500 error
        logger.error(f"An unhandled error occurred during scraping: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"An internal server error occurred: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    # Uvicorn will handle the main event loop and run the FastAPI app.
    # Uvicorn's output combined with the logging configured above will show the process.
    logger.info("Starting Uvicorn server...")
    uvicorn.run(app, host="0.0.0.0", port=8000)