# from fastapi import FastAPI, HTTPException
# from pydantic import BaseModel, HttpUrl
# import asyncio
# import os
# import json
# import re
# from urllib.parse import urljoin, urlparse
# from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
# from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
# from typing import Optional

# app = FastAPI()

# class CrawlRequest(BaseModel):
#     start_url: HttpUrl
#     output_dir: str = "./crawl_output2"
#     max_concurrency: int = 8
#     max_depth: int = 2

# def clean_markdown(md_text: str) -> str:
#     """
#     Cleans Markdown content by:
#     - Removing inline links, footnotes, raw URLs, images
#     - Removing bold, italic, blockquotes
#     - Removing empty headings
#     - Compacting whitespace
#     """
#     md_text = re.sub(r'\[([^\]]+)\]\((http[s]?://[^\)]+)\)', r'\1', md_text)
#     md_text = re.sub(r'http[s]?://\S+', '', md_text)
#     md_text = re.sub(r'!\[([^\]]*)\]\((http[s]?://[^\)]+)\)', '', md_text)
#     md_text = re.sub(r'\[\^?\d+\]', '', md_text)
#     md_text = re.sub(r'^\[\^?\d+\]:\s?.*$', '', md_text, flags=re.MULTILINE)
#     md_text = re.sub(r'^\s{0,3}>\s?', '', md_text, flags=re.MULTILINE)
#     md_text = re.sub(r'(\*\*|__)(.*?)\1', r'\2', md_text)
#     md_text = re.sub(r'(\*|_)(.*?)\1', r'\2', md_text)
#     md_text = re.sub(r'^\s*#+\s*$', '', md_text, flags=re.MULTILINE)
#     md_text = re.sub(r'\(\)', '', md_text)
#     md_text = re.sub(r'\n\s*\n+', '\n\n', md_text)
#     md_text = re.sub(r'[ \t]+', ' ', md_text)
#     return md_text.strip()

# async def crawl_website(start_url: str, output_dir: str, max_concurrency: int, max_depth: int):
#     """
#     Crawl a website deeply and save each page as a cleaned Markdown file, with parallelization.
#     """
#     try:
#         os.makedirs(output_dir, exist_ok=True)

#         md_generator = DefaultMarkdownGenerator(
#             options={
#                 "ignore_links": True,
#                 "escape_html": True,
#                 "body_width": 0
#             }
#         )

#         config = CrawlerRunConfig(
#             markdown_generator=md_generator,
#             cache_mode="BYPASS",
#             exclude_external_links=True,
#             exclude_social_media_links=True,
#         )

#         visited_urls = set()
#         queued_urls = set()
#         crawl_queue = asyncio.Queue()
#         semaphore = asyncio.Semaphore(max_concurrency)
#         crawl_queue.put_nowait((start_url, 0))

#         def sanitize_filename(url: str) -> str:
#             parsed = urlparse(url)
#             path = parsed.path.strip("/").replace("/", "_") or "index"
#             netloc = parsed.netloc.replace(".", "_")
#             filename = f"{netloc}_{path}.md"
#             filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
#             return filename[:200]

#         async def crawl_page(current_url: str, current_depth: int):
#             if current_url in visited_urls:
#                 return

#             visited_urls.add(current_url)
#             print(f"Crawling ({len(visited_urls)}): {current_url}")

#             async with semaphore:
#                 async with AsyncWebCrawler(verbose=True) as crawler:
#                     result = await crawler.arun(url=current_url, config=config)

#                     if result.success:
#                         markdown_content = result.markdown.raw_markdown
#                         cleaned_markdown = clean_markdown(markdown_content)

#                         filename = sanitize_filename(current_url)
#                         output_path = os.path.join(output_dir, filename)

#                         with open(output_path, "w", encoding="utf-8") as f:
#                             f.write(f"# {current_url}\n\n{cleaned_markdown}\n")
#                         print(f"Saved cleaned Markdown to: {output_path}")

#                         if current_depth < max_depth:
#                             internal_links = result.links.get("internal", [])
#                             for link in internal_links:
#                                 href = link["href"]
#                                 absolute_url = urljoin(current_url, href)
#                                 if urlparse(absolute_url).netloc == urlparse(start_url).netloc:
#                                     if absolute_url not in visited_urls and absolute_url not in queued_urls:
#                                         crawl_queue.put_nowait((absolute_url, current_depth + 1))
#                                         queued_urls.add(absolute_url)
#                     else:
#                         print(f"Failed to crawl {current_url}: {result.error_message}")

#         async def worker():
#             while not crawl_queue.empty():
#                 current_url, current_depth = await crawl_queue.get()
#                 await crawl_page(current_url, current_depth)
#                 crawl_queue.task_done()

#         tasks = [asyncio.create_task(worker()) for _ in range(max_concurrency)]
#         await crawl_queue.join()

#         for task in tasks:
#             task.cancel()

#         metadata_path = os.path.join(output_dir, "metadata.json")
#         with open(metadata_path, "w", encoding="utf-8") as f:
#             json.dump({"crawled_urls": list(visited_urls)}, f, indent=2)
#         print(f"Metadata saved to {metadata_path}")

#         return {"status": "success", "crawled_urls": list(visited_urls), "metadata_path": metadata_path}

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=f"Error during crawling: {str(e)}")

# @app.post("/crawl")
# async def crawl_endpoint(request: CrawlRequest):
#     """
#     FastAPI endpoint to crawl a website and save content as cleaned Markdown files.
#     """
#     result = await crawl_website(
#         start_url=str(request.start_url),
#         output_dir=request.output_dir,
#         max_concurrency=request.max_concurrency,
#         max_depth=request.max_depth
#     )
#     return result

# if __name__ == "__main__":
#     import uvicorn
#     uvicorn.run(app, host="0.0.0.0", port=8002)




from fastapi import FastAPI, HTTPException, UploadFile, File, Form
from pydantic import BaseModel, Field
import asyncio
import os
import json
import re
from urllib.parse import urljoin, urlparse
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from typing import List, Dict, Any
import csv
import io
import uvicorn # Import uvicorn here for the main block

app = FastAPI()

# Define the exclusion keywords for filenames (case-insensitive check will be used)
EXCLUDE_KEYWORDS = ['pdf', 'jpeg', 'jpg', 'png', 'webp']

class CrawlCSVRequest(BaseModel):
    """Request model for crawling URLs from a CSV."""
    output_dir: str = "./crawl_output_csv"
    max_concurrency_per_site: int = Field(default=8, ge=1, description="Maximum concurrent requests *per site being crawled*.")
    max_depth: int = Field(default=2, ge=0, description="Maximum depth to crawl from each starting URL in the CSV.")
    # Note: The CSV file content is provided via the /crawl_csv_upload endpoint

def clean_markdown(md_text: str) -> str:
    """
    Cleans Markdown content by removing or modifying specific elements.
    """
    # Remove image links/tags first
    md_text = re.sub(r'!\[([^\]]*)\]\((http[s]?://[^\)]+)\)', '', md_text)
    # Remove inline links, keeping the link text
    md_text = re.sub(r'\[([^\]]+)\]\((http[s]?://[^\)]+)\)', r'\1', md_text)
    # Remove raw URLs (handle common cases, avoid removing parts of valid markdown)
    md_text = re.sub(r'(?<!\]\()https?://\S+', '', md_text)
    # Remove footnote references ([^1])
    md_text = re.sub(r'\[\^?\d+\]', '', md_text)
    # Remove footnote definitions (e.g., [^1]: http://example.com)
    md_text = re.sub(r'^\[\^?\d+\]:\s?.*$', '', md_text, flags=re.MULTILINE)
    # Remove blockquotes
    md_text = re.sub(r'^\s{0,3}>\s?', '', md_text, flags=re.MULTILINE)
    # Remove bold markdown
    md_text = re.sub(r'(\*\*|__)(.*?)\1', r'\2', md_text)
    # Remove italic markdown
    md_text = re.sub(r'(\*|_)(.*?)\1', r'\2', md_text)
    # Remove empty headings (lines starting with # followed only by whitespace)
    md_text = re.sub(r'^\s*#+\s*$', '', md_text, flags=re.MULTILINE)
    # Remove empty parentheses that might remain from link removal
    md_text = re.sub(r'\(\)', '', md_text)
    # Compact multiple newlines into single newlines
    md_text = re.sub(r'\n\s*\n+', '\n\n', md_text)
    # Compact multiple spaces/tabs into single spaces
    md_text = re.sub(r'[ \t]+', ' ', md_text)
    return md_text.strip()

def read_urls_from_csv(csv_content: str) -> List[str]:
    """Reads URLs from CSV content string. Assumes one URL per line."""
    urls = []
    # Use StringIO to treat string content as a file
    csvfile = io.StringIO(csv_content)
    # Use csv.reader to handle potential different line endings and quoting,
    # although simple splitlines works for very basic cases.
    # Assuming simple list of URLs, one per row, no headers.
    reader = csv.reader(csvfile)
    for i, row in enumerate(reader):
        if not row: # Skip empty rows
            continue
        url = row[0].strip() # Take the first column and remove leading/trailing whitespace
        # Basic URL validation
        if url and (url.startswith("http://") or url.startswith("https://")):
            try:
                # Attempt to parse to catch malformed URLs before adding
                urlparse(url)
                urls.append(url)
            except Exception:
                 print(f"Skipping invalid URL format on line {i+1}: '{row[0]}'")
        else:
            print(f"Skipping non-HTTP/HTTPS or empty entry on line {i+1}: '{row[0]}'")
    return urls

def sanitize_filename(url: str) -> str:
    """Sanitizes a URL to create a safe filename."""
    try:
        parsed = urlparse(url)
        # Use domain and path for filename
        netloc = parsed.netloc.replace(".", "_")
        path = parsed.path.strip("/").replace("/", "_").replace(".", "_") # Replace dots in path too
        if not path:
            path = "index"
        # Include query parameters if any, might help distinguish pages
        # Simplify query string for filenames
        query = parsed.query.replace("=", "-").replace("&", "_")
        if query:
             filename = f"{netloc}_{path}_{query}.md"
        else:
             filename = f"{netloc}_{path}.md"

        # Remove any characters still unsafe or problematic
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Ensure filename doesn't start or end with specific characters or have multiple consecutive ones
        filename = re.sub(r'[\s\._-]+', '_', filename) # Replace spaces, dots, underscores, hyphens with single underscore
        filename = re.sub(r'^_+', '', filename) # Remove leading underscores
        filename = re.sub(r'_+$', '', filename) # Remove trailing underscores

        if not filename: # Fallback if sanitization results in empty string
             filename = f"url_{abs(hash(url))}.md" # Use a hash as a unique identifier

        # Prevent overly long filenames (most file systems have limits, e.g., 255 chars)
        return filename[:200] + ".md" if not filename.lower().endswith(".md") else filename[:200]

    except Exception as e:
        print(f"Error sanitizing URL {url}: {e}")
        # Fallback filename if sanitization fails
        return f"error_parsing_{abs(hash(url))}.md"


async def crawl_website_single_site(
    start_url: str,
    output_dir: str,
    max_concurrency: int,
    max_depth: int
) -> Dict[str, Any]:
    """
    Crawl a single website deeply and save each page as a cleaned Markdown file, with parallelization.
    Applies filename exclusion rules and limits crawl to the start URL's domain.
    """
    crawled_urls = set()
    queued_urls = set()
    crawl_queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(max_concurrency)
    results = {"success": [], "failed": [], "skipped_by_filter": [], "initial_url": start_url}

    # Initial URL added to the queue
    if start_url:
        crawl_queue.put_nowait((start_url, 0))
        queued_urls.add(start_url)
    else:
        results["failed"].append({"url": "N/A", "error": "Empty start URL provided"})
        print("Error: Empty start URL provided to crawl_website_single_site")
        return results

    # --- Extract domain for allowed_domains parameter ---
    start_domain = urlparse(start_url).netloc
    allowed_domains_list = []
    if start_domain:
        allowed_domains_list.append(start_domain)
        print(f"Limiting crawl for {start_url} to domain: {start_domain}")
    else:
        print(f"Warning: Could not extract domain from {start_url}. Domain limiting may not work as expected by crawl4ai. Proceeding without explicit domain limit in config.")
        # If domain extraction fails, allowed_domains_list will be empty,
        # which means crawl4ai will not enforce domain limits based on this parameter.
        # The exclude_external_links=True config might still help, but allowed_domains is preferred.


    print(f"Starting crawl for: {start_url} with max_depth={max_depth}, max_concurrency={max_concurrency}")

    md_generator = DefaultMarkdownGenerator(
        options={
            "ignore_links": True,
            "escape_html": True,
            "body_width": 0 # Prevent line wrapping
        }
    )

    # --- CORRECTED CrawlerRunConfig parameters ---
    config = CrawlerRunConfig(
        markdown_generator=md_generator,
        cache_mode="BYPASS", # Always fetch fresh content
        exclude_external_links=True, # Keep this, it's an additional filter that can complement allowed_domains
        exclude_social_media_links=True,
        allowed_domains=allowed_domains_list # <-- Use the correct parameter with the list
    )
    # --------------------------------------------

    async def crawl_page(current_url: str, current_depth: int):
        """Worker function to crawl a single page."""
        if current_url in crawled_urls:
            # print(f"Already visited: {current_url}") # Optional: log already visited
            return

        # Add to visited set BEFORE attempting crawl to prevent race conditions
        crawled_urls.add(current_url)
        print(f"Crawling ({len(crawled_urls)}): {current_url} (Depth: {current_depth})")

        filename = sanitize_filename(current_url)
        # Ensure filename doesn't exceed OS limits BEFORE creating path
        # (Redundant with sanitize_filename[:200], but defensive)
        max_filename_len = 200 # Reasonable limit
        if len(filename) > max_filename_len:
             filename = filename[:max_filename_len] # Truncate if needed

        output_path = os.path.join(output_dir, filename)

        # --- Filename Exclusion Check ---
        if any(keyword in filename.lower() for keyword in EXCLUDE_KEYWORDS):
             print(f"Skipping save for {current_url} due to filename filter: {filename}")
             results["skipped_by_filter"].append(current_url)
             # Still process links if depth allows, even if not saving this page's content
             # The config with allowed_domains handles limiting to the specific domain
             if current_depth < max_depth:
                 # Need to perform the crawl to get links
                 async with semaphore:
                     async with AsyncWebCrawler(verbose=False) as crawler: # Turn off verbose for individual crawls
                         # Check if the URL itself is within the allowed domains as a final safeguard
                         if allowed_domains_list and urlparse(current_url).netloc not in allowed_domains_list:
                             print(f"Warning: Queue contained URL outside allowed domains (should not happen with correct config): {current_url}")
                             crawl_queue.task_done() # Mark done to prevent getting stuck
                             return # Skip processing this URL
                         result = await crawler.arun(url=current_url, config=config)
                         # crawl4ai with allowed_domains will automatically filter links
                         if result.success:
                             internal_links = result.links.get("internal", []) # These are already filtered by domain by crawl4ai config
                             # Add allowed links to queue if within depth and not visited/queued
                             for link in internal_links:
                                 href = link["href"]
                                 absolute_url = urljoin(current_url, href)
                                 # No need for explicit domain check here because allowed_domains handles it
                                 if absolute_url not in crawled_urls and absolute_url not in queued_urls:
                                     crawl_queue.put_nowait((absolute_url, current_depth + 1))
                                     queued_urls.add(absolute_url)
                         else:
                              print(f"Failed to get links from {current_url} (skipped save): {result.error_message}")
                              # Don't add to failed results here, as the page itself wasn't the primary goal (saving was skipped)
             crawl_queue.task_done() # Important: Mark task done even if skipped
             return # Exit crawl_page function for this URL

        # --- Normal Crawl and Save ---
        async with semaphore:
            async with AsyncWebCrawler(verbose=False) as crawler: # Turn off verbose for individual crawls
                 # Check if the URL itself is within the allowed domains as a final safeguard
                if allowed_domains_list and urlparse(current_url).netloc not in allowed_domains_list:
                    print(f"Warning: Queue contained URL outside allowed domains (should not happen with correct config): {current_url}")
                    crawl_queue.task_done() # Mark done to prevent getting stuck
                    return # Skip processing this URL
                result = await crawler.arun(url=current_url, config=config)

            if result.success:
                markdown_content = result.markdown.raw_markdown
                cleaned_markdown = clean_markdown(markdown_content)

                try:
                    # Ensure directory exists before writing file
                    os.makedirs(os.path.dirname(output_path), exist_ok=True)
                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(f"# {current_url}\n\n{cleaned_markdown}\n")
                    print(f"Saved cleaned Markdown to: {output_path}")
                    results["success"].append(current_url)

                    # Add links to queue if within max_depth
                    if current_depth < max_depth:
                        internal_links = result.links.get("internal", [])
                        # Note: The config with allowed_domains handles filtering by domain
                        for link in internal_links:
                            href = link["href"]
                            absolute_url = urljoin(current_url, href)
                            # No need for explicit domain check here because allowed_domains handles it
                            if absolute_url not in crawled_urls and absolute_url not in queued_urls:
                                # Although allowed_domains is set, adding an extra check before queuing
                                # can be slightly safer, though redundant with correct config.
                                # Let's rely on allowed_domains filtering *within* crawl4ai for simplicity.
                                crawl_queue.put_nowait((absolute_url, current_depth + 1))
                                queued_urls.add(absolute_url)

                except IOError as e:
                     print(f"Error saving file {output_path}: {e}")
                     results["failed"].append({"url": current_url, "error": f"File save error: {e}"})
                except Exception as e:
                     print(f"An unexpected error occurred processing {current_url}: {e}")
                     results["failed"].append({"url": current_url, "error": f"Processing error: {e}"})

            else:
                print(f"Failed to crawl {current_url}: {result.error_message}")
                results["failed"].append({"url": current_url, "error": result.error_message})

            crawl_queue.task_done() # Mark task done after processing

    # Create worker tasks
    # Ensure we don't try to create more tasks than queue items if queue is small initially
    # Also handle the case where the queue might be empty from the start (e.g., invalid initial URL)
    num_initial_workers = min(max_concurrency, crawl_queue.qsize())
    worker_tasks = [asyncio.create_task(crawl_page(*await crawl_queue.get())) for _ in range(num_initial_workers)]

    # Wait for all tasks in the queue to be processed
    await crawl_queue.join()

    # Cancel remaining worker tasks (they might be waiting for queue items that won't come)
    # Using gather with return_exceptions=True is a robust way to handle cancelled tasks
    for task in worker_tasks:
        if not task.done(): # Only cancel if not already finished
            task.cancel()

    # Await cancellation to avoid warnings and clean up
    await asyncio.gather(*worker_tasks, return_exceptions=True)


    print(f"Finished crawl for: {start_url}")
    return results


@app.post("/crawl_csv_upload")
async def crawl_csv_upload_endpoint(
    csv_file: UploadFile = File(...),
    output_dir: str = Form("./crawl_output_csv"),
    max_concurrency_per_site: int = Form(default=8, ge=1),
    max_depth: int = Form(default=2, ge=0)
):
    """
    FastAPI endpoint to crawl URLs provided in an uploaded CSV file.
    Each URL in the CSV is treated as a starting point for a deep crawl within its domain.
    """
    if not csv_file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted.")

    try:
        # Read CSV content
        csv_content = await csv_file.read()
        csv_content = csv_content.decode("utf-8")
        urls_to_crawl = read_urls_from_csv(csv_content)

        if not urls_to_crawl:
            return {"status": "warning", "message": "No valid URLs found in the CSV file to crawl."}

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        overall_results: Dict[str, Any] = {
            "status": "processing",
            "total_urls_from_csv": len(urls_to_crawl),
            "site_crawl_results": {}
        }

        # Sequentially crawl each site listed in the CSV
        # This means concurrency is per site, not across all sites simultaneously
        for i, url in enumerate(urls_to_crawl):
            print(f"\n--- Processing site {i+1}/{len(urls_to_crawl)}: {url} ---")
            # Ensure a clean state (sets, queue) for each new site crawl
            try:
                site_results = await crawl_website_single_site(
                    start_url=url,
                    output_dir=output_dir,
                    max_concurrency=max_concurrency_per_site,
                    max_depth=max_depth
                )
                overall_results["site_crawl_results"][url] = site_results
            except Exception as e:
                 print(f"An unexpected error occurred during the crawl of {url}: {e}")
                 overall_results["site_crawl_results"][url] = {"status": "error", "message": f"Unexpected error: {str(e)}"}


        # Save overall metadata
        metadata_path = os.path.join(output_dir, "overall_metadata.json")
        try:
            # Convert sets to lists for JSON serialization
            serializable_results = overall_results.copy()
            for url, res in serializable_results["site_crawl_results"].items():
                 # Check if keys exist before trying to convert
                if "success" in res and isinstance(res["success"], set):
                    res["success"] = list(res["success"])
                if "skipped_by_filter" in res and isinstance(res["skipped_by_filter"], set):
                     res["skipped_by_filter"] = list(res["skipped_by_filter"])
                # Failed results should already be serializable (list of dicts)

            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(serializable_results, f, indent=2)
            overall_results["metadata_path"] = metadata_path
            print(f"\nOverall metadata saved to {metadata_path}")
        except Exception as e:
             print(f"Error saving overall metadata: {e}")
             overall_results["metadata_save_error"] = str(e)


        overall_results["status"] = "completed"
        return overall_results

    except Exception as e:
        # Catch any other unexpected errors during file processing or initial setup
        # Log the error for debugging
        print(f"Critical error in crawl_csv_upload_endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"An internal server error occurred: {str(e)}")


if __name__ == "__main__":
    # To run, save the code as e.g., main.py and run 'uvicorn main:app --reload'
    # The /crawl_csv_upload endpoint expects a file upload.
    print("Starting FastAPI application...")
    print("Navigate to http://0.0.0.0:8002/docs for interactive documentation (Swagger UI).")
    uvicorn.run(app, host="0.0.0.0", port=8002)