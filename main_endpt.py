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

app = FastAPI()

# Define the exclusion keywords for filenames
EXCLUDE_KEYWORDS = ['pdf', 'jpeg', 'jpg', 'png', 'webp']

class CrawlCSVRequest(BaseModel):
    """Request model for crawling URLs from a CSV."""
    output_dir: str = "./crawl_output_csv"
    max_concurrency_per_site: int = Field(default=8, ge=1, description="Maximum concurrent requests *per site being crawled*.")
    max_depth: int = Field(default=2, ge=0, description="Maximum depth to crawl from each starting URL in the CSV.")
    # Note: The CSV file content is provided via the /crawl_csv_upload endpoint

def clean_markdown(md_text: str) -> str:
    """
    Cleans Markdown content by:
    - Removing inline links, footnotes, raw URLs, images
    - Removing bold, italic, blockquotes
    - Removing empty headings
    - Compacting whitespace
    """
    # Remove image links/tags first
    md_text = re.sub(r'!\[([^\]]*)\]\((http[s]?://[^\)]+)\)', '', md_text)
    # Remove inline links, keeping the link text
    md_text = re.sub(r'\[([^\]]+)\]\((http[s]?://[^\)]+)\)', r'\1', md_text)
    # Remove raw URLs
    md_text = re.sub(r'(?<!\]\()http[s]?://\S+', '', md_text) # Avoid removing URLs inside markdown links already handled
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
    for row in reader:
        if row: # Check if row is not empty
            url = row[0].strip() # Take the first column and remove leading/trailing whitespace
            if url and (url.startswith("http://") or url.startswith("https://")):
                urls.append(url)
            else:
                print(f"Skipping invalid or empty URL: {row}")
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
        query = parsed.query.replace("=", "_").replace("&", "__")
        if query:
             filename = f"{netloc}_{path}_{query}.md"
        else:
             filename = f"{netloc}_{path}.md"

        # Remove any characters still unsafe or problematic
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Ensure filename doesn't start or end with a dot or underscore multiple times
        filename = re.sub(r'^[\._]+', '', filename)
        filename = re.sub(r'[\._]+$', '', filename)

        # Prevent overly long filenames
        return filename[:200]

    except Exception as e:
        print(f"Error sanitizing URL {url}: {e}")
        # Fallback filename if sanitization fails
        return f"error_parsing_{hash(url)}.md"


async def crawl_website_single_site(
    start_url: str,
    output_dir: str,
    max_concurrency: int,
    max_depth: int
) -> Dict[str, Any]:
    """
    Crawl a single website deeply and save each page as a cleaned Markdown file, with parallelization.
    Applies filename exclusion rules.
    """
    crawled_urls = set()
    queued_urls = set()
    crawl_queue = asyncio.Queue()
    semaphore = asyncio.Semaphore(max_concurrency)
    results = {"success": [], "failed": [], "skipped_by_filter": []}

    # Initial URL added to the queue
    if start_url:
        crawl_queue.put_nowait((start_url, 0))
        queued_urls.add(start_url)
    else:
        results["failed"].append({"url": "N/A", "error": "Empty start URL provided"})
        return results

    print(f"Starting crawl for: {start_url} with max_depth={max_depth}, max_concurrency={max_concurrency}")

    md_generator = DefaultMarkdownGenerator(
        options={
            "ignore_links": True,
            "escape_html": True,
            "body_width": 0 # Prevent line wrapping
        }
    )

    config = CrawlerRunConfig(
        markdown_generator=md_generator,
        cache_mode="BYPASS", # Always fetch fresh content
        exclude_external_links=True, # Only follow links within the starting domain
        exclude_social_media_links=True,
        stay_in_domain=True # Explicitly limit to the domain of the start_url
    )

    async def crawl_page(current_url: str, current_depth: int):
        """Worker function to crawl a single page."""
        if current_url in crawled_urls:
            # print(f"Already visited: {current_url}") # Optional: log already visited
            return

        crawled_urls.add(current_url)
        print(f"Crawling ({len(crawled_urls)}): {current_url} (Depth: {current_depth})")

        filename = sanitize_filename(current_url)
        output_path = os.path.join(output_dir, filename)

        # --- Filename Exclusion Check ---
        if any(keyword in filename.lower() for keyword in EXCLUDE_KEYWORDS):
             print(f"Skipping save for {current_url} due to filename filter: {filename}")
             results["skipped_by_filter"].append(current_url)
             # Still process links if depth allows, even if not saving this page's content
             if current_depth < max_depth:
                 # Need to perform the crawl to get links, even if not saving content
                 async with semaphore:
                     async with AsyncWebCrawler(verbose=False) as crawler: # Turn off verbose for individual crawls
                         result = await crawler.arun(url=current_url, config=config)
                         if result.success:
                             internal_links = result.links.get("internal", [])
                             # Add internal links to queue if within depth and domain, and not visited/queued
                             domain_of_start_url = urlparse(start_url).netloc
                             for link in internal_links:
                                 href = link["href"]
                                 absolute_url = urljoin(current_url, href)
                                 if urlparse(absolute_url).netloc == domain_of_start_url:
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
                result = await crawler.arun(url=current_url, config=config)

            if result.success:
                markdown_content = result.markdown.raw_markdown
                cleaned_markdown = clean_markdown(markdown_content)

                try:
                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(f"# {current_url}\n\n{cleaned_markdown}\n")
                    print(f"Saved cleaned Markdown to: {output_path}")
                    results["success"].append(current_url)

                    # Add links to queue if within max_depth
                    if current_depth < max_depth:
                        internal_links = result.links.get("internal", [])
                        domain_of_start_url = urlparse(start_url).netloc
                        for link in internal_links:
                            href = link["href"]
                            absolute_url = urljoin(current_url, href)
                            # Only add links within the original starting domain
                            if urlparse(absolute_url).netloc == domain_of_start_url:
                                if absolute_url not in crawled_urls and absolute_url not in queued_urls:
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
    worker_tasks = [asyncio.create_task(crawl_page(*await crawl_queue.get())) for _ in range(min(max_concurrency, crawl_queue.qsize()))]

    # Wait for all tasks in the queue to be processed
    await crawl_queue.join()

    # Cancel remaining worker tasks (they might be waiting for queue items that won't come)
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
            return {"status": "warning", "message": "No valid URLs found in the CSV file."}

        # Ensure output directory exists
        os.makedirs(output_dir, exist_ok=True)

        overall_results: Dict[str, Any] = {
            "status": "processing",
            "total_urls_from_csv": len(urls_to_crawl),
            "site_crawl_results": {}
        }

        # Sequentially crawl each site listed in the CSV
        # Note: To parallelize across sites as well, a more complex queue management
        # across sites would be needed, but this sequential approach per site is simpler
        # and respects the concurrency setting per domain which is often desirable.
        for i, url in enumerate(urls_to_crawl):
            print(f"\n--- Processing site {i+1}/{len(urls_to_crawl)}: {url} ---")
            try:
                site_results = await crawl_website_single_site(
                    start_url=url,
                    output_dir=output_dir,
                    max_concurrency=max_concurrency_per_site,
                    max_depth=max_depth
                )
                overall_results["site_crawl_results"][url] = site_results
            except Exception as e:
                 print(f"An error occurred during the crawl of {url}: {e}")
                 overall_results["site_crawl_results"][url] = {"status": "error", "message": str(e)}


        # Optional: Save overall metadata
        metadata_path = os.path.join(output_dir, "overall_metadata.json")
        try:
            # Convert sets to lists for JSON serialization
            serializable_results = overall_results.copy()
            for url, res in serializable_results["site_crawl_results"].items():
                if "success" in res: res["success"] = list(res["success"])
                if "skipped_by_filter" in res: res["skipped_by_filter"] = list(res["skipped_by_filter"])
                # Failed results are already serializable

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
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")

# You can keep the original /crawl endpoint if you still need it,
# but the primary functionality for the CSV is now in /crawl_csv_upload.
# If you don't need the single URL endpoint anymore, you can remove it.
# For now, keeping it commented out.

# @app.post("/crawl")
# async def crawl_endpoint(request: CrawlRequest):
#     """
#     FastAPI endpoint to crawl a single website and save content as cleaned Markdown files.
#     """
#     print(f"Received request to crawl single URL: {request.start_url}")
#     result = await crawl_website_single_site(
#         start_url=str(request.start_url),
#         output_dir=request.output_dir,
#         max_concurrency=request.max_concurrency,
#         max_depth=request.max_depth
#     )
#     # Add metadata saving for the single crawl if this endpoint is kept
#     metadata_path = os.path.join(request.output_dir, f"metadata_{urlparse(str(request.start_url)).netloc.replace('.', '_')}.json")
#     try:
#          serializable_results = result.copy()
#          if "success" in serializable_results: serializable_results["success"] = list(serializable_results["success"])
#          if "skipped_by_filter" in serializable_results: serializable_results["skipped_by_filter"] = list(serializable_results["skipped_by_filter"])
#          with open(metadata_path, "w", encoding="utf-8") as f:
#              json.dump(serializable_results, f, indent=2)
#          result["metadata_path"] = metadata_path
#          print(f"Metadata saved to {metadata_path}")
#     except Exception as e:
#          print(f"Error saving metadata: {e}")
#          result["metadata_save_error"] = str(e)
#
#     return result


if __name__ == "__main__":
    import uvicorn
    # To run, save the code as e.g., main.py and run 'uvicorn main:app --reload'
    # The /crawl_csv_upload endpoint expects a file upload.
    uvicorn.run(app, host="0.0.0.0", port=8002)