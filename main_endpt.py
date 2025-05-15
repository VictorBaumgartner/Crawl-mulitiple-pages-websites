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
from typing import List, Dict, Any, Tuple
import csv
import io
import uvicorn

app = FastAPI()

# Define the exclusion keywords for filenames (case-insensitive check will be used)
EXCLUDE_KEYWORDS = ['pdf', 'jpeg', 'jpg', 'png', 'webp']

class CrawlCSVRequest(BaseModel):
    """Request model for crawling URLs from a CSV."""
    output_dir: str = "./crawl_output_csv"
    max_concurrency_per_site: int = Field(default=8, ge=1, description="Maximum concurrent requests *per site being crawled*.")
    max_depth: int = Field(default=2, ge=0, description="Maximum depth to crawl from each starting URL in the CSV.")

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
    csvfile = io.StringIO(csv_content)
    reader = csv.reader(csvfile)
    for i, row in enumerate(reader):
        if not row:  # Skip empty rows
            continue
        url = row[0].strip()
        if url and (url.startswith("http://") or url.startswith("https://")):
            try:
                parsed_url = urlparse(url)
                if parsed_url.netloc:
                    urls.append(url)
                else:
                    print(f"Skipping URL with no recognizable domain on line {i+1}: '{row[0]}'")
            except Exception:
                print(f"Skipping invalid URL format on line {i+1}: '{row[0]}'")
        else:
            print(f"Skipping non-HTTP/HTTPS or empty entry on line {i+1}: '{row[0]}'")
    return urls

def sanitize_filename(url: str) -> str:
    """Sanitizes a URL to create a safe filename."""
    try:
        parsed = urlparse(url)
        netloc = parsed.netloc.replace(".", "_")
        path = parsed.path.strip("/").replace("/", "_").replace(".", "_")
        if not path:
            path = "index"
        query = parsed.query.replace("=", "-").replace("&", "_")
        if query:
            filename = f"{netloc}_{path}_{query}"  # No .md suffix yet
        else:
            filename = f"{netloc}_{path}"  # No .md suffix yet

        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        filename = re.sub(r'[\s\._-]+', '_', filename)
        filename = re.sub(r'^_+', '', filename)
        filename = re.sub(r'_+$', '', filename)

        if not filename:
            filename = f"url_{abs(hash(url))}"  # No .md suffix yet

        max_len_without_suffix = 200 - 3  # Leave space for .md
        filename = filename[:max_len_without_suffix] + ".md"

        return filename
    except Exception as e:
        print(f"Error sanitizing URL filename for {url}: {e}")
        return f"error_parsing_{abs(hash(url))}.md"

def sanitize_dirname(url: str) -> str:
    """Sanitizes a URL's domain to create a safe directory name."""
    try:
        parsed = urlparse(url)
        dirname = parsed.netloc.replace(".", "_")
        dirname = re.sub(r'[<>:"/\\|?*]', '_', dirname)
        dirname = re.sub(r'[\s\._-]+', '_', dirname)
        dirname = re.sub(r'^_+', '', dirname)
        dirname = re.sub(r'_+$', '', dirname)

        if not dirname:
            dirname = f"domain_{abs(hash(url))}"

        return dirname[:200]
    except Exception as e:
        print(f"Error sanitizing URL directory name for {url}: {e}")
        return f"domain_error_{abs(hash(url))}"

CrawlQueueItem = Tuple[str, int, str, str]  # (url, depth, start_domain, site_output_dir)

async def crawl_website_single_site(
    start_url: str,
    output_dir: str,
    max_concurrency: int,
    max_depth: int
) -> Dict[str, Any]:
    """
    Crawl a single website deeply and save each page as a cleaned Markdown file
    in a site-specific subdirectory, with parallelization.
    """
    crawled_urls = set()
    queued_urls = set()
    crawl_queue: asyncio.Queue[CrawlQueueItem] = asyncio.Queue()
    semaphore = asyncio.Semaphore(max_concurrency)
    results = {"success": [], "failed": [], "skipped_by_filter": [], "initial_url": start_url}

    try:
        parsed_start_url = urlparse(start_url)
        start_domain = parsed_start_url.netloc
        if not start_domain:
            results["failed"].append({"url": start_url, "error": "Could not extract domain from start URL"})
            print(f"Error: Could not extract domain from start URL: {start_url}")
            return results

        site_subdir_name = sanitize_dirname(start_url)
        site_output_path = os.path.join(output_dir, site_subdir_name)

        print(f"Crawl limited to domain: {start_domain}")
        print(f"Saving files for this site in: {site_output_path}")

    except Exception as e:
        results["failed"].append({"url": start_url, "error": f"Error parsing start URL or determining output path: {e}"})
        print(f"Error processing start URL {start_url} or determining output path: {e}")
        return results

    crawl_queue.put_nowait((start_url, 0, start_domain, site_output_path))
    queued_urls.add(start_url)

    print(f"Starting crawl for: {start_url} with max_depth={max_depth}, max_concurrency={max_concurrency}")

    md_generator = DefaultMarkdownGenerator(
        options={
            "ignore_links": True,
            "escape_html": True,
            "body_width": 0
        }
    )

    config = CrawlerRunConfig(
        markdown_generator=md_generator,
        cache_mode="BYPASS",
        exclude_social_media_links=True,
    )

    async def crawl_page():
        """Worker function to process URLs from the queue."""
        while not crawl_queue.empty():
            try:
                current_url, current_depth, crawl_start_domain, current_site_output_path = await crawl_queue.get()

                if current_url in crawled_urls:
                    crawl_queue.task_done()
                    continue

                try:
                    current_domain = urlparse(current_url).netloc
                    if current_domain != crawl_start_domain:
                        print(f"Skipping external URL: {current_url} (Domain: {current_domain}, Expected: {crawl_start_domain})")
                        crawled_urls.add(current_url)
                        crawl_queue.task_done()
                        continue
                except Exception as e:
                    print(f"Error parsing domain for URL {current_url}: {e}. Skipping.")
                    crawled_urls.add(current_url)
                    crawl_queue.task_done()
                    continue

                crawled_urls.add(current_url)
                print(f"Crawling ({len(crawled_urls)}): {current_url} (Depth: {current_depth})")

                filename = sanitize_filename(current_url)
                max_filename_len = 200
                if len(filename) > max_filename_len:
                    filename = filename[:max_filename_len]

                output_path = os.path.join(current_site_output_path, filename)

                if any(keyword in filename.lower() for keyword in EXCLUDE_KEYWORDS):
                    print(f"Skipping save for {current_url} due to filename filter: {filename}")
                    results["skipped_by_filter"].append(current_url)
                    if current_depth < max_depth:
                        async with semaphore:
                            async with AsyncWebCrawler(verbose=False) as crawler:
                                result = await crawler.arun(url=current_url, config=config)
                                if result.success:
                                    internal_links = result.links.get("internal", [])
                                    for link in internal_links:
                                        href = link["href"]
                                        try:
                                            absolute_url = urljoin(current_url, href)
                                            parsed_absolute_url = urlparse(absolute_url)
                                            if parsed_absolute_url.netloc == crawl_start_domain:
                                                if absolute_url not in crawled_urls and absolute_url not in queued_urls:
                                                    crawl_queue.put_nowait((absolute_url, current_depth + 1, crawl_start_domain, current_site_output_path))
                                                    queued_urls.add(absolute_url)
                                        except Exception as link_e:
                                            print(f"Error processing link {href} from {current_url}: {link_e}")
                                else:
                                    print(f"Failed to get links from {current_url} (skipped save): {result.error_message}")
                    crawl_queue.task_done()
                    continue

                async with semaphore:
                    async with AsyncWebCrawler(verbose=False) as crawler:
                        result = await crawler.arun(url=current_url, config=config)

                    if result.success:
                        markdown_content = result.markdown.raw_markdown
                        cleaned_markdown = clean_markdown(markdown_content)

                        try:
                            os.makedirs(os.path.dirname(output_path), exist_ok=True)
                            with open(output_path, "w", encoding="utf-8") as f:
                                f.write(f"# {current_url}\n\n{cleaned_markdown}\n")
                            print(f"Saved cleaned Markdown to: {output_path}")
                            results["success"].append(current_url)

                            if current_depth < max_depth:
                                internal_links = result.links.get("internal", [])
                                for link in internal_links:
                                    href = link["href"]
                                    try:
                                        absolute_url = urljoin(current_url, href)
                                        parsed_absolute_url = urlparse(absolute_url)
                                        if parsed_absolute_url.netloc == crawl_start_domain:
                                            if absolute_url not in crawled_urls and absolute_url not in queued_urls:
                                                crawl_queue.put_nowait((absolute_url, current_depth + 1, crawl_start_domain, current_site_output_path))
                                                queued_urls.add(absolute_url)
                                    except Exception as link_e:
                                        print(f"Error processing link {href} from {current_url}: {link_e}")
                        except IOError as e:
                            print(f"Error saving file {output_path}: {e}")
                            results["failed"].append({"url": current_url, "error": f"File save error: {e}"})
                        except Exception as e:
                            print(f"An unexpected error occurred processing {current_url}: {e}")
                            results["failed"].append({"url": current_url, "error": f"Processing error: {e}"})
                    else:
                        print(f"Failed to crawl {current_url}: {result.error_message}")
                        results["failed"].append({"url": current_url, "error": result.error_message})

                crawl_queue.task_done()
            except Exception as e:
                print(f"Error in crawl_page worker: {e}")
                crawl_queue.task_done()

    # Create initial worker tasks
    worker_tasks = []
    for _ in range(max_concurrency):
        task = asyncio.create_task(crawl_page())
        worker_tasks.append(task)

    # Wait for the queue to be fully processed
    await crawl_queue.join()

    # Cancel any remaining tasks
    for task in worker_tasks:
        task.cancel()

    # Wait for tasks to complete or be cancelled
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
    """
    if not csv_file.filename.lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted.")

    try:
        csv_content = await csv_file.read()
        csv_content = csv_content.decode("utf-8")
        urls_to_crawl = read_urls_from_csv(csv_content)

        if not urls_to_crawl:
            return {"status": "warning", "message": "No valid URLs found in the CSV file to crawl."}

        os.makedirs(output_dir, exist_ok=True)

        overall_results: Dict[str, Any] = {
            "status": "processing",
            "total_urls_from_csv": len(urls_to_crawl),
            "site_crawl_results": {}
        }

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
                print(f"An unexpected error occurred during the crawl of {url}: {e}")
                overall_results["site_crawl_results"][url] = {"status": "error", "message": f"Unexpected error during site processing: {str(e)}"}

        metadata_path = os.path.join(output_dir, "overall_metadata.json")
        try:
            serializable_results = overall_results.copy()
            for url, res in serializable_results["site_crawl_results"].items():
                if "success" in res and isinstance(res["success"], set):
                    res["success"] = list(res["success"])
                if "skipped_by_filter" in res and isinstance(res["skipped_by_filter"], set):
                    res["skipped_by_filter"] = list(res["skipped_by_filter"])

            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(serializable_results, f, indent=2)
            overall_results["metadata_path"] = metadata_path
            print(f"\nOverall metadata saved to {metadata_path}")
        except Exception as e:
            print(f"Error saving overall metadata: {e}")
            overall_results["metadata_save_error"] = str(e)

        overall_results["status"] = "completed"
        print("\n--- Overall CSV processing completed ---")
        return overall_results
    except Exception as e:
        print(f"Critical error in crawl_csv_upload_endpoint: {e}")
        raise HTTPException(status_code=500, detail=f"An internal server error occurred during request processing: {str(e)}")

if __name__ == "__main__":
    print("Starting FastAPI application...")
    print("Navigate to http://0.0.0.0:8002/docs for interactive documentation (Swagger UI).")
    uvicorn.run(app, host="0.0.0.0", port=8002)