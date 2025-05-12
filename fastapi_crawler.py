import logging
from fastapi import FastAPI, BackgroundTasks
from pydantic import BaseModel
import asyncio
import os
import json
import re
from urllib.parse import urljoin, urlparse
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

def clean_markdown(md_text):
    """
    Cleans Markdown content by removing inline links, footnotes, URLs, images,
    bold/italic formatting, blockquotes, empty headings, and compacting whitespace.
    """
    md_text = re.sub(r'\[([^\]]+)\]\((http[s]?://[^\)]+)\)', r'\1', md_text)
    md_text = re.sub(r'http[s]?://\S+', '', md_text)
    md_text = re.sub(r'!\[([^\]]*)\]\((http[s]?://[^\)]+)\)', '', md_text)
    md_text = re.sub(r'\[\^?\d+\]', '', md_text)
    md_text = re.sub(r'^\[\^?\d+\]:\s?.*$', '', md_text, flags=re.MULTILINE)
    md_text = re.sub(r'^\s{0,3}>\s?', '', md_text, flags=re.MULTILINE)
    md_text = re.sub(r'(\*\*|__)(.*?)\1', r'\2', md_text)
    md_text = re.sub(r'(\*|_)(.*?)\1', r'\2', md_text)
    md_text = re.sub(r'^\s*#+\s*$', '', md_text, flags=re.MULTILINE)
    md_text = re.sub(r'\(\)', '', md_text)
    md_text = re.sub(r'\n\s*\n+', '\n\n', md_text)
    md_text = re.sub(r'[ \t]+', ' ', md_text)
    return md_text.strip()

async def crawl_website(start_url, output_dir="crawl_output", max_concurrency=8):
    """
    Crawl a website deeply and save each page as a cleaned Markdown file, with parallelization.
    """
    logger.info(f"Starting crawl for {start_url} with output_dir {output_dir} and max_concurrency {max_concurrency}")
    
    try:
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Output directory ensured: {output_dir}")
    except Exception as e:
        logger.error(f"Failed to create output directory {output_dir}: {e}")
        return

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
        exclude_external_links=True,
        exclude_social_media_links=True,
    )

    visited_urls = set()
    queued_urls = set()
    crawl_queue = asyncio.Queue()
    crawl_queue.put_nowait(start_url)
    logger.info(f"Queue initialized with start_url: {start_url}")
    semaphore = asyncio.Semaphore(max_concurrency)

    def sanitize_filename(url):
        parsed = urlparse(url)
        path = parsed.path.strip("/").replace("/", "_") or "index"
        netloc = parsed.netloc.replace(".", "_")
        filename = f"{netloc}_{path}.md"
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        return filename[:200]

    async def crawl_page(current_url):
        if current_url in visited_urls:
            return

        visited_urls.add(current_url)
        logger.info(f"Crawling ({len(visited_urls)}): {current_url}")

        async with semaphore:
            async with AsyncWebCrawler(verbose=True) as crawler:
                try:
                    result = await crawler.arun(url=current_url, config=config)
                    if result.success:
                        markdown_content = result.markdown.raw_markdown
                        cleaned_markdown = clean_markdown(markdown_content)

                        filename = sanitize_filename(current_url)
                        output_path = os.path.join(output_dir, filename)

                        try:
                            with open(output_path, "w", encoding="utf-8") as f:
                                f.write(f"# {current_url}\n\n{cleaned_markdown}\n")
                            logger.info(f"Saved cleaned Markdown to: {output_path}")
                        except Exception as e:
                            logger.error(f"Error saving file {output_path}: {e}")

                        internal_links = result.links.get("internal", [])
                        logger.info(f"Found {len(internal_links)} internal links on {current_url}")
                        for link in internal_links:
                            href = link["href"]
                            absolute_url = urljoin(current_url, href)
                            if urlparse(absolute_url).netloc == urlparse(start_url).netloc:
                                if absolute_url not in visited_urls and absolute_url not in queued_urls:
                                    crawl_queue.put_nowait(absolute_url)
                                    queued_urls.add(absolute_url)
                    else:
                        logger.warning(f"Failed to crawl {current_url}: {result.error_message}")
                except Exception as e:
                    logger.error(f"Exception during crawling {current_url}: {e}")

    async def worker():
        while not crawl_queue.empty():
            current_url = await crawl_queue.get()
            await crawl_page(current_url)
            crawl_queue.task_done()

    tasks = []
    for _ in range(max_concurrency):
        tasks.append(asyncio.create_task(worker()))
    logger.info(f"Created {max_concurrency} worker tasks")

    await crawl_queue.join()
    logger.info("All crawling tasks completed")

    for task in tasks:
        task.cancel()

    metadata_path = os.path.join(output_dir, "metadata.json")
    try:
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump({"crawled_urls": list(visited_urls)}, f, indent=2)
        logger.info(f"Metadata saved to {metadata_path}")
    except Exception as e:
        logger.error(f"Error saving metadata {metadata_path}: {e}")

class CrawlRequest(BaseModel):
    start_url: str
    output_dir: str = "crawl_output"
    max_concurrency: int = 8

@app.post("/crawl")
async def start_crawl(request: CrawlRequest, background_tasks: BackgroundTasks):
    logger.info(f"Received crawl request for {request.start_url}")
    background_tasks.add_task(crawl_website, request.start_url, request.output_dir, request.max_concurrency)
    return {"message": f"Crawling started in the background. Results will be saved to {request.output_dir}"}