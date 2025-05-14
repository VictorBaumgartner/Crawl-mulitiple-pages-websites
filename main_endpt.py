from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl
import asyncio
import os
import json
import re
from urllib.parse import urljoin, urlparse
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from typing import Optional

app = FastAPI()

class CrawlRequest(BaseModel):
    start_url: HttpUrl
    output_dir: str = "./crawl_output2"
    max_concurrency: int = 8
    max_depth: int = 2

def clean_markdown(md_text: str) -> str:
    """
    Cleans Markdown content by:
    - Removing inline links, footnotes, raw URLs, images
    - Removing bold, italic, blockquotes
    - Removing empty headings
    - Compacting whitespace
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

async def crawl_website(start_url: str, output_dir: str, max_concurrency: int, max_depth: int):
    """
    Crawl a website deeply and save each page as a cleaned Markdown file, with parallelization.
    """
    try:
        os.makedirs(output_dir, exist_ok=True)

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
        semaphore = asyncio.Semaphore(max_concurrency)
        crawl_queue.put_nowait((start_url, 0))

        def sanitize_filename(url: str) -> str:
            parsed = urlparse(url)
            path = parsed.path.strip("/").replace("/", "_") or "index"
            netloc = parsed.netloc.replace(".", "_")
            filename = f"{netloc}_{path}.md"
            filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
            return filename[:200]

        async def crawl_page(current_url: str, current_depth: int):
            if current_url in visited_urls:
                return

            visited_urls.add(current_url)
            print(f"Crawling ({len(visited_urls)}): {current_url}")

            async with semaphore:
                async with AsyncWebCrawler(verbose=True) as crawler:
                    result = await crawler.arun(url=current_url, config=config)

                    if result.success:
                        markdown_content = result.markdown.raw_markdown
                        cleaned_markdown = clean_markdown(markdown_content)

                        filename = sanitize_filename(current_url)
                        output_path = os.path.join(output_dir, filename)

                        with open(output_path, "w", encoding="utf-8") as f:
                            f.write(f"# {current_url}\n\n{cleaned_markdown}\n")
                        print(f"Saved cleaned Markdown to: {output_path}")

                        if current_depth < max_depth:
                            internal_links = result.links.get("internal", [])
                            for link in internal_links:
                                href = link["href"]
                                absolute_url = urljoin(current_url, href)
                                if urlparse(absolute_url).netloc == urlparse(start_url).netloc:
                                    if absolute_url not in visited_urls and absolute_url not in queued_urls:
                                        crawl_queue.put_nowait((absolute_url, current_depth + 1))
                                        queued_urls.add(absolute_url)
                    else:
                        print(f"Failed to crawl {current_url}: {result.error_message}")

        async def worker():
            while not crawl_queue.empty():
                current_url, current_depth = await crawl_queue.get()
                await crawl_page(current_url, current_depth)
                crawl_queue.task_done()

        tasks = [asyncio.create_task(worker()) for _ in range(max_concurrency)]
        await crawl_queue.join()

        for task in tasks:
            task.cancel()

        metadata_path = os.path.join(output_dir, "metadata.json")
        with open(metadata_path, "w", encoding="utf-8") as f:
            json.dump({"crawled_urls": list(visited_urls)}, f, indent=2)
        print(f"Metadata saved to {metadata_path}")

        return {"status": "success", "crawled_urls": list(visited_urls), "metadata_path": metadata_path}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during crawling: {str(e)}")

@app.post("/crawl")
async def crawl_endpoint(request: CrawlRequest):
    """
    FastAPI endpoint to crawl a website and save content as cleaned Markdown files.
    """
    result = await crawl_website(
        start_url=str(request.start_url),
        output_dir=request.output_dir,
        max_concurrency=request.max_concurrency,
        max_depth=request.max_depth
    )
    return result

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8002)