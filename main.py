import asyncio
import os
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from urllib.parse import urljoin, urlparse
import json
import re

async def crawl_website(url, max_pages=10, output_dir="markdown_files"):
    """
    Crawl a website and save a Markdown file for each URL with full body content.
    
    Args:
        url (str): The starting URL of the website
                max_pages (int): Maximum number of pages to crawl
        output_dir (str): Directory to save Markdown files
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Configure Markdown generator without pruning
    md_generator = DefaultMarkdownGenerator(
        options={
            "ignore_links": False,
            "escape_html": True,
            "body_width": 0  # No width limit for full content
        }
    )
    
    # Crawler configuration
    config = CrawlerRunConfig(
        markdown_generator=md_generator,
        cache_mode="BYPASS",
        exclude_external_links=True,
        exclude_social_media_links=True,
    )
    
    # Track visited URLs to avoid duplicates
    visited_urls = set()
    all_markdown = []

    def sanitize_filename(url):
        """Convert URL to a valid filename."""
        parsed = urlparse(url)
        path = parsed.path.strip("/").replace("/", "_") or "index"
        netloc = parsed.netloc.replace(".", "_")
        filename = f"{netloc}_{path}.md"
        # Remove invalid characters for filenames
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        return filename[:200]  # Truncate to avoid overly long filenames
    
    async def crawl_page(current_url):
        if len(visited_urls) >= max_pages or current_url in visited_urls:
            return
        
        visited_urls.add(current_url)
        print(f"Crawling: {current_url} (Total pages: {len(visited_urls)})")
        
        async with AsyncWebCrawler(verbose=True) as crawler:
            result = await crawler.arun(url=current_url, config=config)
            
            if result.success:
                # Get full Markdown content
                markdown_content = result.markdown.raw_markdown
                
                # Generate filename from URL
                filename = sanitize_filename(current_url)
                output_path = os.path.join(output_dir, filename)
                
                # Save Markdown to file
                with open(output_path, "w", encoding="utf-8") as f:
                    f.write(f"# {current_url}\n\n{markdown_content}\n")
                print(f"Saved Markdown to {output_path}")
                
                # Extract and crawl internal links
                internal_links = result.links.get("internal", [])
                for link in internal_links:
                    href = link["href"]
                    absolute_url = urljoin(current_url, href)
                    if urlparse(absolute_url).netloc == urlparse(url).netloc:
                        await crawl_page(absolute_url)
            else:
                print(f"Failed to crawl {current_url}: {result.error_message}")
    
    # Start crawling
    await crawl_page(url)
    
    # Save metadata
    with open(os.path.join(output_dir, "crawl_metadata.json"), "w", encoding="utf-8") as f:
        json.dump({"crawled_urls": list(visited_urls)}, f, indent=2)
    print(f"Metadata saved to {os.path.join(output_dir, 'crawl_metadata.json')}")

async def main():
    target_url = "https://youtube.com"
    await crawl_website(target_url, output_dir="markdown_files", max_pages=10)

if __name__ == "__main__":
    asyncio.run(main())