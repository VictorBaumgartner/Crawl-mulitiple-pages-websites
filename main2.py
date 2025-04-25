import asyncio
import os
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from urllib.parse import urljoin, urlparse
import json
import re
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def crawl_website(url, output_dir="markdown_files", use_js=False, respect_robots_txt=True):
    """
    Crawl a website and save a Markdown file for each URL with full body content.
    
    Args:
        url (str): The starting URL of the website
        output_dir (str): Directory to save Markdown files
        use_js (bool): Enable JavaScript rendering for dynamic sites
        respect_robots_txt (bool): Respect robots.txt rules
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Configure Markdown generator without pruning
    md_generator = DefaultMarkdownGenerator(
        options={
            "ignore_links": False,
            "escape_html": True,
            "body_width": 0  # No width limit
        }
    )
    
    # Crawler configuration
    config = CrawlerRunConfig(
        markdown_generator=md_generator,
        cache_mode="BYPASS",
        exclude_external_links=True,
        exclude_social_media_links=True
    )
    
    # Track visited URLs
    visited_urls = set()
    
    def sanitize_filename(url):
        """Convert URL to a valid filename."""
        parsed = urlparse(url)
        path = parsed.path.strip("/").replace("/", "_") or "index"
        netloc = parsed.netloc.replace(".", "_")
        filename = f"{netloc}_{path}.md"
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        return filename[:200]
    
    def extract_links_fallback(html, base_url):
        """Fallback method to extract links using regex."""
        links = []
        href_pattern = r'href=[\'"]?([^\'" >]+)[\'">]'
        for match in re.finditer(href_pattern, html):
            href = match.group(1)
            absolute_url = urljoin(base_url, href)
            if urlparse(absolute_url).netloc.endswith(urlparse(base_url).netloc):
                links.append({"href": href})
        return links
    
    async def crawl_page(current_url):
        if current_url in visited_urls:
            logger.info(f"Skipping already visited URL: {current_url}")
            return
        
        visited_urls.add(current_url)
        logger.info(f"Crawling: {current_url} (Total pages: {len(visited_urls)})")
        
        async with AsyncWebCrawler(verbose=True) as crawler:
            try:
                result = await crawler.arun(url=current_url, config=config)
                
                if result.success:
                    # Get full Markdown content
                    markdown_content = result.markdown.raw_markdown
                    logger.info(f"Markdown length: {len(markdown_content)} characters")
                    
                    # Save Markdown
                    filename = sanitize_filename(current_url)
                    output_path = os.path.join(output_dir, filename)
                    with open(output_path, "w", encoding="utf-8") as f:
                        f.write(f"# {current_url}\n\n{markdown_content}\n")
                    logger.info(f"Saved Markdown to {output_path}")
                    
                    # Extract internal links
                    internal_links = result.links.get("internal", [])
                    logger.info(f"Found {len(internal_links)} internal links")
                    
                    # Fallback link extraction if no links found
                    if not internal_links and result.html:
                        logger.warning(f"No internal links found, trying fallback extraction")
                        internal_links = extract_links_fallback(result.html, current_url)
                        logger.info(f"Fallback found {len(internal_links)} links")
                    
                    # Log links for debugging
                    for link in internal_links:
                        logger.debug(f"Internal link: {link['href']}")
                    
                    # Crawl internal links
                    for link in internal_links:
                        href = link["href"]
                        absolute_url = urljoin(current_url, href)
                        # Relaxed domain check to include subdomains
                        if urlparse(absolute_url).netloc.endswith(urlparse(url).netloc):
                            await crawl_page(absolute_url)
                        else:
                            logger.debug(f"Skipping non-matching domain: {absolute_url}")
                else:
                    logger.error(f"Failed to crawl {current_url}: {result.error_message}")
            except Exception as e:
                logger.error(f"Exception while crawling {current_url}: {str(e)}")
    
    # Start crawling
    await crawl_page(url)
    
    # Save metadata
    metadata_path = os.path.join(output_dir, r"C:\Users\victo\Desktop\crawl\crawl_metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump({r"C:\Users\victo\Desktop\crawl\crawled_urls": list(visited_urls)}, f, indent=2)
    logger.info(f"Metadata saved to {metadata_path}")

async def main():
    target_url = "https://www.w3.org"  # Replace with your target URL
    await crawl_website(
        target_url,
        output_dir="markdown_files",
        use_js=False,  # Set to True for JavaScript-heavy sites
    )

if __name__ == "__main__":
    asyncio.run(main())