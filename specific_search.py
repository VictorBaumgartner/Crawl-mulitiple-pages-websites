import asyncio
import os
import hashlib
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator
from urllib.parse import urljoin, urlparse
import json
import re
import logging
from bs4 import BeautifulSoup

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

async def crawl_website(url, output_dir="markdown_files", use_js=False, extraction_prompt=None):
    """
    Crawl a website and save a Markdown file for each URL with targeted content.
    
    Args:
        url (str): The starting URL of the website
        output_dir (str): Directory to save Markdown files
        use_js (bool): Enable JavaScript rendering for dynamic sites
        respect_robots_txt (bool): Respect robots.txt rules
        extraction_prompt (dict): Rules for targeted extraction (e.g., CSS selectors, regex)
    """
    # Create output directory
    os.makedirs(output_dir, exist_ok=True)
    
    # Configure Markdown generator
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
        exclude_external_links=False,  # Allow broader link following
        exclude_social_media_links=True
    )
    
    # Track visited URLs
    visited_urls = set()
    
    def normalize_url(url):
        """Normalize URL to handle redirects and variations."""
        parsed = urlparse(url)
        query = "&".join(sorted(parsed.query.split("&"))) if parsed.query else ""
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path or '/'}"
        if query:
            normalized += f"?{query}"
        return normalized
    
    def sanitize_filename(url):
        """Convert URL to a unique, valid filename."""
        parsed = urlparse(url)
        path = parsed.path.strip("/").replace("/", "_") or "index"
        query = parsed.query.replace("&", "_").replace("=", "_") if parsed.query else ""
        netloc = parsed.netloc.replace(".", "_")
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        filename = f"{netloc}_{path}_{query}_{url_hash}.md" if query else f"{netloc}_{path}_{url_hash}.md"
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        return filename[:200]
    
    def extract_targeted_content(html, prompt):
        """Extract content based on user-defined prompt (CSS selectors or regex)."""
        if not prompt or not html:
            return None
        
        try:
            soup = BeautifulSoup(html, "html.parser")
            extracted = []
            
            # CSS selector extraction
            if "css_selector" in prompt:
                elements = soup.select(prompt["css_selector"])
                extracted.extend([elem.get_text(strip=True) for elem in elements if elem.get_text(strip=True)])
                logger.info(f"Extracted {len(extracted)} items using CSS selector: {prompt['css_selector']}")
            
            # Regex extraction
            if "regex" in prompt:
                matches = re.findall(prompt["regex"], html, re.DOTALL)
                extracted.extend([match.strip() for match in matches if match.strip()])
                logger.info(f"Extracted {len(matches)} items using regex: {prompt['regex']}")
            
            return "\n\n".join(extracted) if extracted else None
        except Exception as e:
            logger.error(f"Extraction failed: {str(e)}")
            return None
    
    def extract_links_fallback(html, base_url):
        """Extract links using BeautifulSoup and regex."""
        links = []
        try:
            soup = BeautifulSoup(html, "html.parser")
            for a_tag in soup.find_all("a", href=True):
                href = a_tag["href"]
                links.append({"href": href})
            
            href_pattern = r'href=[\'"]?([^\'" >]+)[\'">]'
            for match in re.finditer(href_pattern, html):
                href = match.group(1)
                if href not in [link["href"] for link in links]:
                    links.append({"href": href})
        except Exception as e:
            logger.warning(f"Fallback link extraction failed: {str(e)}")
        return links
    
    async def crawl_page(current_url, depth=0):
        normalized_url = normalize_url(current_url)
        if normalized_url in visited_urls:
            logger.info(f"Skipping already visited URL: {current_url} (Normalized: {normalized_url})")
            return
        
        visited_urls.add(normalized_url)
        logger.info(f"Crawling: {current_url} (Depth: {depth}, Total pages: {len(visited_urls)})")
        
        async with AsyncWebCrawler(verbose=True) as crawler:
            for attempt in range(3):  # Retry up to 3 times
                try:
                    result = await crawler.arun(url=current_url, config=config)
                    
                    if result.success:
                        # Extract content (targeted or full)
                        content = extract_targeted_content(result.html, extraction_prompt) if extraction_prompt else result.markdown.raw_markdown
                        if not content:
                            logger.warning(f"No content extracted for {current_url}")
                            content = result.markdown.raw_markdown  # Fallback to full content
                        
                        logger.info(f"Content length: {len(content)} characters")
                        
                        # Save content
                        filename = sanitize_filename(current_url)
                        output_path = os.path.join(output_dir, filename)
                        with open(output_path, "w", encoding="utf-8") as f:
                            f.write(f"# {current_url}\n\n{content}\n")
                        logger.info(f"Saved content to {output_path}")
                        
                        # Extract internal links
                        internal_links = result.links.get("internal", [])
                        logger.info(f"Found {len(internal_links)} internal links via Crawl4AI")
                        
                        # Fallback link extraction
                        if not internal_links and result.html:
                            logger.warning(f"No internal links found, trying fallback extraction")
                            internal_links = extract_links_fallback(result.html, current_url)
                            logger.info(f"Fallback found {len(internal_links)} links")
                        
                        # Log links
                        for link in internal_links:
                            logger.debug(f"Internal link: {link['href']}")
                        
                        # Crawl internal links
                        for link in internal_links:
                            href = link["href"]
                            absolute_url = urljoin(current_url, href)
                            if urlparse(absolute_url).netloc.endswith(urlparse(url).netloc):
                                await crawl_page(absolute_url, depth + 1)
                            else:
                                logger.debug(f"Skipping non-matching domain: {absolute_url}")
                        break  # Exit retry loop on success
                    else:
                        logger.error(f"Attempt {attempt + 1} failed for {current_url}: {result.error_message}")
                except Exception as e:
                    logger.error(f"Attempt {attempt + 1} exception for {current_url}: {str(e)}")
                if attempt < 2:
                    logger.info(f"Retrying {current_url} in 2 seconds...")
                    await asyncio.sleep(2)
    
    # Start crawling
    await crawl_page(url, depth=0)
    
    # Save metadata
    metadata_path = os.path.join(output_dir, "crawl_metadata.json")
    with open(metadata_path, "w", encoding="utf-8") as f:
        json.dump({"crawled_urls": list(visited_urls)}, f, indent=2)
    logger.info(f"Metadata saved to {metadata_path}")

async def main():
    target_url = "https://www.w3.org"  # Replace with your target URL
    # Example extraction prompt (customize as needed)
    extraction_prompt = {
        "css_selector": "article, .content, main",  # Extract main content sections
        "regex": r"<h[1-6][^>]*>.*?</h[1-6]>"  # Extract all headings
    }
    await crawl_website(
        target_url,
        output_dir="markdown_files",
        use_js=True,  # Enable JavaScript
        respect_robots_txt=True,
        extraction_prompt=extraction_prompt
    )

if __name__ == "__main__":
    asyncio.run(main())