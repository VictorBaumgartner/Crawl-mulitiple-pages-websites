import asyncio
import os
import json
import re
from urllib.parse import urljoin, urlparse
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

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
    
    # Remove leftover empty parentheses
    md_text = re.sub(r'\(\)', '', md_text)
    
    # Compact multiple empty lines into one
    md_text = re.sub(r'\n\s*\n+', '\n\n', md_text)
    
    # Clean extra spaces
    md_text = re.sub(r'[ \t]+', ' ', md_text)
    
    return md_text.strip()

async def crawl_website(start_url, output_dir=r"C:\Users\victo\Desktop\crawl\crawl_output", max_concurrency=8, max_depth=2):
    """
    Crawl a website deeply and save each page as a cleaned Markdown file, with parallelization.
    """
    os.makedirs(output_dir, exist_ok=True)

    md_generator = DefaultMarkdownGenerator(
        options={
            "ignore_links": True,  # Not important because we clean after
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
    #crawl_queue.put_nowait(start_url)
    semaphore = asyncio.Semaphore(max_concurrency)
    crawl_queue.put_nowait((start_url, 0))  # ← Stocke aussi la profondeur initiale

    def sanitize_filename(url):
        parsed = urlparse(url)
        path = parsed.path.strip("/").replace("/", "_") or "index"
        netloc = parsed.netloc.replace(".", "_")
        filename = f"{netloc}_{path}.md"
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        return filename[:200]

    async def crawl_page(current_url, current_depth):
        #if len(visited_urls) >= max_pages:
        #    return

        if current_url in visited_urls:
            return

        visited_urls.add(current_url)
        print(f"Crawling ({len(visited_urls)}): {current_url}")

        async with semaphore:
            async with AsyncWebCrawler(verbose=True) as crawler:
                result = await crawler.arun(url=current_url, config=config)

                if result.success:
                    markdown_content = result.markdown.raw_markdown
                    
                    # ✅ Clean the Markdown before saving
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

                #     internal_links = result.links.get("internal", [])
                #     for link in internal_links:
                #         href = link["href"]
                #         absolute_url = urljoin(current_url, href)
                #         if urlparse(absolute_url).netloc == urlparse(start_url).netloc:
                #             if absolute_url not in visited_urls and absolute_url not in queued_urls:
                #                 crawl_queue.put_nowait(absolute_url)
                #                 queued_urls.add(absolute_url)
                # else:
                #     print(f"Failed to crawl {current_url}: {result.error_message}")

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

async def main():
    target_url = "https://www.saint-quentin.fr/109-musee-lecuyer.htm"
    output_dir = r"C:\Users\victo\Desktop\crawl\crawl_output2\musee-lecuyer-saint-quentin"
    await crawl_website(
        start_url=target_url,
        output_dir=output_dir,
        max_concurrency=20,
        max_depth=2
    )

if __name__ == "__main__":
    asyncio.run(main())



# import asyncio
# import os
# import json
# import re
# import csv
# from urllib.parse import urljoin, urlparse
# from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
# from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

# def clean_markdown(md_text):
#     """
#     Cleans Markdown content by:
#     - Removing inline links, footnotes, raw URLs, images
#     - Removing bold, italic, blockquotes
#     - Removing empty headings
#     - Compacting whitespace
#     """
#     # Remove Markdown links but keep the text
#     md_text = re.sub(r'\[([^\]]+)\]\((http[s]?://[^\)]+)\)', r'\1', md_text)
    
#     # Remove raw URLs
#     md_text = re.sub(r'http[s]?://\S+', '', md_text)
    
#     # Remove images
#     md_text = re.sub(r'!\[([^\]]*)\]\((http[s]?://[^\)]+)\)', '', md_text)
    
#     # Remove footnotes
#     md_text = re.sub(r'\[\^?\d+\]', '', md_text)
#     md_text = re.sub(r'^\[\^?\d+\]:\s?.*$', '', md_text, flags=re.MULTILINE)
    
#     # Remove blockquotes
#     md_text = re.sub(r'^\s{0,3}>\s?', '', md_text, flags=re.MULTILINE)
    
#     # Remove bold and italic formatting (**bold**, *italic*, __underline__, _italic_)
#     md_text = re.sub(r'(\*\*|__)(.*?)\1', r'\2', md_text)
#     md_text = re.sub(r'(\*|_)(.*?)\1', r'\2', md_text)
    
#     # Remove empty headings (lines that are only '#', '##', etc.)
#     md_text = re.sub(r'^\s*#+\s*$', '', md_text, flags=re.MULTILINE)
    
#     # Remove leftover empty parentheses
#     md_text = re.sub(r'\(\)', '', md_text)
    
#     # Compact multiple empty lines into one
#     md_text = re.sub(r'\n\s*\n+', '\n\n', md_text)
    
#     # Clean extra spaces
#     md_text = re.sub(r'[ \t]+', ' ', md_text)
    
#     return md_text.strip()


# async def crawl_website(start_url, output_dir, max_pages=10, max_concurrency=5):
#     """
#     Crawl a website deeply and save each page as a cleaned Markdown file, with parallelization.
#     """
#     os.makedirs(output_dir, exist_ok=True)

#     md_generator = DefaultMarkdownGenerator(
#         options={
#             "ignore_links": True,  # Not important because we clean after
#             "escape_html": True,
#             "body_width": 0
#         }
#     )

#     config = CrawlerRunConfig(
#         markdown_generator=md_generator,
#         cache_mode="BYPASS",
#         exclude_external_links=True,
#         exclude_social_media_links=True,
#     )

#     visited_urls = set()
#     crawl_queue = asyncio.Queue()
#     crawl_queue.put_nowait(start_url)
#     semaphore = asyncio.Semaphore(max_concurrency)

#     def sanitize_filename(url):
#         parsed = urlparse(url)
#         path = parsed.path.strip("/").replace("/", "_") or "index"
#         netloc = parsed.netloc.replace(".", "_")
#         filename = f"{netloc}_{path}.md"
#         filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
#         return filename[:200]

#     async def crawl_page(current_url):
#         if len(visited_urls) >= max_pages:
#             return

#         if current_url in visited_urls:
#             return

#         visited_urls.add(current_url)
#         print(f"Crawling ({len(visited_urls)}/{max_pages}): {current_url}")

#         async with semaphore:
#             async with AsyncWebCrawler(verbose=True) as crawler:
#                 result = await crawler.arun(url=current_url, config=config)

#                 if result.success:
#                     markdown_content = result.markdown.raw_markdown
                    
#                     # ✅ Clean the Markdown before saving
#                     cleaned_markdown = clean_markdown(markdown_content)

#                     filename = sanitize_filename(current_url)
#                     output_path = os.path.join(output_dir, filename)

#                     with open(output_path, "w", encoding="utf-8") as f:
#                         f.write(f"# {current_url}\n\n{cleaned_markdown}\n")
#                     print(f"Saved cleaned Markdown to: {output_path}")

#                     internal_links = result.links.get("internal", [])
#                     for link in internal_links:
#                         href = link["href"]
#                         absolute_url = urljoin(current_url, href)
#                         if urlparse(absolute_url).netloc == urlparse(start_url).netloc:
#                             if absolute_url not in visited_urls:
#                                 crawl_queue.put_nowait(absolute_url)
#                 else:
#                     print(f"Failed to crawl {current_url}: {result.error_message}")

#     async def worker():
#         while len(visited_urls) < max_pages and not crawl_queue.empty():
#             current_url = await crawl_queue.get()
#             await crawl_page(current_url)
#             crawl_queue.task_done()

#     tasks = []
#     for _ in range(max_concurrency):
#         tasks.append(asyncio.create_task(worker()))

#     await crawl_queue.join()

#     for task in tasks:
#         task.cancel()

#     metadata_path = os.path.join(output_dir, "metadata.json")
#     with open(metadata_path, "w", encoding="utf-8") as f:
#         json.dump({"crawled_urls": list(visited_urls)}, f, indent=2)
#     print(f"Metadata saved to {metadata_path}")


# async def main():
#     input_csv = r'C:\Users\victo\Desktop\crawl\field_csv.csv'  # Path to your CSV file containing URLs

#     # Read URLs from the CSV file
#     with open(input_csv, mode='r', encoding='utf-8') as file:
#         csv_reader = csv.DictReader(file)
#         urls = [row['url'] for row in csv_reader]

#         for i, row in enumerate(csv_reader):
#             if i >= 3:  # Commencer à la ligne 4
#                 urls.append(row['url'])

#     # Crawl each URL one by one
#     for url in urls:
#         domain_name = urlparse(url).netloc.replace(".", "_")
#         output_dir = os.path.join(r"C:\Users\victo\Desktop\crawl\crawl_output", domain_name)
#         print(f"Starting crawl for {url}...")
#         await crawl_website(start_url=url, output_dir=output_dir, max_pages=100, max_concurrency=5)


# if __name__ == "__main__":
#     asyncio.run(main())


