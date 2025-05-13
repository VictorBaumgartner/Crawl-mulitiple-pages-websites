import logging
import re
import os
import json
import asyncio
import hashlib
from pathlib import Path
from urllib.parse import urljoin, urlparse
from datetime import datetime
from typing import Optional, List, Dict, Any, Union

import aiofiles
import aiohttp
import html2text
from bs4 import BeautifulSoup
from fastapi import FastAPI, HTTPException, Query
from playwright.async_api import async_playwright
from pydantic import BaseModel

# (Keep your existing logging setup, FastAPI app, and Pydantic models)
# Configuration du logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Web Scraper API", description="API pour scraper des pages web statiques et dynamiques")

# Modèle pour les requêtes de scraping
class ScrapeRequest(BaseModel):
    url: str
    output_path: str = "./scraped_data"
    dynamic: bool = False
    wait_time: int = 5
    wait_for_selector: Optional[str] = None
    extract_links: bool = True
    extract_images: bool = True
    extract_text: bool = True
    extract_markdown: bool = True
    css_selector: Optional[str] = None
    filename_prefix: Optional[str] = None
    max_pages: int = 3000

# Modèle pour les requêtes de scraping multiple
class MultiScrapeRequest(BaseModel):
    urls: List[str]
    output_path: str = "./scraped_data"
    dynamic: bool = False
    wait_time: int = 5
    wait_for_selector: Optional[str] = None
    extract_links: bool = True
    extract_images: bool = True
    extract_text: bool = True
    extract_markdown: bool = True
    css_selector: Optional[str] = None
    parallel_workers: int = 18
    filename_prefix: Optional[str] = None
    max_pages: int = 3000


# (Keep your discover_site_urls, scrape_site_endpoint, get_safe_filename, html_to_markdown, extract_metadata, save_scraped_data functions as they are generally fine)
# ... (all helper functions from your provided code) ...
# For brevity, I'll assume they are present here. The key changes are in scrape_static_page, scrape_dynamic_page (minor), and scrape_page.

async def scrape_static_page(url: str) -> tuple[str, Optional[str]]:
    """
    Scrapes a static page.
    Returns:
        A tuple (html_content, content_type). html_content can be empty if not text/html.
    """
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=30, allow_redirects=True) as response:
                if response.status >= 400:
                    raise HTTPException(status_code=response.status, 
                                        detail=f"Erreur HTTP {response.status} pour {url}")
                
                content_type = response.headers.get('Content-Type', '').lower()
                content = await response.text() # Read content regardless for now
                return content, content_type
                
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        logger.error(f"Erreur lors du scraping statique de {url}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du scraping statique de {url}: {str(e)}")

async def scrape_dynamic_page(url: str, wait_time: int = 5, wait_for_selector: Optional[str] = None) -> str:
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = await context.new_page()
            
            # Handle cases where the URL itself might be a direct link to an image/binary
            # Playwright will try to render it, page.content() will give the HTML representation.
            # So, the content_type check will be more crucial in scrape_page
            try:
                await page.goto(url, wait_until="networkidle", timeout=60000)
            except Exception as e: # Broad exception for goto errors
                logger.error(f"Playwright: Erreur 'goto' pour {url}: {e}")
                await browser.close()
                # Return a minimal HTML structure or raise, so scrape_page can handle it
                # This indicates the page itself might not be navigable as standard HTML
                raise HTTPException(status_code=500, detail=f"Playwright: Erreur 'goto' pour {url}: {e}")


            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, timeout=wait_time * 1000)
                except Exception: # Catch Playwright's TimeoutError
                    logger.warning(f"Sélecteur '{wait_for_selector}' non trouvé sur {url} (dynamique)")
                    pass # Continue even if selector isn't found
            else:
                await asyncio.sleep(wait_time)
            
            content = await page.content()
            await browser.close()
            return content
    except Exception as e:
        logger.error(f"Erreur lors du scraping dynamique de {url}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du scraping dynamique de {url}: {str(e)}")

async def scrape_page(
    url: str,
    output_path: str,
    dynamic: bool = False,
    wait_time: int = 5,
    wait_for_selector: Optional[str] = None,
    extract_links: bool = True,
    extract_images: bool = True,
    extract_text: bool = True,
    extract_markdown: bool = True,
    css_selector: Optional[str] = None,
    filename_prefix: Optional[str] = None
) -> Dict[str, Any]:
    try:
        logger.info(f"Scraping de {url} (mode: {'dynamique' if dynamic else 'statique'})")
        
        if not re.match(r'^https?://', url):
            logger.error(f"URL invalide: {url}. Tentative de scraping annulée.")
            return {"url": url, "error": f"URL invalide: {url}. L'URL doit commencer par http:// ou https://"}

        # --- Solution Part 1: Handle problematic 'string' selector ---
        if css_selector and css_selector.lower() == "string":
            logger.warning(f"Le sélecteur CSS non valide 'string' a été fourni pour {url}. "
                           f"Il sera ignoré et le contenu de la page entière sera traité.")
            css_selector = None 

        html_content = ""
        content_type_header = None # For static pages

        # --- Solution Part 2: Handle non-HTML content (basic check for URL extension) ---
        # More robust check will be done after fetching content for static pages
        is_likely_binary = any(url.lower().endswith(ext) for ext in 
                                ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.pdf', '.zip', '.gz', '.webp', '.svg',
                                 '.mp4', '.mov', '.avi', '.mp3', '.wav'])
        
        if is_likely_binary and not dynamic: # For static, we can be more certain. Dynamic might wrap it.
             logger.info(f"L'URL {url} semble pointer vers un fichier binaire/image (vérification par extension). "
                        f"Extraction de contenu HTML structuré sera probablement ignorée.")
             # Attempt to fetch anyway, content_type will confirm
        
        if dynamic:
            html_content = await scrape_dynamic_page(url, wait_time, wait_for_selector)
            # For dynamic content, we assume it's HTML-like as Playwright renders it.
            # If the URL was a direct image, Playwright might wrap it in a simple HTML.
        else:
            html_content, content_type_header = await scrape_static_page(url)
            if content_type_header and not any(ct in content_type_header for ct in ['text/html', 'application/xhtml+xml', 'application/xml']):
                logger.warning(f"Contenu non-HTML (type: {content_type_header}) détecté pour {url} via requête statique. "
                               f"Extraction de texte/liens/images HTML sera limitée.")
                # Create metadata and save what we have (e.g., the raw content if it's text-based, or just URL)
                metadata = {"url": url, "scrape_time": datetime.now().isoformat(), "title": os.path.basename(urlparse(url).path), "content_type": content_type_header}
                result_data = {"metadata": metadata}
                if "text/" in content_type_header: # if it's some form of text
                     result_data["text"] = html_content # Store the raw text
                
                filename_base = get_safe_filename(url, filename_prefix)
                saved_files = await save_scraped_data(result_data, output_path, filename_base)
                result_data["saved_files"] = saved_files
                return result_data

        if not html_content:
            logger.warning(f"Aucun contenu HTML récupéré pour {url}.")
            return {"url": url, "error": "Aucun contenu HTML récupéré."}

        soup = BeautifulSoup(html_content, 'html.parser')
        metadata = extract_metadata(soup, url) # Extract metadata from the whole page first
        
        target_soup = soup # This will be the soup used for extraction (either full or selected part)

        # --- Solution Part 3: Refined CSS selector logic ---
        if css_selector: # This is after "string" has been handled (set to None)
            selected_elements = soup.select(css_selector)
            if not selected_elements:
                logger.warning(f"Aucun contenu trouvé avec le sélecteur CSS spécifié: '{css_selector}' pour {url}. "
                               f"Le contenu de la page entière sera utilisé pour l'extraction.")
                # target_soup remains the original full soup, which is the desired fallback
            else:
                logger.info(f"Contenu trouvé et filtré avec le sélecteur '{css_selector}' pour {url}.")
                # Create a new soup from selected elements to isolate them
                # This is better than modifying the original 'soup' in place
                # We join their string representations to form a new HTML snippet
                selected_html_snippet = "".join(str(el) for el in selected_elements)
                target_soup = BeautifulSoup(selected_html_snippet, 'html.parser')
        
        result = {"metadata": metadata, "html": html_content} # Always save original full HTML

        # Perform extractions using the target_soup (which is either full page or selected part)
        if extract_text:
            # Create a temporary soup for text extraction to avoid modifying target_soup if it's used elsewhere
            text_extraction_soup = BeautifulSoup(str(target_soup), 'html.parser')
            for script_or_style in text_extraction_soup(["script", "style"]):
                script_or_style.extract()
            text = text_extraction_soup.get_text(separator="\n", strip=True)
            text = re.sub(r'\n\s*\n+', '\n', text) # Consolidate multiple newlines
            text = re.sub(r' +', ' ', text) # Consolidate multiple spaces
            result["text"] = text.strip()
        
        if extract_markdown:
            markdown_content = html_to_markdown(str(target_soup))
            result["markdown"] = markdown_content
        
        if extract_links:
            links = []
            for a_tag in target_soup.find_all('a', href=True):
                href = a_tag['href']
                absolute_url = urljoin(url, href) # Base URL for resolving relative links is the original page URL
                link_text = a_tag.get_text(strip=True) or "[Lien sans texte]"
                links.append({"url": absolute_url, "text": link_text})
            result["links"] = links
            
        if extract_images:
            images = []
            for img_tag in target_soup.find_all('img'):
                src = img_tag.get('src')
                if src: # Ensure src attribute exists
                    absolute_src = urljoin(url, src) # Base URL for resolving relative links
                    alt_text = img_tag.get('alt', '').strip()
                    images.append({"src": absolute_src, "alt": alt_text})
            result["images"] = images
            
        filename_base = get_safe_filename(url, filename_prefix)
        saved_files = await save_scraped_data(result, output_path, filename_base)
        result["saved_files"] = saved_files
        
        return result
    
    except HTTPException as he: # Re-raise HTTPExceptions from called functions
        logger.error(f"HTTPException lors du scraping de {url}: {he.detail}")
        return {"url": url, "error": f"HTTPException: {he.status_code} - {he.detail}"}
    except Exception as e:
        import traceback
        logger.error(f"Erreur inattendue lors du scraping de {url}: {str(e)}\n{traceback.format_exc()}")
        return {"url": url, "error": str(e), "traceback": traceback.format_exc()}

# (Keep your discover_site_urls, get_safe_filename, html_to_markdown, extract_metadata, save_scraped_data,
#  scrape_site_endpoint, scrape_endpoint, scrape_multiple_endpoint, and root endpoint functions as they were.)
# Make sure they call the modified scrape_page.

# Example of how discover_site_urls, scrape_endpoint etc. would remain:
async def discover_site_urls(base_url: str, html_content: str, current_depth: int = 0, max_depth: int = 2, 
                             already_discovered: Optional[set] = None) -> set:
    if already_discovered is None:
        already_discovered = set()
    
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    new_urls = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        absolute_url = urljoin(base_url, href)
        parsed_url = urlparse(absolute_url)
        
        if parsed_url.netloc == base_domain and parsed_url.scheme in ('http', 'https'):
            normalized_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
            if parsed_url.query:
                normalized_url += f"?{parsed_url.query}"
            
            if normalized_url not in already_discovered:
                new_urls.add(normalized_url)
                already_discovered.add(normalized_url)
    
    if current_depth < max_depth and new_urls:
        tasks = []
        # Limit concurrent discovery calls to avoid overwhelming servers or getting rate-limited
        # This part is illustrative; actual concurrency management might be needed if many new URLs are found
        for i, url_to_discover in enumerate(list(new_urls)):
            if i >= 20: # Limit discovery per level to prevent explosion
                 logger.warning(f"Limitation de la découverte à 20 URLs pour le niveau {current_depth+1} à partir de {base_url}")
                 break
            try:
                # Using a new session for each discovery to simplify, but a shared session could be more efficient
                async with aiohttp.ClientSession() as session:
                    async with session.get(url_to_discover, timeout=10) as response:
                        if response.status == 200:
                            content, content_type = await response.text(), response.headers.get('Content-Type', '').lower()
                            if 'text/html' in content_type: # Only discover from HTML pages
                                await discover_site_urls(url_to_discover, content, current_depth + 1, max_depth, already_discovered)
            except Exception as e:
                logger.warning(f"Erreur lors de la tentative de découverte de {url_to_discover}: {str(e)}")
                
    return already_discovered

def get_safe_filename(url: str, prefix: Optional[str] = None) -> str:
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.replace("www.", "")
    path = parsed_url.path.rstrip("/")
    
    if not path or path == "/": # Handle root URL
        path_segment = "index"
    else:
        path_segment = os.path.basename(path) if os.path.basename(path) else os.path.basename(os.path.dirname(path))

    # Clean the path segment
    path_segment = re.sub(r'[^\w\-_.]', '_', path_segment) # Allow dots for extensions
    path_segment = re.sub(r'_+', '_', path_segment).strip('_')

    # Create a hash for uniqueness if path is too generic or long
    path_hash = hashlib.md5(url.encode()).hexdigest()[:8]

    # Limit length
    safe_path = (path_segment[:50] if path_segment else "page") + "_" + path_hash
    
    filename = f"{domain}_{safe_path}" if not prefix else f"{prefix}_{domain}_{safe_path}"
    return filename[:150] # Overall filename length limit

def html_to_markdown(html_content: str) -> str:
    h2t = html2text.HTML2Text()
    h2t.ignore_links = False
    h2t.body_width = 0 
    h2t.protect_links = True
    h2t.unicode_snob = True
    h2t.single_line_break = True
    h2t.mark_code = True
    try:
        return h2t.handle(html_content)
    except Exception as e:
        logger.error(f"Erreur de conversion HTML vers Markdown: {e}")
        return f"Erreur de conversion en Markdown: {e}"


def extract_metadata(soup: BeautifulSoup, url: str) -> Dict[str, Any]:
    metadata = {
        "url": url,
        "title": None,
        "description": None,
        "keywords": None,
        "author": None,
        "og_tags": {},
        "twitter_tags": {},
        "scrape_time": datetime.now().isoformat()
    }
    
    title_tag = soup.find("title")
    if title_tag and title_tag.string:
        metadata["title"] = title_tag.string.strip()
    
    for meta in soup.find_all("meta"):
        name = meta.get("name", "").lower()
        prop = meta.get("property", "").lower() # Corrected variable name
        content = meta.get("content", "")
        
        if not content: continue # Skip empty content

        if name == "description":
            metadata["description"] = content
        elif name == "keywords":
            metadata["keywords"] = content
        elif name == "author":
            metadata["author"] = content
        elif prop.startswith("og:"):
            metadata["og_tags"][prop[3:]] = content
        elif prop.startswith("twitter:"): # Corrected from property to prop
            metadata["twitter_tags"][prop[8:]] = content # Corrected from property to prop
    
    return metadata

async def save_scraped_data(data: Dict[str, Any], output_path: str, filename_base: str) -> Dict[str, str]:
    saved_files = {}
    
    Path(output_path).mkdir(parents=True, exist_ok=True) # Use pathlib for path creation
    
    if data.get("metadata"):
        metadata_path = os.path.join(output_path, f"{filename_base}_metadata.json")
        async with aiofiles.open(metadata_path, mode='w', encoding='utf-8') as f:
            await f.write(json.dumps(data["metadata"], ensure_ascii=False, indent=4))
        saved_files["metadata"] = metadata_path
    
    if data.get("text"):
        text_path = os.path.join(output_path, f"{filename_base}_content.txt")
        async with aiofiles.open(text_path, mode='w', encoding='utf-8') as f:
            await f.write(data["text"])
        saved_files["text"] = text_path
    
    if data.get("markdown"):
        md_path = os.path.join(output_path, f"{filename_base}.md")
        async with aiofiles.open(md_path, mode='w', encoding='utf-8') as f:
            await f.write(data["markdown"])
        saved_files["markdown"] = md_path
    
    if data.get("links"):
        links_path = os.path.join(output_path, f"{filename_base}_links.json")
        async with aiofiles.open(links_path, mode='w', encoding='utf-8') as f:
            await f.write(json.dumps(data["links"], ensure_ascii=False, indent=4))
        saved_files["links"] = links_path
    
    if data.get("images"):
        images_path = os.path.join(output_path, f"{filename_base}_images.json")
        async with aiofiles.open(images_path, mode='w', encoding='utf-8') as f:
            await f.write(json.dumps(data["images"], ensure_ascii=False, indent=4))
        saved_files["images"] = images_path
    
    if data.get("html"): # Save raw HTML
        html_path = os.path.join(output_path, f"{filename_base}.html")
        async with aiofiles.open(html_path, mode='w', encoding='utf-8') as f:
            await f.write(data["html"])
        saved_files["html"] = html_path
            
    return saved_files

@app.post("/scrape-site")
async def scrape_site_endpoint(
    request: ScrapeRequest, # Use ScrapeRequest for a single site base URL
    discovery_depth: int = Query(1, description="Profondeur de découverte des URLs du site (0-3)", ge=0, le=3),
):
    if not re.match(r'^https?://', request.url):
        raise HTTPException(status_code=400, 
                            detail=f"URL de base invalide: {request.url}. Doit commencer par http:// ou https://")
    
    logger.info(f"Début du scraping du site: {request.url} avec profondeur de découverte: {discovery_depth}")
    
    initial_html_content = ""
    initial_content_type = None

    try:
        if request.dynamic:
            initial_html_content = await scrape_dynamic_page(request.url, request.wait_time, request.wait_for_selector)
        else:
            initial_html_content, initial_content_type = await scrape_static_page(request.url)
            if initial_content_type and 'text/html' not in initial_content_type:
                 logger.warning(f"La page de base {request.url} n'est pas HTML (type: {initial_content_type}). La découverte de liens pourrait être inefficace.")
                 # Proceed but discovery might yield few results
    except HTTPException as e:
        logger.error(f"Impossible de scraper la page initiale {request.url} pour la découverte: {e.detail}")
        raise HTTPException(status_code=e.status_code, detail=f"Impossible de scraper la page initiale {request.url}: {e.detail}")

    discovered_urls = set([request.url]) # Always include the base URL
    if initial_html_content and discovery_depth > 0 and (not initial_content_type or 'text/html' in initial_content_type) :
        logger.info(f"Découverte des URLs à partir de {request.url} (profondeur: {discovery_depth})")
        try:
            discovered_urls.update(await discover_site_urls(request.url, initial_html_content, 0, discovery_depth, discovered_urls.copy()))
            logger.info(f"{len(discovered_urls)} URLs uniques trouvées pour le site {request.url}.")
        except Exception as e:
            logger.error(f"Erreur durant la découverte d'URLs pour {request.url}: {e}")
            # Continue with URLs found so far
    else:
        logger.info("Skipping URL discovery (page initiale non HTML, contenu vide, ou profondeur=0).")

    
    url_list = sorted(list(discovered_urls))
    
    if len(url_list) > request.max_pages:
        logger.warning(f"Nombre d'URLs ({len(url_list)}) dépasse max_pages ({request.max_pages}). Limitation à {request.max_pages} URLs.")
        url_list = url_list[:request.max_pages]
        
    multi_request = MultiScrapeRequest(
        urls=url_list,
        output_path=request.output_path,
        dynamic=request.dynamic,
        wait_time=request.wait_time,
        wait_for_selector=request.wait_for_selector, # This might be the "string" if not careful
        extract_links=request.extract_links,
        extract_images=request.extract_images,
        extract_text=request.extract_text,
        extract_markdown=request.extract_markdown,
        css_selector=request.css_selector, # This might be the "string"
        filename_prefix=request.filename_prefix or urlparse(request.url).netloc.replace("www.",""), # Default prefix from domain
        max_pages=request.max_pages 
    )
    
    logger.info(f"Lancement du scraping multiple pour {len(url_list)} URLs découvertes.")
    report = await scrape_multiple_endpoint(multi_request)
    
    report["discovery_summary"] = {
        "base_url": request.url,
        "discovery_depth_requested": discovery_depth,
        "initial_urls_found_for_discovery": len(discovered_urls),
        "urls_submitted_for_scraping": len(url_list)
    }
    return report


@app.post("/scrape")
async def scrape_endpoint(request: ScrapeRequest):
    result = await scrape_page(
        url=request.url,
        output_path=request.output_path,
        dynamic=request.dynamic,
        wait_time=request.wait_time,
        wait_for_selector=request.wait_for_selector,
        extract_links=request.extract_links,
        extract_images=request.extract_images,
        extract_text=request.extract_text,
        extract_markdown=request.extract_markdown,
        css_selector=request.css_selector, # This is where "string" could be passed
        filename_prefix=request.filename_prefix
    )
    if "error" in result:
        # Consider raising HTTPException for errors to give proper HTTP status codes
        # For now, returning JSON with error details
        logger.error(f"Erreur pour {request.url}: {result.get('error')}")
    return result

@app.post("/scrape-multiple")
async def scrape_multiple_endpoint(request: MultiScrapeRequest):
    valid_urls = []
    invalid_url_details = []
    for u in request.urls:
        if not re.match(r'^https?://', u):
            logger.warning(f"URL invalide fournie dans la liste: {u}")
            invalid_url_details.append({"url": u, "error": "URL invalide, doit commencer par http:// ou https://"})
        else:
            valid_urls.append(u)

    if not valid_urls:
        # If all URLs were invalid or list was empty
        if invalid_url_details:
             raise HTTPException(status_code=400, detail={"message": "Aucune URL valide fournie.", "errors": invalid_url_details})
        else:
             raise HTTPException(status_code=400, detail="Aucune URL fournie pour le scraping.")


    urls_to_scrape = valid_urls
    if len(valid_urls) > request.max_pages:
        logger.warning(f"Nombre d'URLs valides ({len(valid_urls)}) dépasse max_pages ({request.max_pages}). Limitation à {request.max_pages} URLs.")
        urls_to_scrape = valid_urls[:request.max_pages]

    Path(request.output_path).mkdir(parents=True, exist_ok=True)
    
    semaphore = asyncio.Semaphore(request.parallel_workers)
    
    async def scrape_with_semaphore(url_to_scrape: str): # Renamed variable
        async with semaphore:
            return await scrape_page( # Ensure all parameters are passed correctly
                url=url_to_scrape,
                output_path=request.output_path,
                dynamic=request.dynamic,
                wait_time=request.wait_time,
                wait_for_selector=request.wait_for_selector,
                extract_links=request.extract_links,
                extract_images=request.extract_images,
                extract_text=request.extract_text,
                extract_markdown=request.extract_markdown,
                css_selector=request.css_selector, # This is where "string" could be passed
                filename_prefix=request.filename_prefix
            )
            
    tasks = [scrape_with_semaphore(u) for u in urls_to_scrape]
    results = await asyncio.gather(*tasks, return_exceptions=False) # Errors are handled inside scrape_page

    processed_results = {}
    successful_count = 0
    failed_count = 0

    for res in results:
        if isinstance(res, dict) and "url" in res:
            processed_results[res["url"]] = {k: v for k, v in res.items() if k != "url"} # Store details under URL key
            if "error" in res:
                failed_count += 1
            else:
                successful_count += 1
        else:
            # This case should ideally not happen if scrape_page always returns a dict with 'url'
            logger.error(f"Résultat de scraping inattendu: {res}")
            failed_count +=1 # Count as failure


    report_data = {
        "scrape_start_time": datetime.now().isoformat(),
        "total_urls_provided": len(request.urls),
        "valid_urls_processed": len(urls_to_scrape),
        "invalid_urls_skipped": len(invalid_url_details),
        "max_pages_setting": request.max_pages,
        "successful_scrapes": successful_count,
        "failed_scrapes": failed_count,
        "output_path": request.output_path,
        "details": processed_results,
        "invalid_url_reports": invalid_url_details
    }
    
    report_filename = f"scrape_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    report_path = os.path.join(request.output_path, report_filename)
    
    async with aiofiles.open(report_path, mode='w', encoding='utf-8') as f:
        await f.write(json.dumps(report_data, ensure_ascii=False, indent=4))
        
    return {
        "message": f"Scraping terminé pour {len(urls_to_scrape)} URLs. Rapport généré.",
        "report_file": report_path,
        "summary": {
            "successful": successful_count,
            "failed": failed_count,
            "total_processed": len(urls_to_scrape)
        }
    }

@app.get("/")
async def root():
    return {
        "message": "Web Scraper API",
        "version": "2.1", # Updated version
        "endpoints": [
            {"path": "/scrape", "method": "POST", "description": "Scraper une seule page web."},
            {"path": "/scrape-multiple", "method": "POST", "description": "Scraper plusieurs pages web."},
            {"path": "/scrape-site", "method": "POST", "description": "Découvrir et scraper un site web entier."}
        ]
    }

if __name__ == "__main__":
    import uvicorn
    # Ensure correct app object is passed if filename is different, e.g., main:app
    uvicorn.run("dynamic_crawl:app", host="0.0.0.0", port=8000, reload=True) # Replace your_filename