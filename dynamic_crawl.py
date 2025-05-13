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
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError, Error as PlaywrightError
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from aiohttp.client_exceptions import ClientConnectorError, ServerDisconnectedError, ClientResponseError

# Configuration du logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Web Scraper API", description="API pour scraper des pages web statiques et dynamiques")

# Retry decorator for network operations
retry_policy = retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
    retry=retry_if_exception_type((ClientConnectorError, ServerDisconnectedError, asyncio.TimeoutError, PlaywrightTimeoutError)),
    before_sleep=lambda retry_state: logger.info(f"Retrying {retry_state.fn.__name__} after {retry_state.next_action.sleep}s due to {retry_state.exception}")
)

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

async def scrape_static_page(url: str) -> tuple[str, Optional[str]]:
    """
    Scrapes a static page with retry logic and improved error handling.
    Returns: (html_content, content_type). html_content can be empty if not text/html.
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8"
    }

    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
        @retry_policy
        async def attempt_fetch():
            try:
                async with session.get(url, headers=headers, allow_redirects=True) as response:
                    if response.status >= 400:
                        raise HTTPException(status_code=response.status,
                                            detail=f"Erreur HTTP {response.status} pour {url}")
                    content_type = response.headers.get('Content-Type', '').lower()
                    content = await response.text(errors='ignore')
                    return content, content_type
            except ClientResponseError as e:
                logger.error(f"Erreur de réponse pour {url}: {e}")
                raise HTTPException(status_code=500, detail=f"Erreur de réponse pour {url}: {e}")
            except (ClientConnectorError, ServerDisconnectedError, asyncio.TimeoutError) as e:
                logger.warning(f"Erreur réseau pour {url}: {e}")
                raise
            except Exception as e:
                logger.error(f"Erreur inattendue pour {url}: {e}")
                raise HTTPException(status_code=500, detail=f"Erreur inattendue pour {url}: {e}")

        try:
            return await attempt_fetch()
        except Exception as e:
            logger.error(f"Échec définitif du scraping statique de {url} après retries: {e}")
            raise HTTPException(status_code=500, detail=f"Échec du scraping statique de {url}: {e}")

async def scrape_dynamic_page(url: str, wait_time: int = 5, wait_for_selector: Optional[str] = None) -> str:
    """
    Scrapes a dynamic page with retry logic and pre-validation.
    """
    non_html_extensions = ['.jpg', '.jpeg', '.png', '.gif', '.bmp', '.pdf', '.zip', '.gz', '.webp', '.svg', '.mp4', '.mov', '.avi', '.mp3', '.wav']
    if any(url.lower().endswith(ext) for ext in non_html_extensions):
        logger.warning(f"URL {url} semble être un fichier non-HTML. Skipping dynamic scraping.")
        raise HTTPException(status_code=400, detail=f"URL {url} pointe vers un contenu non-HTML.")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        )
        page = await context.new_page()

        @retry_policy
        async def attempt_navigation():
            try:
                await page.goto(url, wait_until="networkidle", timeout=60000)
                if wait_for_selector:
                    await page.wait_for_selector(wait_for_selector, timeout=wait_time * 1000)
                else:
                    await asyncio.sleep(wait_time)
                return await page.content()
            except PlaywrightTimeoutError as e:
                logger.warning(f"Timeout lors de la navigation vers {url}: {e}")
                raise
            except PlaywrightError as e:
                logger.error(f"Erreur Playwright pour {url}: {e}")
                raise
            except Exception as e:
                logger.error(f"Erreur inattendue lors de la navigation vers {url}: {e}")
                raise HTTPException(status_code=500, detail=f"Erreur de navigation pour {url}: {e}")

        try:
            content = await attempt_navigation()
            await browser.close()
            return content
        except Exception as e:
            await browser.close()
            logger.error(f"Échec définitif du scraping dynamique de {url} après retries: {e}")
            raise HTTPException(status_code=500, detail=f"Échec du scraping dynamique de {url}: {e}")

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
            logger.error(f"URL invalide: {url}")
            return {"url": url, "error": "URL invalide, doit commencer par http:// ou https://"}

        if css_selector and css_selector.lower() == "string":
            logger.warning(f"Sélecteur CSS non valide 'string' pour {url}. Ign text extraction.")
            css_selector = None

        html_content = ""
        content_type_header = None

        if dynamic:
            html_content = await scrape_dynamic_page(url, wait_time, wait_for_selector)
        else:
            html_content, content_type_header = await scrape_static_page(url)
            if content_type_header and not any(ct in content_type_header for ct in ['text/html', 'application/xhtml+xml']):
                logger.warning(f"Contenu non-HTML (type: {content_type_header}) pour {url}. Pas de liens extraits.")
                metadata = {
                    "url": url,
                    "scrape_time": datetime.now().isoformat(),
                    "content_type": content_type_header
                }
                result_data = {"metadata": metadata}
                if "text/" in content_type_header:
                    result_data["text"] = html_content
                filename_base = get_safe_filename(url, filename_prefix)
                saved_files = await save_scraped_data(result_data, output_path, filename_base)
                result_data["saved_files"] = saved_files
                return result_data

        if not html_content:
            logger.warning(f"Aucun contenu HTML pour {url}. Pas de liens extraits.")
            return {"url": url, "error": "Aucun contenu HTML récupéré"}

        soup = BeautifulSoup(html_content, 'html.parser')
        metadata = extract_metadata(soup, url)
        target_soup = soup

        if css_selector:
            selected_elements = soup.select(css_selector)
            if not selected_elements:
                logger.warning(f"Aucun contenu pour le sélecteur '{css_selector}' sur {url}. Liens potentiellement limités.")
            else:
                selected_html = "".join(str(el) for el in selected_elements)
                target_soup = BeautifulSoup(selected_html, 'html.parser')

        result = {"metadata": metadata, "html": html_content}

        if extract_text:
            text_soup = BeautifulSoup(str(target_soup), 'html.parser')
            for script_or_style in text_soup(["script", "style"]):
                script_or_style

        if extract_markdown:
            result["markdown"] = html_to_markdown(str(target_soup))

        if extract_links:
            links = [{"url": urljoin(url, a['href']), "text": a.get_text(strip=True) or "[Lien sans texte]", "source_page": url}
                     for a in target_soup.find_all('a', href=True)]
            result["links"] = links
            logger.info(f"Extrait {len(links)} liens de {url}")

        if extract_images:
            images = [{"src": urljoin(url, img.get('src', '')), "alt": img.get('alt', '').strip(), "source_page": url}
                      for img in target_soup.find_all('img') if img.get('src')]
            result["images"] = images
            logger.info(f"Extrait {len(images)} images de {url}")

        filename_base = get_safe_filename(url, filename_prefix)
        saved_files = await save_scraped_data(result, output_path, filename_base)
        result["saved_files"] = saved_files

        return result

    except HTTPException as he:
        logger.error(f"HTTPException pour {url}: {he.detail}")
        return {"url": url, "error": f"HTTPException: {he.status_code} - {he.detail}"}
    except Exception as e:
        logger.error(f"Erreur inattendue pour {url}: {str(e)}")
        return {"url": url, "error": str(e)}

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
        for i, url_to_discover in enumerate(list(new_urls)):
            # Changé: Supprimé la limite de 20 URLs pour découvrir plus de pages
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url_to_discover, timeout=10) as response:
                        if response.status == 200:
                            content, content_type = await response.text(), response.headers.get('Content-Type', '').lower()
                            if 'text/html' in content_type:
                                await discover_site_urls(url_to_discover, content, current_depth + 1, max_depth, already_discovered)
            except Exception as e:
                logger.warning(f"Erreur lors de la découverte de {url_to_discover}: {e}")

    return already_discovered

def get_safe_filename(url: str, prefix: Optional[str] = None) -> str:
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.replace("www.", "")
    path = parsed_url.path.rstrip("/")

    if not path or path == "/":
        path_segment = "index"
    else:
        path_segment = os.path.basename(path) if os.path.basename(path) else os.path.basename(os.path.dirname(path))

    path_segment = re.sub(r'[^\w\-_.]', '_', path_segment)
    path_segment = re.sub(r'_+', '_', path_segment).strip('_')

    path_hash = hashlib.md5(url.encode()).hexdigest()[:8]
    safe_path = (path_segment[:50] if path_segment else "page") + "_" + path_hash

    filename = f"{domain}_{safe_path}" if not prefix else f"{prefix}_{domain}_{safe_path}"
    return filename[:150]

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
        prop = meta.get("property", "").lower()
        content = meta.get("content", "")

        if not content:
            continue

        if name == "description":
            metadata["description"] = content
        elif name == "keywords":
            metadata["keywords"] = content
        elif name == "author":
            metadata["author"] = content
        elif prop.startswith("og:"):
            metadata["og_tags"][prop[3:]] = content
        elif prop.startswith("twitter:"):
            metadata["twitter_tags"][prop[8:]] = content

    return metadata

async def save_scraped_data(data: Dict[str, Any], output_path: str, filename_base: str) -> Dict[str, str]:
    saved_files = {}

    # Assurer que le répertoire de sortie existe
    Path(output_path).mkdir(parents=True, exist_ok=True)

    # Définir le chemin pour le fichier de métadonnées unique du site
    metadata_path = os.path.join(output_path, "website_metadata.json")

    # Initialiser la structure des métadonnées
    metadata_to_save = {
        "scrape_time": data.get("metadata", {}).get("scrape_time", ""),
        "title": data.get("metadata", {}).get("title"),
        "description": data.get("metadata", {}).get("description"),
        "keywords": data.get("metadata", {}).get("keywords"),
        "author": data.get("metadata", {}).get("author"),
        "og_tags": data.get("metadata", {}).get("og_tags", {}),
        "twitter_tags": data.get("metadata", {}).get("twitter_tags", {}),
        "links": [],
        "images": []
    }

    # Charger les métadonnées existantes si elles existent
    if os.path.exists(metadata_path):
        async with aiofiles.open(metadata_path, mode='r', encoding='utf-8') as f:
            existing_metadata = json.loads(await f.read())
            metadata_to_save.update({
                "scrape_time": existing_metadata.get("scrape_time", metadata_to_save["scrape_time"]),
                "title": existing_metadata.get("title", metadata_to_save["title"]),
                "description": existing_metadata.get("description", metadata_to_save["description"]),
                "keywords": existing_metadata.get("keywords", metadata_to_save["keywords"]),
                "author": existing_metadata.get("author", metadata_to_save["author"]),
                "og_tags": existing_metadata.get("og_tags", metadata_to_save["og_tags"]),
                "twitter_tags": existing_metadata.get("twitter_tags", metadata_to_save["twitter_tags"]),
                "links": existing_metadata.get("links", []),
                "images": existing_metadata.get("images", [])
            })

    # Ajouter les nouveaux liens et images sans déduplication
    new_links = data.get("links", [])
    new_images = data.get("images", [])
    metadata_to_save["links"].extend(new_links)
    metadata_to_save["images"].extend(new_images)

    # Journaliser le nombre de liens ajoutés
    logger.info(f"Ajouté {len(new_links)} nouveaux liens et {len(new_images)} nouvelles images pour {data.get('metadata', {}).get('url', 'inconnu')}")

    # Sauvegarder les métadonnées mises à jour
    async with aiofiles.open(metadata_path, mode='w', encoding='utf-8') as f:
        await f.write(json.dumps(metadata_to_save, ensure_ascii=False, indent=4))
    saved_files["metadata"] = metadata_path

    # Sauvegarder le fichier markdown si présent
    if data.get("markdown"):
        md_path = os.path.join(output_path, f"{filename_base}.md")
        async with aiofiles.open(md_path, mode='w', encoding='utf-8') as f:
            await f.write(data["markdown"])
        saved_files["markdown"] = md_path

    return saved_files

@app.post("/scrape-site")
async def scrape_site_endpoint(
    request: ScrapeRequest,
    discovery_depth: int = Query(1, description="Profondeur de découverte des URLs du site (0-3)", ge=0, le=3),
):
    if not re.match(r'^https?://', request.url):
        raise HTTPException(status_code=400,
                            detail=f"URL de base invalide: {request.url}")

    logger.info(f"Début du scraping du site: {request.url} avec profondeur: {discovery_depth}")

    initial_html_content = ""
    initial_content_type = None

    try:
        if request.dynamic:
            initial_html_content = await scrape_dynamic_page(request.url, request.wait_time, request.wait_for_selector)
        else:
            initial_html_content, initial_content_type = await scrape_static_page(request.url)
            if initial_content_type and 'text/html' not in initial_content_type:
                logger.warning(f"La page de base {request.url} n'est pas HTML (type: {initial_content_type})")
    except HTTPException as e:
        logger.error(f"Impossible de scraper la page initiale {request.url}: {e.detail}")
        raise HTTPException(status_code=e.status_code, detail=f"Impossible de scraper la page initiale {request.url}: {e.detail}")

    discovered_urls = set([request.url])
    if initial_html_content and discovery_depth > 0 and (not initial_content_type or 'text/html' in initial_content_type):
        logger.info(f"Découverte des URLs à partir de {request.url} (profondeur: {discovery_depth})")
        try:
            discovered_urls.update(await discover_site_urls(request.url, initial_html_content, 0, discovery_depth, discovered_urls.copy()))
            logger.info(f"{len(discovered_urls)} URLs uniques trouvées pour le site {request.url}")
        except Exception as e:
            logger.error(f"Erreur durant la découverte d'URLs pour {request.url}: {e}")

    url_list = sorted(list(discovered_urls))

    if len(url_list) > request.max_pages:
        logger.warning(f"Nombre d'URLs ({len(url_list)}) dépasse max_pages ({request.max_pages})")
        url_list = url_list[:request.max_pages]

    multi_request = MultiScrapeRequest(
        urls=url_list,
        output_path=request.output_path,
        dynamic=request.dynamic,
        wait_time=request.wait_time,
        wait_for_selector=request.wait_for_selector,
        extract_links=request.extract_links,
        extract_images=request.extract_images,
        extract_text=request.extract_text,
        extract_markdown=request.extract_markdown,
        css_selector=request.css_selector,
        filename_prefix=request.filename_prefix or urlparse(request.url).netloc.replace("www.",""),
        max_pages=request.max_pages
    )

    logger.info(f"Lancement du scraping multiple pour {len(url_list)} URLs")
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
        css_selector=request.css_selector,
        filename_prefix=request.filename_prefix
    )
    if "error" in result:
        logger.error(f"Erreur pour {request.url}: {result.get('error')}")
    return result

@app.post("/scrape-multiple")
async def scrape_multiple_endpoint(request: MultiScrapeRequest):
    valid_urls = [u for u in request.urls if re.match(r'^https?://', u)]
    invalid_url_details = [{"url": u, "error": "URL invalide, doit commencer par http:// ou https://"}
                           for u in request.urls if u not in valid_urls]

    if not valid_urls:
        raise HTTPException(status_code=400, detail={"message": "Aucune URL valide.", "errors": invalid_url_details})

    urls_to_scrape = valid_urls[:request.max_pages]
    if len(valid_urls) > request.max_pages:
        logger.warning(f"Limitation à {request.max_pages} URLs sur {len(valid_urls)} valides")

    Path(request.output_path).mkdir(parents=True, exist_ok=True)

    semaphore = asyncio.Semaphore(min(request.parallel_workers, 10))

    async def scrape_with_semaphore(url: str):
        async with semaphore:
            return await scrape_page(
                url=url,
                output_path=request.output_path,
                dynamic=request.dynamic,
                wait_time=request.wait_time,
                wait_for_selector=request.wait_for_selector,
                extract_links=request.extract_links,
                extract_images=request.extract_images,
                extract_text=request.extract_text,
                extract_markdown=request.extract_markdown,
                css_selector=request.css_selector,
                filename_prefix=request.filename_prefix
            )

    tasks = [scrape_with_semaphore(u) for u in urls_to_scrape]
    results = await asyncio.gather(*tasks, return_exceptions=False)

    processed_results = {}
    successful_count = 0
    failed_count = 0

    for res in results:
        if isinstance(res, dict) and "url" in res:
            processed_results[res["url"]] = {k: v for k, v in res.items() if k != "url"}
            if "error" in res:
                failed_count += 1
            else:
                successful_count += 1

    return {
        "message": f"Scraping terminé pour {len(urls_to_scrape)} URLs",
        "summary": {
            "successful": successful_count,
            "failed": failed_count,
            "total_processed": len(urls_to_scrape)
        },
        "details": processed_results,
        "invalid_url_reports": invalid_url_details
    }

@app.get("/")
async def root():
    return {
        "message": "Web Scraper API",
        "version": "2.1",
        "endpoints": [
            {"path": "/scrape", "method": "POST", "description": "Scraper une seule page web"},
            {"path": "/scrape-multiple", "method": "POST", "description": "Scraper plusieurs pages web"},
            {"path": "/scrape-site", "method": "POST", "description": "Découvrir et scraper un site web entier"}
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("dynamic_crawl:app", host="0.0.0.0", port=8000, reload=True)