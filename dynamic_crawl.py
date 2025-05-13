from fastapi import FastAPI, HTTPException, Query, Body
import requests
from bs4 import BeautifulSoup
from typing import Optional, List, Dict, Any, Union
import asyncio
from playwright.async_api import async_playwright
import re
import json
import os
from urllib.parse import urljoin, urlparse
import html2text
import aiofiles
import aiohttp
from pydantic import BaseModel
from functools import partial
import logging
from datetime import datetime
import hashlib
from pathlib import Path

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
    max_pages: int = 3000  # Pour la cohérence avec MultiScrapeRequest

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
    max_pages: int = 3000  # Limite de pages maximale

# Fonction pour ajouter des URLs découvertes à la liste de scraping (avec fonction de découverte de site)
async def discover_site_urls(base_url: str, html_content: str, current_depth: int = 0, max_depth: int = 2, 
                            already_discovered: Optional[set] = None) -> set:
    """
    Découvre récursivement toutes les URLs d'un site web à partir d'une URL de base.
    Limite la profondeur de découverte pour éviter des boucles infinies.
    """
    if already_discovered is None:
        already_discovered = set()
    
    # Extraire le domaine de l'URL de base
    parsed_base = urlparse(base_url)
    base_domain = parsed_base.netloc
    
    # Analyser le contenu HTML
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Trouver tous les liens
    new_urls = set()
    for a_tag in soup.find_all('a', href=True):
        href = a_tag['href']
        absolute_url = urljoin(base_url, href)
        parsed_url = urlparse(absolute_url)
        
        # Ne conserver que les URLs du même domaine et avec http/https
        if parsed_url.netloc == base_domain and parsed_url.scheme in ('http', 'https'):
            # Normaliser l'URL (supprimer fragments, etc.)
            normalized_url = f"{parsed_url.scheme}://{parsed_url.netloc}{parsed_url.path}"
            if parsed_url.query:
                normalized_url += f"?{parsed_url.query}"
            
            # Ajouter l'URL si elle n'est pas déjà découverte
            if normalized_url not in already_discovered:
                new_urls.add(normalized_url)
                already_discovered.add(normalized_url)
    
    # Si la profondeur maximale n'est pas atteinte, explorer les nouveaux liens
    if current_depth < max_depth and new_urls:
        # Limiter le nombre d'URLs à explorer à chaque niveau pour éviter une explosion exponentielle
        for url in list(new_urls)[:20]:  # Limiter à 20 URLs par niveau
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.get(url, timeout=10) as response:
                        if response.status == 200:
                            content = await response.text()
                            # Appel récursif pour explorer les URLs découvertes
                            await discover_site_urls(url, content, current_depth + 1, max_depth, already_discovered)
            except Exception as e:
                logger.warning(f"Erreur lors de la découverte de {url}: {str(e)}")
    
    return already_discovered

@app.post("/scrape-site")
async def scrape_site_endpoint(
    request: ScrapeRequest,
    discovery_depth: int = Query(1, description="Profondeur de découverte des URLs du site (0-3)"),
):
    """
    Endpoint pour scraper un site web entier en découvrant automatiquement les URLs.
    
    - url: URL de base du site à scraper
    - output_path: Chemin où sauvegarder les fichiers générés
    - dynamic: Utiliser le mode dynamique (Playwright) si True, sinon mode statique (requests)
    - wait_time: Temps d'attente en secondes pour le chargement du contenu dynamique
    - discovery_depth: Profondeur de découverte des URLs (0-3)
    - max_pages: Limite maximale de pages à scraper (défaut: 3000)
    """
    # Valider l'URL
    if not re.match(r'^https?://', request.url):
        raise HTTPException(status_code=400, 
                           detail=f"URL invalide: {request.url}. L'URL doit commencer par http:// ou https://")
    
    # Limiter la profondeur de découverte entre 0 et 3
    discovery_depth = max(0, min(3, discovery_depth))
    
    # Première étape : scraper la page initiale
    try:
        logger.info(f"Scraping initial de {request.url}")
        if request.dynamic:
            html_content = await scrape_dynamic_page(request.url, request.wait_time, request.wait_for_selector)
        else:
            html_content = await scrape_static_page(request.url)
        
        # Découvrir les URLs du site si la profondeur > 0
        discovered_urls = set([request.url])
        if discovery_depth > 0:
            logger.info(f"Découverte des URLs du site {request.url} avec profondeur {discovery_depth}")
            discovered_urls = await discover_site_urls(request.url, html_content, 0, discovery_depth)
            logger.info(f"Découverte terminée : {len(discovered_urls)} URLs trouvées")
        
        # Convertir en liste et trier pour un traitement déterministe
        url_list = sorted(list(discovered_urls))
        
        # Respecter la limite max_pages
        if len(url_list) > request.max_pages:
            logger.warning(f"Nombre d'URLs limité à {request.max_pages} (découvert: {len(url_list)})")
            url_list = url_list[:request.max_pages]
        
        # Créer une requête multi-scrape avec les URLs découvertes
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
            filename_prefix=request.filename_prefix,
            max_pages=request.max_pages
        )
        
        # Scraper toutes les URLs découvertes
        result = await scrape_multiple_endpoint(multi_request)
        
        # Ajouter des informations sur la découverte
        result["discovery_info"] = {
            "base_url": request.url,
            "discovery_depth": discovery_depth,
            "discovered_urls_count": len(discovered_urls),
            "scraped_urls_count": len(url_list)
        }
        
        return result
        
    except Exception as e:
        logger.error(f"Erreur lors du scraping du site {request.url}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du scraping du site: {str(e)}")

# Pour les sites statiques - utilise requests et BeautifulSoup
async def scrape_static_page(url: str) -> str:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, timeout=30) as response:
                if response.status >= 400:
                    raise HTTPException(status_code=response.status, 
                                       detail=f"Erreur HTTP {response.status} pour {url}")
                return await response.text()
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors du scraping statique: {str(e)}")

# Pour les sites dynamiques - utilise Playwright
async def scrape_dynamic_page(url: str, wait_time: int = 5, wait_for_selector: Optional[str] = None) -> str:
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = await context.new_page()
            await page.goto(url, wait_until="networkidle", timeout=60000)
            
            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, timeout=wait_time * 1000)
                except:
                    logger.warning(f"Sélecteur '{wait_for_selector}' non trouvé sur {url}")
                    # Continue even if selector isn't found
                    pass
            else:
                await asyncio.sleep(wait_time)  # Attendre que le contenu dynamique se charge
            
            content = await page.content()
            await browser.close()
            return content
    except Exception as e:
        logger.error(f"Erreur lors du scraping dynamique de {url}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Erreur lors du scraping dynamique: {str(e)}")

# Génère un nom de fichier sécurisé basé sur l'URL
def get_safe_filename(url: str, prefix: Optional[str] = None) -> str:
    parsed_url = urlparse(url)
    domain = parsed_url.netloc.replace("www.", "")
    path = parsed_url.path.rstrip("/")
    
    if not path:
        path = "index"
    else:
        # Nettoyer le chemin pour un nom de fichier valide
        path = re.sub(r'[^\w\-_]', '_', path)
        path = re.sub(r'_+', '_', path)
        path = path.strip('_')
    
    # Limiter la longueur pour éviter des problèmes de système de fichiers
    if len(path) > 100:
        # Utiliser un hash pour les chemins longs
        hash_obj = hashlib.md5(path.encode())
        path = path[:50] + "_" + hash_obj.hexdigest()[:10]
    
    if prefix:
        return f"{prefix}_{domain}_{path}"
    
    return f"{domain}_{path}"

# Convertit HTML en Markdown
def html_to_markdown(html_content: str) -> str:
    h2t = html2text.HTML2Text()
    h2t.ignore_links = False
    h2t.body_width = 0  # Désactive le wrapping automatique
    h2t.protect_links = True
    h2t.unicode_snob = True
    h2t.single_line_break = True
    h2t.mark_code = True
    
    return h2t.handle(html_content)

# Extrait les métadonnées d'une page
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
    
    # Extraire le titre
    title_tag = soup.find("title")
    if title_tag:
        metadata["title"] = title_tag.text.strip()
    
    # Extraire les meta tags
    for meta in soup.find_all("meta"):
        name = meta.get("name", "").lower()
        property = meta.get("property", "").lower()
        content = meta.get("content", "")
        
        if name == "description":
            metadata["description"] = content
        elif name == "keywords":
            metadata["keywords"] = content
        elif name == "author":
            metadata["author"] = content
        elif property.startswith("og:"):
            metadata["og_tags"][property[3:]] = content
        elif property.startswith("twitter:"):
            metadata["twitter_tags"][property[8:]] = content
    
    return metadata

# Enregistre les données dans des fichiers
async def save_scraped_data(data: Dict[str, Any], output_path: str, filename_base: str) -> Dict[str, str]:
    saved_files = {}
    
    # Créer le répertoire si nécessaire
    os.makedirs(output_path, exist_ok=True)
    
    # Sauvegarder les métadonnées en JSON
    metadata_path = os.path.join(output_path, f"{filename_base}_metadata.json")
    async with aiofiles.open(metadata_path, mode='w', encoding='utf-8') as f:
        await f.write(json.dumps(data.get("metadata", {}), ensure_ascii=False, indent=2))
    saved_files["metadata"] = metadata_path
    
    # Sauvegarder le contenu textuel
    if "text" in data:
        text_path = os.path.join(output_path, f"{filename_base}_content.txt")
        async with aiofiles.open(text_path, mode='w', encoding='utf-8') as f:
            await f.write(data["text"])
        saved_files["text"] = text_path
    
    # Sauvegarder le contenu Markdown
    if "markdown" in data:
        md_path = os.path.join(output_path, f"{filename_base}.md")
        async with aiofiles.open(md_path, mode='w', encoding='utf-8') as f:
            await f.write(data["markdown"])
        saved_files["markdown"] = md_path
    
    # Sauvegarder les liens
    if "links" in data:
        links_path = os.path.join(output_path, f"{filename_base}_links.json")
        async with aiofiles.open(links_path, mode='w', encoding='utf-8') as f:
            await f.write(json.dumps(data["links"], ensure_ascii=False, indent=2))
        saved_files["links"] = links_path
    
    # Sauvegarder les images
    if "images" in data:
        images_path = os.path.join(output_path, f"{filename_base}_images.json")
        async with aiofiles.open(images_path, mode='w', encoding='utf-8') as f:
            await f.write(json.dumps(data["images"], ensure_ascii=False, indent=2))
        saved_files["images"] = images_path
    
    # Sauvegarder le HTML brut
    if "html" in data:
        html_path = os.path.join(output_path, f"{filename_base}.html")
        async with aiofiles.open(html_path, mode='w', encoding='utf-8') as f:
            await f.write(data["html"])
        saved_files["html"] = html_path
    
    return saved_files

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
    """
    Fonction pour scraper une page web et sauvegarder les données.
    """
    try:
        logger.info(f"Scraping de {url} (mode: {'dynamique' if dynamic else 'statique'})")
        
        # Vérifier l'URL
        if not re.match(r'^https?://', url):
            raise HTTPException(status_code=400, 
                               detail=f"URL invalide: {url}. L'URL doit commencer par http:// ou https://")
        
        # Scraping de la page en fonction du mode choisi
        if dynamic:
            html_content = await scrape_dynamic_page(url, wait_time, wait_for_selector)
        else:
            html_content = await scrape_static_page(url)
        
        # Parsing avec BeautifulSoup
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extraire les métadonnées avant d'appliquer le sélecteur CSS
        metadata = extract_metadata(soup, url)
        
        # Si un sélecteur CSS est fourni, limiter l'analyse à cette partie
        selected_content = None
        if css_selector:
            selected_content = soup.select(css_selector)
            if not selected_content:
                logger.warning(f"Aucun contenu trouvé avec le sélecteur: {css_selector} pour {url}")
            else:
                # Créer un nouveau soup avec seulement les éléments sélectionnés
                new_soup = BeautifulSoup("<html><body></body></html>", "html.parser")
                for element in selected_content:
                    new_soup.body.append(element)
                soup = new_soup
        
        result = {"metadata": metadata, "html": html_content}
        
        # Extraire le texte
        if extract_text:
            # Créer une copie du soup pour l'extraction de texte
            text_soup = BeautifulSoup(str(soup), 'html.parser')
            # Supprimer les scripts et styles qui contiennent du texte non pertinent
            for script in text_soup(["script", "style"]):
                script.extract()
            
            text = text_soup.get_text(separator="\n", strip=True)
            # Nettoyer les espaces et les sauts de ligne multiples
            text = re.sub(r'\n+', '\n', text)
            text = re.sub(r' +', ' ', text)
            result["text"] = text
        
        # Extraire le Markdown
        if extract_markdown:
            # Utiliser le soup complet ou filtré
            markdown_content = html_to_markdown(str(soup))
            result["markdown"] = markdown_content
        
        # Extraire les liens
        if extract_links:
            links = []
            for a_tag in soup.find_all('a', href=True):
                href = a_tag['href']
                # Convertir les URLs relatives en URLs absolues
                absolute_url = urljoin(url, href)
                link_text = a_tag.get_text(strip=True) or "[Sans texte]"
                links.append({"url": absolute_url, "text": link_text})
            result["links"] = links
        
        # Extraire les images
        if extract_images:
            images = []
            for img_tag in soup.find_all('img'):
                src = img_tag.get('src')
                if src:
                    # Convertir les URLs relatives en URLs absolues
                    absolute_src = urljoin(url, src)
                    alt_text = img_tag.get('alt', '')
                    images.append({"src": absolute_src, "alt": alt_text})
            result["images"] = images
        
        # Générer un nom de fichier sécurisé basé sur l'URL
        filename_base = get_safe_filename(url, filename_prefix)
        
        # Sauvegarder les données sur disque
        saved_files = await save_scraped_data(result, output_path, filename_base)
        
        # Ajouter les chemins des fichiers sauvegardés au résultat
        result["saved_files"] = saved_files
        
        return result
    
    except Exception as e:
        logger.error(f"Erreur lors du scraping de {url}: {str(e)}")
        return {"url": url, "error": str(e)}

@app.post("/scrape")
async def scrape_endpoint(request: ScrapeRequest):
    """
    Endpoint pour scraper une page web et enregistrer les résultats dans des fichiers.
    
    - url: URL de la page à scraper
    - output_path: Chemin où sauvegarder les fichiers générés
    - dynamic: Utiliser le mode dynamique (Playwright) si True, sinon mode statique (requests)
    - wait_time: Temps d'attente en secondes pour le chargement du contenu dynamique
    - wait_for_selector: Sélecteur CSS à attendre avant d'extraire le contenu
    - extract_links: Extraire les liens de la page
    - extract_images: Extraire les images de la page
    - extract_text: Extraire le texte de la page
    - extract_markdown: Extraire le contenu au format Markdown
    - css_selector: Sélecteur CSS pour cibler une partie spécifique de la page
    - filename_prefix: Préfixe pour les noms de fichiers générés
    """
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
    return result

@app.post("/scrape-multiple")
async def scrape_multiple_endpoint(request: MultiScrapeRequest):
    """
    Endpoint pour scraper plusieurs pages web en parallèle et enregistrer les résultats.
    
    - urls: Liste des URLs à scraper
    - output_path: Chemin où sauvegarder les fichiers générés
    - dynamic: Utiliser le mode dynamique (Playwright) si True, sinon mode statique (requests)
    - wait_time: Temps d'attente en secondes pour le chargement du contenu dynamique
    - wait_for_selector: Sélecteur CSS à attendre avant d'extraire le contenu
    - extract_links: Extraire les liens de la page
    - extract_images: Extraire les images de la page
    - extract_text: Extraire le texte de la page
    - extract_markdown: Extraire le contenu au format Markdown
    - css_selector: Sélecteur CSS pour cibler une partie spécifique de la page
    - parallel_workers: Nombre de workers pour le crawling parallèle (défaut: 18)
    - filename_prefix: Préfixe pour les noms de fichiers générés
    - max_pages: Nombre maximum de pages à scraper (défaut: 3000)
    """
    # Vérifier que les URLs sont valides
    for url in request.urls:
        if not re.match(r'^https?://', url):
            raise HTTPException(status_code=400, 
                               detail=f"URL invalide: {url}. L'URL doit commencer par http:// ou https://")
    
    # Appliquer la limite de 3000 pages maximum
    if len(request.urls) > request.max_pages:
        logger.warning(f"Nombre d'URLs limité à {request.max_pages} (demandé: {len(request.urls)})")
        request.urls = request.urls[:request.max_pages]
    
    # Créer le répertoire de sortie
    os.makedirs(request.output_path, exist_ok=True)
    
    # Limiter le nombre de tâches concurrentes
    semaphore = asyncio.Semaphore(request.parallel_workers)
    
    async def scrape_with_semaphore(url):
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
    
    # Lancer toutes les tâches en parallèle avec limitation
    tasks = [scrape_with_semaphore(url) for url in request.urls]
    results = await asyncio.gather(*tasks, return_exceptions=False)
    
    # Traiter les résultats
    processed_results = {}
    for result in results:
        if "url" in result:
            processed_results[result["url"]] = result
        elif "error" in result:
            processed_results["error"] = result
    
    # Créer un fichier de rapport global
    report = {
        "scrape_time": datetime.now().isoformat(),
        "number_of_urls": len(request.urls),
        "max_pages_setting": request.max_pages,
        "successful_scrapes": len([r for r in results if "error" not in r]),
        "failed_scrapes": len([r for r in results if "error" in r]),
        "summary": processed_results
    }
    
    report_path = os.path.join(request.output_path, "scrape_report.json")
    async with aiofiles.open(report_path, mode='w', encoding='utf-8') as f:
        await f.write(json.dumps(report, ensure_ascii=False, indent=2))
    
    return {
        "message": f"Scraping terminé pour {len(request.urls)} URLs (max configuré: {request.max_pages})",
        "report_file": report_path,
        "results": processed_results
    }

@app.get("/")
async def root():
    return {
        "message": "Web Scraper API",
        "version": "2.0",
        "endpoints": [
            {"path": "/scrape", "method": "POST", "description": "Scraper une page web"},
            {"path": "/scrape-multiple", "method": "POST", "description": "Scraper plusieurs pages web"}
        ]
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)