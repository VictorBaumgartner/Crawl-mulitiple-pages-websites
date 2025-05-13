from fastapi import FastAPI, HTTPException, Query
import requests
from bs4 import BeautifulSoup
from typing import Optional, List, Dict, Any
import asyncio
from playwright.async_api import async_playwright
import re
import json
from urllib.parse import urljoin

app = FastAPI()

# Pour les sites statiques - utilise requests et BeautifulSoup
async def scrape_static_page(url: str) -> str:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
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
            await page.goto(url, wait_until="networkidle")
            
            if wait_for_selector:
                try:
                    await page.wait_for_selector(wait_for_selector, timeout=wait_time * 1000)
                except:
                    # Continue even if selector isn't found
                    pass
            else:
                await asyncio.sleep(wait_time)  # Attendre que le contenu dynamique se charge
            
            content = await page.content()
            await browser.close()
            return content
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur lors du scraping dynamique: {str(e)}")

@app.get("/scrape")
async def scrape(
    url: str,
    dynamic: bool = False,
    wait_time: int = 5,
    wait_for_selector: Optional[str] = None,
    extract_links: bool = False,
    extract_images: bool = False,
    extract_text: bool = True,
    css_selector: Optional[str] = None
):
    """
    Endpoint pour scraper une page web.
    
    - url: URL de la page à scraper
    - dynamic: Utiliser le mode dynamique (Playwright) si True, sinon mode statique (requests)
    - wait_time: Temps d'attente en secondes pour le chargement du contenu dynamique
    - wait_for_selector: Sélecteur CSS à attendre avant d'extraire le contenu
    - extract_links: Extraire les liens de la page
    - extract_images: Extraire les images de la page
    - extract_text: Extraire le texte de la page
    - css_selector: Sélecteur CSS pour cibler une partie spécifique de la page
    """
    
    # Vérifier l'URL
    if not re.match(r'^https?://', url):
        raise HTTPException(status_code=400, detail="URL invalide. L'URL doit commencer par http:// ou https://")
    
    # Scraping de la page en fonction du mode choisi
    if dynamic:
        html_content = await scrape_dynamic_page(url, wait_time, wait_for_selector)
    else:
        html_content = await scrape_static_page(url)
    
    # Parsing avec BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Si un sélecteur CSS est fourni, limiter l'analyse à cette partie
    if css_selector:
        selected_content = soup.select(css_selector)
        if not selected_content:
            raise HTTPException(status_code=404, detail=f"Aucun contenu trouvé avec le sélecteur: {css_selector}")
        # Créer un nouveau soup avec seulement les éléments sélectionnés
        new_soup = BeautifulSoup("<html><body></body></html>", "html.parser")
        for element in selected_content:
            new_soup.body.append(element)
        soup = new_soup
    
    result = {}
    
    # Extraire le texte
    if extract_text:
        # Supprimer les scripts et styles qui contiennent du texte non pertinent
        for script in soup(["script", "style"]):
            script.extract()
        
        text = soup.get_text(separator="\n", strip=True)
        # Nettoyer les espaces et les sauts de ligne multiples
        text = re.sub(r'\n+', '\n', text)
        text = re.sub(r' +', ' ', text)
        result["text"] = text
    
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
    
    return result

@app.get("/scrape-multiple")
async def scrape_multiple(
    urls: List[str] = Query(...),
    dynamic: bool = False,
    wait_time: int = 5,
    extract_links: bool = False,
    extract_images: bool = False,
    extract_text: bool = True,
    css_selector: Optional[str] = None
):
    """
    Endpoint pour scraper plusieurs pages web en parallèle.
    """
    # Vérifier que les URLs sont valides
    for url in urls:
        if not re.match(r'^https?://', url):
            raise HTTPException(status_code=400, detail=f"URL invalide: {url}. L'URL doit commencer par http:// ou https://")
    
    tasks = []
    for url in urls:
        task = scrape(
            url=url,
            dynamic=dynamic,
            wait_time=wait_time,
            extract_links=extract_links,
            extract_images=extract_images,
            extract_text=extract_text,
            css_selector=css_selector
        )
        tasks.append(task)
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Traiter les résultats et les erreurs
    processed_results = {}
    for i, result in enumerate(results):
        if isinstance(result, Exception):
            processed_results[urls[i]] = {"error": str(result)}
        else:
            processed_results[urls[i]] = result
    
    return processed_results

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)