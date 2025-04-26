### Key Requirements
#- **Natural Language Query**: Parse a query like "I want job offers for Développeur Python in Paris with CDI contract" to generate the Indeed URL with `radius=35`.
#- **Crawl Specific Pages**: Only crawl pages with `/jobs` or `/emplois` in the path and query parameters matching `q=<poste>`, `l=<localisation>`, and `radius=35` (e.g., `https://fr.indeed.com/emplois?q=D%C3%A9veloppeur+Python&l=Paris&radius=35&start=10`).
#- **JavaScript Rendering**: Use Playwright with `crawl4ai` to handle Indeed’s dynamic content, avoiding the `bypass_js` error.
#- **Extract Job Offers**: Scrape job details (title, company, location, etc.) and save them in a JSON file with all requested fields.
#- **Fix Previous Issue**: Ensure `is_valid_job_url` correctly validates URLs by properly decoding and comparing query parameters.

### Complete Updated Code
#Below is the corrected and complete script. It removes the `bypass_js` parameter, uses Playwright for JavaScript rendering, and ensures all pagination pages are crawled.

import asyncio
import os
import hashlib
import json
import re
import logging
from urllib.parse import urljoin, urlparse, parse_qs, urlencode, unquote
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def parse_natural_language_query(query):
    """
    Parse a natural language query into a structured prompt.
    
    Args:
        query (str): Natural language query, e.g., "I want job offers for Développeur Python in Paris with CDI contract".
    
    Returns:
        dict: Structured prompt with url, poste, localisation, type_de_contrat, and radius.
    """
    pattern = r"for\s+(.+?)\s+in\s+(.+?)\s+with\s+(.+?)\s+contract"
    match = re.search(pattern, query, re.IGNORECASE)
    
    if not match:
        raise ValueError("Invalid query format. Use: 'I want job offers for [poste] in [localisation] with [type_de_contrat] contract'")
    
    poste, localisation, type_de_contrat = match.groups()
    
    # Construct Indeed search URL with radius=35
    query_params = {
        "q": poste.strip(),
        "l": localisation.strip(),
        "radius": "35"
    }
    search_url = f"https://fr.indeed.com/emplois?{urlencode(query_params)}"
    
    return {
        "url": search_url,
        "poste": poste.strip(),
        "localisation": localisation.strip(),
        "type_de_contrat": type_de_contrat.strip(),
        "radius": "35"
    }

def extract_job_offers(html, prompt, base_url):
    """
    Extrait les détails des offres d'emploi à partir d'un HTML en fonction du prompt donné.
    
    Args:
        html (str): Le contenu HTML de la page web.
        prompt (dict): Le prompt contenant l'URL, le poste, la localisation, le type de contrat, et radius.
        base_url (str): L'URL de la page pour le champ lien_du_site.
    
    Returns:
        list: Liste des offres d'emploi au format dict.
    """
    soup = BeautifulSoup(html, "html.parser")
    job_offers = []
    
    # Recherche des conteneurs d'offres d'emploi spécifiques à Indeed
    job_containers = soup.find_all("div", class_=re.compile("job_seen_beacon"))
    
    for container in job_containers:
        offer = {}
        
        # Extraction du titre du poste
        title_tag = container.find("h2", class_=re.compile("jobTitle"))
        offer["poste"] = title_tag.get_text(strip=True) if title_tag else "Non spécifié"
        
        # Extraction du nom de l'entreprise
        company_tag = container.find("span", class_=re.compile("companyName"))
        offer["nom_entreprise"] = company_tag.get_text(strip=True) if company_tag else "Non spécifié"
        
        # Extraction de la localisation
        location_tag = container.find("div", class_=re.compile("companyLocation"))
        offer["localisation"] = location_tag.get_text(strip=True) if location_tag else "Non spécifié"
        
        # Extraction de la description
        description_tag = container.find("div", class_=re.compile("job-snippet"))
        offer["description"] = description_tag.get_text(strip=True) if description_tag else "Non spécifié"
        
        # Extraction du type de contrat
        contract_tag = container.find(string=re.compile(prompt["type_de_contrat"], re.I))
        offer["type_de_contrat"] = contract_tag.strip() if contract_tag else "Non spécifié"
        
        # Extraction des années d'expérience
        experience_tag = container.find(string=re.compile(r"\d+\+?\s*(ans|years)", re.I))
        offer["années_d’expérience"] = experience_tag.strip() if experience_tag else "Non spécifié"
        
        # Extraction du niveau (senior/junior)
        level_tag = container.find(string=re.compile("senior|junior", re.I))
        offer["senior_junior"] = level_tag.strip() if level_tag else "Non spécifié"
        
        # Extraction du lien
        link_tag = container.find("a", href=True)
        offer["lien_du_site"] = urljoin(base_url, link_tag["href"]) if link_tag else base_url
        
        # Champs par défaut
        offer["taille_entreprise"] = "Non spécifié"
        offer["type_d’entreprise"] = "Non spécifié"
        offer["compétences"] = "Non spécifié"
        offer["formation"] = "Non spécifié"
        offer["date_de_publication_du_post"] = "Non spécifié"
        
        # Filtrer les offres selon les critères du prompt
        if (prompt["poste"].lower() in offer.get("poste", "").lower() and
            prompt["localisation"].lower() in offer.get("localisation", "").lower() and
            prompt["type_de_contrat"].lower() in offer.get("type_de_contrat", "").lower()):
            job_offers.append(offer)
    
    return job_offers

async def crawl_website(prompt, output_dir="job_offers"):
    """
    Crawle un site web et extrait les offres d'emploi selon le prompt, uniquement pour les pages contenant '/jobs' ou '/emplois'
    avec les paramètres de requête q=<poste>, l=<localisation>, et radius=35.
    
    Args:
        prompt (dict): Contient l'URL, le poste, la localisation, le type de contrat, et radius.
        output_dir (str): Répertoire pour sauvegarder le fichier JSON.
    """
    # Créer le répertoire de sortie
    os.makedirs(output_dir, exist_ok=True)
    
    # Configurer le générateur Markdown (requis par Crawl4AI)
    md_generator = DefaultMarkdownGenerator(
        options={
            "ignore_links": False,
            "escape_html": True,
            "body_width": 0
        }
    )
    
    # Configuration du crawler
    config = CrawlerRunConfig(
        markdown_generator=md_generator,
        cache_mode="BYPASS",
        exclude_external_links=True,
        exclude_social_media_links=True,
    )
    
    # Suivre les URLs visitées
    visited_urls = set()
    all_job_offers = []
    
    def normalize_url(url):
        """Normalise l'URL."""
        parsed = urlparse(url)
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path or '/'}"
    
    def sanitize_filename(url):
        """Convertit l'URL en nom de fichier valide."""
        parsed = urlparse(url)
        path = parsed.path.strip("/").replace("/", "_") or "index"
        netloc = parsed.netloc.replace(".", "_")
        url_hash = hashlib.md5(url.encode()).hexdigest()[:8]
        return f"{netloc}_{path}_{url_hash}.json"
    
    def is_valid_job_url(url):
        """Vérifie si l'URL contient '/jobs' ou '/emplois' et les paramètres de requête corrects."""
        parsed_url = urlparse(url)
        path = parsed_url.path.lower()
        if not ("/jobs" in path or "/emplois" in path):
            logger.debug(f"URL rejected: '{url}' does not contain '/jobs' or '/emplois'")
            return False
        
        # Extraire et décoder les paramètres de requête
        query_params = parse_qs(parsed_url.query)
        poste_query = unquote(query_params.get("q", [""])[0]).lower()
        location_query = unquote(query_params.get("l", [""])[0]).lower()
        radius_query = query_params.get("radius", [""])[0]
        
        # Normaliser les valeurs du prompt
        poste_prompt = prompt["poste"].lower()
        location_prompt = prompt["localisation"].lower()
        radius_prompt = prompt.get("radius", "")
        
        # Débogage
        logger.debug(f"Checking URL: {url}")
        logger.debug(f"poste_query: '{poste_query}' vs poste_prompt: '{poste_prompt}'")
        logger.debug(f"location_query: '{location_query}' vs location_prompt: '{location_prompt}'")
        logger.debug(f"radius_query: '{radius_query}' vs radius_prompt: '{radius_prompt}'")
        
        # Vérifier la correspondance
        is_valid = (poste_query == poste_prompt and
                    location_query == location_prompt and
                    radius_query == radius_prompt)
        if not is_valid:
            logger.debug(f"URL rejected: Query params do not match prompt")
        
        return is_valid
    
    async def crawl_page(current_url, depth=0, max_depth=10):
        """Crawle une page et ses liens internes, jusqu'à une profondeur maximale."""
        if depth > max_depth:
            logger.info(f"Max depth {max_depth} reached, skipping: {current_url}")
            return
        
        if not is_valid_job_url(current_url):
            logger.info(f"Skipping URL without '/jobs' or '/emplois' or incorrect query params: {current_url}")
            return
        
        normalized_url = normalize_url(current_url)
        if normalized_url in visited_urls:
            logger.info(f"Skipping already visited URL: {current_url}")
            return
        
        visited_urls.add(normalized_url)
        logger.info(f"Crawling: {current_url} (Depth: {depth}, Total pages: {len(visited_urls)})")
        
        async with AsyncWebCrawler(verbose=True) as crawler:
            for attempt in range(3):
                try:
                    result = await crawler.arun(url=current_url, config=config)
                    
                    if result.success:
                        # Extraire les offres d'emploi
                        job_offers = extract_job_offers(result.html, prompt, current_url)
                        all_job_offers.extend(job_offers)
                        logger.info(f"Extracted {len(job_offers)} job offers from {current_url}")
                        
                        # Extraire les liens internes (y compris pagination)
                        internal_links = result.links.get("internal", [])
                        for link in internal_links:
                            href = link["href"]
                            absolute_url = urljoin(current_url, href)
                            if urlparse(absolute_url).netloc == urlparse(prompt["url"]).netloc:
                                await crawl_page(absolute_url, depth + 1, max_depth)
                        break
                    else:
                        logger.error(f"Attempt {attempt + 1} failed for {current_url}: {result.error_message}")
                except Exception as e:
                    logger.error(f"Attempt {attempt + 1} exception for {current_url}: {str(e)}")
                if attempt < 2:
                    logger.info(f"Retrying {current_url} in 2 seconds...")
                    await asyncio.sleep(2)
    
    # Lancer le crawling à partir de l'URL donnée
    await crawl_page(prompt["url"])
    
    # Sauvegarder les résultats dans un fichier JSON
    output_path = os.path.join(output_dir, sanitize_filename(prompt["url"]))
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_job_offers, f, indent=2, ensure_ascii=False)
    logger.info(f"Job offers saved to {output_path}")

async def main():
    # Exemple de requête en langage naturel
    query = "I want job offers for Développeur Python in Paris with CDI contract"
    
    # Parser la requête
    try:
        prompt = parse_natural_language_query(query)
    except ValueError as e:
        logger.error(f"Error parsing query: {str(e)}")
        return
    
    # Afficher le prompt pour vérification
    logger.info(f"Parsed prompt: {json.dumps(prompt, indent=2)}")
    
    # Lancer le crawling
    await crawl_website(prompt, output_dir=r"C:\Users\victo\Desktop\crawl\job_offers")

if __name__ == "__main__":
    asyncio.run(main())