import asyncio
import os
import hashlib
import json
import re
import logging
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig
from crawl4ai.markdown_generation_strategy import DefaultMarkdownGenerator

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def extract_job_offers(html, prompt, base_url):
    """
    Extrait les détails des offres d'emploi à partir d'un HTML en fonction du prompt donné.
    
    Args:
        html (str): Le contenu HTML de la page web.
        prompt (dict): Le prompt contenant l'URL, le poste, la localisation et le type de contrat.
        base_url (str): L'URL de la page pour le champ lien_du_site.
    
    Returns:
        list: Liste des offres d'emploi au format dict.
    """
    soup = BeautifulSoup(html, "html.parser")
    job_offers = []
    
    # Recherche des conteneurs d'offres d'emploi
    job_containers = soup.find_all(["div", "article", "section"], class_=re.compile("job|offer|listing|emploi", re.I))
    
    for container in job_containers:
        offer = {}
        
        # Extraction du titre du poste
        title_tag = container.find(["h1", "h2", "h3"])
        if title_tag:
            offer["poste"] = title_tag.get_text(strip=True)
        
        # Extraction de la description
        description_tag = container.find("p")
        if description_tag:
            offer["description"] = description_tag.get_text(strip=True)
        
        # Extraction de la localisation
        location_tag = container.find(string=re.compile(prompt["localisation"], re.I))
        if location_tag:
            offer["localisation"] = location_tag.strip()
        
        # Extraction du nom de l'entreprise
        company_tag = container.find(string=re.compile("company|entreprise", re.I))
        if company_tag:
            offer["nom_entreprise"] = company_tag.strip()
        
        # Extraction du type de contrat
        contract_tag = container.find(string=re.compile(prompt["type_de_contrat"], re.I))
        if contract_tag:
            offer["type_de_contrat"] = contract_tag.strip()
        
        # Extraction des années d'expérience
        experience_tag = container.find(string=re.compile(r"\d+\+?\s*(ans|years)", re.I))
        if experience_tag:
            offer["années_d’expérience"] = experience_tag.strip()
        else:
            offer["années_d’expérience"] = "Non spécifié"
        
        # Extraction du niveau (senior/junior)
        level_tag = container.find(string=re.compile("senior|junior", re.I))
        if level_tag:
            offer["senior_junior"] = level_tag.strip()
        else:
            offer["senior_junior"] = "Non spécifié"
        
        # Champs supplémentaires
        offer["lien_du_site"] = base_url
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
    Crawle un site web et extrait les offres d'emploi selon le prompt.
    
    Args:
        prompt (dict): Contient l'URL, le poste, la localisation et le type de contrat.
        output_dir (str): Répertoire pour sauvegarder le fichier JSON.
    """
    # Créer le répertoire de sortie
    os.makedirs(output_dir, exist_ok=True)
    
    # Configurer le générateur Markdown (non utilisé pour l'extraction, mais requis par Crawl4AI)
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
        exclude_social_media_links=True
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
    
    async def crawl_page(current_url, depth=0):
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
                        
                        # Extraire les liens internes
                        internal_links = result.links.get("internal", [])
                        for link in internal_links:
                            href = link["href"]
                            absolute_url = urljoin(current_url, href)
                            if urlparse(absolute_url).netloc == urlparse(prompt["url"]).netloc:
                                await crawl_page(absolute_url, depth + 1)
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
    # Exemple de prompt avec l'URL
    prompt = {
        "url": "https://fr.indeed.com/",  # Remplacez par l'URL réelle du site
        "poste": "Développeur Python",
        "localisation": "Paris",
        "type_de_contrat": "CDI"
    }
    
    await crawl_website(prompt, output_dir=r"C:\Users\victo\Desktop\crawl\job_offers")

if __name__ == "__main__":
    asyncio.run(main())