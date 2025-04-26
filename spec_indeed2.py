import json
import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

def fetch_indeed_jobs(url):
    """
    Fetches job postings from the given Indeed URL and returns them as a list of dictionaries.
    
    Args:
        url (str): The Indeed job search URL to fetch.
    
    Returns:
        list: A list of dictionaries, each containing job details.
    """
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.goto(url)
        html = page.content()
        browser.close()
    
    soup = BeautifulSoup(html, "html.parser")
    job_containers = soup.find_all("div", class_=re.compile("job_seen_beacon"))
    jobs = []
    
    for container in job_containers:
        job = {}
        
        # Extract title
        title_tag = container.find("h2", class_=re.compile("jobTitle"))
        job["title"] = title_tag.get_text(strip=True) if title_tag else "Non spécifié"
        
        # Extract company
        company_tag = container.find("span", class_=re.compile("companyName"))
        job["company"] = company_tag.get_text(strip=True) if company_tag else "Non spécifié"
        
        # Extract location
        location_tag = container.find("div", class_=re.compile("companyLocation"))
        job["location"] = location_tag.get_text(strip=True) if location_tag else "Non spécifié"
        
        # Extract description snippet
        description_tag = container.find("div", class_=re.compile("job-snippet"))
        job["description"] = description_tag.get_text(strip=True) if description_tag else "Non spécifié"
        
        # Extract link
        link_tag = container.find("a", href=True)
        job["link"] = urljoin(url, link_tag["href"]) if link_tag else url
        
        # Extract type_de_contrat (approximate, based on text search)
        contract_text = container.get_text().lower()
        if "cdi" in contract_text:
            job["type_de_contrat"] = "CDI"
        elif "cdd" in contract_text:
            job["type_de_contrat"] = "CDD"
        elif "intérim" in contract_text:
            job["type_de_contrat"] = "Intérim"
        elif "stage" in contract_text:
            job["type_de_contrat"] = "Stage"
        else:
            job["type_de_contrat"] = "Non spécifié"
        
        # Extract years of experience (approximate, based on text search)
        experience_match = re.search(r"(\d+)\s*(ans|years)", container.get_text(), re.I)
        job["années_d’expérience"] = f"{experience_match.group(1)} ans" if experience_match else "Non spécifié"
        
        # Extract senior/junior level (approximate, based on text search)
        level_match = re.search(r"(senior|junior)", container.get_text(), re.I)
        job["senior_junior"] = level_match.group(1) if level_match else "Non spécifié"
        
        # Additional fields (set to "Non spécifié" as they are not directly available on the search page)
        job["taille_entreprise"] = "Non spécifié"
        job["type_d’entreprise"] = "Non spécifié"
        job["compétences"] = "Non spécifié"
        job["formation"] = "Non spécifié"
        job["date_de_publication_du_post"] = "Non spécifié"
        
        jobs.append(job)
    
    return jobs

# Example usage
url = "https://fr.indeed.com/jobs?q=D%C3%A9veloppeur+Python&l=Paris&radius=35"
jobs = fetch_indeed_jobs(url)

# Save to JSON file
with open(r"C:\Users\victo\Desktop\crawl\jobs.json", "w", encoding="utf-8") as f:
    json.dump(jobs, f, ensure_ascii=False, indent=4)