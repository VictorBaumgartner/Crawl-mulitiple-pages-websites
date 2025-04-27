import requests
from bs4 import BeautifulSoup
import json
import time
import uuid
from urllib.parse import quote
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def crawl_hellowork_jobs(job_title, location, use_selenium=False):
    """
    Crawl job offers from HelloWork based on job title and location.
    Returns a JSON with job details including link, position, experience, seniority,
    description, location, company name, company size, company type, contract type,
    skills, and education. Uses requests/BeautifulSoup or selenium for dynamic content.
    """
    base_url = "https://www.hellowork.com/fr-fr/emploi/recherche.html"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    jobs = []
    
    # Encode job title and location for URL
    job_title_encoded = quote(job_title)
    location_encoded = quote(location)
    
    def parse_job_card(card, soup_obj):
        """Helper function to parse a job card."""
        job = {'id': str(uuid.uuid4())}
        
        # Extract job link
        link_elem = card.find('a', href=True)
        job['link'] = f"https://www.hellowork.com{link_elem['href']}" if link_elem and link_elem['href'].startswith('/') else link_elem['href'] if link_elem else 'N/A'
        
        # Extract job title
        title_elem = card.find(['h2', 'h3', 'div'], class_=lambda x: x and ('title' in x.lower() or 'job' in x.lower()))
        job['position'] = title_elem.text.strip() if title_elem else 'N/A'
        
        # Extract company name
        company_elem = card.find(['span', 'div'], class_=lambda x: x and ('company' in x.lower()))
        job['company_name'] = company_elem.text.strip() if company_elem else 'N/A'
        
        # Extract location
        location_elem = card.find(['span', 'div'], class_=lambda x: x and ('location' in x.lower() or 'place' in x.lower()))
        job['location'] = location_elem.text.strip() if location_elem else location
        
        # Extract contract type
        contract_elem = card.find(['span', 'div'], class_=lambda x: x and ('contract' in x.lower() or 'type' in x.lower()))
        job['contract_type'] = contract_elem.text.strip() if contract_elem else 'N/A'
        
        # Fetch job detail page if link exists
        if job['link'] != 'N/A' and not use_selenium:
            try:
                detail_response = requests.get(job['link'], headers=headers, timeout=10)
                detail_response.raise_for_status()
                detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                parse_job_details(job, detail_soup)
            except Exception as e:
                print(f"Error fetching details for {job['link']}: {e}")
        elif job['link'] != 'N/A' and use_selenium:
            parse_selenium_details(job)
        
        return job
    
    def parse_job_details(job, detail_soup):
        """Parse additional details from job detail page."""
        # Extract description
        desc_elem = detail_soup.find(['div', 'section'], class_=lambda x: x and ('description' in x.lower() or 'job' in x.lower()))
        job['description'] = desc_elem.text.strip() if desc_elem else 'N/A'
        
        # Extract experience and seniority
        experience_elem = detail_soup.find(['span', 'div'], class_=lambda x: x and ('experience' in x.lower()))
        experience_text = experience_elem.text.strip() if experience_elem else 'N/A'
        job['years_of_experience'] = experience_text
        
        # Determine seniority
        job['seniority'] = 'N/A'
        if experience_text != 'N/A':
            experience_lower = experience_text.lower()
            if '10 ans' in experience_lower or 'senior' in experience_lower:
                job['seniority'] = 'Senior'
            elif 'débutant' in experience_lower or 'junior' in experience_lower or '0-2 ans' in experience_lower:
                job['seniority'] = 'Junior'
        
        # Extract skills
        skills_elem = detail_soup.find(['ul', 'div'], class_=lambda x: x and ('skill' in x.lower() or 'compétence' in x.lower()))
        job['skills'] = [li.text.strip() for li in skills_elem.find_all('li')] if skills_elem else ['N/A']
        
        # Extract education
        education_elem = detail_soup.find(['span', 'div'], class_=lambda x: x and ('education' in x.lower() or 'formation' in x.lower()))
        job['education'] = education_elem.text.strip() if education_elem else 'N/A'
        
        # Extract company size and type
        company_info_elem = detail_soup.find(['div', 'section'], class_=lambda x: x and ('company' in x.lower()))
        job['company_size'] = 'N/A'
        job['company_type'] = 'N/A'
        if company_info_elem:
            size_elem = company_info_elem.find(['span', 'div'], class_=lambda x: x and 'size' in x.lower())
            job['company_size'] = size_elem.text.strip() if size_elem else 'N/A'
            type_elem = company_info_elem.find(['span', 'div'], class_=lambda x: x and 'type' in x.lower())
            job['company_type'] = type_elem.text.strip() if type_elem else 'N/A'
    
    def parse_selenium_details(job):
        """Parse job details using Selenium."""
        driver = setup_selenium()
        try:
            driver.get(job['link'])
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            detail_soup = BeautifulSoup(driver.page_source, 'html.parser')
            parse_job_details(job, detail_soup)
        except Exception as e:
            print(f"Selenium error for {job['link']}: {e}")
        finally:
            driver.quit()
    
    def setup_selenium():
        """Set up Selenium WebDriver."""
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        driver = webdriver.Chrome(options=options)
        return driver
    
    # Try requests first
    page = 1
    while True:
        params = {'k': job_title_encoded, 'l': location_encoded, 'p': page}
        try:
            response = requests.get(base_url, params=params, headers=headers, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Find job listings (more flexible class search)
            job_cards = soup.find_all(['div', 'article', 'section'], class_=lambda x: x and ('offer' in x.lower() or 'job' in x.lower() or 'card' in x.lower()))
            print(f"Page {page}: Found {len(job_cards)} job cards")
            
            if not job_cards:
                print("No job cards found, switching to Selenium...")
                use_selenium = True
                break
                
            for card in job_cards:
                job = parse_job_card(card, soup)
                jobs.append(job)
            
            # Check for next page
            next_page = soup.find(['a', 'button'], class_=lambda x: x and ('next' in x.lower() or 'suivant' in x.lower()))
            if not next_page:
                break
            page += 1
            time.sleep(1)  # Respect rate limits
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching page {page}: {e}")
            use_selenium = True
            break
    
    # Fallback to Selenium if no jobs found
    if use_selenium or not jobs:
        print("Using Selenium to crawl jobs...")
        driver = setup_selenium()
        try:
            url = f"{base_url}?k={job_title_encoded}&l={location_encoded}"
            driver.get(url)
            WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
            
            page = 1
            while True:
                soup = BeautifulSoup(driver.page_source, 'html.parser')
                job_cards = soup.find_all(['div', 'article', 'section'], class_=lambda x: x and ('offer' in x.lower() or 'job' in x.lower() or 'card' in x.lower()))
                print(f"Selenium Page {page}: Found {len(job_cards)} job cards")
                
                for card in job_cards:
                    job = parse_job_card(card, soup)
                    jobs.append(job)
                
                # Check for next page
                next_button = driver.find_elements(By.XPATH, "//a[contains(@class, 'next') or contains(@class, 'suivant')]")
                if not next_button:
                    break
                next_button[0].click()
                WebDriverWait(driver, 10).until(EC.staleness_of(next_button[0]))
                page += 1
                time.sleep(2)
                
        finally:
            driver.quit()
    
    # Save to JSON
    output = {
        'job_title': job_title,
        'location': location,
        'total_jobs': len(jobs),
        'jobs': jobs
    }
    
    with open('hellowork_jobs.json', 'w', encoding='utf-8') as f:
        json.dump(output, f, ensure_ascii=False, indent=4)
    
    return output

# Example usage
if __name__ == "__main__":
    job_title = "Data Scientist"
    location = "Paris"
    result = crawl_hellowork_jobs(job_title, location)
    print(f"Found {result['total_jobs']} jobs for {job_title} in {location}")