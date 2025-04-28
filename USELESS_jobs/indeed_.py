import asyncio
import json
import uuid
from urllib.parse import quote
from crawl4ai import AsyncWebCrawler
from crawl4ai.extraction_strategy import JsonCssExtractionStrategy

async def crawl_hellowork_jobs(job_title, location):
    """
    Crawl job offers from a HelloWork search URL using Crawl4AI.
    Filters out non-job-post links and returns a JSON with job details including
    link, position, experience, seniority, description, location, company name,
    company size, company type, contract type, skills, and education.
    Handles spaces in job_title by replacing with '+' for search compatibility.
    """
    base_url = "https://www.hellowork.com/fr-fr/emploi/recherche.html"
    jobs = []
    
    # Encode job title (replace spaces with '+' for search parameter) and location
    job_title_encoded = job_title.replace(' ', '+')
    location_encoded = quote(location)
    
    # Define extraction strategy for job listings
    job_list_strategy = JsonCssExtractionStrategy(
        schema={
            "jobs": {
                "selector": "div[class*='offer'], article[class*='job'], section[class*='card']",
                "type": "list",
                "subfields": {
                    "link": {"selector": "a[href]", "type": "attribute", "attribute": "href"},
                    "position": {"selector": "h2, h3, [class*='title'], [class*='job']", "type": "text"},
                    "company_name": {"selector": "[class*='company']", "type": "text"},
                    "location": {"selector": "[class*='location'], [class*='place']", "type": "text"},
                    "contract_type": {"selector": "[class*='contract'], [class*='type']", "type": "text"}
                }
            }
        },
        combine=True
    )
    
    # Define extraction strategy for job details
    job_detail_strategy = JsonCssExtractionStrategy(
        schema={
            "description": {"selector": "div[class*='description'], section[class*='job']", "type": "text"},
            "years_of_experience": {"selector": "[class*='experience']", "type": "text"},
            "skills": {"selector": "ul[class*='skill'], ul[class*='compétence'] li", "type": "list", "subfields": {"skill": {"selector": "", "type": "text"}}},
            "education": {"selector": "[class*='education'], [class*='formation']", "type": "text"},
            "company_size": {"selector": "[class*='company'][class*='size']", "type": "text"},
            "company_type": {"selector": "[class*='company'][class*='type']", "type": "text"}
        },
        combine=True
    )
    
    async with AsyncWebCrawler(verbose=True) as crawler:
        page = 1
        while True:
            # Construct URL with pagination
            url = f"{base_url}?k={job_title_encoded}&l={location_encoded}&p={page}"
            print(f"Crawling page {page}: {url}")
            
            try:
                # Crawl job listing page
                result = await crawler.arun(
                    url=url,
                    extraction_strategy=job_list_strategy,
                    bypass_cache=True,
                    js_code="window.scrollTo(0, document.body.scrollHeight);"
                )
                
                if not result.success:
                    print(f"Failed to crawl page {page}: {result.message}")
                    break
                
                # Extract job cards
                job_cards = result.extracted_content.get('jobs', []) if result.extracted_content else []
                print(f"Page {page}: Found {len(job_cards)} job cards")
                
                if not job_cards:
                    print("No more job cards found, stopping...")
                    break
                
                for card in job_cards:
                    try:
                        job = {'id': str(uuid.uuid4())}
                        
                        # Filter non-job links
                        link = card.get('link', '')
                        if not link or not ('emploi' in link.lower() or 'offre' in link.lower()):
                            print(f"Skipping non-job link: {link}")
                            continue
                        job['link'] = f"https://www.hellowork.com{link}" if link.startswith('/') else link
                        
                        # Populate basic fields
                        job['position'] = card.get('position', 'N/A')
                        job['company_name'] = card.get('company_name', 'N/A')
                        job['location'] = card.get('location', location)
                        job['contract_type'] = card.get('contract_type', 'N/A')
                        
                        # Crawl job detail page
                        try:
                            detail_result = await crawler.arun(
                                url=job['link'],
                                extraction_strategy=job_detail_strategy,
                                bypass_cache=True
                            )
                            
                            if detail_result.success:
                                details = detail_result.extracted_content
                                job['description'] = details.get('description', 'N/A')
                                job['years_of_experience'] = details.get('years_of_experience', 'N/A')
                                job['skills'] = [s.get('skill', 'N/A') for s in details.get('skills', [{'skill': 'N/A'}])]
                                job['education'] = details.get('education', 'N/A')
                                job['company_size'] = details.get('company_size', 'N/A')
                                job['company_type'] = details.get('company_type', 'N/A')
                                
                                # Determine seniority
                                job['seniority'] = 'N/A'
                                experience_text = job['years_of_experience'].lower()
                                if '10 ans' in experience_text or 'senior' in experience_text:
                                    job['seniority'] = 'Senior'
                                elif 'débutant' in experience_text or 'junior' in experience_text or '0-2 ans' in experience_text:
                                    job['seniority'] = 'Junior'
                            else:
                                print(f"Failed to crawl details for {job['link']}: {detail_result.message}")
                                job.update({
                                    'description': 'N/A',
                                    'years_of_experience': 'N/A',
                                    'skills': ['N/A'],
                                    'education': 'N/A',
                                    'company_size': 'N/A',
                                    'company_type': 'N/A',
                                    'seniority': 'N/A'
                                })
                            
                            jobs.append(job)
                            print(f"Added job: {job['position']} at {job['company_name']}")
                        
                        except Exception as e:
                            print(f"Error crawling details for {job['link']}: {e}")
                            continue
                        
                    except Exception as e:
                        print(f"Error processing job card: {e}")
                        continue
                
                page += 1
                await asyncio.sleep(2)  # Respect rate limits
                
            except Exception as e:
                print(f"Error crawling page {page}: {e}")
                break
    
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
    result = asyncio.run(crawl_hellowork_jobs(job_title, location))
    print(f"Found {result['total_jobs']} jobs for {job_title} in {location}")