import asyncio
from playwright.async_api import async_playwright
import json
from urllib.parse import urlencode, urljoin

BASE_URL = "https://fr.indeed.com"

async def build_search_url(poste, localisation, type_contrat):
    params = {
        "q": poste,
        "l": localisation,
        "jt": type_contrat
    }
    return f"{BASE_URL}/jobs?{urlencode(params)}"

async def scrape_page(page):
    await page.wait_for_selector("a[data-jk]")
    jobs = await page.query_selector_all("a[data-jk]")
    results = []
    for job in jobs:
        href = await job.get_attribute('href')
        if href and href.startswith('/rc/clk'):
            job_url = urljoin(BASE_URL, href)
            title = await job.inner_text()
            results.append({
                "site_link": job_url,
                "poste": title,
            })
    return results

async def crawl_indeed(poste, localisation, type_contrat):
    search_url = await build_search_url(poste, localisation, type_contrat)
    all_jobs = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        page_num = 0
        while True:
            url = f"{search_url}&start={page_num * 10}"
            print(f"Scraping: {url}")
            await page.goto(url)
            jobs_on_page = await scrape_page(page)

            if not jobs_on_page:
                break

            all_jobs.extend(jobs_on_page)
            page_num += 1

        await browser.close()
    return all_jobs

if __name__ == "__main__":
    import sys
    poste, localisation, type_contrat = input("Poste / localisation / type contrat (ex: Data Scientist Paris CDI): ").split()

    results = asyncio.run(crawl_indeed(poste, localisation, type_contrat))

    with open(r"C:\Users\victo\Desktop\crawl\jobs_playwright.json", "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    print(f"{len(results)} offres récupérées.")
