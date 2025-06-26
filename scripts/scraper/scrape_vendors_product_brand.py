"""
Process:
1) Scrape the HTML data from each file
2) For each individual company whose web pages were scraped, create a new file called /scripts/private/[companyname.txt] 
3) At the top of each file, paste the website URL that the data was taken from. Then paste the scraped HTML below.
4) The data will be copied directly into https://platform.openai.com/playground/chat?models=gpt-4o and turned into CSV data.
Each product will be added to a database with these fields, with data taken directly from the HTML pages:
- product_name
- brand_name
- certifications
- categories
- descriptors
- ingredients
- product_image_url
- product_url
- dietary_compatibility
- gluten_free_score
- price
- stores
- country


Helpful links:
https://platform.openai.com/playground/chat?models=gpt-4o
https://celiapp1.atlassian.net/browse/OP-35
https://www.perplexity.ai/search/new?q=pending&newFrontendContextUUID=eddd706f-2b8c-497a-9984-ea949598a839


command:
python3 -m scripts.scrape_vendors_product_brand
"""

import requests
from bs4 import BeautifulSoup
import os
import time
from urllib.parse import urljoin, urlparse
import logging
from datetime import datetime


class GlutenFreeWebScraper:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
        }
        self.base_dir = 'scripts/private/gfg'
        self.setup_logging()
        self.ensure_directories()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('scraping.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def ensure_directories(self):
        """Create necessary directories if they don't exist"""
        os.makedirs(self.base_dir, exist_ok=True)

    def get_company_name(self, url):
        """Extract company name from URL using domain pattern"""
        try:
            # Remove protocol (http:// or https://)
            clean_url = url.split('://')[-1]

            # Remove www. if present
            if clean_url.startswith('www.'):
                clean_url = clean_url[4:]

            # Find the domain end (.com/, .ca/, etc.)
            domain_ends = ['.com/', '.ca/']
            for end in domain_ends:
                if end in clean_url:
                    # Get everything before the domain end
                    name = clean_url.split(end)[0]
                    # Get the last part if there are subdomains
                    name = name.split('.')[-1]
                    # Clean the name
                    return name.replace("-", "_")

            # Fallback: If no domain end found, use the hostname
            return clean_url.split('/')[0].split('.')[-2].replace("-", "_")
        except Exception as e:
            # Fallback to a timestamp if parsing fails
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            return f"unknown_site_{timestamp}"

    def fetch_page(self, url, retries=3):
        """Fetch a single page with retries and error handling"""
        for attempt in range(retries):
            try:
                response = requests.get(url, headers=self.headers, timeout=30)
                response.raise_for_status()
                return response.text
            except requests.RequestException as e:
                self.logger.error(f"Attempt {attempt + 1} failed for {url}: {str(e)}")
                if attempt == retries - 1:
                    return None
                time.sleep(2 * (attempt + 1))

    def find_next_page(self, soup, base_url):
        """Find next page URL using common pagination patterns"""
        pagination_selectors = [
            ('a', {'class': ['pagination__next', 'next', 'next-page']}),
            ('link', {'rel': 'next'}),
            ('a', {'aria-label': ['Next', 'Next Page', 'Next page']}),
            ('a', {'title': ['Next', 'Next Page']}),
            ('a', {'class': 'PageArrow__Next'}),
        ]

        for tag, attrs in pagination_selectors:
            element = soup.find(tag, attrs)
            if element and element.get('href'):
                next_url = element['href']
                return urljoin(base_url, next_url)
        return None

    def scrape_site(self, url):
        """Scrape all pages from a site and save the HTML"""
        self.logger.info(f"Starting to scrape: {url}")
        company_name = self.get_company_name(url)

        # Generate file name with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.base_dir}/{company_name}_{timestamp}.txt"

        with open(filename, 'w', encoding='utf-8') as f:
            # Write URL at the top
            f.write(f"Source URL: {url}\n")
            f.write("=" * 80 + "\n\n")

            current_url = url
            page_num = 1

            while current_url:
                self.logger.info(f"Fetching page {page_num} from {current_url}")
                html = self.fetch_page(current_url)

                if not html:
                    self.logger.error(f"Failed to fetch page {page_num} from {current_url}")
                    break

                # Write page number and HTML content
                f.write(f"\nPAGE {page_num}\n")
                f.write("-" * 40 + "\n")
                f.write(html)
                f.write("\n" + "=" * 80 + "\n")

                # Find next page
                soup = BeautifulSoup(html, 'html.parser')
                current_url = self.find_next_page(soup, current_url)
                page_num += 1

                if current_url:
                    time.sleep(2)  # Polite delay between pages

        self.logger.info(f"Completed scraping {company_name}. Data saved to {filename}")
        return filename


def main():
    urls = [
        "http://www.blossombakery.ca/n-menu/",
        "https://aidansglutenfree.com/products",
        "https://bakedbykelly.ca",
        "https://br4trade.com/cheese-rolls/",
        "https://haleyspantry.com/collections/all",
        "https://helanutrition.com/collections/all",
        "https://lapapampa.com.pe/shop/",
        "https://odoughs.com/collections/products",
        "https://plantedinhamilton.com/expo",
        "https://shop.becksbroth.com/",
        "https://shop.nealbrothersfoods.com/collections/crank%C2%AE-coffee-co",
        "https://shop.newtonsnogluten.com/collections/breads-and-buns",
        "https://shophendersonbrewing.com/collections/sollys-craft-soda",
        "https://shoplakeandoak.com/collections/all",
        "https://thelowcarbco.ca/pages/shop",
        "https://www.boldqualitylemonaide.com/collections/all",
        "https://www.brodohouse.com/",
        "https://www.hartandziel.ca/#our-products",
        "https://www.holycannolitoronto.ca/s/order",
        "https://www.jewelsunderthekilt.com/shop",
        "https://www.livbon.ca/copy-of-our-story",
        "https://www.mollybglutenfree.com/all-products-1?page=1",
        "https://www.mollysmarket.ca/collections/mollys-baked-goods",
        "https://www.nolabaking.com/shop",
        "https://www.thebreadessentials.com/collections/all",
    ]

    scraper = GlutenFreeWebScraper()

    for url in urls:
        try:
            output_file = scraper.scrape_site(url)
            time.sleep(3)  # Polite delay between sites
        except Exception as e:
            scraper.logger.error(f"Failed to scrape {url}: {str(e)}")


if __name__ == "__main__":
    main()
