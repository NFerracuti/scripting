import requests
from bs4 import BeautifulSoup
import time
import json


def find_instagram_handle(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(url, headers=headers, timeout=10)
        soup = BeautifulSoup(response.text, 'html.parser')

        instagram_links = []
        links = soup.find_all('a', href=lambda x: x and 'instagram.com' in x.lower())
        social_links = soup.find_all('a', class_=lambda x: x and 'instagram' in x.lower())

        instagram_links.extend(links)
        instagram_links.extend(social_links)

        if instagram_links:
            instagram_url = instagram_links[0]['href']
            if not instagram_url.startswith('http'):
                instagram_url = 'https://' + instagram_url.lstrip('/')
            return instagram_url
        return "(none)"

    except Exception as e:
        print(f"Error processing URL {url}: {e}")
        return "(none)"


def scrape_gluten_free_vendors():
    url = "https://glutenfreegarage.ca/vendors/"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/91.0.4472.124 Safari/537.36'
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        vendors = soup.find_all('div', class_='fg-item')
        vendor_list = []

        for vendor in vendors:
            try:
                anchor = vendor.find('a')
                caption = vendor.find('div', class_='fg-caption-title')

                if anchor and caption:
                    name = caption.text.strip()
                    vendor_url = anchor.get('href')

                    if vendor_url and not vendor_url.endswith(('.jpg', '.png', '.jpeg')):
                        print(f"Processing {name}...")
                        instagram = find_instagram_handle(vendor_url)

                        vendor_info = {
                            'name': name,
                            'url': vendor_url,
                            'instagram': instagram
                        }

                        vendor_list.append(vendor_info)
                        print(f"Name: {name}")
                        print(f"URL: {vendor_url}")
                        print(f"Instagram: {instagram}")
                        print("-" * 50)

                        time.sleep(1)

            except Exception as e:
                print(f"Error processing vendor: {e}")

        # Save to JSON file
        with open('vendors.json', 'w', encoding='utf-8') as f:
            json.dump(vendor_list, f, indent=2)

        return vendor_list

    except Exception as e:
        print(f"Error scraping website: {e}")
        return None


if __name__ == "__main__":
    vendors = scrape_gluten_free_vendors()



"""VENDORS.JSON EXAMPLE:
[
  {
    "name": "Aidan's Gluten Free",
    "url": "https://aidansglutenfree.com",
    "instagram": "https://www.instagram.com/aidansglutenfree/"
  },
  {
    "name": "Beck's Broth",
    "url": "https://www.becksbroth.com",
    "instagram": "https://www.instagram.com/becksbroth/"
  },
  {
    "name": "B\u00f6ld Quality Lemonaide",
    "url": "https://www.boldqualitylemonaide.com",
    "instagram": "https://www.instagram.com/boldlemonaide"
  }
 ]

vendor_list = ["https://www.instagram.com/aidansglutenfree/",
"https://www.instagram.com/bakedbykelly1/",
"https://www.instagram.com/becksbroth/",
"https://www.instagram.com/blossom_bakery/",
"https://www.instagram.com/boldlemonaide",
"https://www.instagram.com/thebreadessentials/",
"https://www.instagram.com/brodohouse/",
"https://www.instagram.com/brunosbakery/",
"https://instagram.com/leasaad",
"https://www.instagram.com/theceleaccorner",
"https://www.instagram.com/ccaceliac/",
"https://www.instagram.com/the.celiapp/",
"https://www.instagram.com/nealbrothers/",
"https://www.instagram.com/crankcoffee_/",
"https://www.instagram.com/fornodeminascanada/",
"https://www.instagram.com/frenchlunch/",
"https://www.instagram.com/haleyspantry/",
"https://www.instagram.com/hartandziel/",
"https://www.instagram.com/hela_nutrition/",
"https://www.instagram.com/happycutemart",
"https://instagram.com/holycannolito",
"https://www.instagram.com/hugsandsarcasm/",
"https://www.instagram.com/jewelsunderthekilt/",
"https://www.instagram.com/ketokookieco/",
"https://www.instagram.com/lakeandoaktea/",
"https://www.instagram.com/suesmarket/",
"https://www.instagram.com/livbon.inc/",
"https://www.instagram.com/love_pipaya/",
"https://www.instagram.com/thelowcarbco_/",
"https://www.instagram.com/mollybglutenfreekitchen/",
"https://www.instagram.com/mollysmarket.ca/",
"https://www.instagram.com/newtonsnogluten/",
"https://www.instagram.com/nolabaking/",
"https://www.instagram.com/odoughs",
"https://www.instagram.com/only.oats/",
"https://instagram.com/pierogi_me",
"https://www.instagram.com/plantedinhamont/",
"https://www.instagram.com/glutenfreebakingcourses/",
"https://www.instagram.com/saltedsunday/",
"https://www.instagram.com/thesimplekitchencanada",
"https://www.instagram.com/hendersonbrewing/",
"https://www.instagram.com/sollyscraftsoda/",
"https://www.instagram.com/sprouty_restaurant/",
"https://www.instagram.com/sweets_by_stephh/",
"https://www.instagram.com/woah_dough_gluten_free/",
"https://instagram.com/yoonas.kitchen"
]
"""
