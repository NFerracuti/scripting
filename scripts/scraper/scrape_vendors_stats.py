from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
import json
import time

"""
INSTRUCTIONS FOR RUNNING selenium / chromedriver:
brew install --cask chromedriver
pip install selenium requests beautifulsoup4
xattr -d com.apple.quarantine /usr/local/bin/chromedriver
"""


def get_instagram_followers(instagram_url):
    if not instagram_url or instagram_url == "(none)":
        return "0"

    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')

    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(instagram_url)

        # Wait for meta tag to load
        wait = WebDriverWait(driver, 10)
        meta = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, 'meta[property="og:description"]')))

        content = meta.get_attribute('content')

        if content:
            import re
            followers_match = re.search(r'(\d+(?:,\d+)*)\s+Followers', content)
            if followers_match:
                return followers_match.group(1)

        return "unavailable"

    except Exception as e:
        print(f"Error getting followers for {instagram_url}: {e}")
        return "error"

    finally:
        driver.quit()


def process_instagram_list():
    try:
        with open('vendors.json', 'r', encoding='utf-8') as f:
            vendors = json.load(f)
    except FileNotFoundError:
        print("Please run the first script first!")
        return None

    results = []

    for vendor in vendors:
        name = vendor['name']
        url = vendor['url']
        instagram = vendor['instagram']

        print(f"\nProcessing {name}...")
        followers = get_instagram_followers(instagram)

        vendor_info = {
            'name': name,
            'url': url,
            'instagram': instagram,
            'followers': followers
        }

        results.append(vendor_info)

        print(f"Name: {name}")
        print(f"Instagram: {instagram}")
        print(f"Followers: {followers}")
        print("-" * 50)

        # Add longer delay between requests
        time.sleep(5)

    with open('vendors_with_followers.json', 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2)

    return results


if __name__ == "__main__":
    vendors = process_instagram_list()
