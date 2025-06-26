from urllib.request import urlopen, Request
from bs4 import BeautifulSoup  # type: ignore
import re
import csv
import os
import json 

# Ensure the output directory exists
output_dir = '../private/scraped_output'
os.makedirs(output_dir, exist_ok=True)

# Define file paths
content_file_path = os.path.join(output_dir, 'gfco_content.txt')
csv_file_path = os.path.join(output_dir, 'products_gfco.csv')

# Scraping data: only if file with scraped data does not exist (delete file to rescrape data)
if not os.path.exists(content_file_path) or os.stat(content_file_path).st_size == 0:
    print(f"'{content_file_path}' does not exist or is empty. Starting to scrape data...")

    url = "https://gfco.org/product-directory/"  # scraping site
    headers = {'User-Agent': 'Mozilla/5.0'}  # adding a User-Agent header
    req = Request(url, headers=headers)
    html = urlopen(req)
    soup = BeautifulSoup(html, "html.parser")  # using python's html.parser, more parsers available in BeautifulSoup documentation

    products = soup.find_all("div", {"class": "so-widget-gfco-product-finder so-widget-gfco-product-finder-base"})

    # write text content to file
    with open(content_file_path, "w") as f:
        for product in products:
            f.write(str(product))  # Write the text of each product
            f.write("\n\n")

extraction_size = 1000
print(f"Extracting {extraction_size} products.")

# Loading data onto .csv file
# Read the content of the gfco_content.txt file
with open(content_file_path, 'r') as file:
    content = file.read()
start = content.find('window.products = [')

if start != -1:
    # Find the position of the closing square bracket of the text
    end = content.rfind(']')
    if end == -1:
        end = content.rfind(']')

    if end != -1:

        # Extract the JSON string
        json_str = content[start + len('window.products = '):end + 1].strip()

        try:
            # Parse the JSON string into Python objects
            products_data = json.loads(json_str)

            # Write to csv file
            with open(csv_file_path, "w", newline='', encoding='utf-8') as csvFile:
                writer = csv.writer(csvFile)

                #Write headers if the file is empty
                if os.stat(csv_file_path).st_size == 0:
                    writer.writerow(["Product_ID", "Product_name", "Brand_name", "GF_certification_name", "Country"])

                # write to CSV file  
                # Columns: Product_ID, Brand_name, Product_name, GF_certification_name, Country (scraped from)
                for product in products_data:
                    prod_id = product.get('prod_id', '')
                    prod_name = product.get('name', '')
                    brand_name = product.get('brand_name', '')
                    cert_name = "Gluten-Free Certification Organization (GFCO)"
                    cert_country = "USA"
                    writer.writerow((prod_id, prod_name, brand_name, cert_name, cert_country))

            print(f"Extracted {extraction_size} products.")

        except json.JSONDecodeError as e:
            print(f"JSON decoding failed: {e}")

    print(f"Data has been successfully extracted and saved to '{csv_file_path}'.")
