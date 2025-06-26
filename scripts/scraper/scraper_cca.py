from urllib.request import urlopen
from bs4 import BeautifulSoup  # type: ignore
import re
#import psycopg2
import csv
import os

extraction_size = 1000
url = "https://www.celiac.ca/living-gluten-free/gf-product-finder/" #scraping site
html = urlopen(url) 
soup = BeautifulSoup(html, "html.parser") #using python's html.parser, more parsers available in BeautifulSoup documentation

# Regular expression to match 'row-' followed by a number and either 'odd' or 'even'
pattern = re.compile(r'row-\d+\s+(odd|even)')

#grab each product
products = soup.find_all('tr', {"class":pattern})

# Define the output directory relative to the current script's location
# Ensure the output directory exists
# And finally: specify the file path for the CSV file
# Write to CSV file. This used to be just: csvFile = open("products_cca.csv", "w")
output_dir = os.path.join(os.path.dirname(__file__), '../private/scraped_output')
os.makedirs(output_dir, exist_ok=True)
csv_file_path = os.path.join(output_dir, 'products_cca.csv')
csvFile = open(csv_file_path, "w", newline='', encoding='utf-8')

try:
    writer = csv.writer(csvFile)
    #Columns: Product_ID, Brand_name, Product_name, UPC_num, GF_certification_name, Country (scraped from)
    writer.writerow(("prod_ID", "prod_name", "brand_name", "cert_name", "cert_country"))
    
    #Fill in .csv file columns for first 1000 products
    #for product in products[1:]:
    for product in products[1:extraction_size+1]:

        brand_name = re.sub(r'\s+', ' ', product.find("td", {"class": "column-1"}).get_text().strip())
        prod_name = re.sub(r'\s+', ' ', product.find("td", {"class": "column-2"}).get_text().strip())
        prod_id = re.sub(r'\s+', ' ', product.find("td", {"class": "column-3"}).get_text().strip())

        cert_name = "Gluten-Free Certification Program (GFCP)"
        cert_country = "Canada"
        writer.writerow((prod_id, prod_name, brand_name, cert_name, cert_country))
finally:
   csvFile.close()

print(f"Extracted {extraction_size} products.")
print("Data has been successfully extracted and saved to 'products_cca.csv'.")

# https://docs.google.com/spreadsheets/d/1j_JKvgTM3fsF8HlhV-V65QB1wmlNOODdk4UbktUX_II/edit?gid=2106620438#gid=2106620438