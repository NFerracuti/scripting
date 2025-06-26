import csv
import os
from urllib.request import urlopen, Request
from bs4 import BeautifulSoup # type: ignore
import re
#import psycopg2



#List of URLs for each page as site contains multiple pages, (MAINTENANCE: As of Nov 2024: site is 2 pages (262 items) long, if it grows, add 3rd page)
urls = [
    "https://gf-finder.com/directory-gluten_free-products-eateries/?_page=1&num=100&settings_cache_id=51e59f08b50eb71d6146cb589ced8f86&sort=post_published",
    "https://gf-finder.com/directory-gluten_free-products-eateries/?_page=2&num=100&settings_cache_id=51e59f08b50eb71d6146cb589ced8f86&sort=post_published",
    #"https://gf-finder.com/directory-gluten_free-products-eateries/?_page=3&num=100&settings_cache_id=51e59f08b50eb71d6146cb589ced8f86&sort=post_published"
]

#adding a User-Agent header
headers = {'User-Agent': 'Mozilla/5.0'}

#Keep track of amount of brands extracted
num = 0

output_dir = os.path.join(os.path.dirname(__file__), '../private/scraped_output') #Define the output directory relative to the current script's location
os.makedirs(output_dir, exist_ok=True) #Ensure the output directory exists
csv_file_path = os.path.join(output_dir, 'products_gffp.csv') #specify the file path for the CSV file
csvFile = open(csv_file_path, "w", newline='', encoding='utf-8') # Write to CSV file. This used to be just: csvFile = open("products_cca.csv", "w")

try:
    writer = csv.writer(csvFile)

    #Define csv file columns
    #writer.writerow(("prod_ID", "prod_name", "brand_name", "cert_name", "cert_country"))

    writer.writerow(("prod_name", "brand_name", "brand_category", "brand_location", "cert_name", "cert_country"))
        
    # Loop through each URL in the list
    for url in urls:
        req = Request(url, headers=headers)
        html = urlopen(req)
        soup = BeautifulSoup(html, "html.parser")

        # grab each brand
        brands = soup.findAll("div", {"class":"drts-col-12 drts-view-entity-container"})

        # Loop through each brand on the current page
        for brand in brands:
            print(num)
            num += 1

            # Find the brand name element that contains both name and URL of brand
            bname_element = brand.find("div", {"class": "drts-display-element-column-1"})
            
            if bname_element:
                # Find the anchor tag within the title element
                anchor = bname_element.find("a")
                if anchor:
                    # Check each element before accessing .getText()
                    brand_name_element = brand.find("div", {"data-name": "entity_field_post_title"})
                    brand_category_element = brand.find("div", {"data-name": "entity_field_directory_category"})
                    brand_location_element = brand.find("div", {"data-name": "entity_field_location_address"})
                    
                    # Extract text or assign "N/A" if element is missing
                    brand_name = brand_name_element.getText().strip() if brand_name_element else "N/A"
                    brand_category = brand_category_element.getText().strip() if brand_category_element else "N/A"
                    brand_location = brand_location_element.getText().strip() if brand_location_element else "N/A"
                    brand_url = anchor.get('href') if anchor else "N/A"

                    #FOR EACH BRAND EXTRACT PRODUCT DATA
                    #brand_url = "https://gf-finder.com/gluten-free-products/aleias-gluten-free-foods/"
                    req = Request(brand_url, headers=headers)
                    html = urlopen(req)
                    soup = BeautifulSoup(html, "html.parser")

                    product_element = soup.find("div", {"data-name": "entity_field_post_content"})

                    if product_element:
                        products = product_element.find_all("li")
                        if products:
                            for product in products:
                                product_name = product.getText().strip()
                                #print (product_name)
                                writer.writerow((product_name, brand_name, brand_category, brand_location, "National Celiac Association (NCA)", "USA/CAD"))

                        else:
                            print (f"{brand_name} has no products")


                    # Test prints
                    print(f"Brand Name: {brand_name}")
                    #print(f"Brand Category: {brand_category}")
                    #print(f"Brand Location: {brand_location}")
                    #print(f"Brand URL: {brand_url}")
                   # print("---")
                
finally:
   
   csvFile.close()

print(f"Extracted {num} products.")
print("Brand Data has been successfully extracted and saved to 'products_gffp.csv'.")