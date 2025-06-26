import os

import fitz  # PyMuPDF
import json
import pandas as pd
from typing import Dict, List


"""
pip install PyMuPDF
docker compose exec back python scripts/scrape_chile.py
"""


class GlutenFreePDFScraper:
    """Single Responsibility: Extract gluten-free product data from PDF"""

    def __init__(self, pdf_path: str):
        self.pdf_path = pdf_path
        self.current_category = ""
        self.current_subcategory = ""

    def extract_products(self) -> List[Dict]:
        """Extract product information from PDF"""
        doc = fitz.open(self.pdf_path)
        products = []

        for page in doc:
            text = page.get_text("text")
            lines = text.split('\n')

            for line in lines:
                # Skip empty lines and headers
                if not line.strip() or "ALIMENTOS CERTIFICADOS" in line:
                    continue

                # Update category if line contains a new one
                if line.strip().isupper() and "PRODUCTO" not in line:
                    self.current_category = line.strip()
                    continue

                # Process product lines
                if "|" in line:
                    product = self._parse_product_line(line)
                    if product:
                        products.append(product)

        return products

    def _parse_product_line(self, line: str) -> Dict:
        """Parse a single product line into structured data"""
        try:
            parts = [part.strip() for part in line.split('|')]
            if len(parts) >= 3:
                subcategory = parts[0]
                product_name = parts[1]
                manufacturer = parts[2]

                # Update subcategory only if it's not empty
                if subcategory:
                    self.current_subcategory = subcategory

                return {
                    'BRAND_NAME': manufacturer.strip(),
                    'PRODUCT_NAME': product_name.strip(),
                    'DESCRIPTORS': self._extract_descriptors(product_name),
                    'certifications': 'CERTIFICADOS LIBRES DE GLUTEN',
                    'category': self.current_category,
                    'subcategories': self.current_subcategory,
                    'Manufacturer': manufacturer.strip(),
                    'Ingredients': '',  # Not available in PDF
                    'product_url': '',  # Not available in PDF
                    'image_url': ''     # Not available in PDF
                }
        except Exception as e:
            print(f"Error parsing line: {line}")
            print(f"Error: {str(e)}")
            return None

    def _extract_descriptors(self, product_name: str) -> List[str]:
        """Extract descriptors from product name"""
        descriptors = []
        if '(' in product_name and ')' in product_name:
            start = product_name.find('(')
            end = product_name.find(')')
            descriptor = product_name[start+1:end].strip()
            descriptors.append(descriptor)
        return descriptors


def main():
    # Initialize scraper
    current_dir = os.path.dirname(os.path.abspath(__file__))
    pdf_path = os.path.join(current_dir, "../private/data", "chile.pdf")

    scraper = GlutenFreePDFScraper(pdf_path)

    # Extract products
    products = scraper.extract_products()

    # Save as JSON
    with open('extracted_products.json', 'w', encoding='utf-8') as f:
        json.dump(products, f, ensure_ascii=False, indent=2)

    # Save as CSV
    df = pd.DataFrame(products)
    df.to_csv('extracted_products.csv', index=False, encoding='utf-8')

    print(f"Extracted {len(products)} products")


if __name__ == "__main__":
    main()
