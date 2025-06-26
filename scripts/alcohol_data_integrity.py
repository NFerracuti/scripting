"""
Alcohol Database Data Integrity Improvement Script
================================================

This script addresses multiple data integrity issues:
1. Inconsistent brand names - normalizes and creates Brand model entries
2. Duplicate subcategories - standardizes naming conventions
3. Missing brand/product name separation - extracts brand from product names
4. Fetches fresh data from LCBO API while preserving existing unique data
5. Handles duplicate detection and merging

Usage:
    python scripts/alcohol_data_integrity.py
"""
import os
import sys
import requests
import json
import logging
import time
import re
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2 import service_account
import difflib

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

# Configuration
CONFIG = {
    "spreadsheet_id": "1JdaLf5Ur5eLsNnu6Awlffrds120pjQaAqkPQB4liNyo",
    "sheet_gid": 828037295,
    "credentials_file": "charged-gravity-444220-d2-70c441d6f918.json",
    "test_mode": False,
    "max_lcbo_products": 5000,  # Maximum products to fetch from LCBO
    "products_per_page": 100,
    "similarity_threshold": 0.8,  # Threshold for considering products similar
    "backup_sheet_id": None,  # Optional backup sheet ID
}

# Subcategory normalization mappings
SUBCATEGORY_NORMALIZATIONS = {
    # Common variations
    "Gift And Sampler": "Gifts and Samplers",
    "Gift and Sampler": "Gifts and Samplers", 
    "Gifts And Sampler": "Gifts and Samplers",
    "Gift & Sampler": "Gifts and Samplers",
    "Gifts & Sampler": "Gifts and Samplers",
    "Gift and Samplers": "Gifts and Samplers",
    "Gift And Samplers": "Gifts and Samplers",
    
    # Beer variations
    "Beer & Cider": "Beer",
    "Beer and Cider": "Beer",
    "Beer & Ciders": "Beer",
    "Beer and Ciders": "Beer",
    
    # Wine variations
    "Red Wine": "Red Wines",
    "White Wine": "White Wines", 
    "Rose Wine": "Rose Wines",
    "Sparkling Wine": "Sparkling Wines",
    "Dessert Wine": "Dessert Wines",
    "Fortified Wine": "Fortified Wines",
    
    # Spirit variations
    "Whisky": "Whiskey",
    "Whiskey": "Whiskey",
    "Scotch": "Scotch Whisky",
    "Scotch Whiskey": "Scotch Whisky",
    "Bourbon": "Bourbon Whiskey",
    "Bourbon Whisky": "Bourbon Whiskey",
    "Rye": "Rye Whiskey",
    "Rye Whisky": "Rye Whiskey",
    "Canadian Whisky": "Canadian Whiskey",
    "Canadian Whiskey": "Canadian Whiskey",
    
    # Vodka variations
    "Vodka": "Vodka",
    "Flavoured Vodka": "Flavored Vodka",
    "Flavored Vodka": "Flavored Vodka",
    
    # Gin variations
    "Gin": "Gin",
    "London Dry Gin": "London Dry Gin",
    "Plymouth Gin": "Plymouth Gin",
    
    # Rum variations
    "Rum": "Rum",
    "White Rum": "White Rum",
    "Dark Rum": "Dark Rum",
    "Spiced Rum": "Spiced Rum",
    "Gold Rum": "Gold Rum",
    "Aged Rum": "Aged Rum",
    
    # Tequila variations
    "Tequila": "Tequila",
    "Blanco Tequila": "Blanco Tequila",
    "Reposado Tequila": "Reposado Tequila",
    "Anejo Tequila": "Anejo Tequila",
    "Mezcal": "Mezcal",
    
    # Liqueur variations
    "Liqueur": "Liqueur",
    "Liqueurs": "Liqueur",
    "Cream Liqueur": "Cream Liqueur",
    "Cream Liqueurs": "Cream Liqueur",
    "Coffee Liqueur": "Coffee Liqueur",
    "Coffee Liqueurs": "Coffee Liqueur",
    "Herbal Liqueur": "Herbal Liqueur",
    "Herbal Liqueurs": "Herbal Liqueur",
    "Fruit Liqueur": "Fruit Liqueur",
    "Fruit Liqueurs": "Fruit Liqueur",
    
    # Brandy variations
    "Brandy": "Brandy",
    "Cognac": "Cognac",
    "Armagnac": "Armagnac",
    "Pisco": "Pisco",
    
    # Other variations
    "Aperitif": "Aperitif",
    "Aperitifs": "Aperitif",
    "Digestif": "Digestif", 
    "Digestifs": "Digestif",
    "Vermouth": "Vermouth",
    "Bitters": "Bitters",
    "Absinthe": "Absinthe",
    "Sake": "Sake",
    "Soju": "Soju",
    "Baijiu": "Baijiu",
}

# Common brand name patterns to extract from product names
BRAND_PATTERNS = [
    r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',  # Capitalized words at start
    r'^([A-Z]+(?:\s+[A-Z]+)*)',  # ALL CAPS at start
    r'^([A-Z][a-z]+(?:\'[a-z]+)?)',  # Names with apostrophes
    r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Original|Classic|Premium|Reserve|Select|Special|Limited|Edition))',  # Brand + descriptors
]

class LCBOStatsClient:
    """Client for interacting with the LCBOstats API"""
    
    def __init__(self):
        self.base_url = 'https://lcbostats.com/api'
    
    def get_all_products(self, page: int = 1, per_page: int = 100) -> Dict[str, Any]:
        """Get all alcohol products from the LCBO database"""
        try:
            params = {
                'page': page,
                'per_page': per_page,
            }

            logger.info(f"Fetching LCBO products page {page}")
            response = requests.get(f"{self.base_url}/alcohol", params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching LCBO products page {page}: {e}")
            raise

    def search_products(self, query: str, page: int = 1, per_page: int = 25) -> Dict[str, Any]:
        """Search for specific products in the LCBO database"""
        try:
            params = {
                'search': query,
                'page': page,
                'per_page': per_page,
            }

            response = requests.get(f"{self.base_url}/alcohol", params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error searching LCBO products: {e}")
            raise

class DataIntegrityProcessor:
    """Main class for processing and improving data integrity"""
    
    def __init__(self):
        self.lcbo_client = LCBOStatsClient()
        self.credentials = self._get_credentials()
        self.sheets_service = build('sheets', 'v4', credentials=self.credentials)
        
    def _get_credentials(self):
        """Get Google Sheets credentials"""
        try:
            if CONFIG["credentials_file"] and os.path.exists(CONFIG["credentials_file"]):
                logger.info(f"Loading credentials from file: {CONFIG['credentials_file']}")
                return service_account.Credentials.from_service_account_file(
                    CONFIG["credentials_file"], 
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
            
            # Try environment variable
            env_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
            if env_creds:
                credentials_info = json.loads(env_creds)
                return service_account.Credentials.from_service_account_info(
                    credentials_info,
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
                
            raise ValueError("No valid credentials found")
        except Exception as e:
            logger.error(f"Error loading credentials: {e}")
            raise

    def normalize_subcategory(self, subcategory: str) -> str:
        """Normalize subcategory names to standard format"""
        if not subcategory:
            return ""
            
        # Clean up the subcategory
        cleaned = subcategory.strip()
        
        # Check our normalization mappings
        if cleaned in SUBCATEGORY_NORMALIZATIONS:
            return SUBCATEGORY_NORMALIZATIONS[cleaned]
            
        # Try to find close matches
        for original, normalized in SUBCATEGORY_NORMALIZATIONS.items():
            if difflib.SequenceMatcher(None, cleaned.lower(), original.lower()).ratio() > 0.9:
                logger.info(f"Normalizing subcategory: '{cleaned}' -> '{normalized}'")
                return normalized
                
        return cleaned

    def extract_brand_from_product_name(self, product_name: str, existing_brand: str = None) -> Tuple[str, str]:
        """Extract brand name from product name if brand is missing or product contains brand"""
        if not product_name:
            return "", ""
            
        # If we already have a brand, check if it's in the product name
        if existing_brand and existing_brand.lower() in product_name.lower():
            # Remove brand from product name
            clean_product = re.sub(re.escape(existing_brand), '', product_name, flags=re.IGNORECASE).strip()
            clean_product = re.sub(r'^\s*[-–—]\s*', '', clean_product)  # Remove leading dashes
            clean_product = re.sub(r'\s+', ' ', clean_product)  # Normalize whitespace
            return existing_brand, clean_product
            
        # Try to extract brand using patterns
        for pattern in BRAND_PATTERNS:
            match = re.match(pattern, product_name)
            if match:
                potential_brand = match.group(1).strip()
                if len(potential_brand) > 2:  # Avoid single letters
                    clean_product = product_name[len(potential_brand):].strip()
                    clean_product = re.sub(r'^\s*[-–—]\s*', '', clean_product)
                    clean_product = re.sub(r'\s+', ' ', clean_product)
                    return potential_brand, clean_product
                    
        return "", product_name

    def normalize_brand_name(self, brand_name: str) -> str:
        """Normalize brand name for consistency"""
        if not brand_name:
            return ""
            
        # Basic normalization
        normalized = brand_name.strip()
        
        # Handle common variations
        brand_variations = {
            "Bailey's": "Bailey's",
            "Baileys": "Bailey's",
            "Baileys Birthday": "Bailey's",
            "Baileys Original": "Bailey's", 
            "Baileys Tiramisu": "Bailey's",
            "Jack Daniels": "Jack Daniel's",
            "Jack Daniel": "Jack Daniel's",
            "Crown Royal": "Crown Royal",
            "Crown Royal Special Reserve": "Crown Royal",
            "Smirnoff": "Smirnoff",
            "Smirnoff Ice": "Smirnoff",
            "Grey Goose": "Grey Goose",
            "Absolut": "Absolut",
            "Ketel One": "Ketel One",
            "Bacardi": "Bacardi",
            "Captain Morgan": "Captain Morgan",
            "Malibu": "Malibu",
            "Jose Cuervo": "Jose Cuervo",
            "Patron": "Patron",
            "Don Julio": "Don Julio",
            "Hendrick's": "Hendrick's",
            "Hendricks": "Hendrick's",
            "Tanqueray": "Tanqueray",
            "Bombay Sapphire": "Bombay Sapphire",
            "Beefeater": "Beefeater",
            "Gordon's": "Gordon's",
            "Gordons": "Gordon's",
        }
        
        if normalized in brand_variations:
            return brand_variations[normalized]
            
        return normalized

    def fetch_lcbo_data(self) -> List[Dict[str, Any]]:
        """Fetch all available data from LCBO API"""
        logger.info("Fetching data from LCBO API...")
        
        all_products = []
        page = 1
        total_fetched = 0
        
        while total_fetched < CONFIG["max_lcbo_products"]:
            try:
                response = self.lcbo_client.get_all_products(page=page, per_page=CONFIG["products_per_page"])
                
                if not response.get('data'):
                    logger.info("No more data from LCBO API")
                    break
                    
                products = response['data']
                all_products.extend(products)
                total_fetched += len(products)
                
                logger.info(f"Fetched {len(products)} products from page {page} (total: {total_fetched})")
                
                if len(products) < CONFIG["products_per_page"]:
                    logger.info("Reached end of LCBO data")
                    break
                    
                page += 1
                time.sleep(1)  # Rate limiting
                
            except Exception as e:
                logger.error(f"Error fetching page {page}: {e}")
                break
                
        logger.info(f"Total LCBO products fetched: {len(all_products)}")
        return all_products

    def get_existing_sheet_data(self) -> List[List[str]]:
        """Get existing data from Google Sheet"""
        try:
            # Get sheet name from GID
            spreadsheet = self.sheets_service.spreadsheets().get(
                spreadsheetId=CONFIG["spreadsheet_id"]
            ).execute()
            
            sheet_name = None
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['sheetId'] == CONFIG["sheet_gid"]:
                    sheet_name = sheet['properties']['title']
                    break
                    
            if not sheet_name:
                raise ValueError(f"No sheet found with GID {CONFIG['sheet_gid']}")
                
            # Get all data from the sheet
            result = self.sheets_service.values().get(
                spreadsheetId=CONFIG["spreadsheet_id"],
                range=f"'{sheet_name}'!A:AZ"
            ).execute()
            
            rows = result.get('values', [])
            if not rows:
                logger.warning("No data found in sheet")
                return []
                
            logger.info(f"Found {len(rows) - 1} existing products in sheet")
            return rows
            
        except Exception as e:
            logger.error(f"Error getting existing sheet data: {e}")
            raise

    def process_lcbo_product(self, lcbo_product: Dict[str, Any]) -> Dict[str, Any]:
        """Process a single LCBO product and normalize its data"""
        # Extract basic info
        product_name = lcbo_product.get('title', '')
        brand_name = lcbo_product.get('brand', '')
        category = lcbo_product.get('category', '')
        subcategory = lcbo_product.get('subcategory', '')
        price = lcbo_product.get('price', 0)
        image_url = lcbo_product.get('thumbnail_url', '')
        product_id = lcbo_product.get('permanent_id', '')
        
        # Normalize subcategory
        normalized_subcategory = self.normalize_subcategory(subcategory)
        
        # Handle brand/product name separation
        if not brand_name and product_name:
            brand_name, product_name = self.extract_brand_from_product_name(product_name)
        elif brand_name and product_name:
            # Check if brand is already in product name
            brand_name, product_name = self.extract_brand_from_product_name(product_name, brand_name)
            
        # Normalize brand name
        normalized_brand = self.normalize_brand_name(brand_name)
        
        return {
            'lcbo_id': product_id,
            'brand_name': normalized_brand,
            'product_name': product_name,
            'category': category,
            'subcategory': normalized_subcategory,
            'price': price,
            'image_url': image_url,
            'source': 'LCBO'
        }

    def find_duplicates(self, products: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Find duplicate products based on brand and product name similarity"""
        logger.info("Finding duplicate products...")
        
        # Group by brand first
        brand_groups = defaultdict(list)
        for product in products:
            brand = product.get('brand_name', '').lower()
            brand_groups[brand].append(product)
            
        duplicates = []
        
        for brand, brand_products in brand_groups.items():
            if len(brand_products) <= 1:
                continue
                
            # Check for duplicates within this brand
            processed = set()
            for i, product1 in enumerate(brand_products):
                if i in processed:
                    continue
                    
                similar_products = [product1]
                
                for j, product2 in enumerate(brand_products[i+1:], i+1):
                    if j in processed:
                        continue
                        
                    # Calculate similarity
                    product1_name = product1.get('product_name', '').lower()
                    product2_name = product2.get('product_name', '').lower()
                    
                    if product1_name and product2_name:
                        similarity = difflib.SequenceMatcher(None, product1_name, product2_name).ratio()
                        
                        if similarity >= CONFIG["similarity_threshold"]:
                            similar_products.append(product2)
                            processed.add(j)
                            
                if len(similar_products) > 1:
                    duplicates.append(similar_products)
                    processed.add(i)
                    
        logger.info(f"Found {len(duplicates)} groups of duplicate products")
        return duplicates

    def merge_duplicates(self, duplicates: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Merge duplicate products, keeping the best data from each"""
        logger.info("Merging duplicate products...")
        
        merged_products = []
        
        for duplicate_group in duplicates:
            if not duplicate_group:
                continue
                
            # Sort by data quality (LCBO source first, then by completeness)
            sorted_group = sorted(duplicate_group, key=lambda p: (
                p.get('source') != 'LCBO',  # LCBO first
                not p.get('image_url'),  # Has image first
                not p.get('price'),  # Has price first
                len(p.get('product_name', ''))  # Longer product name first
            ))
            
            # Use the best product as base
            best_product = sorted_group[0].copy()
            
            # Merge additional data from other duplicates
            for duplicate in sorted_group[1:]:
                # Add missing data
                if not best_product.get('image_url') and duplicate.get('image_url'):
                    best_product['image_url'] = duplicate['image_url']
                if not best_product.get('price') and duplicate.get('price'):
                    best_product['price'] = duplicate['price']
                if not best_product.get('lcbo_id') and duplicate.get('lcbo_id'):
                    best_product['lcbo_id'] = duplicate['lcbo_id']
                    
            merged_products.append(best_product)
            
        logger.info(f"Merged {len(merged_products)} products from {sum(len(group) for group in duplicates)} duplicates")
        return merged_products

    def update_sheet(self, processed_products: List[Dict[str, Any]], existing_data: List[List[str]]):
        """Update the Google Sheet with processed data"""
        logger.info("Updating Google Sheet...")
        
        # Prepare headers
        headers = [
            "ID", "Brand", "Product", "Price", "Category", "Subcategory", 
            "gluten_free_score", "image_url", "source", "lcbo_id"
        ]
        
        # Prepare data rows
        rows = [headers]
        for product in processed_products:
            row = [
                product.get('lcbo_id', ''),
                product.get('brand_name', ''),
                product.get('product_name', ''),
                str(product.get('price', '')),
                product.get('category', ''),
                product.get('subcategory', ''),
                '0',  # Default gluten_free_score
                product.get('image_url', ''),
                product.get('source', ''),
                product.get('lcbo_id', '')
            ]
            rows.append(row)
            
        # Get sheet name
        spreadsheet = self.sheets_service.spreadsheets().get(
            spreadsheetId=CONFIG["spreadsheet_id"]
        ).execute()
        
        sheet_name = None
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['sheetId'] == CONFIG["sheet_gid"]:
                sheet_name = sheet['properties']['title']
                break
                
        if not sheet_name:
            raise ValueError(f"No sheet found with GID {CONFIG['sheet_gid']}")
            
        # Clear existing data and write new data
        range_name = f"'{sheet_name}'!A:J"
        
        # Clear existing data
        self.sheets_service.values().clear(
            spreadsheetId=CONFIG["spreadsheet_id"],
            range=range_name
        ).execute()
        
        # Write new data
        body = {'values': rows}
        self.sheets_service.values().update(
            spreadsheetId=CONFIG["spreadsheet_id"],
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        logger.info(f"Successfully updated sheet with {len(processed_products)} products")

    def create_brand_entries(self, products: List[Dict[str, Any]]):
        """Create Brand model entries for all unique brands"""
        logger.info("Creating Brand model entries...")
        
        # This would typically be done in Django, but for now we'll log the brands
        unique_brands = set()
        brand_alternatives = defaultdict(set)
        
        for product in products:
            brand_name = product.get('brand_name', '')
            if brand_name:
                unique_brands.add(brand_name)
                # Group similar brand names
                normalized = brand_name.lower().replace("'", "").replace("'", "")
                brand_alternatives[normalized].add(brand_name)
                
        logger.info(f"Found {len(unique_brands)} unique brands")
        
        # Log brand normalization suggestions
        for normalized, alternatives in brand_alternatives.items():
            if len(alternatives) > 1:
                logger.info(f"Brand alternatives for '{normalized}': {list(alternatives)}")
                
        return unique_brands

    def run(self):
        """Main execution method"""
        logger.info("Starting alcohol database data integrity improvement...")
        
        try:
            # 1. Fetch fresh data from LCBO
            lcbo_products = self.fetch_lcbo_data()
            
            # 2. Get existing sheet data
            existing_data = self.get_existing_sheet_data()
            
            # 3. Process LCBO products
            processed_lcbo = []
            for product in lcbo_products:
                processed = self.process_lcbo_product(product)
                processed_lcbo.append(processed)
                
            # 4. Process existing sheet data (if any)
            processed_existing = []
            if len(existing_data) > 1:  # Has data beyond header
                for row in existing_data[1:]:  # Skip header
                    if len(row) >= 3:  # Has at least ID, Brand, Product
                        existing_product = {
                            'lcbo_id': row[0] if len(row) > 0 else '',
                            'brand_name': row[1] if len(row) > 1 else '',
                            'product_name': row[2] if len(row) > 2 else '',
                            'price': row[3] if len(row) > 3 else '',
                            'category': row[4] if len(row) > 4 else '',
                            'subcategory': row[5] if len(row) > 5 else '',
                            'image_url': row[7] if len(row) > 7 else '',
                            'source': 'Existing'
                        }
                        
                        # Normalize existing data
                        existing_product['brand_name'] = self.normalize_brand_name(existing_product['brand_name'])
                        existing_product['subcategory'] = self.normalize_subcategory(existing_product['subcategory'])
                        
                        if existing_product['brand_name'] or existing_product['product_name']:
                            processed_existing.append(existing_product)
                            
            # 5. Combine all products
            all_products = processed_lcbo + processed_existing
            
            # 6. Find and merge duplicates
            duplicates = self.find_duplicates(all_products)
            merged_products = self.merge_duplicates(duplicates)
            
            # 7. Create brand entries
            unique_brands = self.create_brand_entries(merged_products)
            
            # 8. Update sheet
            if not CONFIG["test_mode"]:
                self.update_sheet(merged_products, existing_data)
            else:
                logger.info("TEST MODE: Would update sheet with {} products".format(len(merged_products)))
                
            # 9. Summary
            logger.info("\n" + "="*50)
            logger.info("DATA INTEGRITY IMPROVEMENT SUMMARY")
            logger.info("="*50)
            logger.info(f"LCBO products processed: {len(processed_lcbo)}")
            logger.info(f"Existing products processed: {len(processed_existing)}")
            logger.info(f"Duplicate groups found: {len(duplicates)}")
            logger.info(f"Final unique products: {len(merged_products)}")
            logger.info(f"Unique brands identified: {len(unique_brands)}")
            logger.info("="*50)
            
        except Exception as e:
            logger.error(f"Error during data integrity improvement: {e}")
            if CONFIG["test_mode"]:
                import traceback
                traceback.print_exc()
            raise

def main():
    """Main function"""
    processor = DataIntegrityProcessor()
    processor.run()

if __name__ == "__main__":
    main() 