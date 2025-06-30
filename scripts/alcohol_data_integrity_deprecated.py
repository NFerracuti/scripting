"""
Alcohol Database Data Integrity Improvement Script
================================================

This script addresses multiple data integrity issues:
1. Inconsistent brand names - normalizes brand names for consistency
2. Duplicate subcategories - standardizes naming conventions
3. Missing brand/product name separation - extracts brand from product names
4. Processes existing data from Google Sheets
5. Handles duplicate detection and merging
6. Identifies and logs unique brands for future database integration

Usage:
    python scripts/alcohol_data_integrity.py
"""
import os
import sys
import json
import logging
import re
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2 import service_account
import difflib
import openai
import uuid

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
    "backup_sheet_gid": 933968267,  # Backup tab GID
    "credentials_file": "charged-gravity-444220-d2-70c441d6f918.json",
    "test_mode": False,  # Changed to False to actually update the sheet
    "test_product_extraction": False,  # Test mode for product extraction (shows what would be changed)
    "similarity_threshold": 0.9,  # Made more conservative (was 0.8)
    "backup_sheet_id": None,  # Optional backup sheet ID
    "openai_api_key": os.getenv('OPENAI_KEY_NICK'),  # Updated to match your env variable
    
    # Individual operation flags
    "run_ai_brand_extraction": False,  # Extract missing brand names using AI
    "run_product_name_filling": False,  # Fill missing product names
    "run_duplicate_detection": False,  # Find and merge duplicates
    "run_brand_normalization": False,  # Normalize brand names and consolidate alternatives
    "run_sheet_update": True,  # Update the Google Sheet with results
    "run_backup_restoration": True,  # Restore gluten free scores and descriptors from backup
    
    # AI settings
    "use_ai_brand_extraction": False,  # Set to False for testing without API calls
    "use_ai_product_extraction": False,  # Enable AI product name extraction
    "batch_size": 50,  # Process products in batches to avoid rate limits
    "max_ai_products": 500,  # Maximum number of products to process with AI (cost control)
    
    # Fuzzy matching settings
    "fuzzy_match_threshold": 0.85,  # Minimum similarity score for fuzzy matching (lowered from 0.95)
    "fuzzy_match_fields": ["brand_name", "product_name"],  # Fields to use for matching
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

class DataIntegrityProcessor:
    """Main class for processing and improving data integrity"""
    
    def __init__(self):
        self.credentials = self._get_credentials()
        self.sheets_service = build('sheets', 'v4', credentials=self.credentials).spreadsheets()
        
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
        """Extract brand name from product name using AI"""
        if not CONFIG["use_ai_brand_extraction"] or not CONFIG["openai_api_key"]:
            return existing_brand or "", product_name
            
        # Skip if product name is too short, already has a brand, or is too long
        if (len(product_name) < 5 or 
            existing_brand or 
            len(product_name) > 100):  # Skip very long product names to save tokens
            return existing_brand or "", product_name
            
        # Skip if product name looks like it doesn't contain a brand (e.g., generic descriptions)
        generic_indicators = [
            'red wine', 'white wine', 'rose wine', 'sparkling wine', 'beer', 'lager', 'ale',
            'whiskey', 'whisky', 'vodka', 'gin', 'rum', 'tequila', 'brandy', 'liqueur',
            'organic', 'natural', 'premium', 'reserve', 'select', 'classic', 'original'
        ]
        
        product_lower = product_name.lower()
        if any(indicator in product_lower for indicator in generic_indicators):
            # Check if it starts with a generic term (likely no brand)
            words = product_lower.split()
            if words and words[0] in generic_indicators:
                return existing_brand or "", product_name
            
        try:
            # Set up OpenAI client
            openai.api_key = CONFIG["openai_api_key"]
            
            # Optimized prompt - much shorter to save tokens
            prompt = f"""Extract brand from: "{product_name}"
Return JSON: {{"brand": "BrandName", "product": "RemainingProduct"}}
Examples:
"Campbell Kind Wine Tawse Riesling 2019" → {{"brand": "Campbell Kind Wine", "product": "Tawse Riesling 2019"}}
"La Bélière Red Organic Wine 2019" → {{"brand": "La Bélière", "product": "Red Organic Wine 2019"}}
"Red Wine 2019" → {{"brand": "", "product": "Red Wine 2019"}}"""

            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Extract brand names. Return only JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=50  # Reduced from 100 to save tokens
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON response
            try:
                result = json.loads(result_text)
                extracted_brand = result.get("brand", "").strip()
                cleaned_product = result.get("product", "").strip()
                
                # Validate the extraction
                if extracted_brand and len(extracted_brand) > 1:
                    # Make sure the cleaned product is not empty
                    if not cleaned_product:
                        cleaned_product = product_name
                    
                    return extracted_brand, cleaned_product
                else:
                    return existing_brand or "", product_name
                    
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse AI response for '{product_name}': {result_text}")
                return existing_brand or "", product_name
                
        except Exception as e:
            logger.warning(f"AI brand extraction failed for '{product_name}': {e}")
            return existing_brand or "", product_name

    def estimate_ai_costs(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Estimate the cost of AI processing"""
        if not CONFIG["use_ai_brand_extraction"] or not CONFIG["openai_api_key"]:
            return {"total_cost": 0, "products_to_process": 0, "estimated_tokens": 0}
            
        # Filter products that need extraction
        products_needing_extraction = []
        for product in products:
            product_name = product.get('product_name', '')
            existing_brand = product.get('brand_name', '')
            
            if (product_name and 
                not existing_brand and 
                5 <= len(product_name) <= 100):
                
                words = product_name.lower().split()
                generic_starters = [
                    'red', 'white', 'rose', 'sparkling', 'beer', 'lager', 'ale',
                    'whiskey', 'whisky', 'vodka', 'gin', 'rum', 'tequila', 'brandy', 'liqueur',
                    'organic', 'natural', 'premium', 'reserve', 'select', 'classic', 'original'
                ]
                
                if not words or words[0] not in generic_starters:
                    products_needing_extraction.append(product)
        
        # Limit to max products
        max_products = CONFIG["max_ai_products"]
        if len(products_needing_extraction) > max_products:
            products_needing_extraction = products_needing_extraction[:max_products]
        
        # Estimate tokens (rough calculation)
        # Each request: ~100 tokens input + ~50 tokens output = ~150 tokens per product
        estimated_tokens = len(products_needing_extraction) * 150
        
        # Cost calculation (GPT-3.5-turbo: $0.002 per 1K tokens)
        cost_per_1k_tokens = 0.002
        total_cost = (estimated_tokens / 1000) * cost_per_1k_tokens
        
        return {
            "total_cost": round(total_cost, 4),
            "products_to_process": len(products_needing_extraction),
            "estimated_tokens": estimated_tokens,
            "cost_per_product": round(total_cost / len(products_needing_extraction), 4) if products_needing_extraction else 0
        }

    def extract_brand_from_product_name_batch(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract brand names from product names in batches"""
        if not CONFIG["use_ai_brand_extraction"] or not CONFIG["openai_api_key"]:
            return products
            
        # Filter products that actually need brand extraction
        products_needing_extraction = []
        for product in products:
            product_name = product.get('product_name', '')
            existing_brand = product.get('brand_name', '')
            
            # Only process if:
            # 1. Has a product name
            # 2. No existing brand
            # 3. Product name is not too short or too long
            # 4. Product name doesn't start with generic terms
            if (product_name and 
                not existing_brand and 
                5 <= len(product_name) <= 100):
                
                # Check if it starts with generic terms
                words = product_name.lower().split()
                generic_starters = [
                    'red', 'white', 'rose', 'sparkling', 'beer', 'lager', 'ale',
                    'whiskey', 'whisky', 'vodka', 'gin', 'rum', 'tequila', 'brandy', 'liqueur',
                    'organic', 'natural', 'premium', 'reserve', 'select', 'classic', 'original'
                ]
                
                if not words or words[0] not in generic_starters:
                    products_needing_extraction.append(product)
        
        if not products_needing_extraction:
            logger.info("No products need brand extraction")
            return products
            
        # Limit the number of products to process with AI
        max_products = CONFIG["max_ai_products"]
        if len(products_needing_extraction) > max_products:
            logger.info(f"Limiting AI processing to {max_products} products (out of {len(products_needing_extraction)} that need extraction)")
            products_needing_extraction = products_needing_extraction[:max_products]
            
        logger.info(f"Processing {len(products_needing_extraction)} products with AI brand extraction (out of {len(products)} total)")
        
        # Process in batches
        batch_size = CONFIG["batch_size"]
        processed_products = []
        extraction_count = 0
        
        for i in range(0, len(products_needing_extraction), batch_size):
            batch = products_needing_extraction[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(products_needing_extraction) + batch_size - 1)//batch_size}")
            
            for product in batch:
                product_name = product.get('product_name', '')
                existing_brand = product.get('brand_name', '')
                
                extracted_brand, cleaned_product = self.extract_brand_from_product_name(product_name, existing_brand)
                
                if extracted_brand and extracted_brand != existing_brand:
                    product['brand_name'] = extracted_brand
                    product['product_name'] = cleaned_product
                    extraction_count += 1
                    logger.info(f"Extracted brand '{extracted_brand}' from '{product_name}'")
                
                processed_products.append(product)
                
            # Small delay between batches to avoid rate limits
            import time
            time.sleep(1)
        
        logger.info(f"Successfully extracted brands for {extraction_count} products")
        
        # Return all products (both processed and unprocessed)
        processed_ids = {f"{p.get('brand_name', '')}_{p.get('product_name', '')}_{p.get('lcbo_id', '')}" 
                        for p in processed_products}
        
        final_products = []
        for product in products:
            product_id = f"{product.get('brand_name', '')}_{product.get('product_name', '')}_{product.get('lcbo_id', '')}"
            if product_id in processed_ids:
                # Find the processed version
                for processed in processed_products:
                    processed_id = f"{processed.get('brand_name', '')}_{processed.get('product_name', '')}_{processed.get('lcbo_id', '')}"
                    if processed_id == product_id:
                        final_products.append(processed)
                        break
            else:
                final_products.append(product)
                
        return final_products

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

    def get_existing_sheet_data(self) -> List[List[str]]:
        """Get existing data from Google Sheet"""
        try:
            # Get sheet name from GID
            spreadsheet = self.sheets_service.get(
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

    def find_duplicates(self, products: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Find duplicate products based on brand and product name similarity"""
        logger.info("Finding duplicate products...")
        
        # Group by brand first
        brand_groups = defaultdict(list)
        for product in products:
            brand = product.get('brand_name', '').lower()
            brand_groups[brand].append(product)
            
        duplicates = []
        total_products_checked = 0
        
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
                        total_products_checked += 1
                        
                        if similarity >= CONFIG["similarity_threshold"]:
                            logger.info(f"Duplicate found (similarity {similarity:.3f}):")
                            logger.info(f"  Product 1: {product1.get('brand_name', '')} - {product1_name}")
                            logger.info(f"  Product 2: {product2.get('brand_name', '')} - {product2_name}")
                            similar_products.append(product2)
                            processed.add(j)
                            
                if len(similar_products) > 1:
                    duplicates.append(similar_products)
                    processed.add(i)
                    
        logger.info(f"Found {len(duplicates)} groups of duplicate products")
        logger.info(f"Total product comparisons made: {total_products_checked}")
        return duplicates

    def merge_duplicates(self, duplicates: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Merge duplicate products, keeping the best data from each"""
        logger.info("Merging duplicate products...")
        
        merged_products = []
        
        for duplicate_group in duplicates:
            if not duplicate_group:
                continue
                
            # Sort by data quality (completeness first)
            sorted_group = sorted(duplicate_group, key=lambda p: (
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

    def create_backup(self, existing_data: List[List[str]]):
        """Create a backup of the current sheet data"""
        if not CONFIG["backup_sheet_id"]:
            logger.info("No backup sheet configured, skipping backup")
            return
            
        logger.info("Creating backup of current data...")
        
        try:
            # Prepare backup data
            backup_rows = []
            for row in existing_data:
                backup_rows.append(row)
            
            # Write to backup sheet
            backup_range = "Sheet1!A:J"  # Adjust as needed
            body = {'values': backup_rows}
            
            self.sheets_service.values().update(
                spreadsheetId=CONFIG["backup_sheet_id"],
                range=backup_range,
                valueInputOption='RAW',
                body=body
            ).execute()
            
            logger.info(f"Backup created successfully in sheet {CONFIG['backup_sheet_id']}")
            
        except Exception as e:
            logger.warning(f"Failed to create backup: {e}")

    def update_sheet(self, processed_products: List[Dict[str, Any]], existing_data: List[List[str]]):
        """Update the Google Sheet with processed data"""
        logger.info("Updating Google Sheet...")
        
        # Get the original headers to preserve the sheet structure
        original_headers = existing_data[0] if existing_data else []
        logger.info(f"Original headers: {original_headers}")
        
        # Find the gluten free score column index
        gluten_score_col = None
        for i, header in enumerate(original_headers):
            if 'gluten' in header.lower() and 'score' in header.lower():
                gluten_score_col = i
                break
        
        # Prepare data rows using the original structure
        rows = [original_headers]  # Keep original headers
        for i, product in enumerate(processed_products):
            # Start with the original row structure
            if i + 1 < len(existing_data):
                row = existing_data[i + 1].copy()  # Copy original row
            else:
                # If no original row, create a new one with the right number of columns
                row = [''] * len(original_headers)
            
            # Update the fields we want to preserve/restore
            if len(row) > 0:
                row[0] = str(uuid.uuid4())  # id (UUID)
            
            # Find and update brand_name column
            brand_col = None
            for j, header in enumerate(original_headers):
                if 'brand' in header.lower():
                    brand_col = j
                    break
            if brand_col is not None and brand_col < len(row):
                row[brand_col] = product.get('brand_name', '')
            
            # Find and update product_name column
            product_col = None
            for j, header in enumerate(original_headers):
                if 'product' in header.lower():
                    product_col = j
                    break
            if product_col is not None and product_col < len(row):
                row[product_col] = product.get('product_name', '')
            
            # Find and update descriptors column
            desc_col = None
            for j, header in enumerate(original_headers):
                if 'descriptor' in header.lower():
                    desc_col = j
                    break
            if desc_col is not None and desc_col < len(row):
                row[desc_col] = product.get('descriptors', '')
            
            # Update gluten free score if we found the column
            if gluten_score_col is not None and gluten_score_col < len(row):
                row[gluten_score_col] = product.get('gluten_free_score', '')
                logger.info(f"Updated gluten score for row {i+1}: {product.get('gluten_free_score', '')}")
            
            rows.append(row)
            
        # Get sheet name
        spreadsheet = self.sheets_service.get(
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
        range_name = f"'{sheet_name}'!A:{chr(65 + len(original_headers) - 1)}"  # Dynamic range based on headers
        
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
        if gluten_score_col is not None:
            logger.info(f"Gluten free score column found at position {gluten_score_col}")
        else:
            logger.warning("Gluten free score column not found in headers")

    def create_descriptors(self, product: Dict[str, Any]) -> str:
        """Create descriptors field from various product attributes"""
        descriptors = []
        
        # Add image URL if available
        if product.get('image_url'):
            descriptors.append(f"Image: {product.get('image_url')}")
        
        # Add LCBO ID if available
        if product.get('lcbo_id'):
            descriptors.append(f"LCBO ID: {product.get('lcbo_id')}")
        
        # Add source if available
        if product.get('source'):
            descriptors.append(f"Source: {product.get('source')}")
        
        # Add gluten free score if available
        if product.get('gluten_free_score'):
            descriptors.append(f"Gluten Free Score: {product.get('gluten_free_score')}")
        
        return "; ".join(descriptors)

    def normalize_brand_alternatives(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize brand names and consolidate brand alternatives"""
        logger.info("Normalizing brand names and consolidating alternatives...")
        
        # Group brands by normalized name
        brand_groups = defaultdict(list)
        for product in products:
            brand_name = product.get('brand_name', '')
            if brand_name:
                # Normalize brand name for grouping (lowercase, remove apostrophes)
                normalized = brand_name.lower().replace("'", "").replace("'", "").replace('"', '').replace('"', '')
                brand_groups[normalized].append(brand_name)
        
        # Find brand alternatives and choose the best version
        brand_mapping = {}
        consolidated_count = 0
        
        for normalized, alternatives in brand_groups.items():
            if len(alternatives) > 1:
                # Choose the best version (prefer versions with proper apostrophes, proper capitalization)
                best_brand = self.choose_best_brand_name(alternatives)
                
                # Map all alternatives to the best version
                for alternative in alternatives:
                    if alternative != best_brand:
                        brand_mapping[alternative] = best_brand
                        consolidated_count += 1
                        
                logger.info(f"Consolidating brand alternatives for '{normalized}':")
                logger.info(f"  {alternatives} -> '{best_brand}'")
        
        # Apply the brand mapping to all products
        updated_products = []
        for product in products:
            current_brand = product.get('brand_name', '')
            if current_brand in brand_mapping:
                product['brand_name'] = brand_mapping[current_brand]
            updated_products.append(product)
        
        logger.info(f"Consolidated {consolidated_count} brand name variations")
        return updated_products

    def choose_best_brand_name(self, alternatives: List[str]) -> str:
        """Choose the best version of a brand name from alternatives"""
        if not alternatives:
            return ""
        
        # Scoring criteria (higher score = better)
        def score_brand(brand):
            score = 0
            
            # Prefer versions with proper apostrophes
            if "'" in brand:
                score += 10
            
            # Prefer proper capitalization (not all caps)
            if brand.isupper():
                score -= 5
            elif brand[0].isupper() and not brand.isupper():
                score += 5
            
            # Prefer shorter names (less likely to have extra words)
            score -= len(brand.split())
            
            # Prefer names without extra spaces
            score -= brand.count('  ')
            
            return score
        
        # Score all alternatives and return the best
        scored_alternatives = [(brand, score_brand(brand)) for brand in alternatives]
        best_brand = max(scored_alternatives, key=lambda x: x[1])[0]
        
        return best_brand

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

    def extract_product_from_brand_name(self, brand_name: str) -> Tuple[str, str]:
        """Extract product type from brand name if it contains a product type"""
        if not brand_name:
            return "", ""
            
        # Common product types to look for in brand names
        product_types = [
            # Beer types
            'lager', 'pilsner', 'stout', 'porter', 'ale', 'ipa', 'wheat beer', 'hefeweizen',
            'pale ale', 'amber ale', 'brown ale', 'blonde ale', 'cream ale', 'kolsch',
            # Wine types
            'red wine', 'white wine', 'rose wine', 'sparkling wine', 'champagne', 'prosecco',
            'chardonnay', 'cabernet', 'merlot', 'pinot noir', 'sauvignon blanc', 'riesling',
            # Spirit types
            'vodka', 'gin', 'whiskey', 'whisky', 'bourbon', 'scotch', 'rum', 'tequila',
            'brandy', 'cognac', 'liqueur', 'absinthe', 'vermouth', 'bitters',
            # Other
            'cider', 'mead', 'sake', 'soju'
        ]
        
        brand_lower = brand_name.lower()
        
        # Look for product types in the brand name
        for product_type in product_types:
            if product_type in brand_lower:
                # Find the position of the product type
                start_pos = brand_lower.find(product_type)
                end_pos = start_pos + len(product_type)
                
                # Extract brand name (before the product type)
                brand_part = brand_name[:start_pos].strip()
                if brand_part.endswith((' ', '-', '–', '—')):
                    brand_part = brand_part[:-1].strip()
                
                # Extract product name (the product type)
                product_part = brand_name[start_pos:end_pos].strip()
                
                # Clean up product name (capitalize properly)
                if product_part:
                    product_part = product_part.title()
                    # Handle special cases
                    if product_part.lower() in ['ipa', 'cabernet', 'merlot', 'pinot noir', 'sauvignon blanc']:
                        product_part = product_part.upper()
                
                return brand_part, product_part
                
        return brand_name, ""

    def extract_product_from_subcategory(self, subcategory: str) -> str:
        """Extract product name from subcategory field"""
        if not subcategory:
            return ""
            
        # Clean up subcategory
        cleaned = subcategory.strip()
        
        # Remove trailing commas and clean up
        cleaned = cleaned.rstrip(',').strip()
        
        # If it's a simple product type, return it
        if cleaned and len(cleaned) > 1:
            # Capitalize properly
            if cleaned.lower() in ['ipa', 'cabernet', 'merlot', 'pinot noir', 'sauvignon blanc']:
                return cleaned.upper()
            else:
                return cleaned.title()
                
        return ""

    def determine_product_type_with_ai(self, brand_name: str, subcategory: str) -> str:
        """Use AI to determine the product type based on brand name and subcategory"""
        if not CONFIG["openai_api_key"]:
            logger.warning("OpenAI API key not found for AI product extraction")
            return ""
            
        try:
            # Set up OpenAI client
            openai.api_key = CONFIG["openai_api_key"]
            
            # Create a much more strict prompt
            prompt = f"""Given the brand name "{brand_name}" and subcategory "{subcategory}", determine the specific product type.

IMPORTANT: Return ONLY the product type name, nothing else. No explanations, no reasoning, no additional text.

For spirits and liqueurs, be specific about the type:
- Jagermeister → Herbal Liqueur
- Cointreau → Orange Liqueur 
- Grand Marnier → Orange Liqueur
- Baileys → Irish Cream Liqueur
- Kahlua → Coffee Liqueur
- Amaretto → Almond Liqueur
- Frangelico → Hazelnut Liqueur
- Chambord → Raspberry Liqueur
- Midori → Melon Liqueur
- Malibu → Coconut Rum
- Captain Morgan → Spiced Rum
- Bacardi → White Rum
- Grey Goose → Vodka
- Ketel One → Vodka
- Absolut → Vodka
- Smirnoff → Vodka
- Jack Daniels → Tennessee Whiskey
- Jim Beam → Bourbon Whiskey
- Makers Mark → Bourbon Whiskey
- Wild Turkey → Bourbon Whiskey
- Crown Royal → Canadian Whisky
- Canadian Club → Canadian Whisky
- Seagrams → Canadian Whisky
- Dewars → Scotch Whisky
- Johnnie Walker → Scotch Whisky
- Macallan → Single Malt Scotch
- Glenfiddich → Single Malt Scotch
- Glenlivet → Single Malt Scotch
- Lagavulin → Single Malt Scotch
- Ardbeg → Single Malt Scotch
- Laphroaig → Single Malt Scotch
- Talisker → Single Malt Scotch
- Highland Park → Single Malt Scotch
- Balvenie → Single Malt Scotch
- Dalmore → Single Malt Scotch

For other alcohol types:
- Wine brands → Wine (or specific type like Red Wine, White Wine, Rosé Wine)
- Beer brands → Beer (or specific type like Lager, Ale, Stout)
- Cider brands → Cider

If you cannot determine a specific type, return exactly: "Unknown"

Remember: Return ONLY the product type name, nothing else."""

            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a data processor. Return ONLY the requested product type name. No explanations, no reasoning, no additional text."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=20,
                temperature=0.0
            )
            
            product_type = response.choices[0].message.content.strip()
            
            # Clean up the response - be very strict
            if product_type and product_type.lower() not in ['unknown', 'other', 'misc', 'miscellaneous']:
                # Remove any explanatory text that might have been included
                lines = product_type.split('\n')
                first_line = lines[0].strip()
                
                # If it looks like an explanation, return empty
                if len(first_line) > 50 or 'based on' in first_line.lower() or 'therefore' in first_line.lower():
                    return ""
                    
                return first_line
            else:
                return ""
                
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}")
            return ""

    def is_valid_product_name(self, product_name: str) -> bool:
        """Check if a product name is valid (not a generic placeholder)"""
        if not product_name:
            return False
            
        # List of invalid/generic product names
        invalid_names = {
            'other', 'unknown', 'misc', 'miscellaneous', 'general', 'various',
            'assorted', 'mixed', 'selection', 'collection', 'variety'
        }
        
        # Check if the product name is in the invalid list
        if product_name.lower().strip() in invalid_names:
            return False
            
        # Check if it's too short (likely not descriptive)
        if len(product_name.strip()) < 3:
            return False
            
        return True

    def should_use_ai_for_product(self, brand_name: str, subcategory: str) -> bool:
        """Determine if we should use AI for product extraction based on brand and subcategory"""
        if not brand_name:
            return False
            
        # Use AI for spirits with generic subcategories
        spirit_brands = {
            'jagermeister', 'cointreau', 'grand marnier', 'baileys', 'kahlua',
            'amaretto', 'frangelico', 'chambord', 'midori', 'malibu',
            'captain morgan', 'bacardi', 'grey goose', 'ketel one', 'absolut',
            'smirnoff', 'jack daniels', 'jim beam', 'makers mark', 'wild turkey',
            'crown royal', 'canadian club', 'seagrams', 'dewars', 'johnnie walker',
            'macallan', 'glenfiddich', 'glenlivet', 'lagavulin', 'ardbeg',
            'laphroaig', 'talisker', 'highland park', 'balvenie', 'dalmore'
        }
        
        # Check if brand is a known spirit brand
        brand_lower = brand_name.lower().strip()
        if brand_lower in spirit_brands:
            return True
            
        # Check if subcategory is generic
        generic_subcategories = {
            'other', 'unknown', 'misc', 'miscellaneous', 'general', 'various',
            'assorted', 'mixed', 'selection', 'collection', 'variety'
        }
        
        if subcategory.lower().strip() in generic_subcategories:
            return True
            
        return False

    def fill_missing_product_names(self, products: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        """Fill in missing product names using logical rules and AI"""
        logger.info("Filling missing product names...")
        
        filled_count = 0
        updated_products = []
        test_output = []  # For detailed test output
        
        for product in products:
            product_name = product.get('product_name', '')
            brand_name = product.get('brand_name', '')
            subcategory = product.get('subcategory', '')
            
            # Skip if product name already exists and is valid
            if product_name and self.is_valid_product_name(product_name):
                updated_products.append(product)
                continue
                
            # Check if we should use AI directly for this product
            if self.should_use_ai_for_product(brand_name, subcategory):
                if CONFIG["use_ai_product_extraction"] and CONFIG["openai_api_key"]:
                    ai_product = self.determine_product_type_with_ai(brand_name, subcategory)
                    if ai_product and self.is_valid_product_name(ai_product):
                        if CONFIG["test_product_extraction"]:
                            test_output.append({
                                'type': 'ai_extraction_direct',
                                'original_brand': brand_name,
                                'new_brand': brand_name,
                                'new_product': ai_product,
                                'method': f'AI direct (spirit/generic subcategory)'
                            })
                        
                        product['product_name'] = ai_product
                        filled_count += 1
                        logger.info(f"AI determined product '{ai_product}' for spirit brand '{brand_name}'")
                        updated_products.append(product)
                        continue
                else:
                    logger.warning(f"AI product extraction disabled but needed for spirit brand '{brand_name}'")
                    updated_products.append(product)
                    continue
                
            # Step 1: Check if brand name contains a product type
            if brand_name:
                extracted_brand, extracted_product = self.extract_product_from_brand_name(brand_name)
                if extracted_product and self.is_valid_product_name(extracted_product):
                    if CONFIG["test_product_extraction"]:
                        test_output.append({
                            'type': 'brand_extraction',
                            'original_brand': brand_name,
                            'new_brand': extracted_brand,
                            'new_product': extracted_product,
                            'method': 'Extracted from brand name'
                        })
                    
                    product['brand_name'] = extracted_brand
                    product['product_name'] = extracted_product
                    filled_count += 1
                    logger.info(f"Extracted product '{extracted_product}' from brand '{brand_name}'")
                    updated_products.append(product)
                    continue
            
            # Step 2: Extract from subcategory
            if subcategory:
                subcategory_product = self.extract_product_from_subcategory(subcategory)
                if subcategory_product and self.is_valid_product_name(subcategory_product):
                    if CONFIG["test_product_extraction"]:
                        test_output.append({
                            'type': 'subcategory_extraction',
                            'original_brand': brand_name,
                            'new_brand': brand_name,
                            'new_product': subcategory_product,
                            'method': f'Extracted from subcategory "{subcategory}"'
                        })
                    
                    product['product_name'] = subcategory_product
                    filled_count += 1
                    logger.info(f"Extracted product '{subcategory_product}' from subcategory '{subcategory}'")
                    updated_products.append(product)
                    continue
            
            # Step 3: Use AI to determine product type
            if brand_name and CONFIG["use_ai_product_extraction"] and CONFIG["openai_api_key"]:
                ai_product = self.determine_product_type_with_ai(brand_name, subcategory)
                if ai_product and self.is_valid_product_name(ai_product):
                    if CONFIG["test_product_extraction"]:
                        test_output.append({
                            'type': 'ai_extraction',
                            'original_brand': brand_name,
                            'new_brand': brand_name,
                            'new_product': ai_product,
                            'method': f'AI determined based on brand "{brand_name}" and subcategory "{subcategory}"'
                        })
                    
                    product['product_name'] = ai_product
                    filled_count += 1
                    logger.info(f"AI determined product '{ai_product}' for brand '{brand_name}'")
                    updated_products.append(product)
                    continue
            
            # No valid product name could be determined
            if CONFIG["test_product_extraction"] and brand_name:
                test_output.append({
                    'type': 'no_extraction',
                    'original_brand': brand_name,
                    'new_brand': brand_name,
                    'new_product': '',
                    'method': 'Could not determine valid product type'
                })
            
            updated_products.append(product)
        
        # Show detailed test output if in test mode
        if CONFIG["test_product_extraction"] and test_output:
            logger.info("\n" + "="*60)
            logger.info("PRODUCT EXTRACTION TEST RESULTS")
            logger.info("="*60)
            
            # Group by extraction type
            brand_extractions = [item for item in test_output if item['type'] == 'brand_extraction']
            subcategory_extractions = [item for item in test_output if item['type'] == 'subcategory_extraction']
            ai_extractions = [item for item in test_output if item['type'] == 'ai_extraction']
            ai_direct_extractions = [item for item in test_output if item['type'] == 'ai_extraction_direct']
            no_extractions = [item for item in test_output if item['type'] == 'no_extraction']
            
            if brand_extractions:
                logger.info(f"\nBRAND EXTRACTION ({len(brand_extractions)} products):")
                for item in brand_extractions[:10]:  # Show first 10
                    logger.info(f"  '{item['original_brand']}' -> Brand: '{item['new_brand']}', Product: '{item['new_product']}'")
                if len(brand_extractions) > 10:
                    logger.info(f"  ... and {len(brand_extractions) - 10} more")
            
            if subcategory_extractions:
                logger.info(f"\nSUBCATEGORY EXTRACTION ({len(subcategory_extractions)} products):")
                for item in subcategory_extractions[:10]:  # Show first 10
                    logger.info(f"  '{item['original_brand']}' -> Product: '{item['new_product']}' ({item['method']})")
                if len(subcategory_extractions) > 10:
                    logger.info(f"  ... and {len(subcategory_extractions) - 10} more")
            
            if ai_direct_extractions:
                logger.info(f"\nAI DIRECT EXTRACTION ({len(ai_direct_extractions)} products):")
                for item in ai_direct_extractions[:10]:  # Show first 10
                    logger.info(f"  '{item['original_brand']}' -> Product: '{item['new_product']}' (spirit/generic)")
                if len(ai_direct_extractions) > 10:
                    logger.info(f"  ... and {len(ai_direct_extractions) - 10} more")
            
            if ai_extractions:
                logger.info(f"\nAI EXTRACTION ({len(ai_extractions)} products):")
                for item in ai_extractions[:10]:  # Show first 10
                    logger.info(f"  '{item['original_brand']}' -> Product: '{item['new_product']}'")
                if len(ai_extractions) > 10:
                    logger.info(f"  ... and {len(ai_extractions) - 10} more")
            
            if no_extractions:
                logger.info(f"\nNO EXTRACTION POSSIBLE ({len(no_extractions)} products):")
                for item in no_extractions[:10]:  # Show first 10
                    logger.info(f"  '{item['original_brand']}' -> Could not determine valid product type")
                if len(no_extractions) > 10:
                    logger.info(f"  ... and {len(no_extractions) - 10} more")
            
            logger.info("="*60)
        
        logger.info(f"Filled {filled_count} missing product names")
        return updated_products, filled_count

    def get_backup_sheet_data(self) -> Tuple[List[List[str]], Dict[str, int]]:
        """Get data from the backup sheet tab and return column mapping"""
        logger.info("Getting backup sheet data...")
        
        try:
            # Get sheet name for backup tab
            spreadsheet = self.sheets_service.get(
                spreadsheetId=CONFIG["spreadsheet_id"]
            ).execute()
            
            backup_sheet_name = None
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['sheetId'] == CONFIG["backup_sheet_gid"]:
                    backup_sheet_name = sheet['properties']['title']
                    break
                    
            if not backup_sheet_name:
                raise ValueError(f"No backup sheet found with GID {CONFIG['backup_sheet_gid']}")
            
            # Get data from backup sheet
            range_name = f"'{backup_sheet_name}'!A:Z"  # Get all columns
            result = self.sheets_service.values().get(
                spreadsheetId=CONFIG["spreadsheet_id"],
                range=range_name
            ).execute()
            
            backup_data = result.get('values', [])
            logger.info(f"Retrieved {len(backup_data)} rows from backup sheet")
            
            # Create column mapping from headers
            column_mapping = {}
            if backup_data:
                headers = backup_data[0]
                logger.info(f"Backup sheet headers: {headers}")
                
                for i, header in enumerate(headers):
                    header_lower = header.lower().strip()
                    if 'brand' in header_lower:
                        column_mapping['brand_name'] = i
                    elif 'product' in header_lower:
                        column_mapping['product_name'] = i
                    elif 'descriptor' in header_lower:
                        column_mapping['descriptors'] = i
                    elif 'gluten' in header_lower and 'score' in header_lower:
                        column_mapping['gluten_free_score'] = i
                    elif 'gluten' in header_lower:
                        column_mapping['gluten_free_score'] = i
                
                logger.info(f"Backup column mapping: {column_mapping}")
            
            return backup_data, column_mapping
            
        except Exception as e:
            logger.error(f"Error getting backup sheet data: {e}")
            raise

    def get_current_sheet_headers(self) -> Dict[str, int]:
        """Get column mapping from current sheet headers"""
        logger.info("Getting current sheet headers...")
        
        try:
            existing_data = self.get_existing_sheet_data()
            column_mapping = {}
            
            if existing_data:
                headers = existing_data[0]
                logger.info(f"Current sheet headers: {headers}")
                
                for i, header in enumerate(headers):
                    header_lower = header.lower().strip()
                    if 'brand' in header_lower:
                        column_mapping['brand_name'] = i
                    elif 'product' in header_lower:
                        column_mapping['product_name'] = i
                    elif 'descriptor' in header_lower:
                        column_mapping['descriptors'] = i
                    elif 'gluten' in header_lower and 'score' in header_lower:
                        column_mapping['gluten_free_score'] = i
                    elif 'gluten' in header_lower:
                        column_mapping['gluten_free_score'] = i
                
                logger.info(f"Current column mapping: {column_mapping}")
            
            return column_mapping
            
        except Exception as e:
            logger.error(f"Error getting current sheet headers: {e}")
            raise

    def calculate_similarity(self, str1: str, str2: str) -> float:
        """Calculate similarity between two strings using difflib"""
        if not str1 or not str2:
            return 0.0
        
        # Normalize strings for comparison
        str1_norm = str1.lower().strip()
        str2_norm = str2.lower().strip()
        
        if str1_norm == str2_norm:
            return 1.0
        
        # Use difflib for fuzzy matching
        similarity = difflib.SequenceMatcher(None, str1_norm, str2_norm).ratio()
        return similarity

    def find_best_match(self, current_product: Dict[str, Any], backup_products: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Find the best matching product in backup data using fuzzy matching"""
        best_match = None
        best_score = 0.0
        
        for backup_product in backup_products:
            # Calculate similarity for each field we're matching on
            field_scores = []
            
            for field in CONFIG["fuzzy_match_fields"]:
                current_value = current_product.get(field, '')
                backup_value = backup_product.get(field, '')
                similarity = self.calculate_similarity(current_value, backup_value)
                field_scores.append(similarity)
            
            # Use average similarity across all fields
            avg_score = sum(field_scores) / len(field_scores) if field_scores else 0.0
            
            if avg_score > best_score and avg_score >= CONFIG["fuzzy_match_threshold"]:
                best_score = avg_score
                best_match = backup_product
        
        return best_match, best_score

    def restore_from_backup(self, current_products: List[Dict[str, Any]], backup_data: List[List[str]], backup_column_mapping: Dict[str, int]) -> List[Dict[str, Any]]:
        """Restore gluten free scores and descriptors from backup data using fuzzy matching"""
        logger.info("Restoring data from backup using fuzzy matching...")
        
        # Process backup data into structured format using proper column mapping
        backup_products = []
        if len(backup_data) > 1:  # Has data beyond header
            headers = backup_data[0] if backup_data else []
            
            for row in backup_data[1:]:  # Skip header
                if len(row) >= max(backup_column_mapping.values()) + 1:  # Ensure we have enough columns
                    backup_product = {
                        'brand_name': row[backup_column_mapping.get('brand_name', 1)] if 'brand_name' in backup_column_mapping else '',
                        'product_name': row[backup_column_mapping.get('product_name', 2)] if 'product_name' in backup_column_mapping else '',
                        'descriptors': row[backup_column_mapping.get('descriptors', 3)] if 'descriptors' in backup_column_mapping else '',
                        'gluten_free_score': row[backup_column_mapping.get('gluten_free_score', 5)] if 'gluten_free_score' in backup_column_mapping else '',
                    }
                    backup_products.append(backup_product)
        
        logger.info(f"Processed {len(backup_products)} backup products")
        
        # Restore data for each current product
        restored_count = 0
        updated_products = []
        
        for current_product in current_products:
            best_match, score = self.find_best_match(current_product, backup_products)
            
            if best_match and score >= CONFIG["fuzzy_match_threshold"]:
                # Restore gluten free score and descriptors
                if best_match.get('gluten_free_score') and best_match['gluten_free_score'].strip():
                    current_product['gluten_free_score'] = best_match['gluten_free_score']
                    logger.info(f"Restored gluten score '{best_match['gluten_free_score']}' for '{current_product.get('brand_name', '')}' - '{current_product.get('product_name', '')}' (score: {score:.3f})")
                
                if best_match.get('descriptors') and best_match['descriptors'].strip():
                    current_product['descriptors'] = best_match['descriptors']
                    logger.info(f"Restored descriptors for '{current_product.get('brand_name', '')}' - '{current_product.get('product_name', '')}' (score: {score:.3f})")
                
                restored_count += 1
            else:
                logger.warning(f"No backup match found for '{current_product.get('brand_name', '')}' - '{current_product.get('product_name', '')}' (best score: {score:.3f})")
            
            updated_products.append(current_product)
        
        logger.info(f"Restored data for {restored_count} out of {len(current_products)} products")
        return updated_products

    def list_sheet_gids(self):
        """List all sheet GIDs and names for debugging"""
        logger.info("Listing all sheet GIDs and names...")
        
        try:
            spreadsheet = self.sheets_service.get(
                spreadsheetId=CONFIG["spreadsheet_id"]
            ).execute()
            
            print("\n" + "="*60)
            print("SHEET GIDs AND NAMES")
            print("="*60)
            for sheet in spreadsheet['sheets']:
                sheet_id = sheet['properties']['sheetId']
                sheet_name = sheet['properties']['title']
                print(f"GID: {sheet_id} | Name: '{sheet_name}'")
            print("="*60)
            
        except Exception as e:
            logger.error(f"Error listing sheet GIDs: {e}")
            raise

    def run(self):
        """Main execution method"""
        logger.info("Starting alcohol database data integrity improvement...")
        
        try:
            # 1. Get existing sheet data
            existing_data = self.get_existing_sheet_data()
            
            # 2. Process existing sheet data (if any)
            processed_existing = []
            if len(existing_data) > 1:  # Has data beyond header
                # Check if we have the new format or old format
                headers = existing_data[0] if existing_data else []
                is_new_format = len(headers) >= 3 and headers[0].lower() == 'id' and headers[1].lower() == 'brand_name'
                
                for row in existing_data[1:]:  # Skip header
                    if len(row) >= 3:  # Has at least required fields
                        if is_new_format:
                            # New format: id, brand_name, product_name, descriptors, categories, subcategories
                            existing_product = {
                                'id': row[0] if len(row) > 0 else '',
                                'brand_name': row[1] if len(row) > 1 else '',
                                'product_name': row[2] if len(row) > 2 else '',
                                'descriptors': row[3] if len(row) > 3 else '',
                                'category': row[4] if len(row) > 4 else '',
                                'subcategory': row[5] if len(row) > 5 else '',
                                'source': 'Existing'
                            }
                            
                            # Parse descriptors to extract additional info
                            descriptors = existing_product.get('descriptors', '')
                            if descriptors:
                                # Extract image_url, lcbo_id from descriptors
                                for desc in descriptors.split('; '):
                                    if desc.startswith('Image: '):
                                        existing_product['image_url'] = desc[7:]
                                    elif desc.startswith('LCBO ID: '):
                                        existing_product['lcbo_id'] = desc[9:]
                                    elif desc.startswith('Gluten Free Score: '):
                                        existing_product['gluten_free_score'] = desc[19:]
                        else:
                            # Old format: ID, Brand, Product, Category, Subcategory, gluten_free_score, image_url, source, lcbo_id
                            existing_product = {
                                'lcbo_id': row[0] if len(row) > 0 else '',
                                'brand_name': row[1] if len(row) > 1 else '',
                                'product_name': row[2] if len(row) > 2 else '',
                                'category': row[3] if len(row) > 3 else '',
                                'subcategory': row[4] if len(row) > 4 else '',
                                'gluten_free_score': row[5] if len(row) > 5 else '',
                                'image_url': row[6] if len(row) > 6 else '',
                                'source': row[7] if len(row) > 7 else 'Existing'
                            }
                        
                        # Normalize existing data
                        existing_product['brand_name'] = self.normalize_brand_name(existing_product['brand_name'])
                        original_subcategory = existing_product['subcategory']  # Preserve original
                        existing_product['subcategory'] = self.normalize_subcategory(existing_product['subcategory'])
                        existing_product['original_subcategory'] = original_subcategory  # Store original for output
                        
                        if existing_product['brand_name'] or existing_product['product_name']:
                            processed_existing.append(existing_product)
            
            # 3. Restore data from backup if enabled
            if CONFIG["run_backup_restoration"]:
                logger.info("Starting backup restoration process...")
                
                # Get backup data
                backup_data, backup_column_mapping = self.get_backup_sheet_data()
                
                # Restore gluten free scores and descriptors
                processed_existing = self.restore_from_backup(processed_existing, backup_data, backup_column_mapping)
                
                # Update the sheet with restored data
                if CONFIG["run_sheet_update"]:
                    print(f"\nWARNING: About to update Google Sheet with restored data for {len(processed_existing)} products")
                    confirm = input("\nType 'YES' to proceed with the update: ")
                    if confirm != 'YES':
                        logger.info("Update cancelled by user")
                        return
                    
                    self.update_sheet(processed_existing, existing_data)
                    logger.info("Successfully updated sheet with restored data")
                else:
                    logger.info("SHEET UPDATE DISABLED: Would update sheet with restored data")
            else:
                logger.info("Backup restoration disabled")
            
            # 4. Summary
            logger.info("\n" + "="*50)
            logger.info("DATA INTEGRITY IMPROVEMENT SUMMARY")
            logger.info("="*50)
            logger.info(f"Existing products processed: {len(processed_existing)}")
            if CONFIG["run_backup_restoration"]:
                logger.info("Backup restoration completed")
            logger.info("="*50)
            
        except Exception as e:
            logger.error(f"Error during data integrity improvement: {e}")
            raise

def print_configuration():
    """Print the current configuration in a readable format"""
    print("\n" + "="*60)
    print("CURRENT CONFIGURATION")
    print("="*60)
    print(f"Spreadsheet ID: {CONFIG['spreadsheet_id']}")
    print(f"Sheet GID: {CONFIG['sheet_gid']}")
    print(f"Test Mode: {CONFIG['test_mode']}")
    print(f"Test Product Extraction: {CONFIG['test_product_extraction']}")
    print()
    print("OPERATIONS:")
    print(f"  AI Brand Extraction: {'✓' if CONFIG['run_ai_brand_extraction'] else '✗'}")
    print(f"  Product Name Filling: {'✓' if CONFIG['run_product_name_filling'] else '✗'}")
    print(f"  Duplicate Detection: {'✓' if CONFIG['run_duplicate_detection'] else '✗'}")
    print(f"  Brand Normalization: {'✓' if CONFIG['run_brand_normalization'] else '✗'}")
    print(f"  Sheet Update: {'✓' if CONFIG['run_sheet_update'] else '✗'}")
    print()
    print("AI SETTINGS:")
    print(f"  Use AI Brand Extraction: {'✓' if CONFIG['use_ai_brand_extraction'] else '✗'}")
    print(f"  Use AI Product Extraction: {'✓' if CONFIG['use_ai_product_extraction'] else '✗'}")
    print(f"  OpenAI API Key: {'✓' if CONFIG['openai_api_key'] else '✗'}")
    print("="*60)

def main():
    """Main entry point"""
    # Print current configuration
    print_configuration()
    
    # Initialize the processor
    processor = DataIntegrityProcessor()
    
    # List sheet GIDs first to find backup tab
    processor.list_sheet_gids()
    
    # Ask user to confirm backup GID
    print("\nPlease update the CONFIG['backup_sheet_gid'] with the correct GID for the backup tab.")
    print("Then run the script again to perform the restoration.")
    
    # Uncomment the line below after setting the correct backup_sheet_gid
    processor.run()

if __name__ == "__main__":
    main() 