"""
LCBO API Image Scraper
=====================

Uses LCBOstats API to find product images and update a Google Sheet.
"""
import os
import sys
import requests
import json
import logging
import time
from typing import Dict, List, Optional, Any
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2 import service_account

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
    "range_name": None,  # Will be set in main() after getting sheet name
    "image_col_index": 6,  # Index of image_url column (column G, 0-based)
    "product_name_col_index": 2,  # Index of Product column (column C, 0-based)
    "brand_col_index": 1,  # Index of Brand column (column B, 0-based)
    "category_col_index": 4,  # Index of Category column (column E, 0-based)
    "credentials_file": "charged-gravity-444220-d2-70c441d6f918.json",
    "test_mode": True,
    "debug_mode": True,
    "max_rows_to_process": 2500,
    "start_row": 2086  # 0-based index of the first row to process (0 = first row after header)
}

class LCBOStatsClient:
    """Client for interacting with the LCBOstats API"""
    
    def __init__(self):
        self.base_url = 'http://lcbostats.com/api'
    
    def search_products(
        self,
        query: str,
        page: int = 1,
        per_page: int = 25,
        sort_by: Optional[str] = None,
        sort_direction: str = 'asc',
        min_price: Optional[float] = None,
        max_price: Optional[float] = None,
        min_volume: Optional[int] = None,
        max_volume: Optional[int] = None,
        min_alcohol_content: Optional[float] = None,
        max_alcohol_content: Optional[float] = None,
    ) -> Dict[str, Any]:
        """Search for products in the LCBO database"""
        try:
            params = {
                'page': page,
                'per_page': per_page,
            }

            if query:
                params['search'] = query
            if sort_by:
                params[f'sort{sort_direction.capitalize()}'] = sort_by
            if min_price is not None:
                params['minPrice'] = min_price
            if max_price is not None:
                params['maxPrice'] = max_price
            if min_volume is not None:
                params['minVolume'] = min_volume
            if max_volume is not None:
                params['maxVolume'] = max_volume
            if min_alcohol_content is not None:
                params['minAlcoholContent'] = min_alcohol_content
            if max_alcohol_content is not None:
                params['maxAlcoholContent'] = max_alcohol_content

            response = requests.get(f"{self.base_url}/alcohol", params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error searching products: {e}")
            raise

    def get_product_by_id(self, product_id: int) -> Dict[str, Any]:
        """Get detailed information about a specific product"""
        try:
            response = requests.get(f"{self.base_url}/alcohol/{product_id}")
            response.raise_for_status()
            return response.json()['data']
        except Exception as e:
            logger.error(f"Error fetching product {product_id}: {e}")
            raise

    def get_price_history(self, product_id: int) -> Dict[str, Any]:
        """Get price history for a specific product"""
        try:
            response = requests.get(f"{self.base_url}/history/{product_id}")
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Error fetching price history for product {product_id}: {e}")
            raise

def get_google_sheets_credentials():
    """Get Google Sheets credentials from file or environment variable"""
    try:
        # 1. Use the credentials file specified in CONFIG
        if CONFIG["credentials_file"] and os.path.exists(CONFIG["credentials_file"]):
            logger.info(f"Loading credentials from file: {CONFIG['credentials_file']}")
            return service_account.Credentials.from_service_account_file(
                CONFIG["credentials_file"], 
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
        
        # 2. Check for a credentials file from environment variable
        credentials_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_FILE')
        if credentials_file and os.path.exists(credentials_file):
            logger.info(f"Loading credentials from environment file: {credentials_file}")
            return service_account.Credentials.from_service_account_file(
                credentials_file, 
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
        
        # 3. Try to load from formatted .env variable
        env_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if env_creds:
            try:
                # Sometimes the environment variable has escaped quotes or newlines
                # Try to clean it up
                env_creds = env_creds.replace('\\"', '"').replace("\\n", "\n").replace("\\\\", "\\")
                
                # Try to parse the JSON
                credentials_info = json.loads(env_creds)
                logger.info("Successfully loaded credentials from environment variable")
                
                return service_account.Credentials.from_service_account_info(
                    credentials_info,
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
            except json.JSONDecodeError as e:
                logger.warning(f"Could not parse credentials JSON: {e}")
                logger.warning("Please provide a valid JSON string in GOOGLE_APPLICATION_CREDENTIALS or a file path")
        
        # 4. For testing purposes with hardcoded values (ONLY FOR TESTING, REMOVE IN PRODUCTION)
        if CONFIG["test_mode"]:
            logger.warning("Using dummy credentials for test mode (will not connect to real sheet)")
            return None
            
        logger.error("No valid credentials found")
        return None
    except Exception as e:
        logger.error(f"Error creating credentials: {e}")
        return None

def get_sheet_name_from_gid(spreadsheet_id: str, gid: int) -> Optional[str]:
    """Get the actual sheet name from a GID"""
    try:
        credentials = get_google_sheets_credentials()
        service = build('sheets', 'v4', credentials=credentials)
        
        # Get spreadsheet metadata
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()
        
        # Find the sheet with the matching GID
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['sheetId'] == gid:
                return sheet['properties']['title']
                
        logger.error(f"No sheet found with GID {gid}")
        return None
        
    except Exception as e:
        logger.error(f"Error getting sheet name from GID: {e}")
        return None

def get_products_from_sheet() -> List[List[str]]:
    """Get product data from Google Sheet"""
    try:
        credentials = get_google_sheets_credentials()
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()
        
        # Get the data
        result = sheet.values().get(
            spreadsheetId=CONFIG["spreadsheet_id"],
            range=CONFIG["range_name"]
        ).execute()
        
        rows = result.get('values', [])
        if not rows:
            logger.warning("No data found in sheet")
            return []
            
        logger.info(f"Found {len(rows) - 1} products in sheet")  # Subtract 1 for header row
        
        # Skip header row
        return rows[1:] if len(rows) > 1 else []
        
    except Exception as e:
        logger.error(f"Error getting products from sheet: {e}")
        raise

def update_sheet_with_image(row_index: int, image_url: str) -> None:
    """Update a single row in the Google Sheet with an image URL"""
    try:
        credentials = get_google_sheets_credentials()
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()
        
        # Calculate the column letter for the image URL column
        image_col_letter = chr(65 + CONFIG["image_col_index"])  # Convert to column letter (A=65 in ASCII)
        range_name = f"{CONFIG['range_name'].split('!')[0]}!{image_col_letter}{row_index + 2}"  # +2 for header and 1-indexing
        
        # Update the cell
        sheet.values().update(
            spreadsheetId=CONFIG["spreadsheet_id"],
            range=range_name,
            valueInputOption='RAW',
            body={'values': [[image_url]]}
        ).execute()
        
        logger.info(f"Updated image URL for row {row_index + 2}")
        
    except Exception as e:
        logger.error(f"Error updating sheet with image URL: {e}")
        raise

def main():
    """Main function to get products and find images"""
    logger.info("Starting LCBO API image scraper")
    
    # Get the actual sheet name from GID
    sheet_name = get_sheet_name_from_gid(CONFIG["spreadsheet_id"], CONFIG["sheet_gid"])
    if not sheet_name:
        # Fallback to a default sheet name if we can't get it from GID
        logger.warning(f"Could not find sheet with GID {CONFIG['sheet_gid']}, using 'Sheet1' as fallback")
        sheet_name = "Sheet1"
        
    # Set the range name with the actual sheet name
    CONFIG["range_name"] = f"'{sheet_name}'!A:AZ"
    logger.info(f"Using sheet: {sheet_name}")
    
    # Initialize clients
    lcbo_client = LCBOStatsClient()
    
    # Get products from sheet
    products = get_products_from_sheet()
    if not products:
        logger.error("No products found in sheet, exiting")
        return
        
    # Process each product
    processed_count = 0
    updated_count = 0
    rows_to_process = 0
    
    # Skip to start row
    if CONFIG["start_row"] > 0:
        logger.info(f"Starting from row {CONFIG['start_row'] + 2} (after header)")
        products = products[CONFIG["start_row"]:]
    
    for i, row in enumerate(products):
        try:
            # Skip if row doesn't have enough columns
            if len(row) < max(CONFIG["product_name_col_index"], CONFIG["brand_col_index"]) + 1:
                logger.warning(f"Row {i + CONFIG['start_row'] + 2} doesn't have enough columns, skipping")
                continue
                
            # Get product details
            product_name = row[CONFIG["product_name_col_index"]]
            brand_name = row[CONFIG["brand_col_index"]]
            category = row[CONFIG["category_col_index"]] if len(row) > CONFIG["category_col_index"] else None
            
            # Skip if no product name
            if not product_name:
                logger.warning(f"Row {i + CONFIG['start_row'] + 2} doesn't have a product name, skipping")
                continue
                
            # Skip if already has an image URL
            if len(row) > CONFIG["image_col_index"] and row[CONFIG["image_col_index"]] and row[CONFIG["image_col_index"]].startswith('http'):
                logger.info(f"Row {i + CONFIG['start_row'] + 2} already has an image URL, skipping")
                continue
                
            # Check if we've hit the limit of rows to process
            rows_to_process += 1
            if rows_to_process > CONFIG["max_rows_to_process"]:
                logger.info(f"Reached limit of {CONFIG['max_rows_to_process']} rows to process, stopping")
                break
                
            # Search for product in LCBO API
            logger.info(f"\nSearching for: {brand_name} {product_name}")
            search_results = lcbo_client.search_products(
                f"{brand_name} {product_name}",
                page=1,
                per_page=1,
                sort_by='price',
                sort_direction='asc'
            )
            
            if search_results['data']:
                product = search_results['data'][0]
                logger.info(f"Found product: {product['title']}")
                logger.info(f"Image URL: {product['thumbnail_url']}")
                
                # Update sheet with image URL
                update_sheet_with_image(i + CONFIG["start_row"], product['thumbnail_url'])
                updated_count += 1
            else:
                logger.info(f"No matching product found for: {brand_name} {product_name}")
            
            processed_count += 1
            
            # Sleep to avoid rate limits
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error processing row {i + CONFIG['start_row'] + 2}: {e}")
            if CONFIG["debug_mode"]:
                import traceback
                traceback.print_exc()
            continue
    
    logger.info(f"\nSummary:")
    logger.info(f"Processed {processed_count} products")
    logger.info(f"Updated {updated_count} products with new image URLs")

if __name__ == "__main__":
    main() 