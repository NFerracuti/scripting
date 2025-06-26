"""
LCBO Wine Products Scraper
=========================

Fetches wine products from LCBOstats API and adds them to a Google Sheet.
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
SHEET_ID = '1sNyaTE6uCqWSHlezPQcBPaPBDqGvp_epIywaXceK5Cw'
SHEET_NAME = 'Sheet1'  # Default sheet name
CREDENTIALS_FILE = 'nicks-the-admin-761daddb2d59.json'
MAX_PRODUCTS = 189  # Total number of wine products available
PRODUCTS_PER_PAGE = 25  # API's default page size

class LCBOStatsClient:
    """Client for interacting with the LCBOstats API"""
    
    def __init__(self):
        self.base_url = 'https://lcbostats.com/api'  # Updated to HTTPS
    
    def get_wine_products(self, page: int = 1, per_page: int = 25) -> Dict[str, Any]:
        """Get wine products from the LCBO database"""
        try:
            params = {
                'page': page,
                'per_page': per_page,
                'search': 'Wine'  # Search for all wines
            }

            logger.info(f"Making API request to {self.base_url}/alcohol with params: {params}")
            response = requests.get(
                f"{self.base_url}/alcohol", 
                params=params,
                timeout=10  # Add 10 second timeout
            )
            response.raise_for_status()
            logger.info("API request successful")
            return response.json()
        except requests.Timeout:
            logger.error("Request timed out after 10 seconds")
            raise
        except requests.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise
        except Exception as e:
            logger.error(f"Error fetching wine products: {e}")
            raise

def get_google_sheets_credentials():
    """Get Google Sheets credentials from file or environment variable"""
    try:
        # 1. Use the credentials file specified in CONFIG
        if CREDENTIALS_FILE and os.path.exists(CREDENTIALS_FILE):
            logger.info(f"Loading credentials from file: {CREDENTIALS_FILE}")
            credentials = service_account.Credentials.from_service_account_file(
                CREDENTIALS_FILE, 
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            logger.info(f"Using service account: {credentials.service_account_email}")
            return credentials
        
        # 2. Check for a credentials file from environment variable
        credentials_file = os.getenv('GOOGLE_APPLICATION_CREDENTIALS_FILE')
        if credentials_file and os.path.exists(credentials_file):
            logger.info(f"Loading credentials from environment file: {credentials_file}")
            credentials = service_account.Credentials.from_service_account_file(
                credentials_file, 
                scopes=['https://www.googleapis.com/auth/spreadsheets']
            )
            logger.info(f"Using service account: {credentials.service_account_email}")
            return credentials
        
        # 3. Try to load from formatted .env variable
        env_creds = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if env_creds:
            try:
                env_creds = env_creds.replace('\\"', '"').replace("\\n", "\n").replace("\\\\", "\\")
                credentials_info = json.loads(env_creds)
                logger.info("Successfully loaded credentials from environment variable")
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_info,
                    scopes=['https://www.googleapis.com/auth/spreadsheets']
                )
                logger.info(f"Using service account: {credentials.service_account_email}")
                return credentials
            except json.JSONDecodeError as e:
                logger.warning(f"Could not parse credentials JSON: {e}")
        
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

def update_sheet_with_products(products: List[Dict[str, Any]]) -> None:
    """Update the Google Sheet with wine products"""
    try:
        credentials = get_google_sheets_credentials()
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()
        
        # Prepare the data for the sheet
        values = []
        for product in products:
            # Log the full product object
            logger.info("\nProduct object details:")
            for key, value in product.items():
                logger.info(f"{key}: {value}")
            
            # Extract product details
            row = [
                str(product['permanent_id']),  # ID
                product['brand'],              # Brand
                product['title'],              # Product
                str(product['price']),         # Price
                product['category'],           # Category
                product['subcategory'],        # Subcategory
                "0",                           # gluten_free_score (default to 0)
                product['thumbnail_url']       # image_url
            ]
            values.append(row)
            logger.info(f"\nProcessed row: {row}")
        
        # Add header row
        header = ["ID", "Brand", "Product", "Price", "Category", "Subcategory", "gluten_free_score", "image_url"]
        values.insert(0, header)
        
        # Update the sheet
        body = {
            'values': values
        }
        
        result = sheet.values().update(
            spreadsheetId=SHEET_ID,
            range=SHEET_NAME,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        logger.info(f"Updated sheet with {len(products)} products")
        
    except Exception as e:
        logger.error(f"Error updating sheet with products: {e}")
        raise

def main():
    """Main function to fetch wine products and update the sheet"""
    logger.info("Starting LCBO wine products scraper")
    
    # Get the actual sheet name from GID
    sheet_name = get_sheet_name_from_gid(SHEET_ID, 828037295)
    if not sheet_name:
        logger.warning(f"Could not find sheet with GID 828037295, using 'Sheet1' as fallback")
        sheet_name = SHEET_NAME
        
    # Set the range name with the actual sheet name
    range_name = f"'{sheet_name}'!A:G"
    logger.info(f"Using sheet: {sheet_name}")
    
    # Initialize client
    logger.info("Initializing LCBO client...")
    lcbo_client = LCBOStatsClient()
    
    # Fetch all wine products
    logger.info("Starting to fetch wine products...")
    all_products = []
    total_products = 0
    
    # Calculate total pages needed
    total_pages = (MAX_PRODUCTS + PRODUCTS_PER_PAGE - 1) // PRODUCTS_PER_PAGE
    
    for page in range(1, total_pages + 1):
        try:
            logger.info(f"Fetching page {page} of {total_pages}...")
            response = lcbo_client.get_wine_products(page=page)
            products = response['data']
            all_products.extend(products)
            total_products += len(products)
            logger.info(f"Fetched {len(products)} products from page {page}")
            
            if len(all_products) >= MAX_PRODUCTS:
                logger.info(f"Reached maximum number of products ({MAX_PRODUCTS})")
                break
                
            time.sleep(1)  # Rate limiting
            
        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")
            break
    
    # Update the sheet with the products
    if all_products:
        logger.info(f"Found {len(all_products)} total wine products, updating sheet...")
        update_sheet_with_products(all_products)
        logger.info(f"Successfully added {len(all_products)} wine products to the sheet")
    else:
        logger.error("No products found to add to the sheet")

if __name__ == "__main__":
    main()
