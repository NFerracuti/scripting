"""
Product Image Search Script
==========================

Uses Google Custom Search API to find images for products and update a spreadsheet.
"""
import os
import sys
import requests
import json
import logging
import time
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
    "spreadsheet_id": "1sNyaTE6uCqWSHlezPQcBPaPBDqGvp_epIywaXceK5Cw",  # User's spreadsheet ID
    "sheet_gid": 828037295,  # GID of the specific sheet
    "range_name": None,  # Will be set in main() after getting sheet name
    "image_col_index": 7,  # Index of image_url column (column H, 0-based)
    "product_name_col_index": 2,  # Index of Product column (column C, 0-based)
    "brand_col_index": 1,  # Index of Brand column (column B, 0-based)
    "category_col_index": 4,  # Index of Category column (column E, 0-based)
    "google_api_key": os.getenv("GOOGLE_SEARCH_API_KEY"),  # Your Google API key from .env file
    "google_search_cx": "e3d1e4204e4ff4337",  # Your Custom Search Engine ID
    "credentials_file": "nicks-the-admin-761daddb2d59.json",  # Path to credentials file
    "test_mode": True,  # Set to True to only process the first row without image
    "debug_mode": True,  # Set to True for more detailed logging
    "max_rows_to_process": 1000  # Maximum number of rows to process
}


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


def search_image_for_product(product_name, brand_name=None, category=None):
    """
    Search for an image of a product using Google Custom Search API
    """
    try:
        # Check if API key and Search Engine ID are available
        if not CONFIG["google_api_key"] or not CONFIG["google_search_cx"]:
            logger.error("Missing Google API key or Custom Search Engine ID")
            return None
        
        # Build the search query
        if product_name:
            # If we have a product name, use it with brand and category
            query = product_name
            if brand_name:
                query = f"{brand_name} {product_name}"
        else:
            # If no product name, use brand + category as fallback
            if brand_name:
                query = brand_name
            else:
                logger.error("No product name or brand name provided")
                return None
            
        # Add category to the query if available
        if category:
            # Replace "Beer & Cider" with "Beer" for better search results
            if category == "Beer & Cider":
                category = "Beer"
            query = f"{query} {category}"
        else:
            # Default to alcoholic beverage if no category
            query = f"{query} alcoholic beverage"
        
        logger.info(f"Searching for image: {query}")
        
        # Build the service
        service = build(
            "customsearch", "v1",
            developerKey=CONFIG["google_api_key"]
        )
        
        # Execute the search
        result = service.cse().list(
            q=query,
            cx=CONFIG["google_search_cx"],
            searchType="image",
            num=1,  # Just get the first (best) result
            imgSize="LARGE",  # Prefer large images - must be uppercase
            safe="off"  # No safe search filtering
        ).execute()
        
        # Print full result in debug mode
        if CONFIG["debug_mode"]:
            logger.info(f"Full search result: {json.dumps(result, indent=2)}")
        
        # Extract image URL
        if "items" in result and len(result["items"]) > 0:
            image_url = result["items"][0]["link"]
            logger.info(f"Found image: {image_url}")
            return image_url
        else:
            logger.warning(f"No image found for {query}")
            return None
            
    except Exception as e:
        logger.error(f"Error searching for image: {e}")
        return None


def get_products_from_sheet():
    """
    Get product data from Google Sheet
    """
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


def update_sheet_with_images(rows_with_images):
    """
    Update the Google Sheet with image URLs
    """
    try:
        credentials = get_google_sheets_credentials()
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()
        
        # Get the current values to determine the range
        result = sheet.values().get(
            spreadsheetId=CONFIG["spreadsheet_id"],
            range=CONFIG["range_name"]
        ).execute()
        
        current_rows = result.get('values', [])
        if not current_rows:
            logger.warning("No data found in sheet, cannot update")
            return False
            
        # Get sheet name from the range
        sheet_name = CONFIG["range_name"].split("!")[0]
            
        # Create update requests for each row
        batch_update_requests = []
        header_row = current_rows[0]
        image_col_letter = chr(65 + CONFIG["image_col_index"])  # Convert to column letter (A=65 in ASCII)
        
        # For each row with an image
        for i, row_data in enumerate(rows_with_images):
            # Skip rows that don't have an image URL
            if not row_data.get('image_url'):
                continue
                
            # Calculate the row number (adding 2 for 1-indexing and header row)
            row_num = row_data['row_index'] + 2
            
            # Create the update request
            batch_update_requests.append({
                'range': f'{sheet_name}{image_col_letter}{row_num}',
                'values': [[row_data['image_url']]]
            })
            
        # If we have updates, send them in a batch
        if batch_update_requests:
            body = {
                'valueInputOption': 'RAW',
                'data': batch_update_requests
            }
            
            result = sheet.values().batchUpdate(
                spreadsheetId=CONFIG["spreadsheet_id"],
                body=body
            ).execute()
            
            logger.info(f"Updated {len(batch_update_requests)} rows with image URLs")
            return True
        else:
            logger.warning("No rows to update")
            return False
            
    except Exception as e:
        logger.error(f"Error updating sheet with images: {e}")
        raise


def test_api_connection():
    """
    Test the Google Custom Search API connection
    """
    try:
        # Check if API key is available
        if not CONFIG["google_api_key"]:
            logger.error("Google API key is missing. Add GOOGLE_SEARCH_API_KEY to your .env file.")
            return False
            
        # Try a simple search to verify connection
        service = build(
            "customsearch", "v1",
            developerKey=CONFIG["google_api_key"]
        )
        
        # Execute a simple test search
        result = service.cse().list(
            q="test query",
            cx=CONFIG["google_search_cx"],
            searchType="image",
            num=1
        ).execute()
        
        # If we get here without exception, the connection works
        logger.info("Google Custom Search API connection successful!")
        return True
            
    except Exception as e:
        logger.error(f"Error testing API connection: {e}")
        logger.error("Please check your API key and search engine ID")
        return False


def get_sheet_name_from_gid(spreadsheet_id, gid):
    """
    Get the actual sheet name from a GID
    """
    try:
        credentials = get_google_sheets_credentials()
        service = build('sheets', 'v4', credentials=credentials)
        
        # Get spreadsheet metadata
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=spreadsheet_id
        ).execute()
        
        # Find the sheet with the matching GID
        for sheet in spreadsheet['sheets']:
            if sheet['properties']['sheetId'] == int(gid):
                return sheet['properties']['title']
                
        logger.error(f"No sheet found with GID {gid}")
        return None
        
    except Exception as e:
        logger.error(f"Error getting sheet name from GID: {e}")
        return None


def main():
    """
    Main function to get products and find images
    """
    logger.info("Starting product image search")
    
    # Get the actual sheet name from GID
    sheet_name = get_sheet_name_from_gid(CONFIG["spreadsheet_id"], CONFIG["sheet_gid"])
    if not sheet_name:
        # Fallback to a default sheet name if we can't get it from GID
        logger.warning(f"Could not find sheet with GID {CONFIG['sheet_gid']}, using 'Sheet1' as fallback")
        sheet_name = "Sheet1"
        
    # Set the range name with the actual sheet name
    CONFIG["range_name"] = f"'{sheet_name}'!A:AZ"
    logger.info(f"Using sheet: {sheet_name}")
    
    # Test API connection first
    if not test_api_connection():
        logger.error("Cannot proceed without working API connection")
        return
    
    # Get products from sheet
    products = get_products_from_sheet()
    if not products:
        logger.error("No products found in sheet, exiting")
        return
        
    # Prepare list for products with image URLs
    products_with_images = []
    
    # If in test mode, only process the first row without an image
    test_row_processed = False
    
    # Process each product row
    for i, row in enumerate(products):
        try:
            # In test mode, only process one row
            if CONFIG["test_mode"] and test_row_processed:
                logger.info("Test mode - skipping remaining rows")
                break
                
            # Debug: Print the current row
            if CONFIG["debug_mode"]:
                logger.info(f"Processing row {i+2}: {row}")
                
            # Make sure the row has enough columns for product name and brand
            min_cols_needed = max(CONFIG["product_name_col_index"], CONFIG["brand_col_index"]) + 1
            if len(row) < min_cols_needed:
                logger.warning(f"Row {i+2} doesn't have enough columns, skipping")
                products_with_images.append({'row_index': i, 'image_url': None})
                continue
                
            # Get product name and brand
            product_name = row[CONFIG["product_name_col_index"]] if len(row) > CONFIG["product_name_col_index"] else None
            brand_name = row[CONFIG["brand_col_index"]] if len(row) > CONFIG["brand_col_index"] else None
            
            # Get category if available
            category = None
            if len(row) > CONFIG["category_col_index"]:
                category = row[CONFIG["category_col_index"]]
            
            if not product_name and not brand_name:
                logger.warning(f"Row {i+2} - No product name or brand name found")
                products_with_images.append({'row_index': i, 'image_url': None})
                continue
                
            # Skip if row already has an image URL
            has_image = (len(row) > CONFIG["image_col_index"] and 
                         row[CONFIG["image_col_index"]] and 
                         row[CONFIG["image_col_index"]].startswith('http'))
                         
            if has_image:
                logger.info(f"Row {i+2} already has an image URL, skipping")
                products_with_images.append({'row_index': i, 'image_url': row[CONFIG["image_col_index"]]})
                continue
                
            # Search for an image
            image_url = search_image_for_product(product_name, brand_name, category)
            
            # Add to our list
            products_with_images.append({'row_index': i, 'image_url': image_url})
            
            # In test mode, mark as processed
            if CONFIG["test_mode"]:
                test_row_processed = True
                logger.info("Test mode - processed one row successfully")
            
            # Sleep to avoid hitting API rate limits (max 100 queries per day for free tier)
            time.sleep(1)
            
        except Exception as e:
            logger.error(f"Error processing row {i+2}: {e}")
            products_with_images.append({'row_index': i, 'image_url': None})
            continue
            
    # Count results
    found_count = sum(1 for p in products_with_images if p['image_url'])
    logger.info(f"Found images for {found_count} out of {len(products_with_images)} processed products")
    
    # Update the sheet
    if found_count > 0:
        update_sheet_with_images(products_with_images)
        
    logger.info("Product image search completed")


def test_search_only():
    """Test just the image search functionality without spreadsheet access"""
    logger.info("=== Testing image search only ===")
    
    # Test the connection first
    if not test_api_connection():
        logger.error("Cannot proceed without working API connection")
        return
    
    # Sample product to test with
    test_product = "Absolut Vodka"
    test_brand = "Absolut"
    test_category = "Spirits"
    
    logger.info(f"Testing search with product: {test_product}, brand: {test_brand}, category: {test_category}")
    
    # Search for the image
    image_url = search_image_for_product(test_product, test_brand, test_category)
    
    if image_url:
        logger.info(f"SUCCESS! Found image URL: {image_url}")
    else:
        logger.error("Failed to find image")


def test_with_sheet_row():
    """Test with multiple rows from the actual sheet"""
    logger.info(f"=== Testing with up to {CONFIG['max_rows_to_process']} rows from the sheet ===")
    
    try:
        # Get credentials first to make sure they work
        credentials = get_google_sheets_credentials()
        if not credentials:
            logger.error("Failed to get credentials. Please check your credentials file.")
            logger.warning("Falling back to mock data for testing...")
            
            # Create mock data for testing
            mock_rows = [
                ["1", "Crown Royal", "Whisky", "$39.99", "Whisky", "0"],
                ["2", "Grey Goose", "Vodka", "$45.99", "Vodka", "0"],
                ["3", "Bacardi", "Rum", "$29.99", "Rum", "0"]
            ]
            
            # Process just a few mock rows
            for i, mock_row in enumerate(mock_rows[:CONFIG['max_rows_to_process']]):
                product_name = mock_row[CONFIG["product_name_col_index"]] if len(mock_row) > CONFIG["product_name_col_index"] else None
                brand_name = mock_row[CONFIG["brand_col_index"]] if len(mock_row) > CONFIG["brand_col_index"] else None
                category = mock_row[CONFIG["category_col_index"]] if len(mock_row) > CONFIG["category_col_index"] else None
                
                logger.info(f"Testing row {i+1} with MOCK DATA - product: {product_name}, brand: {brand_name}, category: {category}")
                
                # Search for the image
                image_url = search_image_for_product(product_name, brand_name, category)
                
                if image_url:
                    logger.info(f"SUCCESS! Found image URL for mock data: {image_url}")
                    logger.info("Since we're using mock data, we cannot update the real sheet.")
                else:
                    logger.error(f"Failed to find image for mock data row {i+1}")
                    
                # Sleep between requests to avoid hitting API limits
                if i < len(mock_rows) - 1:
                    logger.info("Sleeping for 1 second to avoid hitting API rate limits...")
                    time.sleep(1)
                
            return
            
        # Try to access the spreadsheet to list available sheets
        try:
            service = build('sheets', 'v4', credentials=credentials)
            sheets_service = service.spreadsheets()
            
            # Get spreadsheet info
            spreadsheet = sheets_service.get(
                spreadsheetId=CONFIG["spreadsheet_id"]
            ).execute()
            
            logger.info("Successfully connected to spreadsheet!")
            logger.info(f"Spreadsheet title: {spreadsheet.get('properties', {}).get('title')}")
            
            # List all available sheets
            sheets = spreadsheet.get('sheets', [])
            logger.info(f"Available sheets ({len(sheets)}):")
            for sheet in sheets:
                sheet_name = sheet.get('properties', {}).get('title')
                sheet_id = sheet.get('properties', {}).get('sheetId')
                logger.info(f"  - Sheet: '{sheet_name}' (ID: {sheet_id})")
                
            # Get the actual sheet name from GID
            sheet_name = None
            for sheet in sheets:
                if sheet.get('properties', {}).get('sheetId') == CONFIG["sheet_gid"]:
                    sheet_name = sheet.get('properties', {}).get('title')
                    break
                    
            if not sheet_name:
                # Fallback to a default sheet name if we can't get it from GID
                logger.warning(f"Could not find sheet with GID {CONFIG['sheet_gid']}, trying first available sheet")
                if sheets:
                    sheet_name = sheets[0].get('properties', {}).get('title')
                else:
                    logger.error("No sheets found in the spreadsheet")
                    return
                    
            # Set the range name with the actual sheet name
            CONFIG["range_name"] = f"'{sheet_name}'!A:AZ"
            logger.info(f"Using sheet: {sheet_name}")
            
            # Get the data
            result = sheets_service.values().get(
                spreadsheetId=CONFIG["spreadsheet_id"],
                range=CONFIG["range_name"],
                majorDimension="ROWS",
                valueRenderOption="UNFORMATTED_VALUE"
            ).execute()
            
            rows = result.get('values', [])
            if not rows or len(rows) < 2:  # Need at least header + 1 data row
                logger.error("No rows found in sheet")
                return
                
            # Print header row for debugging
            logger.info(f"Header row: {rows[0]}")
            
            # Calculate how many rows to process
            data_rows = rows[1:]  # Skip header row
            logger.info(f"Found {len(data_rows)} total data rows")
            
            rows_processed = 0
            rows_updated = 0
            rows_needing_images = 0
            
            # Process data rows
            for i, row in enumerate(data_rows):
                try:
                    row_num = i + 2  # Row 2 is the first data row (1-indexed)
                    
                    # Skip if row already has an image URL
                    if len(row) > CONFIG["image_col_index"] and row[CONFIG["image_col_index"]] and isinstance(row[CONFIG["image_col_index"]], str) and row[CONFIG["image_col_index"]].startswith('http'):
                        logger.info(f"Row {row_num} - Already has an image URL: {row[CONFIG['image_col_index']]}")
                        continue
                        
                    # Count this as a row needing an image
                    rows_needing_images += 1
                    
                    # If we've processed enough rows that needed images, stop
                    if rows_needing_images > CONFIG["max_rows_to_process"]:
                        logger.info(f"Reached maximum number of rows to process ({CONFIG['max_rows_to_process']})")
                        break
                    
                    logger.info(f"\nProcessing row {row_num}: {row}")
                    
                    # Make sure the row has enough columns
                    min_cols_needed = max(CONFIG["product_name_col_index"], CONFIG["brand_col_index"]) + 1
                    if len(row) < min_cols_needed:
                        logger.warning(f"Row {row_num} doesn't have enough columns. Needed {min_cols_needed}, got {len(row)}")
                        continue
                        
                    # Get product name and brand
                    product_name = row[CONFIG["product_name_col_index"]] if len(row) > CONFIG["product_name_col_index"] else None
                    brand_name = row[CONFIG["brand_col_index"]] if len(row) > CONFIG["brand_col_index"] else None
                    category = row[CONFIG["category_col_index"]] if len(row) > CONFIG["category_col_index"] else None
                    
                    logger.info(f"Row {row_num} - Testing with product: {product_name}, brand: {brand_name}, category: {category}")
                    
                    if not product_name and not brand_name:
                        logger.warning(f"Row {row_num} - No product name or brand name found")
                        continue
                        
                    # Skip if row already has an image URL
                    has_image = (len(row) > CONFIG["image_col_index"] and 
                                 row[CONFIG["image_col_index"]] and 
                                 row[CONFIG["image_col_index"]].startswith('http'))
                                 
                    if has_image:
                        logger.info(f"Row {row_num} already has an image URL, skipping")
                        products_with_images.append({'row_index': i, 'image_url': row[CONFIG["image_col_index"]]})
                        continue
                    
                    # Search for the image
                    image_url = search_image_for_product(product_name, brand_name, category)
                    
                    if image_url:
                        logger.info(f"Row {row_num} - SUCCESS! Found image URL: {image_url}")
                        
                        # Update the sheet with this URL
                        image_col_letter = chr(65 + CONFIG["image_col_index"])  # Convert to column letter (A=65 in ASCII)
                        update_range = f"'{sheet_name}'!{image_col_letter}{row_num}"
                        
                        logger.info(f"Updating cell {update_range} with image URL")
                        
                        sheets_service.values().update(
                            spreadsheetId=CONFIG["spreadsheet_id"],
                            range=update_range,
                            valueInputOption="RAW",
                            body={"values": [[image_url]]}
                        ).execute()
                        
                        logger.info(f"Row {row_num} - Successfully updated the sheet with image URL")
                        rows_updated += 1
                    else:
                        logger.error(f"Row {row_num} - Failed to find image")
                    
                    rows_processed += 1
                    
                    # Sleep between requests to avoid hitting API limits
                    if i < len(data_rows) - 1:  # Don't sleep after the last row
                        logger.info("Sleeping for 1 second to avoid hitting API rate limits...")
                        time.sleep(1)
                        
                except Exception as e:
                    logger.error(f"Error processing row {row_num}: {e}")
                    if CONFIG["debug_mode"]:
                        import traceback
                        traceback.print_exc()
            
            logger.info(f"\nSummary: Found {rows_needing_images} rows needing images, processed {rows_processed} rows, updated {rows_updated} rows with new image URLs")
            
        except Exception as e:
            logger.error(f"Error accessing sheet: {e}")
            if CONFIG["debug_mode"]:
                import traceback
                traceback.print_exc()
    
    except Exception as e:
        logger.error(f"Error in test: {e}")
        if CONFIG["debug_mode"]:
            import traceback
            traceback.print_exc()


if __name__ == "__main__":
    # For quick testing of just the search functionality
    if CONFIG["test_mode"]:
        # Uncomment whichever test you want to run
        #test_search_only()  # Test with hardcoded sample
        test_with_sheet_row()  # Test with actual row from sheet
    else:
        main() 