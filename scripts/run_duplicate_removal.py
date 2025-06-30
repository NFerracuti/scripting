"""
Run Duplicate Removal Only
==========================

Simple script to run only the duplicate removal functionality.
This is useful when you just want to remove exact duplicates without running other operations.

HOW TO RUN:
1. Activate virtual environment:
   cd /Users/nick/celiapp-official/scripting
   source venv/bin/activate

2. Run the script:
   cd scripts
   python run_duplicate_removal.py

3. Configure settings in alcohol_data_config.py before running

REQUIREMENTS:
- Python 3.7+
- Virtual environment with required packages
- Google Sheets API credentials
- Sheet with data to clean up duplicates from
"""

import os
import sys
import logging
from alcohol_data_config import CONFIG
from alcohol_data_processor import AlcoholDataProcessor
from alcohol_sheets_client import GoogleSheetsClient
from alcohol_backup_restorer import AlcoholBackupRestorer

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Run duplicate removal only"""
    logger.info("Starting duplicate removal only...")
    
    try:
        # Initialize components
        data_processor = AlcoholDataProcessor()
        sheets_client = GoogleSheetsClient()
        backup_restorer = AlcoholBackupRestorer()
        
        # List sheet GIDs to help with configuration
        print("\nListing available sheet GIDs...")
        sheets_client.list_sheet_gids()
        
        # Get existing data
        existing_data = sheets_client.get_existing_sheet_data()
        
        # Process existing data
        processed_products = backup_restorer.process_existing_data(existing_data)
        
        logger.info(f"Processing {len(processed_products)} products for duplicate removal...")
        
        # Find exact duplicates and track which rows to remove
        duplicates = data_processor.find_exact_duplicates(processed_products)
        
        # Track which rows are duplicates (to be removed)
        duplicate_row_indices = []
        if duplicates:
            # Create a set of duplicate product identifiers
            duplicate_identifiers = set()
            for duplicate_group in duplicates:
                for product in duplicate_group:
                    # Create unique identifier for the product
                    identifier = f"{product.get('brand_name', '').lower()}|{product.get('product_name', '').lower()}"
                    duplicate_identifiers.add(identifier)
            
            # Find the row indices of duplicates (keep the first occurrence, remove others)
            seen_identifiers = set()
            for i, product in enumerate(processed_products):
                identifier = f"{product.get('brand_name', '').lower()}|{product.get('product_name', '').lower()}"
                if identifier in duplicate_identifiers:
                    if identifier in seen_identifiers:
                        # This is a duplicate, mark for removal
                        duplicate_row_indices.append(i)
                    else:
                        # First occurrence, keep it
                        seen_identifiers.add(identifier)
        
        # Remove exact duplicates from the processed products list
        cleaned_products = data_processor.remove_duplicates(
            processed_products, 
            use_exact_matching=CONFIG["use_exact_duplicate_matching"]
        )
        
        # Calculate statistics
        removed_count = len(processed_products) - len(cleaned_products)
        logger.info(f"Removed {removed_count} duplicate entries")
        logger.info(f"Final product count: {len(cleaned_products)}")
        
        # Update sheet if enabled
        if CONFIG["run_sheet_update"]:
            if CONFIG["test_mode"]:
                logger.info("TEST MODE: Would update sheet with cleaned data")
                logger.info(f"Would remove {len(duplicate_row_indices)} duplicate rows")
            else:
                print(f"\nWARNING: About to update Google Sheet with cleaned data for {len(cleaned_products)} products")
                print(f"Removed {removed_count} duplicate entries")
                print(f"Will remove {len(duplicate_row_indices)} duplicate rows")
                confirm = input("Type 'YES' to proceed with the update: ")
                if confirm == 'YES':
                    sheets_client.update_sheet(cleaned_products, existing_data, duplicate_row_indices)
                    logger.info("Successfully updated sheet with cleaned data")
                else:
                    logger.info("Update cancelled by user")
        else:
            logger.info("SHEET UPDATE DISABLED: Would update sheet with cleaned data")
        
        # Summary
        logger.info("\n" + "="*50)
        logger.info("DUPLICATE REMOVAL SUMMARY")
        logger.info("="*50)
        logger.info(f"Original products: {len(processed_products)}")
        logger.info(f"Products after cleaning: {len(cleaned_products)}")
        logger.info(f"Duplicates removed: {removed_count}")
        logger.info(f"Duplicate rows to remove: {len(duplicate_row_indices)}")
        logger.info(f"Removal rate: {(removed_count/len(processed_products)*100):.1f}%" if processed_products else "0%")
        logger.info("="*50)
        
    except Exception as e:
        logger.error(f"Error during duplicate removal: {e}")
        raise

if __name__ == "__main__":
    main() 