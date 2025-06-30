"""
Run Backup Restoration Only
==========================

Simple script to run only the backup restoration functionality.
This is useful when you just want to restore data from backup without running other operations.

HOW TO RUN:
1. Activate virtual environment:
   cd /Users/nick/celiapp-official/scripting
   source venv/bin/activate

2. Run the script:
   cd scripts
   python run_backup_restoration.py

3. Configure settings in alcohol_data_config.py before running

REQUIREMENTS:
- Python 3.7+
- Virtual environment with required packages
- Google Sheets API credentials
- Backup sheet with data to restore from
"""

import os
import sys
import logging
from alcohol_data_config import CONFIG
from alcohol_backup_restorer import AlcoholBackupRestorer
from alcohol_sheets_client import GoogleSheetsClient

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def main():
    """Run backup restoration only"""
    logger.info("Starting backup restoration only...")
    
    try:
        # Initialize components
        backup_restorer = AlcoholBackupRestorer()
        sheets_client = GoogleSheetsClient()
        
        # List sheet GIDs to help with configuration
        print("\nListing available sheet GIDs...")
        sheets_client.list_sheet_gids()
        
        # Get existing data
        existing_data = sheets_client.get_existing_sheet_data()
        
        # Process existing data
        processed_products = backup_restorer.process_existing_data(existing_data)
        
        # Get backup data
        backup_data, backup_column_mapping = backup_restorer.get_backup_sheet_data()
        
        # Restore data
        restored_products = backup_restorer.restore_from_backup(processed_products, backup_data, backup_column_mapping)
        
        # Update sheet if enabled
        if CONFIG["run_sheet_update"]:
            if CONFIG["test_mode"]:
                logger.info("TEST MODE: Would update sheet with restored data")
            else:
                print(f"\nWARNING: About to update Google Sheet with restored data for {len(restored_products)} products")
                confirm = input("Type 'YES' to proceed with the update: ")
                if confirm == 'YES':
                    sheets_client.update_sheet(restored_products, existing_data)
                    logger.info("Successfully updated sheet with restored data")
                else:
                    logger.info("Update cancelled by user")
        else:
            logger.info("SHEET UPDATE DISABLED: Would update sheet with restored data")
        
        # Summary
        logger.info("\n" + "="*50)
        logger.info("BACKUP RESTORATION SUMMARY")
        logger.info("="*50)
        logger.info(f"Products processed: {len(processed_products)}")
        logger.info(f"Products with restored data: {len([p for p in restored_products if p.get('gluten_free_score') or p.get('descriptors')])}")
        logger.info("="*50)
        
    except Exception as e:
        logger.error(f"Error during backup restoration: {e}")
        raise

if __name__ == "__main__":
    main() 