"""
Debug Column Mapping
===================

Debug script to help identify column mapping issues and show what headers are being detected.
This script will read the sheet data and show the column mapping without making any changes.

HOW TO RUN:
1. Activate virtual environment:
   cd /Users/nick/celiapp-official/scripting
   source venv/bin/activate

2. Run the script:
   cd scripts
   python debug_column_mapping.py

3. Review the output to see how columns are being mapped
"""

import os
import sys
import logging
from alcohol_data_config import CONFIG
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
    """Debug column mapping"""
    logger.info("Starting column mapping debug...")
    
    try:
        # Initialize components
        sheets_client = GoogleSheetsClient()
        backup_restorer = AlcoholBackupRestorer()
        
        # List sheet GIDs to help with configuration
        print("\nListing available sheet GIDs...")
        sheets_client.list_sheet_gids()
        
        # Get existing data
        print("\n" + "="*60)
        print("ANALYZING MAIN SHEET")
        print("="*60)
        existing_data = sheets_client.get_existing_sheet_data()
        
        if existing_data:
            headers = existing_data[0]
            print(f"Headers found: {headers}")
            print(f"Number of columns: {len(headers)}")
            
            # Get column mapping
            column_mapping = sheets_client.get_column_mapping(headers)
            print(f"Column mapping: {column_mapping}")
            
            # Show sample data
            if len(existing_data) > 1:
                print(f"\nSample data (first 3 rows):")
                for i, row in enumerate(existing_data[1:4]):
                    print(f"Row {i+1}: {row}")
            
            # Process existing data to see what gets extracted
            processed_products = backup_restorer.process_existing_data(existing_data)
            print(f"\nProcessed {len(processed_products)} products")
            
            if processed_products:
                print(f"\nSample processed product:")
                sample_product = processed_products[0]
                for key, value in sample_product.items():
                    print(f"  {key}: '{value}'")
        
        # Get backup data
        print("\n" + "="*60)
        print("ANALYZING BACKUP SHEET")
        print("="*60)
        backup_data, backup_column_mapping = backup_restorer.get_backup_sheet_data()
        
        if backup_data:
            headers = backup_data[0]
            print(f"Backup headers found: {headers}")
            print(f"Number of columns: {len(headers)}")
            print(f"Backup column mapping: {backup_column_mapping}")
            
            # Show sample backup data
            if len(backup_data) > 1:
                print(f"\nSample backup data (first 3 rows):")
                for i, row in enumerate(backup_data[1:4]):
                    print(f"Row {i+1}: {row}")
        
        print("\n" + "="*60)
        print("DEBUG SUMMARY")
        print("="*60)
        print("Review the column mappings above to ensure they match your expectations.")
        print("If columns are not being mapped correctly, check the header names.")
        print("="*60)
        
    except Exception as e:
        logger.error(f"Error during column mapping debug: {e}")
        raise

if __name__ == "__main__":
    main() 