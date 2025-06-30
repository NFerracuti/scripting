"""
Alcohol Data Integrity Main Script
==================================

Main orchestration script for alcohol data integrity operations.
This script coordinates all the different modules to perform data integrity improvements.

HOW TO RUN:
1. Activate virtual environment:
   cd /Users/nick/celiapp-official/scripting
   source venv/bin/activate

2. Run the script:
   cd scripts
   python alcohol_data_integrity_main.py

3. Configure settings in alcohol_data_config.py before running

REQUIREMENTS:
- Python 3.7+
- Virtual environment with required packages
- Google Sheets API credentials
- OpenAI API key (optional, for AI features)
"""

import os
import sys
import logging
from typing import List, Dict, Any
from alcohol_data_config import CONFIG
from alcohol_sheets_client import GoogleSheetsClient
from alcohol_data_processor import AlcoholDataProcessor
from alcohol_ai_processor import AlcoholAIProcessor
from alcohol_backup_restorer import AlcoholBackupRestorer

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AlcoholDataIntegrityOrchestrator:
    """Main orchestrator for alcohol data integrity operations"""
    
    def __init__(self):
        self.sheets_client = GoogleSheetsClient()
        self.data_processor = AlcoholDataProcessor()
        self.ai_processor = AlcoholAIProcessor()
        self.backup_restorer = AlcoholBackupRestorer()
    
    def print_configuration(self):
        """Print the current configuration in a readable format"""
        print("\n" + "="*60)
        print("CURRENT CONFIGURATION")
        print("="*60)
        print(f"Spreadsheet ID: {CONFIG['spreadsheet_id']}")
        print(f"Sheet GID: {CONFIG['sheet_gid']}")
        print(f"Backup Sheet GID: {CONFIG['backup_sheet_gid']}")
        print(f"Test Mode: {CONFIG['test_mode']}")
        print(f"Test Product Extraction: {CONFIG['test_product_extraction']}")
        print()
        print("OPERATIONS:")
        print(f"  AI Brand Extraction: {'✓' if CONFIG['run_ai_brand_extraction'] else '✗'}")
        print(f"  Product Name Filling: {'✓' if CONFIG['run_product_name_filling'] else '✗'}")
        print(f"  Duplicate Detection: {'✓' if CONFIG['run_duplicate_detection'] else '✗'}")
        print(f"  Brand Normalization: {'✓' if CONFIG['run_brand_normalization'] else '✗'}")
        print(f"  Sheet Update: {'✓' if CONFIG['run_sheet_update'] else '✗'}")
        print(f"  Backup Restoration: {'✓' if CONFIG['run_backup_restoration'] else '✗'}")
        print()
        print("AI SETTINGS:")
        print(f"  Use AI Brand Extraction: {'✓' if CONFIG['use_ai_brand_extraction'] else '✗'}")
        print(f"  Use AI Product Extraction: {'✓' if CONFIG['use_ai_product_extraction'] else '✗'}")
        print(f"  OpenAI API Key: {'✓' if CONFIG['openai_api_key'] else '✗'}")
        print("="*60)

    def run_ai_brand_extraction(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run AI brand extraction if enabled"""
        if not CONFIG["run_ai_brand_extraction"]:
            logger.info("AI brand extraction disabled")
            return products
        
        logger.info("Running AI brand extraction...")
        
        # Estimate costs first
        cost_estimate = self.ai_processor.estimate_ai_costs(products)
        logger.info(f"AI cost estimate: ${cost_estimate['total_cost']} for {cost_estimate['products_to_process']} products")
        
        if cost_estimate['products_to_process'] > 0:
            # Ask for confirmation if costs are significant
            if cost_estimate['total_cost'] > 1.0:  # More than $1
                print(f"\nWARNING: AI processing will cost approximately ${cost_estimate['total_cost']}")
                confirm = input("Type 'YES' to proceed with AI processing: ")
                if confirm != 'YES':
                    logger.info("AI processing cancelled by user")
                    return products
        
        # Run the extraction
        return self.ai_processor.extract_brand_from_product_name_batch(products)

    def run_product_name_filling(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run product name filling if enabled"""
        if not CONFIG["run_product_name_filling"]:
            logger.info("Product name filling disabled")
            return products
        
        logger.info("Running product name filling...")
        return self.data_processor.fill_missing_product_names(products)[0]

    def run_exact_duplicate_removal(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run exact duplicate removal if enabled"""
        if not CONFIG["run_exact_duplicate_removal"]:
            logger.info("Exact duplicate removal disabled")
            return products
        
        logger.info("Running exact duplicate removal...")
        return self.data_processor.remove_duplicates(products, use_exact_matching=True)

    def run_duplicate_detection(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run duplicate detection and merging if enabled"""
        if not CONFIG["run_duplicate_detection"]:
            logger.info("Duplicate detection disabled")
            return products
        
        logger.info("Running duplicate detection...")
        
        # Find duplicates
        duplicates = self.data_processor.find_duplicates(products, use_exact_matching=CONFIG["use_exact_duplicate_matching"])
        
        if duplicates:
            # Merge duplicates
            return self.data_processor.merge_duplicates(duplicates)
        else:
            logger.info("No duplicates found")
            return products

    def run_brand_normalization(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run brand normalization if enabled"""
        if not CONFIG["run_brand_normalization"]:
            logger.info("Brand normalization disabled")
            return products
        
        logger.info("Running brand normalization...")
        return self.data_processor.normalize_brand_alternatives(products)

    def run_backup_restoration(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Run backup restoration if enabled"""
        if not CONFIG["run_backup_restoration"]:
            logger.info("Backup restoration disabled")
            return products
        
        logger.info("Running backup restoration...")
        
        # Get backup data
        backup_data, backup_column_mapping = self.backup_restorer.get_backup_sheet_data()
        
        # Restore data
        return self.backup_restorer.restore_from_backup(products, backup_data, backup_column_mapping)

    def run_sheet_update(self, products: List[Dict[str, Any]], existing_data: List[List[str]]):
        """Run sheet update if enabled"""
        if not CONFIG["run_sheet_update"]:
            logger.info("Sheet update disabled")
            return
        
        logger.info("Running sheet update...")
        
        if CONFIG["test_mode"]:
            logger.info("TEST MODE: Would update sheet with processed data")
            return
        
        # Ask for confirmation before updating
        print(f"\nWARNING: About to update Google Sheet with processed data for {len(products)} products")
        confirm = input("Type 'YES' to proceed with the update: ")
        if confirm != 'YES':
            logger.info("Sheet update cancelled by user")
            return
        
        # Update the sheet
        self.sheets_client.update_sheet(products, existing_data)
        logger.info("Successfully updated sheet")

    def process_existing_data(self, existing_data: List[List[str]]) -> List[Dict[str, Any]]:
        """Process existing sheet data into structured format"""
        return self.backup_restorer.process_existing_data(existing_data)

    def run(self):
        """Main execution method"""
        logger.info("Starting alcohol database data integrity improvement...")
        
        try:
            # 1. Get existing sheet data
            existing_data = self.sheets_client.get_existing_sheet_data()
            
            # 2. Process existing sheet data
            processed_products = self.process_existing_data(existing_data)
            
            # 3. Run enabled operations
            if CONFIG["run_ai_brand_extraction"]:
                processed_products = self.run_ai_brand_extraction(processed_products)
            
            if CONFIG["run_product_name_filling"]:
                processed_products = self.run_product_name_filling(processed_products)
            
            if CONFIG["run_exact_duplicate_removal"]:
                processed_products = self.run_exact_duplicate_removal(processed_products)
            
            if CONFIG["run_duplicate_detection"]:
                processed_products = self.run_duplicate_detection(processed_products)
            
            if CONFIG["run_brand_normalization"]:
                processed_products = self.run_brand_normalization(processed_products)
            
            if CONFIG["run_backup_restoration"]:
                processed_products = self.run_backup_restoration(processed_products)
            
            # 4. Update sheet if enabled
            self.run_sheet_update(processed_products, existing_data)
            
            # 5. Summary
            logger.info("\n" + "="*50)
            logger.info("DATA INTEGRITY IMPROVEMENT SUMMARY")
            logger.info("="*50)
            logger.info(f"Products processed: {len(processed_products)}")
            logger.info(f"AI Brand Extraction: {'✓' if CONFIG['run_ai_brand_extraction'] else '✗'}")
            logger.info(f"Product Name Filling: {'✓' if CONFIG['run_product_name_filling'] else '✗'}")
            logger.info(f"Exact Duplicate Removal: {'✓' if CONFIG['run_exact_duplicate_removal'] else '✗'}")
            logger.info(f"Duplicate Detection: {'✓' if CONFIG['run_duplicate_detection'] else '✗'}")
            logger.info(f"Brand Normalization: {'✓' if CONFIG['run_brand_normalization'] else '✗'}")
            logger.info(f"Backup Restoration: {'✓' if CONFIG['run_backup_restoration'] else '✗'}")
            logger.info(f"Sheet Update: {'✓' if CONFIG['run_sheet_update'] else '✗'}")
            logger.info("="*50)
            
        except Exception as e:
            logger.error(f"Error during data integrity improvement: {e}")
            raise

def main():
    """Main entry point"""
    # Print current configuration
    orchestrator = AlcoholDataIntegrityOrchestrator()
    orchestrator.print_configuration()
    
    # List sheet GIDs first to help with configuration
    print("\nListing available sheet GIDs...")
    orchestrator.sheets_client.list_sheet_gids()
    
    # Run the main process
    orchestrator.run()

if __name__ == "__main__":
    main() 