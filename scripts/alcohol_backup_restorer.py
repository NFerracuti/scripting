"""
Alcohol Data Backup Restorer
============================

Handles backup restoration and fuzzy matching operations for alcohol data integrity.

HOW TO USE:
This module is imported by other scripts to handle backup restoration operations.
You don't run this file directly.

FUNCTIONS:
- Fuzzy matching algorithms for data restoration
- Backup data processing and parsing
- Data restoration logic
- Existing data processing and validation
- Column mapping for different sheet formats

REQUIREMENTS:
- Google Sheets API access
- Backup sheet with data to restore from
- Proper column mapping between sheets
"""

import logging
import difflib
from typing import Dict, List, Any, Optional, Tuple
from alcohol_data_config import CONFIG
from alcohol_sheets_client import GoogleSheetsClient

logger = logging.getLogger(__name__)

class AlcoholBackupRestorer:
    """Handles backup restoration and fuzzy matching operations"""
    
    def __init__(self):
        self.sheets_client = GoogleSheetsClient()
    
    def get_backup_sheet_data(self) -> Tuple[List[List[str]], Dict[str, int]]:
        """Get data from the backup sheet tab and return column mapping"""
        logger.info("Getting backup sheet data...")
        
        try:
            backup_data = self.sheets_client.get_backup_sheet_data()
            logger.info(f"Retrieved {len(backup_data)} rows from backup sheet")
            
            # Create column mapping from headers
            column_mapping = {}
            if backup_data:
                headers = backup_data[0]
                logger.info(f"Backup sheet headers: {headers}")
                
                column_mapping = self.sheets_client.get_column_mapping(headers)
                logger.info(f"Backup column mapping: {column_mapping}")
            
            return backup_data, column_mapping
            
        except Exception as e:
            logger.error(f"Error getting backup sheet data: {e}")
            raise

    def get_current_sheet_headers(self) -> Dict[str, int]:
        """Get column mapping from current sheet headers"""
        logger.info("Getting current sheet headers...")
        
        try:
            existing_data = self.sheets_client.get_existing_sheet_data()
            column_mapping = {}
            
            if existing_data:
                headers = existing_data[0]
                logger.info(f"Current sheet headers: {headers}")
                
                column_mapping = self.sheets_client.get_column_mapping(headers)
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

    def find_best_match(self, current_product: Dict[str, Any], backup_products: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], float]:
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
                    backup_product = {}
                    
                    # Map each field using the column mapping
                    for field_name, col_idx in backup_column_mapping.items():
                        if col_idx < len(row):
                            backup_product[field_name] = row[col_idx]
                        else:
                            backup_product[field_name] = ''
                    
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

    def process_existing_data(self, existing_data: List[List[str]]) -> List[Dict[str, Any]]:
        """Process existing sheet data into structured format using proper column mapping"""
        processed_existing = []
        
        if len(existing_data) > 1:  # Has data beyond header
            headers = existing_data[0] if existing_data else []
            
            # Get proper column mapping
            column_mapping = self.sheets_client.get_column_mapping(headers)
            logger.info(f"Processing existing data with column mapping: {column_mapping}")
            
            for row in existing_data[1:]:  # Skip header
                if len(row) >= 3:  # Has at least required fields
                    # Create product dictionary using column mapping
                    existing_product = {}
                    
                    # Map each field using the column mapping
                    for field_name, col_idx in column_mapping.items():
                        if col_idx < len(row):
                            existing_product[field_name] = row[col_idx]
                        else:
                            existing_product[field_name] = ''
                    
                    # Add source field
                    existing_product['source'] = 'Existing'
                    
                    # Only add products that have at least a brand_name or product_name
                    if existing_product.get('brand_name') or existing_product.get('product_name'):
                        processed_existing.append(existing_product)
        
        logger.info(f"Processed {len(processed_existing)} existing products")
        return processed_existing

    def run_backup_restoration(self) -> List[Dict[str, Any]]:
        """Run the complete backup restoration process"""
        logger.info("Starting backup restoration process...")
        
        # Get existing data
        existing_data = self.sheets_client.get_existing_sheet_data()
        
        # Process existing data
        processed_existing = self.process_existing_data(existing_data)
        
        # Get backup data
        backup_data, backup_column_mapping = self.get_backup_sheet_data()
        
        # Restore gluten free scores and descriptors
        restored_products = self.restore_from_backup(processed_existing, backup_data, backup_column_mapping)
        
        return restored_products 