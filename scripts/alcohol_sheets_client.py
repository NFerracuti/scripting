"""
Alcohol Data Google Sheets Client
================================

Handles all Google Sheets operations for alcohol data integrity scripts.

HOW TO USE:
This module is imported by other scripts to handle Google Sheets operations.
You don't run this file directly.

FUNCTIONS:
- Authentication and credentials management
- Reading data from Google Sheets
- Writing data to Google Sheets
- Backup operations
- Column mapping utilities

REQUIREMENTS:
- Google Sheets API credentials
- Service account JSON file or environment variables
- Proper permissions on target spreadsheets
"""

import os
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from googleapiclient.discovery import build
from google.oauth2 import service_account
from alcohol_data_config import CONFIG

logger = logging.getLogger(__name__)

class GoogleSheetsClient:
    """Client for Google Sheets operations"""
    
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

    def get_sheet_name_by_gid(self, gid: int) -> str:
        """Get sheet name by GID"""
        try:
            spreadsheet = self.sheets_service.get(
                spreadsheetId=CONFIG["spreadsheet_id"]
            ).execute()
            
            for sheet in spreadsheet['sheets']:
                if sheet['properties']['sheetId'] == gid:
                    return sheet['properties']['title']
                    
            raise ValueError(f"No sheet found with GID {gid}")
            
        except Exception as e:
            logger.error(f"Error getting sheet name for GID {gid}: {e}")
            raise

    def get_sheet_data(self, gid: int, range_name: str = "A:AZ") -> List[List[str]]:
        """Get data from a specific sheet by GID"""
        try:
            sheet_name = self.get_sheet_name_by_gid(gid)
            
            result = self.sheets_service.values().get(
                spreadsheetId=CONFIG["spreadsheet_id"],
                range=f"'{sheet_name}'!{range_name}"
            ).execute()
            
            rows = result.get('values', [])
            if not rows:
                logger.warning(f"No data found in sheet {sheet_name}")
                return []
                
            logger.info(f"Found {len(rows) - 1} rows in sheet {sheet_name}")
            return rows
            
        except Exception as e:
            logger.error(f"Error getting sheet data for GID {gid}: {e}")
            raise

    def get_existing_sheet_data(self) -> List[List[str]]:
        """Get existing data from main sheet"""
        return self.get_sheet_data(CONFIG["sheet_gid"])

    def get_backup_sheet_data(self) -> List[List[str]]:
        """Get data from backup sheet"""
        return self.get_sheet_data(CONFIG["backup_sheet_gid"])

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

    def remove_duplicate_rows(self, duplicate_row_indices: List[int]):
        """Remove duplicate rows from the sheet"""
        if not duplicate_row_indices:
            logger.info("No duplicate rows to remove")
            return
        
        logger.info(f"Removing {len(duplicate_row_indices)} duplicate rows")
        
        # Get sheet name
        sheet_name = self.get_sheet_name_by_gid(CONFIG["sheet_gid"])
        
        # Sort row indices in descending order to avoid shifting issues
        duplicate_row_indices.sort(reverse=True)
        
        # Prepare batch delete requests
        requests = []
        for row_index in duplicate_row_indices:
            # Convert to 1-indexed sheet row number (add 1 for header, add 1 for 1-indexing)
            sheet_row_number = row_index + 2
            
            requests.append({
                'deleteDimension': {
                    'range': {
                        'sheetId': CONFIG["sheet_gid"],
                        'dimension': 'ROWS',
                        'startIndex': sheet_row_number - 1,  # 0-indexed for API
                        'endIndex': sheet_row_number  # 0-indexed for API
                    }
                }
            })
        
        # Execute batch delete
        body = {'requests': requests}
        self.sheets_service.batchUpdate(
            spreadsheetId=CONFIG["spreadsheet_id"],
            body=body
        ).execute()
        
        logger.info(f"Successfully removed {len(duplicate_row_indices)} duplicate rows")

    def update_sheet(self, processed_products: List[Dict[str, Any]], existing_data: List[List[str]], duplicate_row_indices: List[int] = None):
        """Update the Google Sheet with processed data while preserving formulas and structure"""
        logger.info("Updating Google Sheet with formula preservation...")
        
        if not existing_data or len(existing_data) < 1:
            logger.error("No existing data found to update")
            return
        
        # Get the original headers and create column mapping
        original_headers = existing_data[0]
        column_mapping = self.get_column_mapping(original_headers)
        
        logger.info(f"Original headers: {original_headers}")
        logger.info(f"Column mapping: {column_mapping}")
        
        # Get sheet name
        sheet_name = self.get_sheet_name_by_gid(CONFIG["sheet_gid"])
        
        # Step 1: Store formulas before making changes
        logger.info("Storing existing formulas...")
        formulas_to_restore = self._store_formulas(existing_data, column_mapping)
        
        # Step 2: Remove duplicate rows first (if any)
        if duplicate_row_indices and len(duplicate_row_indices) > 0:
            logger.info(f"Removing {len(duplicate_row_indices)} duplicate rows first...")
            self.remove_duplicate_rows(duplicate_row_indices)
        
        # Step 3: Rebuild the entire sheet with correct data
        logger.info("Rebuilding sheet with correct data structure...")
        
        # Create the new sheet data
        new_sheet_data = [original_headers]  # Keep original headers
        
        for product in processed_products:
            # Create a new row with the same structure as original
            new_row = [''] * len(original_headers)
            
            # Fill in the data from the processed product
            for field_name, col_idx in column_mapping.items():
                if col_idx < len(new_row):
                    new_row[col_idx] = product.get(field_name, '')
            
            new_sheet_data.append(new_row)
        
        # Step 4: Update the entire sheet range
        # Determine the range to update
        last_col_letter = chr(65 + len(original_headers) - 1) if len(original_headers) <= 26 else 'Z'
        range_name = f"'{sheet_name}'!A:{last_col_letter}"
        
        logger.info(f"Updating range: {range_name}")
        logger.info(f"Updating {len(new_sheet_data)} rows with {len(original_headers)} columns each")
        
        # Clear the existing data first
        self.sheets_service.values().clear(
            spreadsheetId=CONFIG["spreadsheet_id"],
            range=range_name
        ).execute()
        
        # Write the new data
        body = {'values': new_sheet_data}
        self.sheets_service.values().update(
            spreadsheetId=CONFIG["spreadsheet_id"],
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        
        # Step 5: Restore formulas
        if formulas_to_restore:
            logger.info("Restoring formulas...")
            self._restore_formulas(formulas_to_restore, sheet_name)
        
        logger.info(f"Successfully updated sheet with {len(processed_products)} products")
        logger.info(f"Column mapping used: {column_mapping}")

    def _store_formulas(self, existing_data: List[List[str]], column_mapping: Dict[str, int]) -> List[Dict[str, Any]]:
        """Store formulas from the existing data"""
        formulas = []
        
        # Look for columns that might contain formulas (like image display)
        formula_columns = []
        for i, header in enumerate(existing_data[0]):
            header_lower = header.lower()
            if 'image' in header_lower and 'display' in header_lower:
                formula_columns.append(i)
            elif 'formula' in header_lower:
                formula_columns.append(i)
        
        if not formula_columns:
            logger.info("No formula columns detected")
            return formulas
        
        logger.info(f"Detected formula columns: {formula_columns}")
        
        # Store formulas from these columns
        for row_idx, row in enumerate(existing_data[1:], 1):  # Skip header
            for col_idx in formula_columns:
                if col_idx < len(row) and row[col_idx]:
                    # Check if this looks like a formula
                    cell_value = str(row[col_idx])
                    if cell_value.startswith('='):
                        formulas.append({
                            'row': row_idx,
                            'col': col_idx,
                            'formula': cell_value
                        })
        
        logger.info(f"Stored {len(formulas)} formulas")
        return formulas

    def _restore_formulas(self, formulas: List[Dict[str, Any]], sheet_name: str):
        """Restore formulas to the sheet"""
        if not formulas:
            return
        
        # Prepare batch updates for formulas
        batch_updates = []
        
        for formula_info in formulas:
            col_letter = chr(65 + formula_info['col']) if formula_info['col'] < 26 else chr(64 + formula_info['col'] // 26) + chr(65 + formula_info['col'] % 26)
            cell_range = f"'{sheet_name}'!{col_letter}{formula_info['row'] + 1}"  # +1 for 1-indexed
            
            batch_updates.append({
                'range': cell_range,
                'values': [[formula_info['formula']]]
            })
        
        # Execute batch update for formulas
        body = {
            'valueInputOption': 'USER_ENTERED',  # Use USER_ENTERED to preserve formulas
            'data': batch_updates
        }
        
        self.sheets_service.values().batchUpdate(
            spreadsheetId=CONFIG["spreadsheet_id"],
            body=body
        ).execute()
        
        logger.info(f"Restored {len(formulas)} formulas")

    def _rebuild_products_for_updated_sheet(self, original_products: List[Dict[str, Any]], removed_row_indices: List[int]) -> List[Dict[str, Any]]:
        """Rebuild the products list to match the updated sheet after row removal"""
        logger.info("Rebuilding products list to match updated sheet structure...")
        
        # Create a set of removed indices for fast lookup
        removed_indices_set = set(removed_row_indices)
        
        # Filter out the removed products
        remaining_products = []
        for i, product in enumerate(original_products):
            if i not in removed_indices_set:
                remaining_products.append(product)
        
        logger.info(f"Rebuilt products list: {len(remaining_products)} products remaining from {len(original_products)} original")
        return remaining_products

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

    def get_column_mapping(self, headers: List[str]) -> Dict[str, int]:
        """Get column mapping from headers with improved detection"""
        column_mapping = {}
        
        logger.info(f"Analyzing headers: {headers}")
        
        for i, header in enumerate(headers):
            header_lower = header.lower().strip()
            header_clean = header_lower.replace('_', ' ').replace('-', ' ')
            
            # More specific matching to avoid false positives
            if 'brand' in header_clean and ('name' in header_clean or header_clean == 'brand'):
                column_mapping['brand_name'] = i
                logger.info(f"Found brand_name column at position {i}: '{header}'")
            elif 'product' in header_clean and ('name' in header_clean or header_clean == 'product'):
                column_mapping['product_name'] = i
                logger.info(f"Found product_name column at position {i}: '{header}'")
            elif 'descriptor' in header_clean:
                column_mapping['descriptors'] = i
                logger.info(f"Found descriptors column at position {i}: '{header}'")
            elif 'gluten' in header_clean and 'score' in header_clean:
                column_mapping['gluten_free_score'] = i
                logger.info(f"Found gluten_free_score column at position {i}: '{header}'")
            elif 'gluten' in header_clean and 'free' in header_clean:
                column_mapping['gluten_free_score'] = i
                logger.info(f"Found gluten_free_score column at position {i}: '{header}'")
            elif 'id' in header_clean and header_clean == 'id':
                column_mapping['id'] = i
                logger.info(f"Found id column at position {i}: '{header}'")
            elif 'lcbo' in header_clean and 'id' in header_clean:
                column_mapping['lcbo_id'] = i
                logger.info(f"Found lcbo_id column at position {i}: '{header}'")
            elif 'image' in header_clean and 'url' in header_clean:
                column_mapping['image_url'] = i
                logger.info(f"Found image_url column at position {i}: '{header}'")
            elif 'price' in header_clean:
                column_mapping['price'] = i
                logger.info(f"Found price column at position {i}: '{header}'")
            elif 'category' in header_clean and 'sub' not in header_clean:
                column_mapping['category'] = i
                logger.info(f"Found category column at position {i}: '{header}'")
            elif 'subcategory' in header_clean or ('sub' in header_clean and 'category' in header_clean):
                column_mapping['subcategory'] = i
                logger.info(f"Found subcategory column at position {i}: '{header}'")
            elif 'source' in header_clean:
                column_mapping['source'] = i
                logger.info(f"Found source column at position {i}: '{header}'")
        
        logger.info(f"Final column mapping: {column_mapping}")
        return column_mapping 