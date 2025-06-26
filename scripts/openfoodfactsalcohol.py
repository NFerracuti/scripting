"""
Open Food Facts Script
======================

Scrapes product data from Open Food Facts API and writes it to Google Sheets.
"""
import os
import sys
import requests
import json
from pprint import pprint
from googleapiclient.discovery import build
import logging
from dotenv import load_dotenv
import time
import random
from google.oauth2 import service_account

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sysconfigs.client_creds import get_google_sheets_credentials

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
    "spreadsheet_id": "1fJOPwK8vWM4zR3Ar_ipkvwbJaMT0IPzoeHLOeRRD-vc",
    "range_name": "Sheet1!A1:AZ"  # Updated sheet name to Sheet1
}


class OpenFoodFactsAPI:
    def __init__(self):
        self.base_url = "https://world.openfoodfacts.org/api/v2"
        self.headers = {
            'User-Agent': 'CeliApp - Mac OS X - Version 1.0'
        }

    def search_products(self, category: str, page: int = 1, page_size: int = 50) -> dict:
        """Search products by category"""
        url = f"{self.base_url}/search"

        params = {
            "categories_tags": f"en:{category}",  # Add 'en:' prefix
            "countries_tags": "en:canada",  # Filter for products from Canada
            "page": page,
            "page_size": page_size,
            "fields": "code,product_name,brands,labels,image_url,ingredients_text,ingredients_analysis_tags,nutriments,categories,allergens,traces,brand_owner,ingredients_tags,quantity,serving_size,packaging,countries,stores,manufacturing_places,purchase_places,nova_group,ecoscore_grade,nutriscore_grade,additives_tags,states_tags,data_quality_tags,data_quality_warnings_tags,data_quality_errors_tags,data_quality_info_tags,languages,lang,labels_tags,categories_tags"
        }

        logger.info(f"Searching for {category} products from Canada (page {page})")

        try:
            response = requests.get(url, params=params, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                if data and 'products' in data:
                    total_products = data.get('count', 0)
                    total_pages = (total_products + page_size - 1) // page_size
                    logger.info(f"Found {total_products} total products")
                    logger.info(f"Current page: {page} of {total_pages}")
                    return data
                else:
                    logger.warning(f"No products found in response: {data}")
                    return {}
            else:
                logger.error(f"Error response: {response.text}")
                return {}
        except Exception as e:
            logger.error(f"Error searching products: {e}")
            return {}


def process_product(product: dict) -> list:
    """Convert product data to row format with only Open Food Facts fields"""

    # Helper function to convert lists to strings
    def list_to_string(lst):
        if isinstance(lst, list):
            return ', '.join(str(item) for item in lst)
        return str(lst) if lst is not None else ''

    # Get all headers to ensure correct order
    headers = get_headers_from_api()

    # Create a row with values in the same order as headers
    row = []
    for header in headers:
        if header.startswith('nutriment_'):
            # Handle nutriments specially since they're nested
            nutriments = product.get('nutriments', {})
            nutriment_name = header.replace('nutriment_', '')
            value = nutriments.get(nutriment_name, '')
        else:
            # Get the value for this header from the product
            value = product.get(header, '')

            # Convert lists to strings
            if isinstance(value, list):
                value = list_to_string(value)
            # Convert dictionaries to strings
            elif isinstance(value, dict):
                value = str(value)

        row.append(value)

    # Add debug logging
    logger.debug(f"Processing product: {product.get('code', 'NO_CODE')} - {product.get('product_name', 'NO_NAME')}")
    logger.debug(f"Row has {len(row)} columns, expected {len(headers)}")

    return row


def get_google_sheets_credentials():
    """Get Google Sheets credentials from environment variable"""
    try:
        # Get credentials from environment variable
        credentials_json = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')
        if not credentials_json:
            raise ValueError("GOOGLE_APPLICATION_CREDENTIALS environment variable not found")

        # Parse the JSON string
        credentials_info = json.loads(credentials_json)

        # Create credentials object
        credentials = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )

        logger.info(f"Successfully loaded credentials for service account: {credentials.service_account_email}")
        return credentials
    except ValueError as e:
        logger.error(f"Environment variable error: {e}")
        logger.error(f"Credentials value: {credentials_json[:100]}...")  # Log first 100 chars for debugging
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in credentials: {e}")
        logger.error(f"Credentials value: {credentials_json[:100]}...")  # Log first 100 chars for debugging
        raise
    except Exception as e:
        logger.error(f"Error loading credentials: {e}")
        raise


def verify_sheet_access():
    """Verify access to the Google Sheet and log permissions"""
    try:
        credentials = get_google_sheets_credentials()
        service = build('sheets', 'v4', credentials=credentials)

        # Get spreadsheet metadata
        spreadsheet = service.spreadsheets().get(
            spreadsheetId=CONFIG["spreadsheet_id"]
        ).execute()

        logger.info(f"Successfully accessed sheet: {spreadsheet.get('properties', {}).get('title')}")
        return True
    except Exception as e:
        logger.error(f"Sheet access verification failed: {e}")
        return False


def update_sheet(products_data, is_first_batch=False):
    """Update Google Sheet with scraped data"""
    try:
        credentials = get_google_sheets_credentials()
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()

        # First verify we can access the sheet
        try:
            sheet.get(
                spreadsheetId=CONFIG["spreadsheet_id"]
            ).execute()
            logger.info("Successfully verified sheet access")
        except Exception as e:
            logger.error(f"Cannot access sheet: {e}")
            raise

        # Get all existing data to find the last row
        existing_data = sheet.values().get(
            spreadsheetId=CONFIG["spreadsheet_id"],
            range="Sheet1!A:AZ"  # Get all columns
        ).execute()

        # Find the last row with data
        last_row = 1  # Start at 1 to account for header row
        if 'values' in existing_data:
            last_row = len(existing_data['values'])

        # If this is the first batch, we want to start at row 2 (after headers)
        if is_first_batch:
            start_row = 2
        else:
            start_row = last_row + 1

        # Filter out products that already exist
        new_products = []
        duplicates = 0
        existing_codes = set()
        if 'values' in existing_data:
            # Skip header row
            existing_codes = {row[0] for row in existing_data['values'][1:] if row}

        for product in products_data:
            if product[0] not in existing_codes:  # product[0] is the product code
                new_products.append(product)
            else:
                duplicates += 1

        if duplicates > 0:
            logger.info(f"Skipped {duplicates} duplicate products")

        if not new_products:
            logger.info("No new products to add")
            return

        if is_first_batch:
            # Clear existing content
            sheet.values().clear(
                spreadsheetId=CONFIG["spreadsheet_id"],
                range="Sheet1!A1:AZ"
            ).execute()

            # Define headers for Open Food Facts fields
            headers = [[
                "code",
                "product_name",
                "brands",
                "brand_owner",
                "categories",
                "categories_tags",
                "labels",
                "labels_tags",
                "quantity",
                "serving_size",
                "packaging",
                "packaging_tags",
                "image_url",
                "ingredients_text",
                "ingredients_tags",
                "allergens",
                "traces",
                "ingredients_analysis_tags",
                "energy_kcal_100g",
                "proteins_100g",
                "carbohydrates_100g",
                "fat_100g",
                "fiber_100g",
                "sugars_100g",
                "salt_100g",
                "nutriscore_grade",
                "ecoscore_grade",
                "nova_group",
                "countries",
                "stores",
                "manufacturing_places",
                "purchase_places",
                "additives_tags",
                "ingredients_from_palm_oil_n",
                "states_tags",
                "data_quality_tags",
                "data_quality_warnings_tags",
                "data_quality_errors_tags",
                "data_quality_info_tags",
                "languages",
                "lang"
            ]]

            values = headers + new_products
            range_name = "Sheet1!A1"
        else:
            values = new_products
            range_name = f"Sheet1!A{start_row}"

        if values:  # Only update if we have data to add
            result = sheet.values().update(
                spreadsheetId=CONFIG["spreadsheet_id"],
                range=range_name,
                valueInputOption='RAW',
                body={'values': values}
            ).execute()

            # Log the number of columns in data vs what was written
            if values and len(values[0]) > 0:
                logger.info(f"Number of columns in data: {len(values[0])}")
                if 'updates' in result:
                    logger.info(f"Updated range: {result['updates'].get('updatedRange', 'unknown')}")

            logger.info(f"Batch updated successfully: {len(new_products)} new products added")

    except Exception as e:
        logger.error(f"Error updating Google Sheet: {e}")
        raise


def get_headers_from_api():
    """Get all possible fields from Open Food Facts API"""
    url = "https://world.openfoodfacts.org/api/v2/search"
    params = {
        "page_size": 1,  # Just get one product to see fields
        "categories_tags": "en:chips",  # Use a common category to ensure we get a product
    }

    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            if data and 'products' in data and len(data['products']) > 0:
                # Get first product to extract all possible fields
                product = data['products'][0]

                # Define the order of headers exactly as they appear in the API
                headers = [
                    '_id', '_keywords', 'added_countries_tags', 'additives_n',
                    'additives_original_tags', 'additives_tags', 'allergens',
                    'allergens_from_ingredients', 'allergens_from_user', 'allergens_hierarchy',
                    'allergens_lc', 'allergens_tags', 'amino_acids_prev_tags', 'amino_acids_tags',
                    'brands', 'brands_tags', 'categories', 'categories_hierarchy', 'categories_lc',
                    'categories_old', 'categories_properties', 'categories_properties_tags',
                    'categories_tags', 'category_properties', 'checkers_tags', 'ciqual_food_name_tags',
                    'cities_tags', 'code', 'codes_tags', 'compared_to_category', 'complete',
                    'completed_t', 'completeness', 'correctors_tags', 'countries', 'countries_hierarchy',
                    'countries_lc', 'countries_tags', 'created_t', 'creator', 'data_quality_bugs_tags',
                    'data_quality_errors_tags', 'data_quality_info_tags', 'data_quality_tags',
                    'data_quality_warnings_tags', 'data_sources', 'data_sources_tags',
                    'debug_param_sorted_langs', 'debug_tags', 'ecoscore_data', 'ecoscore_grade',
                    'ecoscore_score', 'ecoscore_tags', 'editors', 'editors_tags', 'emb_codes',
                    'emb_codes_20141016', 'emb_codes_orig', 'emb_codes_tags', 'entry_dates_tags',
                    'environment_impact_level', 'environment_impact_level_tags', 'expiration_date',
                    'food_groups', 'food_groups_tags', 'fruits-vegetables-nuts_100g_estimate',
                    'generic_name', 'generic_name_de', 'generic_name_en', 'generic_name_fr',
                    'generic_name_it', 'generic_name_nl', 'grades', 'id', 'image_front_small_url',
                    'image_front_thumb_url', 'image_front_url', 'image_small_url', 'image_thumb_url',
                    'image_url', 'images', 'informers_tags', 'ingredients', 'ingredients_analysis',
                    'ingredients_analysis_tags', 'ingredients_debug', 'ingredients_from_or_that_may_be_from_palm_oil_n',
                    'ingredients_from_palm_oil_n', 'ingredients_from_palm_oil_tags', 'ingredients_hierarchy',
                    'ingredients_ids_debug', 'ingredients_lc', 'ingredients_n', 'ingredients_n_tags',
                    'ingredients_non_nutritive_sweeteners_n', 'ingredients_original_tags',
                    'ingredients_percent_analysis', 'ingredients_sweeteners_n', 'ingredients_tags',
                    'ingredients_text', 'ingredients_text_de', 'ingredients_text_debug',
                    'ingredients_text_en', 'ingredients_text_fr', 'ingredients_text_it',
                    'ingredients_text_nl', 'ingredients_text_with_allergens',
                    'ingredients_text_with_allergens_de', 'ingredients_text_with_allergens_en',
                    'ingredients_text_with_allergens_fr', 'ingredients_text_with_allergens_it',
                    'ingredients_text_with_allergens_nl', 'ingredients_that_may_be_from_palm_oil_n',
                    'ingredients_that_may_be_from_palm_oil_tags', 'ingredients_with_specified_percent_n',
                    'ingredients_with_specified_percent_sum', 'ingredients_with_unspecified_percent_n',
                    'ingredients_with_unspecified_percent_sum', 'ingredients_without_ciqual_codes',
                    'ingredients_without_ciqual_codes_n', 'ingredients_without_ecobalyse_ids',
                    'ingredients_without_ecobalyse_ids_n', 'interface_version_created',
                    'interface_version_modified', 'known_ingredients_n', 'labels', 'labels_hierarchy',
                    'labels_lc', 'labels_old', 'labels_tags', 'lang', 'languages', 'languages_codes',
                    'languages_hierarchy', 'languages_tags', 'last_edit_dates_tags', 'last_editor',
                    'last_image_dates_tags', 'last_image_t', 'last_modified_by', 'last_modified_t',
                    'last_updated_t', 'lc', 'link', 'main_countries_tags', 'manufacturing_places',
                    'manufacturing_places_tags', 'max_imgid', 'minerals_prev_tags', 'minerals_tags',
                    'misc_tags', 'no_nutrition_data', 'nova_group', 'nova_group_debug', 'nova_groups',
                    'nova_groups_markers', 'nova_groups_tags', 'nucleotides_prev_tags',
                    'nucleotides_tags', 'nutrient_levels', 'nutrient_levels_tags'
                ]

                # Add nutriment fields
                nutriments = product.get('nutriments', {})
                for nutriment in nutriments.keys():
                    headers.append(f'nutriment_{nutriment}')

                # Add remaining fields
                remaining_fields = [
                    'nutriments', 'nutriments_estimated', 'nutriscore', 'nutriscore_2021_tags',
                    'nutriscore_2023_tags', 'nutriscore_data', 'nutriscore_grade', 'nutriscore_score',
                    'nutriscore_score_opposite', 'nutriscore_tags', 'nutriscore_version',
                    'nutrition_data', 'nutrition_data_per', 'nutrition_data_prepared',
                    'nutrition_data_prepared_per', 'nutrition_grade_fr', 'nutrition_grades',
                    'nutrition_grades_tags', 'nutrition_score_beverage', 'nutrition_score_debug',
                    'obsolete', 'obsolete_since_date', 'origin', 'origin_de', 'origin_en',
                    'origin_fr', 'origin_it', 'origin_nl', 'origins', 'origins_hierarchy',
                    'origins_lc', 'origins_old', 'origins_tags', 'other_nutritional_substances_tags',
                    'packaging', 'packaging_hierarchy', 'packaging_lc', 'packaging_materials_tags',
                    'packaging_old', 'packaging_old_before_taxonomization', 'packaging_recycling_tags',
                    'packaging_shapes_tags', 'packaging_tags', 'packaging_text', 'packaging_text_de',
                    'packaging_text_en', 'packaging_text_fr', 'packaging_text_it', 'packaging_text_nl',
                    'packagings', 'packagings_complete', 'packagings_materials',
                    'packagings_materials_main', 'packagings_n', 'photographers_tags',
                    'pnns_groups_1', 'pnns_groups_1_tags', 'pnns_groups_2', 'pnns_groups_2_tags',
                    'popularity_key', 'popularity_tags', 'product_name', 'product_name_de',
                    'product_name_en', 'product_name_fr', 'product_name_it', 'product_name_nl',
                    'product_quantity', 'product_quantity_unit', 'product_type', 'purchase_places',
                    'purchase_places_tags', 'quantity', 'removed_countries_tags', 'rev', 'scans_n',
                    'scores', 'selected_images', 'serving_quantity', 'serving_quantity_unit',
                    'serving_size', 'sortkey', 'sources', 'states', 'states_hierarchy',
                    'states_tags', 'stores', 'stores_tags', 'teams', 'teams_tags', 'traces',
                    'traces_from_ingredients', 'traces_from_user', 'traces_hierarchy', 'traces_lc',
                    'traces_tags', 'unique_scans_n', 'unknown_ingredients_n',
                    'unknown_nutrients_tags', 'update_key', 'url', 'vitamins_prev_tags',
                    'vitamins_tags', 'weighers_tags'
                ]
                headers.extend(remaining_fields)

                logger.info(f"Successfully retrieved {len(headers)} headers from API")
                return headers
            else:
                logger.error("No products found in API response")
                return []
    except Exception as e:
        logger.error(f"Error getting headers from API: {e}")
        return []


def write_headers_to_sheet(headers):
    """Write headers to the first row of the sheet"""
    try:
        credentials = get_google_sheets_credentials()
        service = build('sheets', 'v4', credentials=credentials)
        sheet = service.spreadsheets()

        # Clear existing content
        sheet.values().clear(
            spreadsheetId=CONFIG["spreadsheet_id"],
            range="Sheet1!A1:AZ"
        ).execute()

        # Write headers
        result = sheet.values().update(
            spreadsheetId=CONFIG["spreadsheet_id"],
            range="Sheet1!A1",
            valueInputOption='RAW',
            body={'values': [headers]}
        ).execute()

        logger.info(f"Successfully wrote {len(headers)} headers to sheet")
        return True
    except Exception as e:
        logger.error(f"Error writing headers to sheet: {e}")
        return False


def main():
    # Verify sheet access first
    if not verify_sheet_access():
        logger.error("Cannot proceed without sheet access")
        return

    # Get headers from API and write them to sheet
    headers = get_headers_from_api()
    if not headers:
        logger.error("Failed to get headers from API")
        return

    if not write_headers_to_sheet(headers):
        logger.error("Failed to write headers to sheet")
        return

    api = OpenFoodFactsAPI()

    categories = [
        "alcoholic-beverages"
    ]

    for category in categories:
        logger.info(f"\nProcessing category: {category}")
        logger.info("=" * 50)

        try:
            # Get first page with 100 results
            results = api.search_products(category, page=1, page_size=100)

            if not results or 'products' not in results:
                logger.warning(f"No results found for category {category}")
                continue

            products = results.get('products', [])
            if not products:
                logger.warning(f"No products found for category {category}")
                continue

            # Process first 100 products
            processed_products = [process_product(product) for product in products]
            update_sheet(processed_products, is_first_batch=False)
            logger.info(f"Processed {len(processed_products)} products from page 1")

            # Get total pages
            total_products = results.get('count', 0)
            total_pages = (total_products + 100 - 1) // 100  # Using page_size=100

            logger.info(f"Found {total_pages} total pages. Getting additional pages...")

            # Get remaining pages to reach 500
            for page in range(2, min(total_pages + 1, 6)):  # Will get up to 500 products (5 pages of 100)
                logger.info(f"Getting page {page} of {total_pages}")
                time.sleep(2)  # Add small delay between requests

                results = api.search_products(category, page=page, page_size=100)
                if not results or 'products' not in results:
                    logger.warning(f"No valid results found on page {page}")
                    continue

                products = results.get('products', [])
                if not products:
                    logger.warning(f"No valid products found on page {page}")
                    continue

                processed_products = [process_product(product) for product in products]
                update_sheet(processed_products, is_first_batch=False)
                logger.info(f"Processed {len(processed_products)} products from page {page}")

            logger.info("Completed processing up to 500 products")

        except Exception as e:
            logger.error(f"Error processing category {category}: {e}")
            continue


if __name__ == "__main__":
    main()