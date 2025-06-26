"""
Product Database Cleaner
=======================

This script connects to a Google Sheet containing product data and uses Google's Gemini AI
to clean and verify the data. It processes the data row by row, analyzing each field
for accuracy and formatting.

Features:
- Connects to Google Sheets API to fetch product data
- Uses Gemini AI to analyze and verify product information
- Processes data in manageable chunks to avoid API limits
- Includes comprehensive error handling and logging
- Cleans and formats text before API processing

Rate Limiting:
-------------
The script includes several measures to handle Gemini API rate limits:
- Processes data in small chunks (5 rows at a time)
- Implements exponential backoff retry logic
- Adds delays between API calls (3 seconds)
- Adds delays between row processing (2 seconds)
- Adds delays between chunks (5 seconds)

If you continue to hit rate limits, consider:
1. Increasing the delays between API calls
2. Reducing the chunk size
3. Implementing a queue system
4. Upgrading your API quota if available

Prerequisites:
-------------
1. Environment variables in .env file:
   - GOOGLE_APPLICATION_CREDENTIALS_NEW: Path to Google service account credentials
   - GEMINI_API_KEY: API key for Google's Gemini AI     https://aistudio.google.com/app/apikey
   - GOOGLE_CSE_ID: Custom search engine ID https://programmablesearchengine.google.com/controlpanel/all
   - GOOGLE_API_KEY: API key

2. Required packages:
```
pip install google-auth-oauthlib google-auth-httplib2 google-api-python-client google-generativeai pandas python-dotenv tenacity
```

Usage:
------
1. Ensure all environment variables are set
2. Run the script:
```
python3 -m scripts.product_cleaner

```

Output:
-------
- Returns a list of dictionaries containing cleaned product data
- Logs progress and errors to console
"""

import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
import logging
from typing import List, Dict
import time
from googleapiclient.errors import HttpError
from tenacity import retry, stop_after_attempt, wait_exponential
import google.generativeai as genai

from sysconfigs.client_creds import get_google_sheets_credentials

load_dotenv()
# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_gemini_model():
    genai.configure(api_key=os.getenv('GEMINI_API_KEY'))
    return genai.GenerativeModel('gemini-pro')


def get_search_service():
    try:
        cse_id = os.getenv('GOOGLE_CSE_ID')
        service = build("customsearch", "v1", developerKey=os.getenv('GOOGLE_API_KEY'))

        def search_images(query: str):
            return service.cse().list(
                q=query,
                cx=cse_id,
                searchType='image',
                num=1,
                imgType='photo',
                safe='active'
            ).execute()

        service.search_images = search_images
        return service
    except Exception as e:
        logging.error(f"Error setting up search service: {str(e)}")
        raise


class ProductDataCleaner:
    # nicks https://docs.google.com/spreadsheets/d/1a3QC2j7WL9FsjGySCEGMROXa7tK-msBSjs3oBWfZG6k
    # admin https://docs.google.com/spreadsheets/d/1gw8g7_o_Mrz0gDlyYIZsisRzgia1KtVCCe_3tnJUC4Y
    SPREADSHEET_ID = '1gw8g7_o_Mrz0gDlyYIZsisRzgia1KtVCCe_3tnJUC4Y'
    RANGE_NAME = 'Products!A1:Z'  # looks for a sheet named Products

    def __init__(self):
        self.sheets_service = build('sheets', 'v4', credentials=get_google_sheets_credentials())
        self.gemini_model = get_gemini_model()
        self.search_service = get_search_service()

    def run_cleaning_process(self):
        """Main function to run the entire cleaning process"""
        try:
            logger.info("Starting cleaning process...")

            # Fetch data
            data = self.fetch_sheet_data()

            # Process in smaller chunks
            chunk_size = 5

            for i in range(0, len(data), chunk_size):
                chunk = data[i:i + chunk_size]
                logger.info(f"Processing chunk {i // chunk_size + 1}/{len(data) // chunk_size + 1}")

                for j, row in enumerate(chunk):
                    cleaned_row = self.process_row(row)
                    # Update the sheet with new data
                    self.update_sheet_row(i + j, cleaned_row)
                    time.sleep(2)  # Add delay between rows

                # Add delay between chunks
                time.sleep(5)

            logger.info("Cleaning process completed successfully")

        except Exception as e:
            logger.error(f"Error in cleaning process: {str(e)}")
            raise

    def fetch_sheet_data(self) -> List[Dict]:
        """Fetch all data from Google Sheet"""
        try:
            logger.info("Fetching data from Google Sheet...")
            sheet = self.sheets_service.spreadsheets()
            try:
                result = sheet.values().get(
                    spreadsheetId=self.SPREADSHEET_ID,
                    range=self.RANGE_NAME
                ).execute()
            except HttpError as e:
                if e.resp.status == 403:
                    logger.error(f"Permission denied. Please ensure the service account has access to the spreadsheet.")
                    raise
                logger.warning(f"Encountered {e.resp.status} {e.resp.reason} with reason \"{e._get_reason()}\"")
                raise
            
            values = result.get('values', [])
            if not values:
                logger.warning("No data found in sheet")
                return []
            
            # Convert to list of dictionaries using first row as headers
            headers = values[0]
            data = []
            for row in values[1:]:
                # Pad row with empty strings if it's shorter than headers
                row_data = row + [''] * (len(headers) - len(row))
                data.append(dict(zip(headers, row_data)))
            
            logger.info(f"Successfully fetched {len(data)} rows")
            return data
            
        except Exception as e:
            logger.error(f"Error fetching sheet data: {str(e)}")
            raise

    def process_row(self, row: Dict) -> Dict:
        try:
            cleaned_row = {k: self._clean_text(v) for k, v in row.items()}
            product_name = cleaned_row.get('product_name', '')
            brand_name = cleaned_row.get('brand_name', '')

            logger.info(f"Processing row for {brand_name} {product_name}")
            verified_row = {k: v for k, v in cleaned_row.items()}

            # Get image URL via Custom Search
            logger.info("Starting image URL search...")
            image_url = self._get_product_image_url(product_name, brand_name)
            verified_row['product_image_url'] = image_url
            logger.info(f"Image URL search completed: {image_url}")

            # Define prompts for all other fields
            field_prompts = {
                'descriptors': f"""
                What are the main characteristics of {brand_name} {product_name}?
                Format: ['characteristic1', 'characteristic2']
                If you cannot find this information, return an empty array: []
                Example good response: ['gluten-free', 'crunchy', 'table crackers']
                Example when info not found: []
                """,

                'certifications': f"""
                What official certifications does {brand_name} {product_name} have?
                Format: ['certification1', 'certification2']
                If you cannot find this information, return an empty array: []
                Example good response: ['Certified Gluten-Free', 'Non-GMO Project Verified']
                Example when info not found: []
                """,

                'ingredients': f"""
                List the ingredients in {brand_name} {product_name}.
                Format: ['ingredient1', 'ingredient2']
                If you cannot find this information, return an empty array: []
                Example good response: ['rice flour', 'corn starch', 'salt']
                Example when info not found: []
                """,

                'product_url': f"What is the official product webpage for {brand_name} {product_name}? Return URL only.",

                'gluten_free_score': f"Rate {brand_name} {product_name}'s gluten-free status from 0-3 (0=contains, 1=may contain, 2=free but not certified, 3=certified free). Return number only.",
            }

            # Process all fields with Gemini
            try:
                for field in field_prompts:
                    if field in cleaned_row:
                        try:
                            logger.info(f"Processing {field} with Gemini...")
                            verified_value = self._call_gemini_api(field_prompts[field])

                            # Special handling for array fields
                            if field in ['descriptors', 'certifications', 'ingredients']:
                                verified_value = self._clean_array_format(verified_value)
                            else:
                                verified_value = self._clean_field_format(field, verified_value)

                            verified_row[field] = verified_value
                            logger.info(f"Successfully processed {field}: {verified_row[field]}")
                        except Exception as e:
                            logger.error(f"Error processing field {field}: {str(e)}")
                            verified_row[field] = '[]' if field in ['descriptors', 'certifications',
                                                                    'ingredients'] else cleaned_row.get(field, '')
            except Exception as e:
                logger.error(f"Error in Gemini processing: {str(e)}")

            logger.info("Processed row data:")
            for key, value in verified_row.items():
                logger.info(f"{key}: {value}")

            return verified_row

        except Exception as e:
            logger.error(f"Error processing row: {str(e)}")
            return row

    def _clean_text(self, text: str) -> str:
        """Clean and format text for API calls"""
        try:
            # Basic cleaning - expand as needed
            cleaned = str(text).strip()
            cleaned = ' '.join(cleaned.split())  # Remove extra whitespace
            return cleaned
        except Exception as e:
            logger.error(f"Error cleaning text: {str(e)}")
            return text

    def _get_product_image_url(self, product_name: str, brand_name: str) -> str:
        """Search Google Images for product and return first result"""
        try:
            logger.info(f"Starting image search for: {brand_name} {product_name}")

            # Construct search query
            query = f"{brand_name} {product_name} product packaging"
            logger.info(f"Search query: {query}")

            # Execute search
            logger.info("Making Custom Search API call...")
            result = self.search_service.search_images(query)

            # Get first image URL if available
            if 'items' in result and len(result['items']) > 0:
                image_url = result['items'][0]['link']
                logger.info(f"Found image URL: {image_url}")
                return image_url

            logger.warning("No image results found")
            return ""

        except Exception as e:
            logger.error(f"Error getting product image: {str(e)}")
            return ""

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=4, max=10)
    )
    def _call_gemini_api(self, prompt: str) -> str:
        try:
            response = self.gemini_model.generate_content(prompt)

            # Check if response was blocked by safety filters
            if not response.candidates:
                safety_ratings = response.prompt_feedback.safety_ratings
                logger.warning(f"Content blocked due to safety filters: {safety_ratings}")
                return '[]'

            time.sleep(3)
            return response.text

        except Exception as e:
            if "safety_ratings" in str(e):
                # This is a safety block, don't retry
                logger.warning(f"Content blocked by safety filters: {str(e)}")
                return '[]'
            # Only retry for actual API errors
            logger.error(f"Error calling Gemini API: {str(e)}")
            raise

    def _clean_array_format(self, value: str) -> str:
        """Clean array format to ensure single-level arrays only"""
        try:
            # Remove any nested arrays
            # First, convert string representation to actual list
            if isinstance(value, str):
                if value.strip() == '':
                    return '[]'
                # Remove any nested brackets
                cleaned = value.replace('[[', '[').replace(']]', ']')
                # Remove any remaining nested arrays
                while '[' in cleaned and ']' in cleaned:
                    start = cleaned.find('[')
                    end = cleaned.find(']')
                    if start != -1 and end != -1:
                        inner = cleaned[start+1:end]
                        # Split by commas, clean each item
                        items = [item.strip().strip("'\"") for item in inner.split(',')]
                        # Filter out empty items and rebuild array
                        items = [f"'{item}'" for item in items if item]
                        return f"[{', '.join(items)}]"
            return '[]'
        except Exception as e:
            logger.error(f"Error cleaning array format: {str(e)}")
            return '[]'

    def _clean_field_format(self, field: str, value: str) -> str:
        """Format field values according to their expected format"""
        try:
            # Handle array fields
            if field in ['certifications', 'descriptors', 'ingredients']:
                if value.startswith('[') and value.endswith(']'):
                    return value  # Already in correct format
                # Convert comma-separated string to array format
                items = [item.strip().strip("'\"") for item in value.split(',')]
                # Remove empty items and wrap in single quotes
                items = [f"'{item}'" for item in items if item]
                return f"[{', '.join(items)}]"
            
            # Handle score fields
            elif 'score' in field:
                # Extract just the number, remove any other characters
                try:
                    # Find the first number in the string
                    import re
                    number = re.search(r'\d', value)
                    if number:
                        return number.group(0)
                    return '0'  # Default to 0 if no number found
                except:
                    return '0'
            
            # Handle URL fields
            elif 'url' in field:
                return value.strip().strip("'\"")
            
            # Handle other string fields
            else:
                return value.strip().strip("'\"")
            
        except Exception as e:
            logger.error(f"Error formatting field {field}: {str(e)}")
            return value

    def update_sheet_row(self, row_index: int, data: Dict):
        """Update a single row in the Google Sheet"""
        try:
            logger.info(f"Updating sheet row {row_index + 2}...")

            # Get headers and convert row data to list format in one API call
            headers = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=self.SPREADSHEET_ID,
                range='Products!A1:Z1'
            ).execute().get('values', [[]])[0]

            row_data = [data.get(header, '') for header in headers]
            range_name = f'Products!A{row_index + 2}'  # +2 because row_index is 0-based and we skip header

            body = {
                'values': [row_data]
            }

            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=self.SPREADSHEET_ID,
                range=range_name,
                valueInputOption='RAW',
                body=body
            ).execute()

            logger.info(f"Updated row {row_index + 2} in sheet with {len(row_data)} columns")
            logger.info(f"Update result: {result}")
        except Exception as e:
            logger.error(f"Error updating sheet row: {str(e)}")
            raise


if __name__ == "__main__":
    cleaner = ProductDataCleaner()
    cleaner.run_cleaning_process()
