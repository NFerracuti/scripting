"""
This script takes in a list of URLs, searches those sites for an email address and LinkedIn profile,
then uses Perplexity to gather company bio, email address, and contact person.

It then creates a Google spreadsheet with the following columns: Vendor, URL, Bio, Email, First Name, Last Name, LinkedIn, Message
If the spreadsheet rejects, it will fall back to a txt file output. Run in terminal with:
python3 -m scripts.email_scraper
"""

import os
import sys

# Add the project root directory to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

import requests
import json
from googleapiclient.discovery import build
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import re
import time
import logging
from datetime import datetime

load_dotenv()


class PerplexityVendorScraper:
    def __init__(self):
        self.api_key = os.getenv('PERPLEXITY_API_KEY')
        self.url = "https://api.perplexity.ai/chat/completions"
        self.headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "authorization": f"Bearer {self.api_key}"
        }
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('perplexity_scraping.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def get_vendor_info(self, vendor_name, url):
        self.logger.info(f"Fetching info for: {vendor_name} ({url})")
        
        prompt = f"""Research the company {vendor_name} (website: {url}) and provide the following information:
        1) A brief company bio/description (2-3 sentences)
        2) Company email address
        3) Owner or primary contact's full name (first and last name)
        4) Any additional contact information
        
        Format the response as JSON:
        {{
            "bio": "company description here",
            "email": "email@example.com",
            "contact": {{
                "first_name": "First",
                "last_name": "Last"
            }},
            "additional": "any other relevant info"
        }}"""
        
        payload = {
            "model": "llama-3.1-sonar-small-128k-online",
            "messages": [
                {
                    "role": "system", 
                    "content": "You are a helpful assistant. Please provide information in the exact JSON format requested."
                },
                {
                    "role": "user", 
                    "content": prompt
                }
            ]
        }

        try:
            response = requests.post(self.url, json=payload, headers=self.headers)
            print(f"\n=== Raw Perplexity Response for {vendor_name} ===")
            print(f"Status Code: {response.status_code}")
            print(f"Response Text: {response.text}")
            print("=====================================\n")
            
            if response.status_code == 200:
                response_data = response.json()
                content = response_data['choices'][0]['message']['content']
                
                # Clean up the content
                content = content.replace('```json', '').replace('```', '').strip()
                
                # Remove any lines after the closing brace of the JSON object
                content = '\n'.join(line for line in content.split('\n') if not line.startswith('Note:'))
                
                # Remove any inline comments
                content = re.sub(r'\s*//.*$', '', content, flags=re.MULTILINE)
                
                try:
                    info = json.loads(content)
                    
                    # Handle empty strings for contact names
                    first_name = info.get('contact', {}).get('first_name', 'N/A')
                    last_name = info.get('contact', {}).get('last_name', 'N/A')
                    first_name = first_name if first_name else 'N/A'
                    last_name = last_name if last_name else 'N/A'
                    
                    # Handle additional info that might be an object
                    additional = info.get('additional', 'N/A')
                    if isinstance(additional, dict):
                        additional = '; '.join(f"{k}: {v}" for k, v in additional.items())
                    
                    return (
                        info.get('bio', 'N/A'),
                        info.get('email', 'N/A'),
                        first_name,
                        last_name,
                        additional
                    )
                except json.JSONDecodeError as e:
                    print(f"Failed to parse JSON content: {e}")
                    print(f"Raw content that failed to parse: {content}")
                    return "N/A", "N/A", "N/A", "N/A", "N/A"
            else:
                print(f"API request failed: {response.status_code}")
                return "N/A", "N/A", "N/A", "N/A", "N/A"
                
        except Exception as e:
            print(f"Error processing request: {e}")
            return "N/A", "N/A", "N/A", "N/A", "N/A"


class GoogleSheetsClient:
    def __init__(self):
        # Use service account credentials instead of OAuth
        from sysconfigs.client_creds import get_google_sheets_credentials
        self.creds = get_google_sheets_credentials()
        self.service = build('sheets', 'v4', credentials=self.creds)

    def create_sheet(self, data):
        spreadsheet = {
            'properties': {
                'title': f'Vendor Information {datetime.now().strftime("%Y-%m-%d %H:%M")}'
            }
        }
        
        def sanitize_value(value):
            """Helper function to sanitize individual values"""
            if value is None:
                return "N/A"
            if isinstance(value, (int, float)):
                return str(value)
            if isinstance(value, bool):
                return str(value)
            if isinstance(value, (list, tuple)):
                return '; '.join(str(v) for v in value)
            if isinstance(value, dict):
                if any(key.endswith('email') for key in value.keys()):
                    return '; '.join(str(v) for v in value.values() if v)
                return '; '.join(f"{k}: {v}" for k, v in value.items() if v)
            
            # Convert to string and clean up
            value = str(value)
            # Remove null bytes and other problematic characters
            value = value.replace('\x00', '').replace('\ufeff', '')
            # Replace multiple spaces and newlines with single space
            value = ' '.join(value.split())
            # Limit length to prevent overflow
            value = value[:50000]  # Google Sheets cell limit
            return value if value else "N/A"

        try:
            # Process data to ensure all values are clean strings
            processed_data = []
            for row in data:
                processed_row = []
                for item in row:
                    try:
                        processed_value = sanitize_value(item)
                        processed_row.append(processed_value)
                    except Exception as e:
                        logging.warning(f"Error processing value {item}: {e}")
                        processed_row.append("N/A")
                processed_data.append(processed_row)

            # Create and share spreadsheet (existing code remains the same)
            spreadsheet = self.service.spreadsheets().create(body=spreadsheet).execute()
            sheet_id = spreadsheet.get('spreadsheetId')
            
            # Share the spreadsheet
            drive_service = build('drive', 'v3', credentials=self.creds)
            drive_service.permissions().create(
                fileId=sheet_id,
                body={
                    'type': 'user',
                    'role': 'writer',
                    'emailAddress': 'nick@theceliapp.com'
                }
            ).execute()
            
            # Define headers
            headers = [["Vendor", "URL", "Bio", "Email", "First Name", "Last Name", "LinkedIn", "Message"]]
            
            # Combine headers with processed data
            values = headers + processed_data
            
            # Verify all rows have the same number of columns
            max_cols = max(len(row) for row in values)
            values = [row + ["N/A"] * (max_cols - len(row)) for row in values]
            
            body = {'values': values}
            
            # Update the sheet with all data at once
            self.service.spreadsheets().values().update(
                spreadsheetId=sheet_id,
                range='Sheet1!A1',
                valueInputOption='RAW',
                body=body
            ).execute()
            
            print(f"Spreadsheet created and shared: https://docs.google.com/spreadsheets/d/{sheet_id}")
            return sheet_id
            
        except Exception as e:
            logging.error(f"Error creating spreadsheet: {e}")
            raise


def search_website(url):
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=30)
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Initialize variables
        email = None
        linkedin = None
        
        # Search for email addresses
        email_pattern = r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}'
        text_content = soup.get_text()
        emails = re.findall(email_pattern, text_content)
        if emails:
            email = emails[0]  # Take the first email found
            
        # Search for LinkedIn profile
        linkedin_links = soup.find_all('a', href=re.compile(r'linkedin\.com'))
        if linkedin_links:
            linkedin = linkedin_links[0]['href']
            
        return email, linkedin
    except Exception as e:
        logging.error(f"Error searching {url}: {str(e)}")
        return None, None


def process_scrape_and_compile_vendor_info():
    # Define the URLs
    urls = [
        "https://aidansglutenfree.com",
        "https://lartisandelices.com",
        "https://bakedbykelly.ca",
        "https://www.becksbroth.com",
        "https://www.instagram.com/blossom_bakery/",
        "https://www.boldqualitylemonaide.com",
        "https://www.thebreadessentials.com",
        "https://www.brodohouse.com/",
        "https://www.facebook.com/BrunosBakeryAndCafe",
        "https://theceleaccorner.com",
        "https://www.celiac.ca",
        "https://theceliapp.com",
        "https://shop.nealbrothersfoods.com/collections/crank-coffee-co",
        "https://br4trade.com/cheese-rolls/",
        "https://frenchlunch.ca",
        "https://haleyspantry.com",
        "https://www.hartandziel.ca",
        "https://helanutrition.com",
        "https://hey-mom.ca",
        "https://www.holy-cannoli.com",
        "https://hugsandsarcasm.com",
        "https://www.jewelsunderthekilt.com",
        "https://www.instagram.com/ketokookieco/",
        "https://shoplakeandoak.com",
        "https://lapapampa.com.pe",
        "https://www.livbon.ca",
        "https://lovepipaya.square.site",
        "https://thelowcarbco.ca",
        "https://www.mollybglutenfree.com",
        "https://www.mollysmarket.ca",
        "https://www.newtonsnogluten.com",
        "https://www.nolabaking.com",
        "https://odoughs.com",
        "https://myonlyoats.com/",
        "https://www.pierogime.ca",
        "https://plantedinhamilton.com",
        "https://www.glutenfreebakingcourses.com",
        "https://www.instagram.com/saltedsunday/",
        "https://www.thesimplekitchencanada.com",
        "https://shophendersonbrewing.com/collections/sollys-craft-soda",
        "https://sprouty.ca",
        "https://sweetsbysteph.ca",
        "https://woahdough.ca",
        "https://www.yoona.ca/yoona"
    ]
    
    perplexity_client = PerplexityVendorScraper()
    google_sheets_client = GoogleSheetsClient()
    
    # Store all data in a list instead of writing to file
    all_data = []
    
    # Process each vendor
    for url in urls:
        try:
            # Extract vendor name from URL
            vendor_name = url.split('://')[1].split('/')[0]
            vendor_name = vendor_name.replace('www.', '').replace('.com', '').replace('.ca', '')
            
            logging.info(f"Processing {vendor_name}...")
            
            # Search website for email and LinkedIn
            website_email, linkedin = search_website(url)
            
            # Get vendor information from Perplexity
            bio, perplexity_email, first_name, last_name, additional = perplexity_client.get_vendor_info(
                vendor_name, 
                url
            )
            
            # Use website email if Perplexity didn't find one
            email = perplexity_email if perplexity_email != "N/A" else (website_email or "N/A")
            
            # Create personalized message
            message = f"Hi {first_name}! My name's Nick. We met some of your team at the Gluten Free Garage Expo in Toronto on Nov 24th. We're featuring company profiles on our app, starting with only trusted gluten free vendors like you guys! When users scan one of your products through our app, they can see your company name and bio. Here's what we found on your website and socials.\n{bio}\nDoes this work as a bio for you, or would you prefer to send one yourself for us to use?\n \nWe want to give an accurate representation to our growing list of celiac users."
            
            # Create row data
            row = [
                vendor_name,
                url,
                bio,
                email,
                first_name,
                last_name,
                linkedin or "N/A",
                message
            ]
            
            # Add row to data list
            all_data.append(row)
            
            # Add delay between API calls
            time.sleep(2)
            
        except Exception as e:
            logging.error(f"Error processing {vendor_name}: {str(e)}")
            continue
    
    try:
        # Create the spreadsheet with all the data
        sheet_id = google_sheets_client.create_sheet(all_data)
        logging.info(f"\nProcessing complete! Check the spreadsheet: https://docs.google.com/spreadsheets/d/{sheet_id}")
    except Exception as e:
        logging.error(f"Failed to create spreadsheet: {e}")
        # Fallback to file output if Google Sheets fails
        output_file = 'scripts/email_scraper_output.txt'
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write("\t".join(["Vendor", "URL", "Bio", "Email", "First Name", "Last Name", "LinkedIn", "Message"]) + "\n")
            for row in all_data:
                f.write("\t".join(str(item) for item in row) + "\n")
        logging.info(f"\nFallback to file output: {output_file}")


if __name__ == "__main__":
    process_scrape_and_compile_vendor_info()
