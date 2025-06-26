"""
python3 -m scripts.openai_scraper

scripts/gfg/gfg_append_openai_data.csv

/scripts/private/gfg/
"""

import time
import requests
import os
from pathlib import Path
import csv
import select
import sys

from sysconfigs.client_creds import get_openai_credentials


class OpenAIScraperClient:
    def __init__(self):
        print("\n=== Initializing OpenAI Scraper Client ===")
        self.api_key = get_openai_credentials()
        print("✓ API credentials loaded")
        self.api_endpoint = "https://api.openai.com/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        self.input_dir = "scripts/private/gfg"
        self.output_file = "scripts/gfg/gfg_append_openai_data.csv"
        print(f"✓ Input directory set to: {self.input_dir}")
        print(f"✓ Output file set to: {self.output_file}")

        # Initialize statistics tracking
        self.stats = {
            'processed_files': [],
            'failed_files': [],
            'skipped_files': [],
            'truncated_files': [],
            'total_products': 0,
            'processing_times': {},
            'file_sizes': {},
            'errors': {}
        }
        print("=== Initialization Complete ===\n")

    def get_user_input(self, prompt, timeout=10):
        """Get user input with timeout"""
        print(prompt, end='', flush=True)
        i, o, e = select.select([sys.stdin], [], [], timeout)
        if i:
            return sys.stdin.readline().strip().lower()
        return 'y'  # Default to yes if no input

    def get_sorted_files(self):
        """Get files sorted by size, smallest first"""
        print("\n=== Sorting Files by Size ===")
        files_with_sizes = []
        for file in os.listdir(self.input_dir):
            filepath = os.path.join(self.input_dir, file)
            size = os.path.getsize(filepath)
            files_with_sizes.append((file, size))

        sorted_files = sorted(files_with_sizes, key=lambda x: x[1])

        for file, size in sorted_files:
            size_kb = size / 1024
            if size_kb > 1024:
                print(f"⚠️  {file}: {size_kb / 1024:.1f}MB")
            else:
                print(f"✓ {file}: {size_kb:.1f}KB")

        return [f[0] for f in sorted_files]

    def process_files(self):
        print("\n=== Step 2: Beginning File Processing ===")
        files = self.get_sorted_files()
        total_files = len(files)

        if not files:
            print("❌ No files found to process.")
            return

        for index, file in enumerate(files, 1):
            print(f"\n--- Processing File {index} of {total_files}: {file} ---")

            # Only ask for confirmation if previous file failed
            if self.stats['failed_files'] and file == self.stats['failed_files'][-1]:
                proceed = self.get_user_input(f"Process this file? (y/n): ")
                if proceed != 'y':
                    print("⏭️  Skipping file...")
                    self.stats['skipped_files'].append(file)
                    continue

            print(f"📂 Opening file: {file}")
            filepath = os.path.join(self.input_dir, file)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    html_content = f.read()
                    file_size = len(html_content)

                    if file_size > 400000:
                        print(f"⚠️  File size ({file_size / 1024:.1f}KB) exceeds 400KB limit")
                        html_content = html_content[:400000] + "\n\n<!-- CONTENT TRUNCATED: File too large! -->"
                        self.stats['truncated_files'].append(file)

                print(f"✓ Successfully read file: {file}")
                self.stats['file_sizes'][file] = len(html_content)
                print(f"📊 Content length: {len(html_content)} characters")

                success = False
                for attempt in range(3):
                    print(f"\n🔄 Attempt {attempt + 1} of 3")
                    try:
                        csv_data = self.get_data_fields(html_content, file)
                        if csv_data:
                            print("✓ Successfully extracted data")
                            self.append_to_csv(csv_data, file)
                            self.stats['processed_files'].append(file)
                            success = True
                            break
                        else:
                            print(f"⚠️  Attempt {attempt + 1} failed, retrying...")
                            if attempt == 2:
                                self.stats['failed_files'].append(file)
                                self.stats['errors'][file] = "Failed all attempts"
                            time.sleep(60)
                    except Exception as e:
                        print(f"❌ Error on attempt {attempt + 1}: {str(e)}")
                        if attempt == 2:
                            print(f"❌ Failed to process {file} after 3 attempts")
                            self.stats['failed_files'].append(file)
                            self.stats['errors'][file] = str(e)
                        time.sleep(60)

                # Add delay based on success/failure
                if success:
                    print("\n⏳ Waiting 3 minutes before processing next file...")
                    time.sleep(180)
                else:
                    print("\n⏳ Waiting 5 minutes after failure before processing next file...")
                    time.sleep(300)
            #
            # except Exception as e:
            #     print(f"❌ Error processing file {file}: {str(e)}")
            #     self.stats['failed_files'].append(file)
            #     self.stats['errors'][file] = str(e)
            #     continue

            except KeyboardInterrupt:
                print("\n\n⚠️  Process interrupted by user. Generating summary report...")
                self.print_summary_report()
                sys.exit(0)
        # Print final summary
        self.print_summary_report()

    def get_data_fields(self, gfg_file_text, source_filename):
        print("\n=== Step 3: Extracting Data Fields ===")
        print(f"🔍 Processing content for {source_filename}")
        start = time.time()

        print("📝 Preparing API request...")
        prompt = """
        Extract product information from the HTML content below and format it as CSV data.
        For each product found, extract these fields:
        - source_file
        - product_name
        - brand_name
        - certifications
        - categories
        - descriptors
        - ingredients
        - product_image_url
        - product_url
        - dietary_compatibility
        - gluten_free_score
        - price
        - stores
        - country

        Return the data in CSV format with headers, one product per line.
        If a field is not found, use 'N/A'.
        For gluten_free_score, use a scale of 0-100 based on certainty of gluten-free status.
        Add the source_file name as the first column for each row.
        """

        data = {
            "model": "gpt-4o",
            "messages": [
                {
                    "role": "system",
                    "content": "You are a precise data extraction assistant that converts HTML content into structured CSV data."
                },
                {
                    "role": "user",
                    "content": f"{prompt}\n\nSource File: {source_filename}\nHTML Content:\n{gfg_file_text}"
                }
            ],
            "temperature": 0.3,
            "max_tokens": 4000,
            "top_p": 0.95,
            "frequency_penalty": 0,
            "presence_penalty": 0
        }

        try:
            print("🌐 Sending request to OpenAI API...")
            response = requests.post(
                self.api_endpoint,
                json=data,
                headers=self.headers,
                timeout=90
            )
            response.raise_for_status()
            print("✓ Received response from API")

            completion = response.json()
            csv_data = completion['choices'][0]['message']['content']

            # Store processing time in stats
            processing_time = time.time() - start
            self.stats['processing_times'][source_filename] = processing_time

            print("\n=== API Response Statistics ===")
            print(f"⏱️  Processing time: {processing_time:.2f}s")
            print(f"📊 Rate limit remaining: {response.headers.get('x-ratelimit-remaining-tokens')}")
            print("=== Data Extraction Complete ===\n")

            return csv_data

        except requests.exceptions.RequestException as e:
            print(f"❌ API request failed: {str(e)}")
            return None

    def append_to_csv(self, csv_data, source_filename):
        print("\n=== Step 4: Saving Data to CSV ===")
        print(f"📁 Preparing to save data from {source_filename}")

        Path(self.output_file).parent.mkdir(parents=True, exist_ok=True)
        print("✓ Output directory verified")

        file_exists = os.path.exists(self.output_file)
        print(f"📝 Output file {'exists' if file_exists else 'will be created'}")

        lines = csv_data.strip().split('\n')
        if len(lines) < 2:
            print(f"❌ No valid data found in {source_filename}")
            return

        headers = lines[0].split(',')
        data_rows = lines[1:]

        self.stats[f'{source_filename}_products'] = len(data_rows)
        self.stats['total_products'] += len(data_rows)

        print(f"📊 Found {len(data_rows)} products to save")

        mode = 'a' if file_exists else 'w'
        with open(self.output_file, mode, newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(headers)
                print("✓ Wrote headers to new file")

            for row in data_rows:
                writer.writerow(row.split(','))
            print(f"✓ Wrote {len(data_rows)} rows of data")

        print(f"✅ Successfully saved data from {source_filename}")
        print("=== Save Complete ===\n")

    def print_summary_report(self):
        print("\n====== SCRAPING SUMMARY REPORT ======")

        print("\n=== Successfully Processed Files ===")
        for file in self.stats['processed_files']:
            size = self.stats['file_sizes'].get(file, 'N/A')
            time = self.stats['processing_times'].get(file, 'N/A')
            products = self.stats.get(f'{file}_products', 0)
            print(f"\n📄 {file}")
            print(f"  Size: {size:,} characters")
            print(f"  Processing Time: {time:.2f}s")
            print(f"  Products Extracted: {products}")

        print("\n=== Failed Files ===")
        for file in self.stats['failed_files']:
            size = self.stats['file_sizes'].get(file, 'N/A')
            error = self.stats['errors'].get(file, 'Unknown error')
            print(f"\n❌ {file}")
            print(f"  Size: {size:,} characters")
            print(f"  Error: {error}")

        print("\n=== Skipped Files ===")
        for file in self.stats['skipped_files']:
            print(f"⏭️  {file}")

        print("\n=== Truncated Files (Need Manual Review) ===")
        for file in self.stats['truncated_files']:
            size = self.stats['file_sizes'].get(file, 'N/A')
            print(f"⚠️  {file}: {size/1024:.1f}KB (only first 400KB processed)")

        print("\n=== Analysis ===")
        total_files = len(self.stats['processed_files']) + len(self.stats['failed_files']) + len(self.stats['skipped_files'])
        print(f"Total Files Found: {total_files}")
        print(f"Successfully Processed: {len(self.stats['processed_files'])}")
        print(f"Failed: {len(self.stats['failed_files'])}")
        print(f"Skipped: {len(self.stats['skipped_files'])}")
        print(f"Truncated: {len(self.stats['truncated_files'])}")
        print(f"Total Products Extracted: {self.stats['total_products']}")

        if self.stats['file_sizes']:
            avg_size = sum(self.stats['file_sizes'].values()) / len(self.stats['file_sizes'])
            print(f"\nAverage File Size: {avg_size:,.0f} characters")

        if self.stats['processing_times']:
            avg_time = sum(self.stats['processing_times'].values()) / len(self.stats['processing_times'])
            print(f"Average Processing Time: {avg_time:.2f}s")

        print("\n====== END OF REPORT ======")


def main():
    print("\n=== Starting OpenAI Scraper ===")
    scraper = OpenAIScraperClient()
    try:
        scraper.process_files()
    except KeyboardInterrupt:
        print("\n\n⚠️  Process interrupted by user. Exiting...")
    finally:
        print("\n=== Scraping Process Complete ===")


if __name__ == "__main__":
    main()
