#!/usr/bin/env python3
"""
Test script for product name extraction functionality
"""

import os
import sys
import logging
from dotenv import load_dotenv

# Add the project root directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from scripting.scripts.alcohol_data_integrity_deprecated import DataIntegrityProcessor, CONFIG

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def test_product_extraction():
    """Test product name extraction with sample data"""
    
    # Test cases for different scenarios
    test_cases = [
        # Case 1: Brand name contains product type
        {
            'brand_name': 'Molson Canadian Lager',
            'product_name': '',
            'subcategory': 'Beer',
            'expected_product': 'Lager',
            'expected_brand': 'Molson Canadian'
        },
        # Case 2: Subcategory contains product type
        {
            'brand_name': 'G-Vine',
            'product_name': '',
            'subcategory': 'Gin,',
            'expected_product': 'Gin',
            'expected_brand': 'G-Vine'
        },
        # Case 3: Complex brand name with product type
        {
            'brand_name': 'Jack Daniels Tennessee Whiskey',
            'product_name': '',
            'subcategory': 'Whiskey',
            'expected_product': 'Whiskey',
            'expected_brand': 'Jack Daniels Tennessee'
        },
        # Case 4: No product type in brand or subcategory (will need AI)
        {
            'brand_name': 'Corona',
            'product_name': '',
            'subcategory': 'Beer',
            'expected_product': 'Lager',  # AI should determine this
            'expected_brand': 'Corona'
        },
        # Case 5: Already has product name
        {
            'brand_name': 'Heineken',
            'product_name': 'Lager',
            'subcategory': 'Beer',
            'expected_product': 'Lager',  # Should remain unchanged
            'expected_brand': 'Heineken'
        }
    ]
    
    processor = DataIntegrityProcessor()
    
    print("Testing Product Name Extraction")
    print("=" * 50)
    
    # Create test products
    test_products = []
    for i, test_case in enumerate(test_cases):
        test_products.append({
            'lcbo_id': f'test_{i}',
            'brand_name': test_case['brand_name'],
            'product_name': test_case['product_name'],
            'category': 'Alcohol',
            'subcategory': test_case['subcategory'],
            'image_url': '',
            'source': 'Test'
        })
    
    # Temporarily enable AI extraction
    CONFIG["use_ai_product_extraction"] = True
    
    # Test individual extraction methods
    print(f"\nTesting individual extraction methods:")
    
    for i, test_case in enumerate(test_cases):
        print(f"\nTest Case {i+1}:")
        print(f"  Brand: '{test_case['brand_name']}'")
        print(f"  Product: '{test_case['product_name']}'")
        print(f"  Subcategory: '{test_case['subcategory']}'")
        
        # Test brand name extraction
        extracted_brand, extracted_product = processor.extract_product_from_brand_name(test_case['brand_name'])
        print(f"  Brand extraction: '{extracted_brand}' -> '{extracted_product}'")
        
        # Test subcategory extraction
        subcategory_product = processor.extract_product_from_subcategory(test_case['subcategory'])
        print(f"  Subcategory extraction: '{subcategory_product}'")
        
        print(f"  Expected: '{test_case['expected_product']}'")
        print("-" * 30)
    
    # Test the full fill_missing_product_names method
    print(f"\nTesting full product name filling:")
    updated_products, filled_count = processor.fill_missing_product_names(test_products)
    
    print(f"Filled {filled_count} product names")
    
    for i, product in enumerate(updated_products):
        print(f"  {i+1}. Brand: '{product['brand_name']}' -> Product: '{product['product_name']}'")

if __name__ == "__main__":
    test_product_extraction() 