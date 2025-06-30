#!/usr/bin/env python3
"""
Test script for AI brand extraction functionality
"""

import os
import sys
import json
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

def test_ai_extraction():
    """Test AI brand extraction with sample data"""
    
    # Test cases - mix of products that should and shouldn't be processed
    test_cases = [
        "Campbell Kind Wine Tawse Riesling 2019",  # Should extract brand
        "La Bélière Red Organic Wine 2019",  # Should extract brand
        "Jack Daniel's Tennessee Whiskey",  # Should extract brand
        "Heineken Lager Beer",  # Should extract brand
        "Red Wine 2019",  # Should skip (generic)
        "Budweiser American Lager",  # Should extract brand
        "Grey Goose Vodka",  # Should extract brand
        "Johnnie Walker Black Label Scotch Whisky",  # Should extract brand
        "Moët & Chandon Champagne",  # Should extract brand
        "Corona Extra Beer",  # Should extract brand
        "White Wine 2020",  # Should skip (generic)
        "Premium Vodka",  # Should skip (generic)
        "Organic Red Wine",  # Should skip (generic)
        "Reserve Whiskey",  # Should skip (generic)
    ]
    
    processor = DataIntegrityProcessor()
    
    print("Testing AI Brand Extraction")
    print("=" * 50)
    
    # Create test products
    test_products = []
    for i, product_name in enumerate(test_cases):
        test_products.append({
            'lcbo_id': f'test_{i}',
            'brand_name': '',  # Empty brand to trigger extraction
            'product_name': product_name,
            'price': '',
            'category': 'Alcohol',
            'subcategory': 'Wine',
            'image_url': '',
            'source': 'Test'
        })
    
    # Temporarily enable AI extraction
    CONFIG["use_ai_brand_extraction"] = True
    
    # Check if API key is available
    api_key = CONFIG.get("openai_api_key")
    if not api_key:
        print("WARNING: No OpenAI API key found!")
        print("Please set OPENAI_API_KEY in your .env file")
        return
    else:
        print(f"OpenAI API key found: {api_key[:10]}...")
    
    # Show cost estimate
    cost_estimate = processor.estimate_ai_costs(test_products)
    print(f"\nCost Estimate:")
    print(f"  Products to process: {cost_estimate['products_to_process']}")
    print(f"  Estimated tokens: {cost_estimate['estimated_tokens']}")
    print(f"  Estimated cost: ${cost_estimate['total_cost']}")
    if cost_estimate['products_to_process'] > 0:
        print(f"  Cost per product: ${cost_estimate['cost_per_product']}")
    else:
        print(f"  Cost per product: $0.00")
    
    # Debug: Show which products are being filtered out
    print(f"\nDebug: Checking which products would be processed:")
    for product in test_products:
        product_name = product.get('product_name', '')
        existing_brand = product.get('brand_name', '')
        
        print(f"  '{product_name}' (brand: '{existing_brand}')")
        
        # Check each filter condition
        if not product_name:
            print(f"    -> Skipped: No product name")
            continue
        if existing_brand:
            print(f"    -> Skipped: Has existing brand")
            continue
        if len(product_name) < 5:
            print(f"    -> Skipped: Too short ({len(product_name)} chars)")
            continue
        if len(product_name) > 100:
            print(f"    -> Skipped: Too long ({len(product_name)} chars)")
            continue
            
        words = product_name.lower().split()
        generic_starters = [
            'red', 'white', 'rose', 'sparkling', 'beer', 'lager', 'ale',
            'whiskey', 'whisky', 'vodka', 'gin', 'rum', 'tequila', 'brandy', 'liqueur',
            'organic', 'natural', 'premium', 'reserve', 'select', 'classic', 'original'
        ]
        
        if words and words[0] in generic_starters:
            print(f"    -> Skipped: Starts with generic term '{words[0]}'")
            continue
            
        print(f"    -> Would be processed")
    
    # Test individual extraction with more debugging
    print(f"\nTesting individual product extraction:")
    for product_name in test_cases[:3]:  # Test first 3 to save API calls
        print(f"\nInput: {product_name}")
        
        # Check if this product would be processed
        if len(product_name) < 5:
            print(f"  -> Skipped: Too short")
            continue
        if len(product_name) > 100:
            print(f"  -> Skipped: Too long")
            continue
            
        words = product_name.lower().split()
        generic_starters = [
            'red', 'white', 'rose', 'sparkling', 'beer', 'lager', 'ale',
            'whiskey', 'whisky', 'vodka', 'gin', 'rum', 'tequila', 'brandy', 'liqueur',
            'organic', 'natural', 'premium', 'reserve', 'select', 'classic', 'original'
        ]
        
        if words and words[0] in generic_starters:
            print(f"  -> Skipped: Starts with generic term '{words[0]}'")
            continue
        
        print(f"  -> Processing with AI...")
        extracted_brand, cleaned_product = processor.extract_brand_from_product_name(product_name)
        
        print(f"  Brand: '{extracted_brand}'")
        print(f"  Product: '{cleaned_product}'")
        print("-" * 30)

if __name__ == "__main__":
    test_ai_extraction() 