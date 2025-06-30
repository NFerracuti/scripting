"""
Alcohol Data AI Processor
=========================

Handles all AI/OpenAI operations for alcohol data integrity scripts.

HOW TO USE:
This module is imported by other scripts to handle AI operations.
You don't run this file directly.

FUNCTIONS:
- Brand extraction using OpenAI API
- Product type determination
- Batch processing for cost management
- Cost estimation for AI operations
- AI decision logic for when to use AI

REQUIREMENTS:
- OpenAI API key
- Internet connection for API calls
- Proper API rate limits and quotas
"""

import json
import logging
import time
from typing import Dict, List, Any, Tuple, Optional
import openai
from alcohol_data_config import CONFIG, GENERIC_INDICATORS, SPIRIT_BRANDS, GENERIC_SUBCATEGORIES

logger = logging.getLogger(__name__)

class AlcoholAIProcessor:
    """Handles AI operations for alcohol data processing"""
    
    def __init__(self):
        if CONFIG["openai_api_key"]:
            openai.api_key = CONFIG["openai_api_key"]
        else:
            logger.warning("OpenAI API key not configured")
    
    def extract_brand_from_product_name(self, product_name: str, existing_brand: str = None) -> Tuple[str, str]:
        """Extract brand name from product name using AI"""
        if not CONFIG["use_ai_brand_extraction"] or not CONFIG["openai_api_key"]:
            return existing_brand or "", product_name
            
        # Skip if product name is too short, already has a brand, or is too long
        if (len(product_name) < 5 or 
            existing_brand or 
            len(product_name) > 100):  # Skip very long product names to save tokens
            return existing_brand or "", product_name
            
        # Skip if product name looks like it doesn't contain a brand (e.g., generic descriptions)
        product_lower = product_name.lower()
        if any(indicator in product_lower for indicator in GENERIC_INDICATORS):
            # Check if it starts with a generic term (likely no brand)
            words = product_lower.split()
            if words and words[0] in GENERIC_INDICATORS:
                return existing_brand or "", product_name
            
        try:
            # Optimized prompt - much shorter to save tokens
            prompt = f"""Extract brand from: "{product_name}"
Return JSON: {{"brand": "BrandName", "product": "RemainingProduct"}}
Examples:
"Campbell Kind Wine Tawse Riesling 2019" → {{"brand": "Campbell Kind Wine", "product": "Tawse Riesling 2019"}}
"La Bélière Red Organic Wine 2019" → {{"brand": "La Bélière", "product": "Red Organic Wine 2019"}}
"Red Wine 2019" → {{"brand": "", "product": "Red Wine 2019"}}"""

            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "Extract brand names. Return only JSON."},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.1,
                max_tokens=50  # Reduced from 100 to save tokens
            )
            
            result_text = response.choices[0].message.content.strip()
            
            # Parse JSON response
            try:
                result = json.loads(result_text)
                extracted_brand = result.get("brand", "").strip()
                cleaned_product = result.get("product", "").strip()
                
                # Validate the extraction
                if extracted_brand and len(extracted_brand) > 1:
                    # Make sure the cleaned product is not empty
                    if not cleaned_product:
                        cleaned_product = product_name
                    
                    return extracted_brand, cleaned_product
                else:
                    return existing_brand or "", product_name
                    
            except json.JSONDecodeError:
                logger.warning(f"Failed to parse AI response for '{product_name}': {result_text}")
                return existing_brand or "", product_name
                
        except Exception as e:
            logger.warning(f"AI brand extraction failed for '{product_name}': {e}")
            return existing_brand or "", product_name

    def determine_product_type_with_ai(self, brand_name: str, subcategory: str) -> str:
        """Use AI to determine the product type based on brand name and subcategory"""
        if not CONFIG["openai_api_key"]:
            logger.warning("OpenAI API key not found for AI product extraction")
            return ""
            
        try:
            # Create a much more strict prompt
            prompt = f"""Given the brand name "{brand_name}" and subcategory "{subcategory}", determine the specific product type.

IMPORTANT: Return ONLY the product type name, nothing else. No explanations, no reasoning, no additional text.

For spirits and liqueurs, be specific about the type:
- Jagermeister → Herbal Liqueur
- Cointreau → Orange Liqueur 
- Grand Marnier → Orange Liqueur
- Baileys → Irish Cream Liqueur
- Kahlua → Coffee Liqueur
- Amaretto → Almond Liqueur
- Frangelico → Hazelnut Liqueur
- Chambord → Raspberry Liqueur
- Midori → Melon Liqueur
- Malibu → Coconut Rum
- Captain Morgan → Spiced Rum
- Bacardi → White Rum
- Grey Goose → Vodka
- Ketel One → Vodka
- Absolut → Vodka
- Smirnoff → Vodka
- Jack Daniels → Tennessee Whiskey
- Jim Beam → Bourbon Whiskey
- Makers Mark → Bourbon Whiskey
- Wild Turkey → Bourbon Whiskey
- Crown Royal → Canadian Whisky
- Canadian Club → Canadian Whisky
- Seagrams → Canadian Whisky
- Dewars → Scotch Whisky
- Johnnie Walker → Scotch Whisky
- Macallan → Single Malt Scotch
- Glenfiddich → Single Malt Scotch
- Glenlivet → Single Malt Scotch
- Lagavulin → Single Malt Scotch
- Ardbeg → Single Malt Scotch
- Laphroaig → Single Malt Scotch
- Talisker → Single Malt Scotch
- Highland Park → Single Malt Scotch
- Balvenie → Single Malt Scotch
- Dalmore → Single Malt Scotch

For other alcohol types:
- Wine brands → Wine (or specific type like Red Wine, White Wine, Rosé Wine)
- Beer brands → Beer (or specific type like Lager, Ale, Stout)
- Cider brands → Cider

If you cannot determine a specific type, return exactly: "Unknown"

Remember: Return ONLY the product type name, nothing else."""

            response = openai.ChatCompletion.create(
                model="gpt-3.5-turbo",
                messages=[
                    {"role": "system", "content": "You are a data processor. Return ONLY the requested product type name. No explanations, no reasoning, no additional text."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=20,
                temperature=0.0
            )
            
            product_type = response.choices[0].message.content.strip()
            
            # Clean up the response - be very strict
            if product_type and product_type.lower() not in ['unknown', 'other', 'misc', 'miscellaneous']:
                # Remove any explanatory text that might have been included
                lines = product_type.split('\n')
                first_line = lines[0].strip()
                
                # If it looks like an explanation, return empty
                if len(first_line) > 50 or 'based on' in first_line.lower() or 'therefore' in first_line.lower():
                    return ""
                    
                return first_line
            else:
                return ""
                
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}")
            return ""

    def extract_brand_from_product_name_batch(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Extract brand names from product names in batches"""
        if not CONFIG["use_ai_brand_extraction"] or not CONFIG["openai_api_key"]:
            return products
            
        # Filter products that actually need brand extraction
        products_needing_extraction = []
        for product in products:
            product_name = product.get('product_name', '')
            existing_brand = product.get('brand_name', '')
            
            # Only process if:
            # 1. Has a product name
            # 2. No existing brand
            # 3. Product name is not too short or too long
            # 4. Product name doesn't start with generic terms
            if (product_name and 
                not existing_brand and 
                5 <= len(product_name) <= 100):
                
                # Check if it starts with generic terms
                words = product_name.lower().split()
                generic_starters = [
                    'red', 'white', 'rose', 'sparkling', 'beer', 'lager', 'ale',
                    'whiskey', 'whisky', 'vodka', 'gin', 'rum', 'tequila', 'brandy', 'liqueur',
                    'organic', 'natural', 'premium', 'reserve', 'select', 'classic', 'original'
                ]
                
                if not words or words[0] not in generic_starters:
                    products_needing_extraction.append(product)
        
        if not products_needing_extraction:
            logger.info("No products need brand extraction")
            return products
            
        # Limit the number of products to process with AI
        max_products = CONFIG["max_ai_products"]
        if len(products_needing_extraction) > max_products:
            logger.info(f"Limiting AI processing to {max_products} products (out of {len(products_needing_extraction)} that need extraction)")
            products_needing_extraction = products_needing_extraction[:max_products]
            
        logger.info(f"Processing {len(products_needing_extraction)} products with AI brand extraction (out of {len(products)} total)")
        
        # Process in batches
        batch_size = CONFIG["batch_size"]
        processed_products = []
        extraction_count = 0
        
        for i in range(0, len(products_needing_extraction), batch_size):
            batch = products_needing_extraction[i:i + batch_size]
            logger.info(f"Processing batch {i//batch_size + 1}/{(len(products_needing_extraction) + batch_size - 1)//batch_size}")
            
            for product in batch:
                product_name = product.get('product_name', '')
                existing_brand = product.get('brand_name', '')
                
                extracted_brand, cleaned_product = self.extract_brand_from_product_name(product_name, existing_brand)
                
                if extracted_brand and extracted_brand != existing_brand:
                    product['brand_name'] = extracted_brand
                    product['product_name'] = cleaned_product
                    extraction_count += 1
                    logger.info(f"Extracted brand '{extracted_brand}' from '{product_name}'")
                
                processed_products.append(product)
                
            # Small delay between batches to avoid rate limits
            time.sleep(1)
        
        logger.info(f"Successfully extracted brands for {extraction_count} products")
        
        # Return all products (both processed and unprocessed)
        processed_ids = {f"{p.get('brand_name', '')}_{p.get('product_name', '')}_{p.get('lcbo_id', '')}" 
                        for p in processed_products}
        
        final_products = []
        for product in products:
            product_id = f"{product.get('brand_name', '')}_{product.get('product_name', '')}_{product.get('lcbo_id', '')}"
            if product_id in processed_ids:
                # Find the processed version
                for processed in processed_products:
                    processed_id = f"{processed.get('brand_name', '')}_{processed.get('product_name', '')}_{processed.get('lcbo_id', '')}"
                    if processed_id == product_id:
                        final_products.append(processed)
                        break
            else:
                final_products.append(product)
                
        return final_products

    def estimate_ai_costs(self, products: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Estimate the cost of AI processing"""
        if not CONFIG["use_ai_brand_extraction"] or not CONFIG["openai_api_key"]:
            return {"total_cost": 0, "products_to_process": 0, "estimated_tokens": 0}
            
        # Filter products that need extraction
        products_needing_extraction = []
        for product in products:
            product_name = product.get('product_name', '')
            existing_brand = product.get('brand_name', '')
            
            if (product_name and 
                not existing_brand and 
                5 <= len(product_name) <= 100):
                
                words = product_name.lower().split()
                generic_starters = [
                    'red', 'white', 'rose', 'sparkling', 'beer', 'lager', 'ale',
                    'whiskey', 'whisky', 'vodka', 'gin', 'rum', 'tequila', 'brandy', 'liqueur',
                    'organic', 'natural', 'premium', 'reserve', 'select', 'classic', 'original'
                ]
                
                if not words or words[0] not in generic_starters:
                    products_needing_extraction.append(product)
        
        # Limit to max products
        max_products = CONFIG["max_ai_products"]
        if len(products_needing_extraction) > max_products:
            products_needing_extraction = products_needing_extraction[:max_products]
        
        # Estimate tokens (rough calculation)
        # Each request: ~100 tokens input + ~50 tokens output = ~150 tokens per product
        estimated_tokens = len(products_needing_extraction) * 150
        
        # Cost calculation (GPT-3.5-turbo: $0.002 per 1K tokens)
        cost_per_1k_tokens = 0.002
        total_cost = (estimated_tokens / 1000) * cost_per_1k_tokens
        
        return {
            "total_cost": round(total_cost, 4),
            "products_to_process": len(products_needing_extraction),
            "estimated_tokens": estimated_tokens,
            "cost_per_product": round(total_cost / len(products_needing_extraction), 4) if products_needing_extraction else 0
        }

    def should_use_ai_for_product(self, brand_name: str, subcategory: str) -> bool:
        """Determine if we should use AI for product extraction based on brand and subcategory"""
        if not brand_name:
            return False
            
        # Check if brand is a known spirit brand
        brand_lower = brand_name.lower().strip()
        if brand_lower in SPIRIT_BRANDS:
            return True
            
        # Check if subcategory is generic
        if subcategory.lower().strip() in GENERIC_SUBCATEGORIES:
            return True
            
        return False 