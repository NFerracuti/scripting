"""
Alcohol Data Processor
======================

Handles data processing, normalization, duplicate detection, and product name extraction.

HOW TO USE:
This module is imported by other scripts to handle data processing operations.
You don't run this file directly.

FUNCTIONS:
- Subcategory normalization
- Brand name normalization and consolidation
- Duplicate detection (exact and fuzzy matching)
- Product name extraction and validation
- Data quality improvements

REQUIREMENTS:
- Python 3.7+
- difflib (built-in)
- collections (built-in)
- logging (built-in)
"""

import logging
import difflib
from typing import Dict, List, Any, Tuple, Optional
from collections import defaultdict
from alcohol_data_config import (
    CONFIG, SUBCATEGORY_NORMALIZATIONS, BRAND_VARIATIONS, 
    PRODUCT_TYPES, INVALID_PRODUCT_NAMES, GENERIC_SUBCATEGORIES
)
from alcohol_ai_processor import AlcoholAIProcessor

logger = logging.getLogger(__name__)

class AlcoholDataProcessor:
    """Handles data processing and normalization operations"""
    
    def __init__(self):
        self.ai_processor = AlcoholAIProcessor()
    
    def normalize_subcategory(self, subcategory: str) -> str:
        """Normalize subcategory names to standard format"""
        if not subcategory:
            return ""
            
        # Clean up the subcategory
        cleaned = subcategory.strip()
        
        # Check our normalization mappings
        if cleaned in SUBCATEGORY_NORMALIZATIONS:
            return SUBCATEGORY_NORMALIZATIONS[cleaned]
            
        # Try to find close matches
        for original, normalized in SUBCATEGORY_NORMALIZATIONS.items():
            if difflib.SequenceMatcher(None, cleaned.lower(), original.lower()).ratio() > 0.9:
                logger.info(f"Normalizing subcategory: '{cleaned}' -> '{normalized}'")
                return normalized
                
        return cleaned

    def normalize_brand_name(self, brand_name: str) -> str:
        """Normalize brand name for consistency"""
        if not brand_name:
            return ""
            
        # Basic normalization
        normalized = brand_name.strip()
        
        # Handle common variations
        if normalized in BRAND_VARIATIONS:
            return BRAND_VARIATIONS[normalized]
            
        return normalized

    def normalize_brand_alternatives(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize brand names and consolidate brand alternatives"""
        logger.info("Normalizing brand names and consolidating alternatives...")
        
        # Group brands by normalized name
        brand_groups = defaultdict(list)
        for product in products:
            brand_name = product.get('brand_name', '')
            if brand_name:
                # Normalize brand name for grouping (lowercase, remove apostrophes)
                normalized = brand_name.lower().replace("'", "").replace("'", "").replace('"', '').replace('"', '')
                brand_groups[normalized].append(brand_name)
        
        # Find brand alternatives and choose the best version
        brand_mapping = {}
        consolidated_count = 0
        
        for normalized, alternatives in brand_groups.items():
            if len(alternatives) > 1:
                # Choose the best version (prefer versions with proper apostrophes, proper capitalization)
                best_brand = self.choose_best_brand_name(alternatives)
                
                # Map all alternatives to the best version
                for alternative in alternatives:
                    if alternative != best_brand:
                        brand_mapping[alternative] = best_brand
                        consolidated_count += 1
                        
                logger.info(f"Consolidating brand alternatives for '{normalized}':")
                logger.info(f"  {alternatives} -> '{best_brand}'")
        
        # Apply the brand mapping to all products
        updated_products = []
        for product in products:
            current_brand = product.get('brand_name', '')
            if current_brand in brand_mapping:
                product['brand_name'] = brand_mapping[current_brand]
            updated_products.append(product)
        
        logger.info(f"Consolidated {consolidated_count} brand name variations")
        return updated_products

    def choose_best_brand_name(self, alternatives: List[str]) -> str:
        """Choose the best version of a brand name from alternatives"""
        if not alternatives:
            return ""
        
        # Scoring criteria (higher score = better)
        def score_brand(brand):
            score = 0
            
            # Prefer versions with proper apostrophes
            if "'" in brand:
                score += 10
            
            # Prefer proper capitalization (not all caps)
            if brand.isupper():
                score -= 5
            elif brand[0].isupper() and not brand.isupper():
                score += 5
            
            # Prefer shorter names (less likely to have extra words)
            score -= len(brand.split())
            
            # Prefer names without extra spaces
            score -= brand.count('  ')
            
            return score
        
        # Score all alternatives and return the best
        scored_alternatives = [(brand, score_brand(brand)) for brand in alternatives]
        best_brand = max(scored_alternatives, key=lambda x: x[1])[0]
        
        return best_brand

    def find_exact_duplicates(self, products: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """Find exact duplicate products based on brand_name and product_name (case-insensitive)"""
        logger.info("Finding exact duplicates (case-insensitive)...")
        
        # Create a dictionary to group products by their normalized key
        duplicate_groups = defaultdict(list)
        
        for product in products:
            brand_name = product.get('brand_name', '').strip().lower()
            product_name = product.get('product_name', '').strip().lower()
            
            # Create a unique key for exact matching
            if brand_name and product_name:
                key = f"{brand_name}|{product_name}"
                duplicate_groups[key].append(product)
        
        # Find groups with more than one product (duplicates)
        exact_duplicates = []
        total_duplicates_found = 0
        
        for key, product_group in duplicate_groups.items():
            if len(product_group) > 1:
                brand_name, product_name = key.split('|', 1)
                logger.info(f"Exact duplicate found for '{brand_name}' - '{product_name}': {len(product_group)} entries")
                
                # Log the duplicate entries for debugging
                for i, product in enumerate(product_group):
                    logger.info(f"  Duplicate {i+1}: {product.get('brand_name', '')} - {product.get('product_name', '')}")
                
                exact_duplicates.append(product_group)
                total_duplicates_found += len(product_group) - 1  # Count extra duplicates (not the first one)
        
        logger.info(f"Found {len(exact_duplicates)} groups of exact duplicates")
        logger.info(f"Total duplicate entries: {total_duplicates_found}")
        return exact_duplicates

    def find_duplicates(self, products: List[Dict[str, Any]], use_exact_matching: bool = False) -> List[List[Dict[str, Any]]]:
        """Find duplicate products based on brand and product name
        
        Args:
            products: List of product dictionaries
            use_exact_matching: If True, use exact case-insensitive matching. 
                               If False, use fuzzy similarity matching.
        """
        if use_exact_matching:
            return self.find_exact_duplicates(products)
        
        logger.info("Finding duplicate products using fuzzy matching...")
        
        # Group by brand first
        brand_groups = defaultdict(list)
        for product in products:
            brand = product.get('brand_name', '').lower()
            brand_groups[brand].append(product)
            
        duplicates = []
        total_products_checked = 0
        
        for brand, brand_products in brand_groups.items():
            if len(brand_products) <= 1:
                continue
                
            # Check for duplicates within this brand
            processed = set()
            for i, product1 in enumerate(brand_products):
                if i in processed:
                    continue
                    
                similar_products = [product1]
                
                for j, product2 in enumerate(brand_products[i+1:], i+1):
                    if j in processed:
                        continue
                        
                    # Calculate similarity
                    product1_name = product1.get('product_name', '').lower()
                    product2_name = product2.get('product_name', '').lower()
                    
                    if product1_name and product2_name:
                        similarity = difflib.SequenceMatcher(None, product1_name, product2_name).ratio()
                        total_products_checked += 1
                        
                        if similarity >= CONFIG["similarity_threshold"]:
                            logger.info(f"Duplicate found (similarity {similarity:.3f}):")
                            logger.info(f"  Product 1: {product1.get('brand_name', '')} - {product1_name}")
                            logger.info(f"  Product 2: {product2.get('brand_name', '')} - {product2_name}")
                            similar_products.append(product2)
                            processed.add(j)
                            
                if len(similar_products) > 1:
                    duplicates.append(similar_products)
                    processed.add(i)
                    
        logger.info(f"Found {len(duplicates)} groups of duplicate products")
        logger.info(f"Total product comparisons made: {total_products_checked}")
        return duplicates

    def merge_duplicates(self, duplicates: List[List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
        """Merge duplicate products, keeping the best data from each"""
        logger.info("Merging duplicate products...")
        
        merged_products = []
        
        for duplicate_group in duplicates:
            if not duplicate_group:
                continue
                
            # Sort by data quality (completeness first)
            sorted_group = sorted(duplicate_group, key=lambda p: (
                not p.get('image_url'),  # Has image first
                not p.get('price'),  # Has price first
                len(p.get('product_name', ''))  # Longer product name first
            ))
            
            # Use the best product as base
            best_product = sorted_group[0].copy()
            
            # Merge additional data from other duplicates
            for duplicate in sorted_group[1:]:
                # Add missing data
                if not best_product.get('image_url') and duplicate.get('image_url'):
                    best_product['image_url'] = duplicate['image_url']
                if not best_product.get('price') and duplicate.get('price'):
                    best_product['price'] = duplicate['price']
                if not best_product.get('lcbo_id') and duplicate.get('lcbo_id'):
                    best_product['lcbo_id'] = duplicate['lcbo_id']
                    
            merged_products.append(best_product)
            
        logger.info(f"Merged {len(merged_products)} products from {sum(len(group) for group in duplicates)} duplicates")
        return merged_products

    def extract_product_from_brand_name(self, brand_name: str) -> Tuple[str, str]:
        """Extract product type from brand name if it contains a product type"""
        if not brand_name:
            return "", ""
            
        brand_lower = brand_name.lower()
        
        # Look for product types in the brand name
        for product_type in PRODUCT_TYPES:
            if product_type in brand_lower:
                # Find the position of the product type
                start_pos = brand_lower.find(product_type)
                end_pos = start_pos + len(product_type)
                
                # Extract brand name (before the product type)
                brand_part = brand_name[:start_pos].strip()
                if brand_part.endswith((' ', '-', '–', '—')):
                    brand_part = brand_part[:-1].strip()
                
                # Extract product name (the product type)
                product_part = brand_name[start_pos:end_pos].strip()
                
                # Clean up product name (capitalize properly)
                if product_part:
                    product_part = product_part.title()
                    # Handle special cases
                    if product_part.lower() in ['ipa', 'cabernet', 'merlot', 'pinot noir', 'sauvignon blanc']:
                        product_part = product_part.upper()
                
                return brand_part, product_part
                
        return brand_name, ""

    def extract_product_from_subcategory(self, subcategory: str) -> str:
        """Extract product name from subcategory field"""
        if not subcategory:
            return ""
            
        # Clean up subcategory
        cleaned = subcategory.strip()
        
        # Remove trailing commas and clean up
        cleaned = cleaned.rstrip(',').strip()
        
        # If it's a simple product type, return it
        if cleaned and len(cleaned) > 1:
            # Capitalize properly
            if cleaned.lower() in ['ipa', 'cabernet', 'merlot', 'pinot noir', 'sauvignon blanc']:
                return cleaned.upper()
            else:
                return cleaned.title()
                
        return ""

    def is_valid_product_name(self, product_name: str) -> bool:
        """Check if a product name is valid (not a generic placeholder)"""
        if not product_name:
            return False
            
        # Check if the product name is in the invalid list
        if product_name.lower().strip() in INVALID_PRODUCT_NAMES:
            return False
            
        # Check if it's too short (likely not descriptive)
        if len(product_name.strip()) < 3:
            return False
            
        return True

    def fill_missing_product_names(self, products: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], int]:
        """Fill in missing product names using logical rules and AI"""
        logger.info("Filling missing product names...")
        
        filled_count = 0
        updated_products = []
        test_output = []  # For detailed test output
        
        for product in products:
            product_name = product.get('product_name', '')
            brand_name = product.get('brand_name', '')
            subcategory = product.get('subcategory', '')
            
            # Skip if product name already exists and is valid
            if product_name and self.is_valid_product_name(product_name):
                updated_products.append(product)
                continue
                
            # Check if we should use AI directly for this product
            if self.ai_processor.should_use_ai_for_product(brand_name, subcategory):
                if CONFIG["use_ai_product_extraction"] and CONFIG["openai_api_key"]:
                    ai_product = self.ai_processor.determine_product_type_with_ai(brand_name, subcategory)
                    if ai_product and self.is_valid_product_name(ai_product):
                        if CONFIG["test_product_extraction"]:
                            test_output.append({
                                'type': 'ai_extraction_direct',
                                'original_brand': brand_name,
                                'new_brand': brand_name,
                                'new_product': ai_product,
                                'method': f'AI direct (spirit/generic subcategory)'
                            })
                        
                        product['product_name'] = ai_product
                        filled_count += 1
                        logger.info(f"AI determined product '{ai_product}' for spirit brand '{brand_name}'")
                        updated_products.append(product)
                        continue
                else:
                    logger.warning(f"AI product extraction disabled but needed for spirit brand '{brand_name}'")
                    updated_products.append(product)
                    continue
                
            # Step 1: Check if brand name contains a product type
            if brand_name:
                extracted_brand, extracted_product = self.extract_product_from_brand_name(brand_name)
                if extracted_product and self.is_valid_product_name(extracted_product):
                    if CONFIG["test_product_extraction"]:
                        test_output.append({
                            'type': 'brand_extraction',
                            'original_brand': brand_name,
                            'new_brand': extracted_brand,
                            'new_product': extracted_product,
                            'method': 'Extracted from brand name'
                        })
                    
                    product['brand_name'] = extracted_brand
                    product['product_name'] = extracted_product
                    filled_count += 1
                    logger.info(f"Extracted product '{extracted_product}' from brand '{brand_name}'")
                    updated_products.append(product)
                    continue
            
            # Step 2: Extract from subcategory
            if subcategory:
                subcategory_product = self.extract_product_from_subcategory(subcategory)
                if subcategory_product and self.is_valid_product_name(subcategory_product):
                    if CONFIG["test_product_extraction"]:
                        test_output.append({
                            'type': 'subcategory_extraction',
                            'original_brand': brand_name,
                            'new_brand': brand_name,
                            'new_product': subcategory_product,
                            'method': f'Extracted from subcategory "{subcategory}"'
                        })
                    
                    product['product_name'] = subcategory_product
                    filled_count += 1
                    logger.info(f"Extracted product '{subcategory_product}' from subcategory '{subcategory}'")
                    updated_products.append(product)
                    continue
            
            # Step 3: Use AI to determine product type
            if brand_name and CONFIG["use_ai_product_extraction"] and CONFIG["openai_api_key"]:
                ai_product = self.ai_processor.determine_product_type_with_ai(brand_name, subcategory)
                if ai_product and self.is_valid_product_name(ai_product):
                    if CONFIG["test_product_extraction"]:
                        test_output.append({
                            'type': 'ai_extraction',
                            'original_brand': brand_name,
                            'new_brand': brand_name,
                            'new_product': ai_product,
                            'method': f'AI determined based on brand "{brand_name}" and subcategory "{subcategory}"'
                        })
                    
                    product['product_name'] = ai_product
                    filled_count += 1
                    logger.info(f"AI determined product '{ai_product}' for brand '{brand_name}'")
                    updated_products.append(product)
                    continue
            
            # No valid product name could be determined
            if CONFIG["test_product_extraction"] and brand_name:
                test_output.append({
                    'type': 'no_extraction',
                    'original_brand': brand_name,
                    'new_brand': brand_name,
                    'new_product': '',
                    'method': 'Could not determine valid product type'
                })
            
            updated_products.append(product)
        
        # Show detailed test output if in test mode
        if CONFIG["test_product_extraction"] and test_output:
            self._print_test_output(test_output)
        
        logger.info(f"Filled {filled_count} missing product names")
        return updated_products, filled_count

    def _print_test_output(self, test_output: List[Dict[str, Any]]):
        """Print detailed test output for product extraction"""
        logger.info("\n" + "="*60)
        logger.info("PRODUCT EXTRACTION TEST RESULTS")
        logger.info("="*60)
        
        # Group by extraction type
        brand_extractions = [item for item in test_output if item['type'] == 'brand_extraction']
        subcategory_extractions = [item for item in test_output if item['type'] == 'subcategory_extraction']
        ai_extractions = [item for item in test_output if item['type'] == 'ai_extraction']
        ai_direct_extractions = [item for item in test_output if item['type'] == 'ai_extraction_direct']
        no_extractions = [item for item in test_output if item['type'] == 'no_extraction']
        
        if brand_extractions:
            logger.info(f"\nBRAND EXTRACTION ({len(brand_extractions)} products):")
            for item in brand_extractions[:10]:  # Show first 10
                logger.info(f"  '{item['original_brand']}' -> Brand: '{item['new_brand']}', Product: '{item['new_product']}'")
            if len(brand_extractions) > 10:
                logger.info(f"  ... and {len(brand_extractions) - 10} more")
        
        if subcategory_extractions:
            logger.info(f"\nSUBCATEGORY EXTRACTION ({len(subcategory_extractions)} products):")
            for item in subcategory_extractions[:10]:  # Show first 10
                logger.info(f"  '{item['original_brand']}' -> Product: '{item['new_product']}' ({item['method']})")
            if len(subcategory_extractions) > 10:
                logger.info(f"  ... and {len(subcategory_extractions) - 10} more")
        
        if ai_direct_extractions:
            logger.info(f"\nAI DIRECT EXTRACTION ({len(ai_direct_extractions)} products):")
            for item in ai_direct_extractions[:10]:  # Show first 10
                logger.info(f"  '{item['original_brand']}' -> Product: '{item['new_product']}' (spirit/generic)")
            if len(ai_direct_extractions) > 10:
                logger.info(f"  ... and {len(ai_direct_extractions) - 10} more")
        
        if ai_extractions:
            logger.info(f"\nAI EXTRACTION ({len(ai_extractions)} products):")
            for item in ai_extractions[:10]:  # Show first 10
                logger.info(f"  '{item['original_brand']}' -> Product: '{item['new_product']}'")
            if len(ai_extractions) > 10:
                logger.info(f"  ... and {len(ai_extractions) - 10} more")
        
        if no_extractions:
            logger.info(f"\nNO EXTRACTION POSSIBLE ({len(no_extractions)} products):")
            for item in no_extractions[:10]:  # Show first 10
                logger.info(f"  '{item['original_brand']}' -> Could not determine valid product type")
            if len(no_extractions) > 10:
                logger.info(f"  ... and {len(no_extractions) - 10} more")
        
        logger.info("="*60)

    def create_descriptors(self, product: Dict[str, Any]) -> str:
        """Create descriptors field from various product attributes"""
        descriptors = []
        
        # Add image URL if available
        if product.get('image_url'):
            descriptors.append(f"Image: {product.get('image_url')}")
        
        # Add LCBO ID if available
        if product.get('lcbo_id'):
            descriptors.append(f"LCBO ID: {product.get('lcbo_id')}")
        
        # Add source if available
        if product.get('source'):
            descriptors.append(f"Source: {product.get('source')}")
        
        # Add gluten free score if available
        if product.get('gluten_free_score'):
            descriptors.append(f"Gluten Free Score: {product.get('gluten_free_score')}")
        
        return "; ".join(descriptors)

    def create_brand_entries(self, products: List[Dict[str, Any]]):
        """Create Brand model entries for all unique brands"""
        logger.info("Creating Brand model entries...")
        
        # This would typically be done in Django, but for now we'll log the brands
        unique_brands = set()
        brand_alternatives = defaultdict(set)
        
        for product in products:
            brand_name = product.get('brand_name', '')
            if brand_name:
                unique_brands.add(brand_name)
                # Group similar brand names
                normalized = brand_name.lower().replace("'", "").replace("'", "")
                brand_alternatives[normalized].add(brand_name)
                
        logger.info(f"Found {len(unique_brands)} unique brands")
        
        # Log brand normalization suggestions
        for normalized, alternatives in brand_alternatives.items():
            if len(alternatives) > 1:
                logger.info(f"Brand alternatives for '{normalized}': {list(alternatives)}")
                
        return unique_brands

    def remove_duplicates(self, products: List[Dict[str, Any]], use_exact_matching: bool = True) -> List[Dict[str, Any]]:
        """Remove duplicate products, keeping the best version of each
        
        Args:
            products: List of product dictionaries
            use_exact_matching: If True, use exact case-insensitive matching.
                               If False, use fuzzy similarity matching.
        
        Returns:
            List of products with duplicates removed
        """
        logger.info(f"Removing duplicates using {'exact' if use_exact_matching else 'fuzzy'} matching...")
        
        # Find duplicates
        duplicates = self.find_duplicates(products, use_exact_matching=use_exact_matching)
        
        if not duplicates:
            logger.info("No duplicates found")
            return products
        
        # Merge duplicates to get the best version of each
        merged_products = self.merge_duplicates(duplicates)
        
        # Get all products that weren't part of duplicate groups
        duplicate_product_ids = set()
        for duplicate_group in duplicates:
            for product in duplicate_group:
                # Create a unique identifier for each product
                product_id = f"{product.get('brand_name', '').lower()}|{product.get('product_name', '').lower()}"
                duplicate_product_ids.add(product_id)
        
        # Add products that weren't duplicates
        final_products = []
        for product in products:
            product_id = f"{product.get('brand_name', '').lower()}|{product.get('product_name', '').lower()}"
            if product_id not in duplicate_product_ids:
                final_products.append(product)
        
        # Add the merged duplicates
        final_products.extend(merged_products)
        
        logger.info(f"Removed {len(products) - len(final_products)} duplicate entries")
        logger.info(f"Final product count: {len(final_products)}")
        
        return final_products 