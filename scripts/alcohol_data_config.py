"""
Alcohol Data Integrity Configuration
===================================

Configuration settings and constants for alcohol data integrity scripts.

HOW TO USE:
1. Edit the CONFIG dictionary below to configure your settings
2. Set spreadsheet IDs, GIDs, and enable/disable operations
3. Configure AI settings if using OpenAI features
4. Save the file and run your chosen script

CONFIGURATION OPTIONS:
- Spreadsheet settings (IDs, GIDs)
- Operation flags (which features to run)
- AI settings (OpenAI API, batch sizes)
- Matching thresholds and limits
- Test mode settings

REQUIREMENTS:
- Google Sheets API credentials
- OpenAI API key (optional, for AI features)
- Environment variables set up
"""

import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configuration
CONFIG = {
    "spreadsheet_id": "1JdaLf5Ur5eLsNnu6Awlffrds120pjQaAqkPQB4liNyo",
    "sheet_gid": 828037295,
    "backup_sheet_gid": 933968267,  # Backup tab GID
    "credentials_file": "charged-gravity-444220-d2-70c441d6f918.json",
    "test_mode": False,  # Changed to False to actually update the sheet
    "test_product_extraction": False,  # Test mode for product extraction (shows what would be changed)
    "similarity_threshold": 0.9,  # Made more conservative (was 0.8)
    "backup_sheet_id": None,  # Optional backup sheet ID
    "openai_api_key": os.getenv('OPENAI_KEY_NICK'),  # Updated to match your env variable
    
    # Individual operation flags
    "run_ai_brand_extraction": False,  # Extract missing brand names using AI
    "run_product_name_filling": False,  # Fill missing product names
    "run_duplicate_detection": False,  # Find and merge duplicates
    "run_exact_duplicate_removal": True,  # Remove exact duplicates (case-insensitive)
    "run_brand_normalization": False,  # Normalize brand names and consolidate alternatives
    "run_sheet_update": True,  # Update the Google Sheet with results
    "run_backup_restoration": True,  # Restore gluten free scores and descriptors from backup
    
    # AI settings
    "use_ai_brand_extraction": False,  # Set to False for testing without API calls
    "use_ai_product_extraction": False,  # Enable AI product name extraction
    "batch_size": 50,  # Process products in batches to avoid rate limits
    "max_ai_products": 500,  # Maximum number of products to process with AI (cost control)
    
    # Fuzzy matching settings
    "fuzzy_match_threshold": 0.85,  # Minimum similarity score for fuzzy matching (lowered from 0.95)
    "fuzzy_match_fields": ["brand_name", "product_name"],  # Fields to use for matching
    
    # Duplicate detection settings
    "use_exact_duplicate_matching": True,  # Use exact case-insensitive matching for duplicates
    "duplicate_removal_enabled": True,  # Enable automatic duplicate removal
}

# Subcategory normalization mappings
SUBCATEGORY_NORMALIZATIONS = {
    # Common variations
    "Gift And Sampler": "Gifts and Samplers",
    "Gift and Sampler": "Gifts and Samplers", 
    "Gifts And Sampler": "Gifts and Samplers",
    "Gift & Sampler": "Gifts and Samplers",
    "Gifts & Sampler": "Gifts and Samplers",
    "Gift and Samplers": "Gifts and Samplers",
    "Gift And Samplers": "Gifts and Samplers",
    
    # Beer variations
    "Beer & Cider": "Beer",
    "Beer and Cider": "Beer",
    "Beer & Ciders": "Beer",
    "Beer and Ciders": "Beer",
    
    # Wine variations
    "Red Wine": "Red Wines",
    "White Wine": "White Wines", 
    "Rose Wine": "Rose Wines",
    "Sparkling Wine": "Sparkling Wines",
    "Dessert Wine": "Dessert Wines",
    "Fortified Wine": "Fortified Wines",
    
    # Spirit variations
    "Whisky": "Whiskey",
    "Whiskey": "Whiskey",
    "Scotch": "Scotch Whisky",
    "Scotch Whiskey": "Scotch Whisky",
    "Bourbon": "Bourbon Whiskey",
    "Bourbon Whisky": "Bourbon Whiskey",
    "Rye": "Rye Whiskey",
    "Rye Whisky": "Rye Whiskey",
    "Canadian Whisky": "Canadian Whiskey",
    "Canadian Whiskey": "Canadian Whiskey",
    
    # Vodka variations
    "Vodka": "Vodka",
    "Flavoured Vodka": "Flavored Vodka",
    "Flavored Vodka": "Flavored Vodka",
    
    # Gin variations
    "Gin": "Gin",
    "London Dry Gin": "London Dry Gin",
    "Plymouth Gin": "Plymouth Gin",
    
    # Rum variations
    "Rum": "Rum",
    "White Rum": "White Rum",
    "Dark Rum": "Dark Rum",
    "Spiced Rum": "Spiced Rum",
    "Gold Rum": "Gold Rum",
    "Aged Rum": "Aged Rum",
    
    # Tequila variations
    "Tequila": "Tequila",
    "Blanco Tequila": "Blanco Tequila",
    "Reposado Tequila": "Reposado Tequila",
    "Anejo Tequila": "Anejo Tequila",
    "Mezcal": "Mezcal",
    
    # Liqueur variations
    "Liqueur": "Liqueur",
    "Liqueurs": "Liqueur",
    "Cream Liqueur": "Cream Liqueur",
    "Cream Liqueurs": "Cream Liqueur",
    "Coffee Liqueur": "Coffee Liqueur",
    "Coffee Liqueurs": "Coffee Liqueur",
    "Herbal Liqueur": "Herbal Liqueur",
    "Herbal Liqueurs": "Herbal Liqueur",
    "Fruit Liqueur": "Fruit Liqueur",
    "Fruit Liqueurs": "Fruit Liqueur",
    
    # Brandy variations
    "Brandy": "Brandy",
    "Cognac": "Cognac",
    "Armagnac": "Armagnac",
    "Pisco": "Pisco",
    
    # Other variations
    "Aperitif": "Aperitif",
    "Aperitifs": "Aperitif",
    "Digestif": "Digestif", 
    "Digestifs": "Digestif",
    "Vermouth": "Vermouth",
    "Bitters": "Bitters",
    "Absinthe": "Absinthe",
    "Sake": "Sake",
    "Soju": "Soju",
    "Baijiu": "Baijiu",
}

# Common brand name patterns to extract from product names
BRAND_PATTERNS = [
    r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)',  # Capitalized words at start
    r'^([A-Z]+(?:\s+[A-Z]+)*)',  # ALL CAPS at start
    r'^([A-Z][a-z]+(?:\'[a-z]+)?)',  # Names with apostrophes
    r'^([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*\s+(?:Original|Classic|Premium|Reserve|Select|Special|Limited|Edition))',  # Brand + descriptors
]

# Brand name variations for normalization
BRAND_VARIATIONS = {
    "Bailey's": "Bailey's",
    "Baileys": "Bailey's",
    "Baileys Birthday": "Bailey's",
    "Baileys Original": "Bailey's", 
    "Baileys Tiramisu": "Bailey's",
    "Jack Daniels": "Jack Daniel's",
    "Jack Daniel": "Jack Daniel's",
    "Crown Royal": "Crown Royal",
    "Crown Royal Special Reserve": "Crown Royal",
    "Smirnoff": "Smirnoff",
    "Smirnoff Ice": "Smirnoff",
    "Grey Goose": "Grey Goose",
    "Absolut": "Absolut",
    "Ketel One": "Ketel One",
    "Bacardi": "Bacardi",
    "Captain Morgan": "Captain Morgan",
    "Malibu": "Malibu",
    "Jose Cuervo": "Jose Cuervo",
    "Patron": "Patron",
    "Don Julio": "Don Julio",
    "Hendrick's": "Hendrick's",
    "Hendricks": "Hendrick's",
    "Tanqueray": "Tanqueray",
    "Bombay Sapphire": "Bombay Sapphire",
    "Beefeater": "Beefeater",
    "Gordon's": "Gordon's",
    "Gordons": "Gordon's",
}

# Known spirit brands for AI processing
SPIRIT_BRANDS = {
    'jagermeister', 'cointreau', 'grand marnier', 'baileys', 'kahlua',
    'amaretto', 'frangelico', 'chambord', 'midori', 'malibu',
    'captain morgan', 'bacardi', 'grey goose', 'ketel one', 'absolut',
    'smirnoff', 'jack daniels', 'jim beam', 'makers mark', 'wild turkey',
    'crown royal', 'canadian club', 'seagrams', 'dewars', 'johnnie walker',
    'macallan', 'glenfiddich', 'glenlivet', 'lagavulin', 'ardbeg',
    'laphroaig', 'talisker', 'highland park', 'balvenie', 'dalmore'
}

# Generic product types for extraction
PRODUCT_TYPES = [
    # Beer types
    'lager', 'pilsner', 'stout', 'porter', 'ale', 'ipa', 'wheat beer', 'hefeweizen',
    'pale ale', 'amber ale', 'brown ale', 'blonde ale', 'cream ale', 'kolsch',
    # Wine types
    'red wine', 'white wine', 'rose wine', 'sparkling wine', 'champagne', 'prosecco',
    'chardonnay', 'cabernet', 'merlot', 'pinot noir', 'sauvignon blanc', 'riesling',
    # Spirit types
    'vodka', 'gin', 'whiskey', 'whisky', 'bourbon', 'scotch', 'rum', 'tequila',
    'brandy', 'cognac', 'liqueur', 'absinthe', 'vermouth', 'bitters',
    # Other
    'cider', 'mead', 'sake', 'soju'
]

# Generic indicators for product names
GENERIC_INDICATORS = [
    'red wine', 'white wine', 'rose wine', 'sparkling wine', 'beer', 'lager', 'ale',
    'whiskey', 'whisky', 'vodka', 'gin', 'rum', 'tequila', 'brandy', 'liqueur',
    'organic', 'natural', 'premium', 'reserve', 'select', 'classic', 'original'
]

# Invalid product names
INVALID_PRODUCT_NAMES = {
    'other', 'unknown', 'misc', 'miscellaneous', 'general', 'various',
    'assorted', 'mixed', 'selection', 'collection', 'variety'
}

# Generic subcategories
GENERIC_SUBCATEGORIES = {
    'other', 'unknown', 'misc', 'miscellaneous', 'general', 'various',
    'assorted', 'mixed', 'selection', 'collection', 'variety'
} 