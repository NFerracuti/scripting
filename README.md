# Scripting Directory

This directory contains utility scripts for the CeliApp project. These scripts were moved from the backend directory to allow for independent version control and easier management.

## Setup

1. **Install dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

2. **Set up environment variables:**
   Create a `.env` file in this directory with the following variables:

   ```
   SECRET_KEY=your_django_secret_key
   DEVELOPMENT_MODE=True
   GOOGLE_APPLICATION_CREDENTIALS_NEW={"your":"google_credentials_json"}
   PERPLEXITY_API_KEY=your_perplexity_key
   CHATGPT_API_KEY=your_openai_key
   DATABASE_URL=your_database_url
   ```

3. **Configure Django settings:**
   The scripts need access to Django models. Make sure the backend directory is in your Python path and Django settings are properly configured.

## Running Scripts

### Using the Script Runner (Recommended)

The easiest way to run scripts is using the script runner:

```bash
# List all available scripts
python run_script.py --list

# Run a specific script
python run_script.py google_sheets
python run_script.py openfoodfacts_nick
python run_script.py scraper/email_scraper
```

### Running Scripts Directly

Scripts can also be run directly from this directory:

```bash
# For scripts that use Django models
python scripts/google_sheets.py

# For scripts that don't use Django
python scripts/openfoodfacts_nick.py
```

## Testing the Setup

Run the test script to verify everything is working:

```bash
python test_setup.py
```

This will test:

- Django setup and model access
- Configuration imports
- Script imports

## Directory Structure

- `scripts/` - Main script files
  - `scraper/` - Web scraping scripts
- `sysconfigs/` - Configuration and credential management
- `requirements.txt` - Python dependencies
- `run_script.py` - Script runner utility
- `test_setup.py` - Setup verification script
- `django_setup.py` - Django configuration for scripts
- `README.md` - This file

## Notes

- Scripts that use Django models will automatically set up Django when imported
- All scripts maintain the same functionality as they had in the backend directory
- The `sysconfigs` directory contains shared configuration that scripts depend on
- The script runner provides a convenient way to execute scripts with proper setup

# Alcohol Data Integrity Script

This script improves the integrity of alcohol product data by:

1. Normalizing brand names and subcategories
2. Finding and merging duplicate products
3. Using AI to extract brand names from product names where brand information is missing
4. Cleaning up existing data from Google Sheets

## Setup

1. Install dependencies:

```bash
pip install -r requirements.txt
```

2. Set up environment variables:

```bash
# Create a .env file with:
OPENAI_API_KEY=your_openai_api_key_here
```

3. Ensure Google credentials file is in place:

- `charged-gravity-444220-d2-70c441d6f918.json`

## Usage

### Test Mode (Default)

The script runs in test mode by default to prevent accidental data loss:

```bash
python scripts/alcohol_data_integrity.py
```

### Production Mode

To actually update the Google Sheet:

1. Set `"test_mode": False` in the CONFIG
2. Set `"use_ai_brand_extraction": True` to enable AI brand extraction
3. Run the script and confirm when prompted

### Test AI Extraction

Test the AI brand extraction functionality:

```bash
python test_ai_extraction.py
```

## Configuration

Key configuration options in `scripts/alcohol_data_integrity.py`:

- `test_mode`: When True, shows what would be updated without making changes
- `use_ai_brand_extraction`: When True, uses OpenAI API to extract brand names
- `similarity_threshold`: Threshold for considering products similar (0.9 = very conservative)
- `batch_size`: Number of products to process in each AI API batch
- `max_ai_products`: Maximum number of products to process with AI (cost control)

## Cost Optimization

The script includes several features to minimize API costs:

### Smart Filtering

- Only processes products that actually need brand extraction (missing brand names)
- Skips products with generic names like "Red Wine", "Premium Vodka", etc.
- Skips products that are too short (< 5 chars) or too long (> 100 chars)

### Token Optimization

- Uses a concise prompt (~100 tokens input, ~50 tokens output per product)
- Reduced max_tokens from 100 to 50
- Optimized system and user messages

### Cost Control

- `max_ai_products` setting limits total products processed
- Cost estimation before processing
- Batch processing with delays to avoid rate limits

### Cost Estimation

The script estimates costs before processing:

```
AI Cost Estimate:
  Products to process: 150
  Estimated tokens: 22,500
  Estimated cost: $0.045
  Cost per product: $0.0003
```

## Safety Features

- **Test Mode**: Default mode prevents accidental data loss
- **Confirmation Prompt**: Requires typing 'YES' to proceed with updates
- **Backup**: Can create backup before making changes (configure `backup_sheet_id`)
- **Conservative Duplicate Detection**: High similarity threshold to avoid false positives

## AI Brand Extraction

The script uses OpenAI's GPT-3.5-turbo to intelligently extract brand names from product names. Examples:

- "Campbell Kind Wine Tawse Riesling 2019" → Brand: "Campbell Kind Wine", Product: "Tawse Riesling 2019"
- "La Bélière Red Organic Wine 2019" → Brand: "La Bélière", Product: "Red Organic Wine 2019"

## Cost Considerations

- **Optimized Processing**: Only processes products that actually need brand extraction
- **Token Efficiency**: ~150 tokens per product (down from ~300+ with original prompt)
- **Cost Control**: Configurable limit on maximum products processed
- **Typical Costs**:
  - 100 products: ~$0.03
  - 500 products: ~$0.15
  - 1000 products: ~$0.30
- **Cost Estimation**: Script shows estimated costs before processing
- **Batch Processing**: Includes delays to avoid rate limits

## Troubleshooting

- **API Errors**: Check your OpenAI API key and billing
- **Rate Limits**: The script includes delays between batches
- **Data Loss**: Always run in test mode first and review the summary
