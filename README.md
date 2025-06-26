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
