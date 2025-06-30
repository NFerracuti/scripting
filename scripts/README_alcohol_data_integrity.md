# Alcohol Data Integrity Scripts

This directory contains modular scripts for improving alcohol data integrity in Google Sheets.

## Overview

The original monolithic `alcohol_data_integrity.py` script has been broken down into smaller, focused modules for better maintainability and reusability.

## Quick Start

### 1. Set Up Virtual Environment

```bash
# Navigate to the scripting directory
cd /Users/nick/celiapp-official/scripting

# Activate the virtual environment
source venv/bin/activate

# Navigate to scripts directory
cd scripts
```

### 2. Configure Settings

Edit `alcohol_data_config.py` to configure:

- Spreadsheet IDs and GIDs
- Which operations to run
- AI settings and API keys
- Thresholds and limits

### 3. Run Scripts

#### Full Data Integrity Process

```bash
python alcohol_data_integrity_main.py
```

#### Backup Restoration Only

```bash
python run_backup_restoration.py
```

#### Duplicate Removal Only

```bash
python run_duplicate_removal.py
```

## Script Structure

### Core Modules

1. **`alcohol_data_config.py`** - Configuration and constants

   - All configuration settings
   - Subcategory normalization mappings
   - Brand name variations
   - Product types and indicators
   - Invalid product names

2. **`alcohol_sheets_client.py`** - Google Sheets operations

   - Authentication and credentials
   - Reading from sheets
   - Writing to sheets
   - Backup operations
   - Column mapping utilities

3. **`alcohol_ai_processor.py`** - AI/OpenAI operations

   - Brand extraction using AI
   - Product type determination
   - Batch processing
   - Cost estimation
   - AI decision logic

4. **`alcohol_data_processor.py`** - Data processing and normalization

   - Subcategory normalization
   - Brand name normalization
   - Duplicate detection and merging
   - Product name extraction
   - Data validation

5. **`alcohol_backup_restorer.py`** - Backup restoration
   - Fuzzy matching algorithms
   - Backup data processing
   - Data restoration logic
   - Existing data processing

### Main Scripts

6. **`alcohol_data_integrity_main.py`** - Main orchestration script

   - Coordinates all modules
   - Runs enabled operations
   - Provides main entry point
   - Configuration display

7. **`run_backup_restoration.py`** - Simple backup restoration script

   - Runs only backup restoration
   - Useful for quick data restoration
   - Minimal configuration needed

8. **`run_duplicate_removal.py`** - Simple duplicate removal script
   - Runs only duplicate removal
   - Useful for quick data cleanup
   - Exact case-insensitive matching

## Usage Examples

### Running the Full Data Integrity Process

```bash
# Activate virtual environment
cd /Users/nick/celiapp-official/scripting
source venv/bin/activate

# Navigate to scripts
cd scripts

# Run the main script
python alcohol_data_integrity_main.py
```

### Running Only Backup Restoration

```bash
# Activate virtual environment
cd /Users/nick/celiapp-official/scripting
source venv/bin/activate

# Navigate to scripts
cd scripts

# Run backup restoration
python run_backup_restoration.py
```

### Running Only Duplicate Removal

```bash
# Activate virtual environment
cd /Users/nick/celiapp-official/scripting
source venv/bin/activate

# Navigate to scripts
cd scripts

# Run duplicate removal
python run_duplicate_removal.py
```

## Configuration

Edit `alcohol_data_config.py` to configure:

- Spreadsheet IDs and GIDs
- Which operations to run
- AI settings and API keys
- Thresholds and limits

### Key Configuration Options

```python
CONFIG = {
    "spreadsheet_id": "your-spreadsheet-id",
    "sheet_gid": 828037295,
    "backup_sheet_gid": 933968267,
    "run_exact_duplicate_removal": True,
    "run_backup_restoration": True,
    "run_sheet_update": True,
    "test_mode": False,
}
```

## Key Features

### Modular Design

- Each module has a single responsibility
- Easy to test individual components
- Can run specific operations independently

### Configuration-Driven

- All settings in one place
- Easy to enable/disable operations
- Cost estimation for AI operations

### Safety Features

- Test mode for dry runs
- User confirmation for destructive operations
- Backup creation before updates

### AI Integration

- OpenAI API integration for brand extraction
- Product type determination
- Batch processing to manage costs
- Fallback to rule-based extraction

### Data Quality

- Fuzzy matching for backup restoration
- Duplicate detection and merging
- Brand name normalization
- Subcategory standardization

## Migration from Original Script

The original `alcohol_data_integrity.py` script is still available but deprecated. To migrate:

1. Update imports in any existing scripts
2. Use the new modular structure
3. Configure settings in `alcohol_data_config.py`
4. Run the appropriate main script

## Dependencies

- `googleapiclient`
- `google.oauth2`
- `openai`
- `python-dotenv`
- `difflib` (built-in)
- `logging` (built-in)

## Environment Variables

- `OPENAI_KEY_NICK` - OpenAI API key
- `GOOGLE_APPLICATION_CREDENTIALS` - Google service account credentials

## File Structure

```
scripting/scripts/
├── alcohol_data_config.py          # Configuration
├── alcohol_sheets_client.py        # Google Sheets operations
├── alcohol_ai_processor.py         # AI operations
├── alcohol_data_processor.py       # Data processing
├── alcohol_backup_restorer.py      # Backup restoration
├── alcohol_data_integrity_main.py  # Main orchestration
├── run_backup_restoration.py       # Simple backup restoration
├── run_duplicate_removal.py        # Simple duplicate removal
├── alcohol_data_integrity.py       # Original script (deprecated)
└── README_alcohol_data_integrity.md # This file
```

## Troubleshooting

### Virtual Environment Issues

```bash
# If virtual environment doesn't exist
cd /Users/nick/celiapp-official/scripting
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Missing Dependencies

```bash
# Install missing packages
pip install google-api-python-client google-auth-httplib2 google-auth-oauthlib openai python-dotenv
```

### Permission Issues

- Ensure Google Sheets API credentials are properly set up
- Check that the service account has access to the target spreadsheets
- Verify environment variables are set correctly
