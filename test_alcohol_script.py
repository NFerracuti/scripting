#!/usr/bin/env python3
"""
Test script for alcohol_data_integrity.py
=========================================

This script tests the key components of alcohol_data_integrity.py
before running the full script to ensure everything works correctly.
"""

import sys
from pathlib import Path

def test_imports():
    """Test that all imports work correctly"""
    print("Testing imports...")
    
    try:
        from scripting.scripts.alcohol_data_integrity_deprecated import (
            LCBOStatsClient, 
            DataIntegrityProcessor,
            CONFIG
        )
        print("✓ All imports successful")
        return True
    except Exception as e:
        print(f"✗ Import failed: {e}")
        return False

def test_credentials():
    """Test that Google credentials can be loaded"""
    print("\nTesting Google credentials...")
    
    try:
        from scripting.scripts.alcohol_data_integrity_deprecated import DataIntegrityProcessor
        processor = DataIntegrityProcessor()
        print("✓ Google credentials loaded successfully")
        return True
    except Exception as e:
        print(f"✗ Credentials failed: {e}")
        return False

def test_lcbo_api():
    """Test that LCBO API can be accessed"""
    print("\nTesting LCBO API...")
    
    try:
        from scripting.scripts.alcohol_data_integrity_deprecated import LCBOStatsClient
        client = LCBOStatsClient()
        
        # Try to fetch a small amount of data
        response = client.get_all_products(page=1, per_page=5)
        
        if response and 'data' in response:
            print(f"✓ LCBO API working - fetched {len(response['data'])} products")
            return True
        else:
            print("✗ LCBO API returned unexpected response format")
            return False
            
    except Exception as e:
        print(f"✗ LCBO API failed: {e}")
        return False

def test_google_sheets():
    """Test that Google Sheets can be accessed"""
    print("\nTesting Google Sheets access...")
    
    try:
        from scripting.scripts.alcohol_data_integrity_deprecated import DataIntegrityProcessor, CONFIG
        processor = DataIntegrityProcessor()
        
        # Try to get sheet metadata
        spreadsheet = processor.sheets_service.spreadsheets().get(
            spreadsheetId=CONFIG["spreadsheet_id"]
        ).execute()
        
        print(f"✓ Google Sheets access working - sheet: {spreadsheet.get('properties', {}).get('title', 'Unknown')}")
        return True
        
    except Exception as e:
        print(f"✗ Google Sheets failed: {e}")
        return False

def main():
    """Run all tests"""
    print("=" * 50)
    print("ALCOHOL DATA INTEGRITY SCRIPT TEST")
    print("=" * 50)
    
    tests = [
        test_imports,
        test_credentials,
        test_lcbo_api,
        test_google_sheets,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 50)
    print(f"TEST RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        print("✓ All tests passed! Ready to run alcohol_data_integrity.py")
        print("\nTo run the script:")
        print("  python run_script.py alcohol_data_integrity")
        print("  or")
        print("  python scripts/alcohol_data_integrity.py")
    else:
        print("✗ Some tests failed. Please check the setup before running.")
    
    print("=" * 50)

if __name__ == '__main__':
    main() 