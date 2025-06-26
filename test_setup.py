#!/usr/bin/env python3
"""
Test Script for Scripting Directory Setup
=========================================

This script tests that the Django setup works correctly and that
scripts can access the backend models from the scripting directory.
"""

import sys
from pathlib import Path

def test_django_setup():
    """Test that Django setup works correctly"""
    print("Testing Django setup...")
    
    try:
        # Import Django setup
        import django_setup
        print("✓ Django setup imported successfully")
        
        # Try to import Django models
        from apps.shopping.models import Product, Poll
        print("✓ Django models imported successfully")
        
        # Try to query the database
        product_count = Product.objects.count()
        poll_count = Poll.objects.count()
        print(f"✓ Database queries work: {product_count} products, {poll_count} polls")
        
        return True
        
    except Exception as e:
        print(f"✗ Django setup failed: {e}")
        return False

def test_sysconfigs():
    """Test that sysconfigs work correctly"""
    print("\nTesting sysconfigs...")
    
    try:
        from sysconfigs.client_creds import get_google_sheets_credentials
        print("✓ sysconfigs imported successfully")
        
        # Note: We won't actually call the credentials function as it requires env vars
        print("✓ sysconfigs structure is correct")
        
        return True
        
    except Exception as e:
        print(f"✗ sysconfigs failed: {e}")
        return False

def test_script_imports():
    """Test that scripts can be imported"""
    print("\nTesting script imports...")
    
    try:
        # Test importing a script that doesn't use Django
        import scripts.openfoodfacts_nick
        print("✓ openfoodfacts_nick.py imported successfully")
        
        # Test importing a script that uses Django
        import scripts.google_sheets
        print("✓ google_sheets.py imported successfully")
        
        return True
        
    except Exception as e:
        print(f"✗ Script imports failed: {e}")
        return False

def main():
    """Run all tests"""
    print("=" * 50)
    print("SCRIPTING DIRECTORY SETUP TEST")
    print("=" * 50)
    
    tests = [
        test_django_setup,
        test_sysconfigs,
        test_script_imports,
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 50)
    print(f"TEST RESULTS: {passed}/{total} tests passed")
    
    if passed == total:
        print("✓ All tests passed! Scripting directory is ready to use.")
    else:
        print("✗ Some tests failed. Please check the setup.")
    
    print("=" * 50)

if __name__ == '__main__':
    main() 