#!/usr/bin/env python3
"""
Script Runner for Scripting Directory
====================================

This script makes it easy to run any script from the scripting directory
with proper Django setup and environment configuration.
"""

import sys
import os
import importlib.util
from pathlib import Path

def run_script(script_name):
    """
    Run a script from the scripts directory with proper Django setup.
    
    Args:
        script_name (str): Name of the script (with or without .py extension)
    """
    # Add .py extension if not present
    if not script_name.endswith('.py'):
        script_name += '.py'
    
    # Get the path to the script
    scripts_dir = Path(__file__).resolve().parent / 'scripts'
    script_path = scripts_dir / script_name
    
    if not script_path.exists():
        print(f"Error: Script '{script_name}' not found in {scripts_dir}")
        print("Available scripts:")
        for script_file in scripts_dir.glob('*.py'):
            if script_file.name != '__init__.py':
                print(f"  - {script_file.name}")
        return False
    
    print(f"Running script: {script_name}")
    print(f"Path: {script_path}")
    print("-" * 50)
    
    try:
        # Import and run the script
        spec = importlib.util.spec_from_file_location(script_name, script_path)
        module = importlib.util.module_from_spec(spec)
        
        # Execute the script
        spec.loader.exec_module(module)
        
        # If the script has a main function, call it
        if hasattr(module, 'main'):
            module.main()
        
        print("-" * 50)
        print(f"Script '{script_name}' completed successfully")
        return True
        
    except Exception as e:
        print(f"Error running script '{script_name}': {e}")
        return False

def list_scripts():
    """List all available scripts"""
    scripts_dir = Path(__file__).resolve().parent / 'scripts'
    
    print("Available scripts:")
    print("-" * 30)
    
    for script_file in sorted(scripts_dir.glob('*.py')):
        if script_file.name != '__init__.py':
            print(f"  {script_file.name}")
    
    # Also check scraper subdirectory
    scraper_dir = scripts_dir / 'scraper'
    if scraper_dir.exists():
        print("\nScraper scripts:")
        print("-" * 30)
        for script_file in sorted(scraper_dir.glob('*.py')):
            if script_file.name != '__init__.py':
                print(f"  scraper/{script_file.name}")

def main():
    """Main function"""
    if len(sys.argv) < 2:
        print("Script Runner for CeliApp Scripting Directory")
        print("=" * 50)
        print("\nUsage:")
        print("  python run_script.py <script_name>")
        print("  python run_script.py --list")
        print("\nExamples:")
        print("  python run_script.py google_sheets")
        print("  python run_script.py openfoodfacts_nick")
        print("  python run_script.py scraper/email_scraper")
        print("\nAvailable scripts:")
        list_scripts()
        return
    
    if sys.argv[1] == '--list':
        list_scripts()
        return
    
    script_name = sys.argv[1]
    success = run_script(script_name)
    
    if not success:
        sys.exit(1)

if __name__ == '__main__':
    main() 