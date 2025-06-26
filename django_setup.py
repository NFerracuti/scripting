"""
Django Setup for Scripts
========================

This module sets up Django so that scripts can access Django models
from the backend directory. It should be imported at the top of any
script that needs to use Django models.
"""

import os
import sys
import django
from pathlib import Path
from django.conf import settings

def setup_django():
    """
    Set up Django environment for scripts to access models from the backend directory.
    """
    # Check if Django is already configured
    if settings.configured:
        print("Django already configured, skipping setup")
        return
    
    # Get the path to the backend directory (parent of scripting directory)
    current_dir = Path(__file__).resolve().parent
    backend_dir = current_dir.parent / 'backend'
    
    # Add backend directory to Python path
    if str(backend_dir) not in sys.path:
        sys.path.insert(0, str(backend_dir))
    
    # Set Django settings module
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'theceliapp.settings')
    
    # Setup Django
    django.setup()
    
    print(f"Django setup complete. Backend directory: {backend_dir}")
    print(f"Django settings module: {os.environ.get('DJANGO_SETTINGS_MODULE')}")

# Auto-setup when imported
if __name__ != '__main__':
    setup_django() 