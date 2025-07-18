"""
Configuration file for the ETL pipeline
Contains database connection settings and file paths
"""

import os
from pathlib import Path

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': '6equj5_root',  # Update based on docker-compose.initial.yml
    'database': 'db_user',  # Update based on docker-compose.initial.yml
    'charset': 'utf8mb4',
    'autocommit': False
}

# File Paths
BASE_DIR = Path(_file_).parent.parent
DATA_DIR = BASE_DIR / 'data'
SQL_DIR = BASE_DIR / 'sql'

# Input files
JSON_FILE = DATA_DIR / 'fake_property_data.json'  
FIELD_CONFIG_FILE = DATA_DIR / 'Field Config.xlsx' 

# Logging Configuration
LOG_CONFIG = {
    'level': 'INFO',
    'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    'file': BASE_DIR / 'logs' / 'etl.log'
}

# ETL Settings
ETL_CONFIG = {
    'batch_size': 1000,
    'max_retries': 3,
    'retry_delay': 1,  # seconds
    'validate_data': True,
    'create_indexes': True
}

# Table Names (for consistency)
TABLES = {
    'properties': 'properties',
    'hoa': 'hoa_details',
    'valuations': 'property_valuations',
    'rehab_estimates': 'rehab_estimates',
    'locations': 'property_locations'
}