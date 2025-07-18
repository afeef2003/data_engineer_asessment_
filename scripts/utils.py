"""
Utility functions for the ETL pipeline
"""

import json
import logging
import mysql.connector
from mysql.connector import Error
from typing import Dict, Any, List, Optional
import pandas as pd
from datetime import datetime
import uuid

from config import DB_CONFIG, LOG_CONFIG

# Set up logging
logging.basicConfig(
    level=getattr(logging, LOG_CONFIG['level']),
    format=LOG_CONFIG['format'],
    handlers=[
        logging.FileHandler(LOG_CONFIG['file']),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(_name_)

class DatabaseManager:
    """Handles database connections and operations"""
    
    def _init_(self):
        self.connection = None
        self.cursor = None
    
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(**DB_CONFIG)
            self.cursor = self.connection.cursor()
            logger.info("Database connection established")
        except Error as e:
            logger.error(f"Error connecting to database: {e}")
            raise
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")
    
    def execute_script(self, script_path: str):
        """Execute SQL script from file"""
        try:
            with open(script_path, 'r') as file:
                script = file.read()
            
            # Split script into individual statements
            statements = [stmt.strip() for stmt in script.split(';') if stmt.strip()]
            
            for statement in statements:
                self.cursor.execute(statement)
            
            self.connection.commit()
            logger.info(f"Successfully executed script: {script_path}")
            
        except Error as e:
            logger.error(f"Error executing script {script_path}: {e}")
            self.connection.rollback()
            raise
    
    def execute_query(self, query: str, params: Optional[tuple] = None):
        """Execute a single query"""
        try:
            self.cursor.execute(query, params)
            self.connection.commit()
            return self.cursor.fetchall()
        except Error as e:
            logger.error(f"Error executing query: {e}")
            self.connection.rollback()
            raise
    
    def insert_batch(self, table: str, data: List[Dict[str, Any]]):
        """Insert batch of data into table"""
        if not data:
            return
        
        # Get column names from first record
        columns = list(data[0].keys())
        placeholders = ', '.join(['%s'] * len(columns))
        columns_str = ', '.join(columns)
        
        query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
        
        try:
            # Convert data to tuples
            values = [tuple(record[col] for col in columns) for record in data]
            
            self.cursor.executemany(query, values)
            self.connection.commit()
            logger.info(f"Inserted {len(data)} records into {table}")
            
        except Error as e:
            logger.error(f"Error inserting batch into {table}: {e}")
            self.connection.rollback()
            raise

class DataProcessor:
    """Handles data processing and transformation"""
    
    @staticmethod
    def load_json(file_path: str) -> List[Dict[str, Any]]:
        """Load JSON data from file"""
        try:
            with open(file_path, 'r') as file:
                data = json.load(file)
            
            # Handle both single object and array of objects
            if isinstance(data, dict):
                data = [data]
            
            logger.info(f"Loaded {len(data)} records from {file_path}")
            return data
            
        except Exception as e:
            logger.error(f"Error loading JSON file {file_path}: {e}")
            raise
    
    @staticmethod
    def clean_string(value: Any) -> Optional[str]:
        """Clean and normalize string values"""
        if value is None or value == '':
            return None
        return str(value).strip()
    
    @staticmethod
    def clean_numeric(value: Any) -> Optional[float]:
        """Clean and convert numeric values"""
        if value is None or value == '':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def clean_date(value: Any) -> Optional[datetime]:
        """Clean and convert date values"""
        if value is None or value == '':
            return None
        
        # Add date parsing logic based on your data format
        # This is a placeholder - adjust based on actual date formats
        try:
            if isinstance(value, str):
                return datetime.strptime(value, '%Y-%m-%d')
            return value
        except (ValueError, TypeError):
            return None
    
    @staticmethod
    def generate_uuid() -> str:
        """Generate UUID for primary keys"""
        return str(uuid.uuid4())
    
    @staticmethod
    def validate_record(record: Dict[str, Any], required_fields: List[str]) -> bool:
        """Validate that record has required fields"""
        for field in required_fields:
            if field not in record or record[field] is None:
                return False
        return True

def create_directories():
    """Create necessary directories"""
    from pathlib import Path
    directories = ['logs', 'sql', 'scripts']
    
    for directory in directories:
        Path(directory).mkdir(exist_ok=True)
    
    logger.info("Created necessary directories")

def read_field_config(file_path: str) -> pd.DataFrame:
    """Read field configuration from Excel file"""
    try:
        df = pd.read_excel(file_path)
        logger.info(f"Loaded field configuration from {file_path}")
        return df
    except Exception as e:
        logger.error(f"Error reading field config: {e}")
        raise