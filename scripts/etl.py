"""
ETL Pipeline for Property Data Normalization
Reads raw JSON data and loads it into normalized MySQL tables
"""

import json
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd

# Import our custom modules
from config import *
from utils import DatabaseManager, DataProcessor, create_directories, read_field_config

logger = logging.getLogger(_name_)

class PropertyETL:
    """Main ETL class for processing property data"""
    
    def _init_(self):
        self.db_manager = DatabaseManager()
        self.data_processor = DataProcessor()
        self.field_config = None
        
    def setup(self):
        """Initialize ETL pipeline"""
        logger.info("Setting up ETL pipeline...")
        
        # Create directories
        create_directories()
        
        # Connect to database
        self.db_manager.connect()
        
        # Load field configuration
        if FIELD_CONFIG_FILE.exists():
            self.field_config = read_field_config(FIELD_CONFIG_FILE)
            logger.info("Field configuration loaded")
        else:
            logger.warning("Field configuration file not found")
        
        # Create database schema
        self.create_schema()
        
    def create_schema(self):
        """Create database schema"""
        logger.info("Creating database schema...")
        schema_file = SQL_DIR / 'schema.sql'
        
        if schema_file.exists():
            self.db_manager.execute_script(schema_file)
            logger.info("Database schema created successfully")
        else:
            logger.error("Schema file not found")
            raise FileNotFoundError(f"Schema file not found: {schema_file}")
    
    def extract_data(self) -> List[Dict[str, Any]]:
        """Extract data from JSON file"""
        logger.info("Extracting data from JSON file...")
        
        if not JSON_FILE.exists():
            logger.error(f"JSON file not found: {JSON_FILE}")
            raise FileNotFoundError(f"JSON file not found: {JSON_FILE}")
        
        raw_data = self.data_processor.load_json(JSON_FILE)
        logger.info(f"Extracted {len(raw_data)} records")
        return raw_data
    
    def transform_data(self, raw_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """Transform raw data into normalized format"""
        logger.info("Transforming data...")
        
        transformed_data = {
            'locations': [],
            'properties': [],
            'hoa_details': [],
            'valuations': [],
            'rehab_estimates': []
        }
        
        for record in raw_data:
            try:
                # Transform each record
                location_data = self.transform_location(record)
                property_data = self.transform_property(record, location_data['location_id'])
                hoa_data = self.transform_hoa(record, property_data['property_id'])
                valuation_data = self.transform_valuations(record, property_data['property_id'])
                rehab_data = self.transform_rehab_estimates(record, property_data['property_id'])
                
                # Add to transformed data
                transformed_data['locations'].append(location_data)
                transformed_data['properties'].append(property_data)
                
                if hoa_data:
                    transformed_data['hoa_details'].append(hoa_data)
                
                transformed_data['valuations'].extend(valuation_data)
                transformed_data['rehab_estimates'].extend(rehab_data)
                
            except Exception as e:
                logger.error(f"Error transforming record: {e}")
                continue
        
        logger.info("Data transformation completed")
        return transformed_data
    
    def transform_location(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Transform location data"""
        location_id = self.data_processor.generate_uuid()
        
        return {
            'location_id': location_id,
            'address_line_1': self.data_processor.clean_string(record.get('address', record.get('street_address'))),
            'address_line_2': self.data_processor.clean_string(record.get('address_line_2')),
            'city': self.data_processor.clean_string(record.get('city')),
            'state': self.data_processor.clean_string(record.get('state')),
            'zip_code': self.data_processor.clean_string(record.get('zip_code', record.get('zip'))),
            'county': self.data_processor.clean_string(record.get('county')),
            'latitude': self.data_processor.clean_numeric(record.get('latitude')),
            'longitude': self.data_processor.clean_numeric(record.get('longitude'))
        }
    
    def transform_property(self, record: Dict[str, Any], location_id: str) -> Dict[str, Any]:
        """Transform property data"""
        property_id = self.data_processor.generate_uuid()
        
        return {
            'property_id': property_id,
            'location_id': location_id,
            'property_type': self.data_processor.clean_string(record.get('property_type', record.get('type'))),
            'bedrooms': int(record.get('bedrooms', record.get('beds', 0))) if record.get('bedrooms') or record.get('beds') else None,
            'bathrooms': self.data_processor.clean_numeric(record.get('bathrooms', record.get('baths'))),
            'square_footage': int(record.get('square_footage', record.get('sqft', 0))) if record.get('square_footage') or record.get('sqft') else None,
            'lot_size': self.data_processor.clean_numeric(record.get('lot_size')),
            'year_built': int(record.get('year_built', 0)) if record.get('year_built') else None,
            'garage_spaces': int(record.get('garage_spaces', 0)) if record.get('garage_spaces') else None,
            'pool': bool(record.get('pool', False)),
            'fireplace': bool(record.get('fireplace', False)),
            'basement': bool(record.get('basement', False)),
            'property_condition': self.data_processor.clean_string(record.get('condition')),
            'listing_status': self.data_processor.clean_string(record.get('status')),
            'mls_number': self.data_processor.clean_string(record.get('mls_number'))
        }
    
    def transform_hoa(self, record: Dict[str, Any], property_id: str) -> Optional[Dict[str, Any]]:
        """Transform HOA data"""
        # Check if HOA data exists
        hoa_fields = ['hoa_name', 'hoa_fee', 'hoa_monthly_fee', 'hoa_amenities']
        has_hoa_data = any(record.get(field) for field in hoa_fields)
        
        if not has_hoa_data:
            return None
        
        return {
            'hoa_id': self.data_processor.generate_uuid(),
            'property_id': property_id,
            'hoa_name': self.data_processor.clean_string(record.get('hoa_name')),
            'monthly_fee': self.data_processor.clean_numeric(record.get('hoa_monthly_fee', record.get('hoa_fee'))),
            'annual_fee': self.data_processor.clean_numeric(record.get('hoa_annual_fee')),
            'hoa_contact_info': self.data_processor.clean_string(record.get('hoa_contact')),
            'amenities': self.data_processor.clean_string(record.get('hoa_amenities')),
            'restrictions': self.data_processor.clean_string(record.get('hoa_restrictions'))
        }
    
    def transform_valuations(self, record: Dict[str, Any], property_id: str) -> List[Dict[str, Any]]:
        """Transform valuation data"""
        valuations = []
        
        # Map different valuation types
        valuation_mappings = {
            'market_value': 'market',
            'assessed_value': 'assessed',
            'arv': 'arv',
            'list_price': 'list',
            'sale_price': 'sale'
        }
        
        for field, valuation_type in valuation_mappings.items():
            value = self.data_processor.clean_numeric(record.get(field))
            if value:
                valuations.append({
                    'valuation_id': self.data_processor.generate_uuid(),
                    'property_id': property_id,
                    'valuation_type': valuation_type,
                    'valuation_amount': value,
                    'valuation_date': datetime.now().date(),
                    'valuation_source': 'Import',
                    'confidence_level': 'Medium',
                    'notes': f'Imported from raw data field: {field}'
                })
        
        return valuations
    
    def transform_rehab_estimates(self, record: Dict[str, Any], property_id: str) -> List[Dict[str, Any]]:
        """Transform rehab estimate data"""
        estimates = []
        
        # Map different rehab estimate types
        rehab_mappings = {
            'rehab_cost': 'full_rehab',
            'repair_cost': 'repair',
            'cosmetic_cost': 'cosmetic',
            'structural_cost': 'structural'
        }
        
        for field, estimate_type in rehab_mappings.items():
            cost = self.data_processor.clean_numeric(record.get(field))
            if cost:
                estimates.append({
                    'estimate_id': self.data_processor.generate_uuid(),
                    'property_id': property_id,
                    'estimate_type': estimate_type,
                    'estimated_cost': cost,
                    'estimate_date': datetime.now().date(),
                    'contractor_name': self.data_processor.clean_string(record.get('contractor_name')),
                    'work_description': self.data_processor.clean_string(record.get(f'{field}_description')),
                    'timeline_weeks': int(record.get('timeline_weeks', 0)) if record.get('timeline_weeks') else None,
                    'materials_cost': self.data_processor.clean_numeric(record.get('materials_cost')),
                    'labor_cost': self.data_processor.clean_numeric(record.get('labor_cost')),
                    'permit_cost': self.data_processor.clean_numeric(record.get('permit_cost')),
                    'contingency_percentage': self.data_processor.clean_numeric(record.get('contingency_percentage')),
                    'status': 'draft'
                })
        
        return estimates
    
    def load_data(self, transformed_data: Dict[str, List[Dict[str, Any]]]):
        """Load transformed data into database"""
        logger.info("Loading data into database...")
        
        # Load data in correct order (respecting foreign key constraints)
        load_order = ['locations', 'properties', 'hoa_details', 'valuations', 'rehab_estimates']
        
        for table_key in load_order:
            table_name = TABLES.get(table_key, table_key)
            data = transformed_data.get(table_key, [])
            
            if data:
                # Filter out None values and empty records
                clean_data = []
                for record in data:
                    clean_record = {k: v for k, v in record.items() if v is not None}
                    if clean_record:
                        clean_data.append(clean_record)
                
                if clean_data:
                    self.db_manager.insert_batch(table_name, clean_data)
                    logger.info(f"Loaded {len(clean_data)} records into {table_name}")
                else:
                    logger.info(f"No valid data to load for {table_name}")
            else:
                logger.info(f"No data found for {table_name}")
    
    def validate_data(self):
        """Validate loaded data"""
        logger.info("Validating loaded data...")
        
        validation_queries = [
            ("SELECT COUNT(*) FROM property_locations", "property_locations"),
            ("SELECT COUNT(*) FROM properties", "properties"),
            ("SELECT COUNT(*) FROM hoa_details", "hoa_details"),
            ("SELECT COUNT(*) FROM property_valuations", "property_valuations"),
            ("SELECT COUNT(*) FROM rehab_estimates", "rehab_estimates"),
            ("SELECT COUNT(*) FROM properties p LEFT JOIN property_locations pl ON p.location_id = pl.location_id WHERE pl.location_id IS NULL", "orphaned_properties"),
        ]
        
        for query, description in validation_queries:
            try:
                result = self.db_manager.execute_query(query)
                count = result[0][0] if result else 0
                logger.info(f"{description}: {count} records")
            except Exception as e:
                logger.error(f"Error validating {description}: {e}")
    
    def run(self):
        """Run the complete ETL pipeline"""
        try:
            logger.info("Starting ETL pipeline...")
            
            # Setup
            self.setup()
            
            # Extract
            raw_data = self.extract_data()
            
            # Transform
            transformed_data = self.transform_data(raw_data)
            
            # Load
            self.load_data(transformed_data)
            
            # Validate
            if ETL_CONFIG['validate_data']:
                self.validate_data()
            
            logger.info("ETL pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
        finally:
            self.db_manager.disconnect()

def main():
    """Main function"""
    try:
        etl = PropertyETL()
        etl.run()
        print("ETL pipeline completed successfully!")
        
    except Exception as e:
        print(f"ETL pipeline failed: {e}")
        sys.exit(1)

if _name_ == "_main_":
    main()