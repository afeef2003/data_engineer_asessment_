"""
Data validation script for the property database
Performs comprehensive validation checks on the loaded data
"""

import logging
from config import *
from utils import DatabaseManager

logger = logging.getLogger(_name_)

class DataValidator:
    """Performs various data validation checks"""
    
    def _init_(self):
        self.db_manager = DatabaseManager()
    
    def run_validation(self):
        """Run all validation checks"""
        logger.info("Starting data validation...")
        
        try:
            self.db_manager.connect()
            
            # Run validation checks
            self.validate_record_counts()
            self.validate_foreign_keys()
            self.validate_data_quality()
            self.validate_business_rules()
            
            logger.info("Data validation completed successfully")
            
        except Exception as e:
            logger.error(f"Data validation failed: {e}")
            raise
        finally:
            self.db_manager.disconnect()
    
    def validate_record_counts(self):
        """Validate record counts in all tables"""
        logger.info("Validating record counts...")
        
        tables = [
            'property_locations',
            'properties', 
            'hoa_details',
            'property_valuations',
            'rehab_estimates'
        ]
        
        for table in tables:
            query = f"SELECT COUNT(*) FROM {table}"
            result = self.db_manager.execute_query(query)
            count = result[0][0] if result else 0
            logger.info(f"{table}: {count} records")
    
    def validate_foreign_keys(self):
        """Validate foreign key relationships"""
        logger.info("Validating foreign key relationships...")
        
        fk_checks = [
            {
                'name': 'Properties without locations',
                'query': '''
                    SELECT COUNT(*) FROM properties p 
                    LEFT JOIN property_locations pl ON p.location_id = pl.location_id 
                    WHERE pl.location_id IS NULL
                '''
            },
            {
                'name': 'HOA details without properties',
                'query': '''
                    SELECT COUNT(*) FROM hoa_details h 
                    LEFT JOIN properties p ON h.property_id = p.property_id 
                    WHERE p.property_id IS NULL
                '''
            },
            {
                'name': 'Valuations without properties',
                'query': '''
                    SELECT COUNT(*) FROM property_valuations pv 
                    LEFT JOIN properties p ON pv.property_id = p.property_id 
                    WHERE p.property_id IS NULL
                '''
            },
            {
                'name': 'Rehab estimates without properties',
                'query': '''
                    SELECT COUNT(*) FROM rehab_estimates re 
                    LEFT JOIN properties p ON re.property_id = p.property_id 
                    WHERE p.property_id IS NULL
                '''
            }
        ]
        
        for check in fk_checks:
            result = self.db_manager.execute_query(check['query'])
            count = result[0][0] if result else 0
            if count > 0:
                logger.warning(f"{check['name']}: {count} orphaned records")
            else:
                logger.info(f"{check['name']}: OK")
    
    def validate_data_quality(self):
        """Validate data quality"""
        logger.info("Validating data quality...")
        
        quality_checks = [
            {
                'name': 'Properties with missing required fields',
                'query': '''
                    SELECT COUNT(*) FROM properties 
                    WHERE property_type IS NULL OR bedrooms IS NULL OR bathrooms IS NULL
                '''
            },
            {
                'name': 'Locations with missing address',
                'query': '''
                    SELECT COUNT(*) FROM property_locations 
                    WHERE address_line_1 IS NULL OR city IS NULL OR state IS NULL
                '''
            },
            {
                'name': 'Valuations with zero or negative amounts',
                'query': '''
                    SELECT COUNT(*) FROM property_valuations 
                    WHERE valuation_amount <= 0
                '''
            },
            {
                'name': 'Properties with unrealistic year built',
                'query': '''
                    SELECT COUNT(*) FROM properties 
                    WHERE year_built < 1800 OR year_built > YEAR(CURDATE())
                '''
            },
            {
                'name': 'Properties with negative square footage',
                'query': '''
                    SELECT COUNT(*) FROM properties 
                    WHERE square_footage < 0
                '''
            }
        ]
        
        for check in quality_checks:
            result = self.db_manager.execute_query(check['query'])
            count = result[0][0] if result else 0
            if count > 0:
                logger.warning(f"{check['name']}: {count} issues found")
            else:
                logger.info(f"{check['name']}: OK")
    
    def validate_business_rules(self):
        """Validate business rules"""
        logger.info("Validating business rules...")
        
        business_checks = [
            {
                'name': 'Properties with more than 20 bedrooms',
                'query': 'SELECT COUNT(*) FROM properties WHERE bedrooms > 20'
            },
            {
                'name': 'Properties with more than 15 bathrooms',
                'query': 'SELECT COUNT(*) FROM properties WHERE bathrooms > 15'
            },
            {
                'name': 'Properties with square footage > 50000',
                'query': 'SELECT COUNT(*) FROM properties WHERE square_footage > 50000'
            },
            {
                'name': 'HOA monthly fees > $5000',
                'query': 'SELECT COUNT(*) FROM hoa_details WHERE monthly_fee > 5000'
            },
            {
                'name': 'Property valuations > $50M',
                'query': 'SELECT COUNT(*) FROM property_valuations WHERE valuation_amount > 50000000'
            }
        ]
        
        for check in business_checks:
            result = self.db_manager.execute_query(check['query'])
            count = result[0][0] if result else 0
            if count > 0:
                logger.warning(f"{check['name']}: {count} outliers found")
            else:
                logger.info(f"{check['name']}: OK")
    
    def generate_summary_report(self):
        """Generate a summary report of the data"""
        logger.info("Generating summary report...")
        
        summary_queries = [
            {
                'name': 'Properties by type',
                'query': '''
                    SELECT property_type, COUNT(*) as count 
                    FROM properties 
                    WHERE property_type IS NOT NULL
                    GROUP BY property_type 
                    ORDER BY count DESC
                '''
            },
            {
                'name': 'Properties by state',
                'query': '''
                    SELECT pl.state, COUNT(*) as count 
                    FROM properties p 
                    JOIN property_locations pl ON p.location_id = pl.location_id
                    WHERE pl.state IS NOT NULL
                    GROUP BY pl.state 
                    ORDER BY count DESC
                '''
            },
            {
                'name': 'Average property values by type',
                'query': '''
                    SELECT p.property_type, AVG(pv.valuation_amount) as avg_value
                    FROM properties p
                    JOIN property_valuations pv ON p.property_id = pv.property_id
                    WHERE p.property_type IS NOT NULL 
                    AND pv.valuation_type = 'market'
                    GROUP BY p.property_type
                    ORDER BY avg_value DESC
                '''
            }
        ]
        
        for query_info in summary_queries:
            try:
                result = self.db_manager.execute_query(query_info['query'])
                logger.info(f"\n{query_info['name']}:")
                for row in result:
                    logger.info(f"  {row}")
            except Exception as e:
                logger.error(f"Error running summary query '{query_info['name']}': {e}")

def main():
    """Main function"""
    try:
        validator = DataValidator()
        validator.run_validation()
        validator.generate_summary_report()
        print("Data validation completed successfully!")
        
    except Exception as e:
        print(f"Data validation failed: {e}")
        return 1
    
    return 0

if _name_ == "_main_":
    exit(main())