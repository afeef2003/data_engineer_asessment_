# Data Engineering Assessment

Welcome!  
This exercise evaluates your core **data-engineering** skills:

| Competency | Focus                                                         |
| ---------- | ------------------------------------------------------------- |
| SQL        | relational modelling, normalisation, DDL/DML scripting        |
| Python ETL | data ingestion, cleaning, transformation, & loading (ELT/ETL) |

---

## 0 Prerequisites & Setup

> **Allowed technologies**

- **Python ≥ 3.8** – all ETL / data-processing code
- **MySQL 8** – the target relational database
- **Lightweight helper libraries only** (e.g. `pandas`, `mysql-connector-python`).  
  List every dependency in **`requirements.txt`** and justify anything unusual.
- **No ORMs / auto-migration tools** – write plain SQL by hand.

---

## 1 Clone the skeleton repo

```
git clone https://github.com/100x-Home-LLC/data_engineer_assessment.git
```

✏️ Note: Rename the repo after cloning and add your full name.

**Start the MySQL database in Docker:**

```
docker-compose -f docker-compose.initial.yml up --build -d
```

- Database is available on `localhost:3306`
- Credentials/configuration are in the Docker Compose file
- **Do not change** database name or credentials

For MySQL Docker image reference:
[MySQL Docker Hub](https://hub.docker.com/_/mysql)

---

### Problem

- You are provided with a raw JSON file containing property records is located in data/
- Each row relates to a property. Each row mixes many unrelated attributes (property details, HOA data, rehab estimates, valuations, etc.).
- There are multiple Columns related to this property.
- The database is not normalized and lacks relational structure.
- Use the supplied Field Config.xlsx (in data/) to understand business semantics.

### Task

- **Normalize the data:**

  - Develop a Python ETL script to read, clean, transform, and load data into your normalized MySQL tables.
  - Refer the field config document for the relation of business logic
  - Use primary keys and foreign keys to properly capture relationships

- **Deliverable:**
  - Write necessary python and sql scripts
  - Place your scripts in `sql/` and `scripts/`
  - The scripts should take the initial json to your final, normalized schema when executed
  - Clearly document how to run your script, dependencies, and how it integrates with your database.

**Tech Stack:**

- Python (include a `requirements.txt`)
  Use **MySQL** and SQL for all database work
- You may use any CLI or GUI for development, but the final changes must be submitted as python/ SQL scripts
- **Do not** use ORM migrations—write all SQL by hand

---

## Submission Guidelines

- Edit the section to the bottom of this README with your solutions and instructions for each section at the bottom.
- Place all scripts/code in their respective folders (`sql/`, `scripts/`, etc.)
- Ensure all steps are fully **reproducible** using your documentation
- Create a new private repo and invite the reviewer https://github.com/mantreshjain

---

**Good luck! We look forward to your submission.**

## Solutions and Instructions (Syeda Shamama Afeef)

# Data Engineering Assessment -Syeda Shamama Afeef

## Overview
This project implements a complete ETL pipeline that normalizes property data from a raw JSON format into a structured MySQL database. The solution includes data extraction, transformation, validation, and loading capabilities with comprehensive error handling and logging.

## Architecture

### Database Schema Design
The normalized schema consists of 5 main tables:

1. *property_locations* - Geographic and address information
2. *properties* - Core property attributes (bedrooms, bathrooms, square footage, etc.)
3. *hoa_details* - Homeowner Association information
4. *property_valuations* - Property valuation data (market value, assessed value, ARV, etc.)
5. *rehab_estimates* - Rehabilitation cost estimates

### Key Design Decisions

- *UUID Primary Keys*: Used for better scalability and avoiding integer overflow
- *Normalized Structure*: Separated concerns into logical entities to reduce redundancy
- *Foreign Key Constraints*: Enforced referential integrity between tables
- *Flexible Valuation System*: Supports multiple valuation types (market, assessed, ARV, etc.)
- *Comprehensive Indexing*: Added indexes for common query patterns
- *Audit Timestamps*: All tables include created_at and updated_at timestamps

## Project Structure


project/
├── sql/
│   └── schema.sql              # Database schema definition
├── scripts/
│   ├── config.py               # Configuration settings
│   ├── utils.py                # Utility functions and database manager
│   ├── etl.py                  # Main ETL pipeline
│   └── validate_data.py        # Data validation script
├── data/
│   ├── properties.json         # Raw property data (you need to add this)
│   └── Field Config.xlsx       # Field configuration (you need to add this)
├── logs/                       # ETL logs (created automatically)
├── requirements.txt            # Python dependencies
├── run_etl.sh                  # ETL runner script
└── README.md                   # This file


## Prerequisites

- Python ≥ 3.8
- MySQL 8.0
- Docker and Docker Compose
- Git

## Setup Instructions

### 1. Clone and Setup Repository

bash
# Clone the repository
git clone https://github.com/100x-Home-LLC/data_engineer_assessment.git

# Rename with your name
mv data_engineer_assessment data_engineer_assessment_[YOUR_NAME]
cd data_engineer_assessment_[YOUR_NAME]


### 2. Start MySQL Database

bash
# Start MySQL in Docker
docker-compose -f docker-compose.initial.yml up --build -d

# Verify database is running
docker ps


### 3. Install Python Dependencies

bash
# Install required packages
pip install -r requirements.txt


### 4. Configure Database Connection

Update the database configuration in scripts/config.py if needed:

python
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'password',  # Update based on docker-compose.initial.yml
    'database': 'property_db',
    'charset': 'utf8mb4',
    'autocommit': False
}


### 5. Add Your Data Files

Place your data files in the data/ directory:
- properties.json - Your raw property data
- Field Config.xlsx - Field configuration document

Update the file paths in scripts/config.py if your files have different names.

## Running the ETL Pipeline

### Option 1: Using the Runner Script (Recommended)

bash
# Make the script executable
chmod +x run_etl.sh

# Run the complete pipeline
./run_etl.sh


### Option 2: Manual Execution

bash
# Navigate to scripts directory
cd scripts

# Run ETL pipeline
python3 etl.py

# Run data validation (optional)
python3 validate_data.py


## ETL Pipeline Details

### Extract Phase
- Reads raw JSON data from the input file
- Handles both single objects and arrays of objects
- Validates file existence and format

### Transform Phase
- *Data Cleaning*: Handles null values, trims whitespace, validates data types
- *Normalization*: Splits denormalized data into separate entities
- *Key Generation*: Creates UUID primary keys for all records
- *Relationship Mapping*: Establishes foreign key relationships
- *Data Type Conversion*: Converts strings to appropriate types (numbers, dates, booleans)

### Load Phase
- *Batch Processing*: Loads data in batches for efficiency
- *Transaction Management*: Uses database transactions for data consistency
- *Error Handling*: Comprehensive error handling with rollback capabilities
- *Logging*: Detailed logging of all operations

## Data Validation

The validation script performs the following checks:

### 1. Record Count Validation
- Verifies data was loaded into all tables
- Reports record counts for each table

### 2. Foreign Key Integrity
- Checks for orphaned records
- Validates all foreign key relationships

### 3. Data Quality Checks
- Identifies missing required fields
- Finds unrealistic values (negative square footage, future build dates)
- Validates data ranges and formats

### 4. Business Rule Validation
- Checks for outliers (properties with >20 bedrooms)
- Validates reasonable value ranges
- Identifies potential data entry errors

## Configuration

### Database Configuration
Edit scripts/config.py to modify:
- Database connection parameters
- File paths
- Logging settings
- ETL behavior (batch size, validation options)

### Field Mappings
The ETL pipeline uses flexible field mappings to handle various JSON field names:
- address or street_address → address_line_1
- bedrooms or beds → bedrooms
- bathrooms or baths → bathrooms
- square_footage or sqft → square_footage

## Logging

The pipeline creates comprehensive logs in the logs/ directory:
- ETL operations and progress
- Data validation results
- Error messages and stack traces
- Performance metrics

## Error Handling

- *Database Errors*: Automatic rollback on failed transactions
- *Data Errors*: Continues processing other records when individual records fail
- *File Errors*: Clear error messages for missing or corrupted files
- *Retry Logic*: Configurable retry attempts for transient failures

## Performance Considerations

- *Batch Processing*: Data is loaded in configurable batches
- *Indexing*: Comprehensive indexing strategy for common queries
- *Memory Management*: Efficient memory usage for large datasets
- *Connection Pooling*: Database connections are properly managed

## Testing

### Unit Testing
bash
# Run unit tests (if implemented)
python -m pytest tests/


### Manual Testing
bash
# Test database connectivity
python3 -c "from scripts.utils import DatabaseManager; db = DatabaseManager(); db.connect(); print('Connection successful')"

# Test data validation
cd scripts
python3 validate_data.py


## Troubleshooting

### Common Issues

1. *Database Connection Failed*
   - Verify MySQL container is running: docker ps
   - Check connection parameters in config.py
   - Ensure port 3306 is available

2. *File Not Found Errors*
   - Verify data files exist in data/ directory
   - Check file paths in config.py
   - Ensure file permissions are correct

3. *Data Type Errors*
   - Check raw data format matches expected schema
   - Verify field mappings in ETL transformation logic
   - Review data cleaning functions

4. *Foreign Key Constraint Errors*
   - Ensure parent records are loaded before child records
   - Check UUID generation is working correctly
   - Verify data relationships in source data

## Database Queries

### Sample Queries

sql
-- Get property summary with location and valuation
SELECT * FROM property_summary;

-- Properties with highest valuations
SELECT p.*, pv.valuation_amount 
FROM properties p
JOIN property_valuations pv ON p.property_id = pv.property_id
ORDER BY pv.valuation_amount DESC
LIMIT 10;

-- Properties by state
SELECT pl.state, COUNT(*) as property_count
FROM properties p
JOIN property_locations pl ON p.location_id = pl.location_id
GROUP BY pl.state
ORDER BY property_count DESC;


## Future Enhancements

1. *Incremental Loading*: Support for updating existing records
2. *Data Lineage*: Track data transformations and sources
3. *Automated Testing*: Comprehensive test suite
4. *Performance Monitoring*: ETL performance metrics
5. *Data Quality Dashboard*: Visual data quality reporting

## Dependencies Justification

- *mysql-connector-python*: Official MySQL database connector
- *pandas*: Data manipulation and analysis (industry standard)
- *openpyxl*: Excel file reading for field configuration
- *jsonschema*: Data validation (optional but recommended)
- *python-dateutil*: Date parsing utilities

All dependencies are lightweight and commonly used in data engineering projects.

## Submission

This solution provides:
-  Complete normalized database schema
-  Comprehensive ETL pipeline
-  Data validation and quality checks
-  Detailed documentation and instructions
-  Error handling and logging
-  Reproducible execution scripts
-  No ORM usage (pure SQL)

The pipeline is designed to handle real-world data challenges including missing values, data type inconsistencies, and varying field names while maintaining data integrity and providing comprehensive validation.