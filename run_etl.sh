#!/bin/bash

# ETL Pipeline Runner Script
# This script runs the complete ETL pipeline for the property data

echo "=== Property Data ETL Pipeline ==="
echo "Starting ETL process..."

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is required but not installed"
    exit 1
fi

# Check if required directories exist
if [ ! -d "scripts" ]; then
    echo "Error: scripts directory not found"
    exit 1
fi

if [ ! -d "data" ]; then
    echo "Error: data directory not found"
    exit 1
fi

# Install dependencies
echo "Installing dependencies..."
pip3 install -r requirements.txt

# Create logs directory if it doesn't exist
mkdir -p logs

# Run the ETL pipeline
echo "Running ETL pipeline..."
cd scripts
python3 etl.py

# Check if ETL was successful
if [ $? -eq 0 ]; then
    echo "ETL pipeline completed successfully!"
    
    # Run data validation
    echo "Running data validation..."
    python3 validate_data.py
    
    if [ $? -eq 0 ]; then
        echo "Data validation completed successfully!"
    else
        echo "Data validation failed!"
        exit 1
    fi
else
    echo "ETL pipeline failed!"
    exit 1
fi

echo "=== ETL Process Complete ==="