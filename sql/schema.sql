-- Database Schema for Property Management System
-- This schema normalizes property data into related tables

-- Create database if it doesn't exist
CREATE DATABASE IF NOT EXISTS property_db;
USE property_db;

-- Drop tables if they exist (for development)
DROP TABLE IF EXISTS rehab_estimates;
DROP TABLE IF EXISTS property_valuations;
DROP TABLE IF EXISTS hoa_details;
DROP TABLE IF EXISTS properties;
DROP TABLE IF EXISTS property_locations;

-- 1. Property Locations Table
-- Stores address and geographic information
CREATE TABLE property_locations (
    location_id VARCHAR(36) PRIMARY KEY,
    address_line_1 VARCHAR(255),
    address_line_2 VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    county VARCHAR(100),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 2. Properties Table
-- Main property information
CREATE TABLE properties (
    property_id VARCHAR(36) PRIMARY KEY,
    location_id VARCHAR(36),
    property_type VARCHAR(50),
    bedrooms INT,
    bathrooms DECIMAL(3,1),
    square_footage INT,
    lot_size DECIMAL(10,2),
    year_built INT,
    garage_spaces INT,
    pool BOOLEAN DEFAULT FALSE,
    fireplace BOOLEAN DEFAULT FALSE,
    basement BOOLEAN DEFAULT FALSE,
    property_condition VARCHAR(50),
    listing_status VARCHAR(50),
    mls_number VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (location_id) REFERENCES property_locations(location_id)
);

-- 3. HOA Details Table
-- Homeowner Association information
CREATE TABLE hoa_details (
    hoa_id VARCHAR(36) PRIMARY KEY,
    property_id VARCHAR(36),
    hoa_name VARCHAR(255),
    monthly_fee DECIMAL(10,2),
    annual_fee DECIMAL(10,2),
    hoa_contact_info TEXT,
    amenities TEXT,
    restrictions TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
);

-- 4. Property Valuations Table
-- Property valuation and pricing information
CREATE TABLE property_valuations (
    valuation_id VARCHAR(36) PRIMARY KEY,
    property_id VARCHAR(36),
    valuation_type VARCHAR(50), -- 'market', 'assessed', 'arv', etc.
    valuation_amount DECIMAL(15,2),
    valuation_date DATE,
    valuation_source VARCHAR(100),
    confidence_level VARCHAR(20),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
);

-- 5. Rehab Estimates Table
-- Rehabilitation cost estimates
CREATE TABLE rehab_estimates (
    estimate_id VARCHAR(36) PRIMARY KEY,
    property_id VARCHAR(36),
    estimate_type VARCHAR(50), -- 'cosmetic', 'structural', 'full_rehab', etc.
    estimated_cost DECIMAL(15,2),
    estimate_date DATE,
    contractor_name VARCHAR(255),
    work_description TEXT,
    timeline_weeks INT,
    materials_cost DECIMAL(15,2),
    labor_cost DECIMAL(15,2),
    permit_cost DECIMAL(15,2),
    contingency_percentage DECIMAL(5,2),
    status VARCHAR(50), -- 'draft', 'approved', 'completed', etc.
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    FOREIGN KEY (property_id) REFERENCES properties(property_id)
);

-- Add indexes for better performance
CREATE INDEX idx_properties_location ON properties(location_id);
CREATE INDEX idx_properties_type ON properties(property_type);
CREATE INDEX idx_properties_bedrooms ON properties(bedrooms);
CREATE INDEX idx_properties_bathrooms ON properties(bathrooms);
CREATE INDEX idx_properties_year_built ON properties(year_built);

CREATE INDEX idx_hoa_property ON hoa_details(property_id);
CREATE INDEX idx_hoa_monthly_fee ON hoa_details(monthly_fee);

CREATE INDEX idx_valuations_property ON property_valuations(property_id);
CREATE INDEX idx_valuations_type ON property_valuations(valuation_type);
CREATE INDEX idx_valuations_date ON property_valuations(valuation_date);
CREATE INDEX idx_valuations_amount ON property_valuations(valuation_amount);

CREATE INDEX idx_rehab_property ON rehab_estimates(property_id);
CREATE INDEX idx_rehab_type ON rehab_estimates(estimate_type);
CREATE INDEX idx_rehab_cost ON rehab_estimates(estimated_cost);
CREATE INDEX idx_rehab_status ON rehab_estimates(status);

CREATE INDEX idx_locations_city ON property_locations(city);
CREATE INDEX idx_locations_state ON property_locations(state);
CREATE INDEX idx_locations_zip ON property_locations(zip_code);

-- Create a view for easy property data retrieval
CREATE VIEW property_summary AS
SELECT 
    p.property_id,
    p.property_type,
    p.bedrooms,
    p.bathrooms,
    p.square_footage,
    p.year_built,
    CONCAT(pl.address_line_1, ', ', pl.city, ', ', pl.state, ' ', pl.zip_code) as full_address,
    pl.city,
    pl.state,
    pl.zip_code,
    h.hoa_name,
    h.monthly_fee as hoa_monthly_fee,
    pv.valuation_amount as current_market_value,
    pv.valuation_date as valuation_date,
    re.estimated_cost as rehab_estimate,
    re.estimate_date as rehab_estimate_date
FROM properties p
LEFT JOIN property_locations pl ON p.location_id = pl.location_id
LEFT JOIN hoa_details h ON p.property_id = h.property_id
LEFT JOIN property_valuations pv ON p.property_id = pv.property_id 
    AND pv.valuation_type = 'market'
LEFT JOIN rehab_estimates re ON p.property_id = re.property_id 
    AND re.status = 'approved';