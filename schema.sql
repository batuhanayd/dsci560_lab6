-- DSCI 560 Lab 6: Oil Well Data - MySQL Schema (two tables only)
-- Run this script to create the database and tables before running the PDF extractor.
-- Example: mysql -u your_user -p < schema.sql

CREATE DATABASE IF NOT EXISTS dsci560_wells;
USE dsci560_wells;

-- Table 1: Well-specific information (Figure 1)
-- Primary key: well_id (one row per well document)
-- Long string columns use TEXT/MEDIUMTEXT to avoid "Data too long" errors
CREATE TABLE IF NOT EXISTS wells (
    well_id INT AUTO_INCREMENT PRIMARY KEY,
    api_number VARCHAR(32) UNIQUE,
    well_name TEXT,
    operator TEXT,
    enseco_job_number VARCHAR(64),
    job_type VARCHAR(64),
    county_state TEXT,
    surface_hole_location MEDIUMTEXT,
    latitude VARCHAR(32),
    longitude VARCHAR(32),
    datum VARCHAR(32),
    source_pdf VARCHAR(512),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_api (api_number),
    INDEX idx_well_name (well_name(100)),
    INDEX idx_operator (operator(100))
);

-- Table 2: Stimulation data including proppant breakdown (Figure 2)
-- Primary key: stimulation_id (one row per stimulation document; proppant stored as JSON in one column)
CREATE TABLE IF NOT EXISTS stimulations (
    stimulation_id INT AUTO_INCREMENT PRIMARY KEY,
    well_id INT NOT NULL,
    date_stimulated DATE,
    stimulated_formation TEXT,
    top_ft INT,
    bottom_ft INT,
    stimulation_stages INT,
    volume DECIMAL(18, 2),
    volume_units VARCHAR(32),
    type_treatment TEXT,
    acid_pct DECIMAL(6, 2),
    lbs_proppant BIGINT,
    max_treatment_pressure_psi INT,
    max_treatment_rate_bbls_min DECIMAL(10, 2),
    proppant_details MEDIUMTEXT COMMENT 'JSON array: [{"proppant_type":"100 Mesh White","lbs":314040}, ...]',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (well_id) REFERENCES wells(well_id) ON DELETE CASCADE,
    INDEX idx_well (well_id),
    INDEX idx_formation (stimulated_formation(64)),
    INDEX idx_date (date_stimulated)
);

-- If tables already exist, run these to allow long strings (no data loss):
-- ALTER TABLE wells MODIFY well_name TEXT, MODIFY operator TEXT, MODIFY county_state TEXT, MODIFY surface_hole_location MEDIUMTEXT, MODIFY source_pdf VARCHAR(512);
-- ALTER TABLE stimulations MODIFY stimulated_formation TEXT, MODIFY type_treatment TEXT, MODIFY proppant_details MEDIUMTEXT;
