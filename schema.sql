CREATE DATABASE IF NOT EXISTS dsci560_wells;
USE dsci560_wells;


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


CREATE TABLE IF NOT EXISTS scraped_wells (
    scraped_id INT AUTO_INCREMENT PRIMARY KEY,
    well_id INT NULL,
    well_name MEDIUMTEXT,
    api_number VARCHAR(32) NULL,
    scraped_url MEDIUMTEXT,
    api_no VARCHAR(32) NULL,
    closest_city VARCHAR(128) NULL,
    county TEXT NULL,
    gas_mcf INT NULL,
    oil_bbl INT NULL,
    operator TEXT NULL,
    production_dates_on_file VARCHAR(255) NULL,
    well_status VARCHAR(64) NULL,
    well_type VARCHAR(64) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_well_id (well_id),
    INDEX idx_api (api_number),
    INDEX idx_well_name (well_name(100)),
    FOREIGN KEY (well_id) REFERENCES wells(well_id) ON DELETE SET NULL
);
