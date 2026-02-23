# DSCI 560 Lab 6 – Data Collection & PDF Extraction (Oil Wells)

Extract **well information** (Figure 1) and **stimulation data** (Figure 2) from oil well PDFs and store them in MySQL.

## Contents

- **`schema.sql`** – Tables: `wells` (PK: `well_id`), `stimulations` (PK: `stimulation_id`, includes proppant as JSON), `scraped_wells` (PK: `scraped_id`, for scraper output).
- **`extract_pdf_wells.py`** – Iterates over PDFs, extracts text with pypdf, parses fields, inserts into MySQL. Skips PDFs already in `wells`. Debug logs go to `extract_wells.log`.
- **`scraper_wells.py`** – Loads wells from the `wells` table, looks up each on DrillingEdge, scrapes the detail page (api_no, well_name, operator, county, well_status, well_type, closest_city, oil_bbl, gas_mcf, production_dates_on_file), and stores one row per well in `scraped_wells`. Logs to `scraper_wells.log`.
- **`config.py`** – Set `PDF_FOLDER` and `MYSQL_CONFIG`.

## Requirements

- Python 3.8+
- MySQL (server and client)
- PDFs in a folder (e.g. from [Google Drive](https://drive.google.com/drive/u/4/folders/12g-bhOylyaMoLF5djocnAeZHBx-gsxgY)); set path in `config.py`

## Setup

**1. Virtual environment and dependencies**

```bash
# Windows
python -m venv venv
venv\Scripts\activate
pip install -r requirements.txt
```

```bash
# Linux / macOS
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

**2. Database**

```bash
mysql -u your_user -p < schema.sql
```

**3. Config**

Create or edit `config.py` with `PDF_FOLDER` (path to PDFs) and `MYSQL_CONFIG` (host, user, password, database `dsci560_wells`).

## Run

```bash
python extract_pdf_wells.py
```

- Processes all PDFs under `PDF_FOLDER`; skips any whose filename is already in `wells.source_pdf`.
- Extracts text with pypdf, parses well + stimulation + proppant, inserts and commits per PDF.
- Raw extracted text is written to `temp/raw_<stem>.txt`. Debug output to `extract_wells.log`.

**Dry run (parse only, no DB):**

```bash
python extract_pdf_wells.py --dry-run
```

Prints parsed well, stimulation, and proppant for the first 3 PDFs.

### Scraper (DrillingEdge)

```bash
python scraper_wells.py
```

- Reads wells from the `wells` table (well_id, well_name, api_number).
- For each well, finds the DrillingEdge URL via search, then fetches the well detail page and parses: api_no, well_name, operator, county, well_status, well_type, closest_city, oil_bbl, gas_mcf, production_dates_on_file.
- Inserts one row per well into `scraped_wells` (linked by well_id). Skips wells already in `scraped_wells`. Logs to `scraper_wells.log`. Use `--dry-run` to list first 5 wells from DB only.

## Data extracted

| Table | Primary key | Fields |
|-------|-------------|--------|
| **wells** | `well_id` | api_number, well_name, operator, enseco_job_number, job_type, county_state, surface_hole_location, latitude, longitude, datum, source_pdf |
| **stimulations** | `stimulation_id` | well_id, date_stimulated, stimulated_formation, top_ft, bottom_ft, stimulation_stages, volume, volume_units, type_treatment, acid_pct, lbs_proppant, max_treatment_pressure_psi, max_treatment_rate_bbls_min, proppant_details (JSON) |
| **scraped_wells** | `scraped_id` | well_id (FK), well_name, api_number, scraped_url, api_no, closest_city, county, gas_mcf, oil_bbl, operator, production_dates_on_file, well_status, well_type |

## Notes

- **Unsupported encoding**: PDFs using encodings pypdf cannot handle (e.g. 90ms-RKSJ) will raise an error; the script does not use OCR.
- **Existing DB**: If the schema was created earlier with shorter columns, run the `ALTER TABLE` lines at the bottom of `schema.sql` to avoid "Data too long" errors.
- Parsing uses regex keyed to the assignment figures; different layouts may need pattern changes in `extract_pdf_wells.py`.
