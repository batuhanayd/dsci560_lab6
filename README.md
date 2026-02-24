# DSCI 560 Lab 6 – Data Collection & PDF Extraction (Oil Wells)

Extract **well information** (Figure 1) and **stimulation data** (Figure 2) from oil well PDFs, store in MySQL, and optionally scrape DrillingEdge for additional well details.

## File structure

```
dsci560_lab6/
├── config.py              # PDF_FOLDER, MYSQL_CONFIG (create from config.example or copy)
├── schema.sql             # MySQL schema: wells, stimulations, scraped_wells
├── extract_pdf_wells.py    # PDF → parse → insert into wells + stimulations
├── scraper_wells.py       # wells table → DrillingEdge scrape → scraped_wells
├── requirements.txt       # pypdf, mysql-connector-python, requests, beautifulsoup4, pandas
├── README.md
├── .gitignore
├── temp/                  # raw_<stem>.txt from PDF extraction (gitignored)
├── extract_wells.log      # debug log for extract_pdf_wells (gitignored)
├── scraper_wells.log      # debug log for scraper_wells (gitignored)
├── wells_data.csv         # optional CSV input (gitignored)
├── wells_data2.csv        # optional CSV input (gitignored)
├── DSCI560_Lab5/          # PDF folder (e.g. oil well PDFs; path set in config)
│   └── *.pdf
└── lab6_task4*.ipynb      # notebooks for exploration / scraping logic
```

## Contents

- **`schema.sql`** – Tables: `wells` (PK: `well_id`), `stimulations` (PK: `stimulation_id`, proppant as JSON), `scraped_wells` (PK: `scraped_id`, one row per well from scraper).
- **`extract_pdf_wells.py`** – Iterates over PDFs in `PDF_FOLDER`, extracts text with pypdf, parses well + stimulation + proppant, inserts into MySQL. Skips PDFs already in `wells.source_pdf`. Writes raw text to `temp/raw_<stem>.txt`, debug to `extract_wells.log`.
- **`scraper_wells.py`** – Reads wells from the `wells` table; for each, finds the DrillingEdge URL, fetches the detail page, parses api_no, well_name, operator, county, well_status, well_type, closest_city, latitude, longitude (split from "lat, long" when present), oil_bbl, gas_mcf, production_dates_on_file; inserts one row per well into `scraped_wells`. Skips wells already in `scraped_wells`. Logs to `scraper_wells.log`.
- **`config.py`** – Set `PDF_FOLDER` and `MYSQL_CONFIG` (database `dsci560_wells`).

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

### PDF extraction

```bash
python extract_pdf_wells.py
```

- Processes all PDFs under `PDF_FOLDER`; skips any whose filename is already in `wells.source_pdf`.
- Extracts text with pypdf, parses well + stimulation + proppant, inserts and commits per PDF.
- Raw extracted text in `temp/raw_<stem>.txt`; debug output in `extract_wells.log`.

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
- For each well: search DrillingEdge → get detail page → parse api_no, well_name, operator, county, well_status, well_type, closest_city, latitude, longitude (split from "lat, long" when present), oil_bbl, gas_mcf, production_dates_on_file.
- Inserts one row per well into `scraped_wells` (linked by well_id). Skips wells already in `scraped_wells`. Logs to `scraper_wells.log`.

**Dry run:** `python scraper_wells.py --dry-run` — lists first 5 wells from DB only (no network, no inserts).

## Data extracted

| Table | Primary key | Fields |
|-------|-------------|--------|
| **wells** | `well_id` | api_number, well_name, operator, enseco_job_number, job_type, county_state, surface_hole_location, latitude, longitude, datum, source_pdf |
| **stimulations** | `stimulation_id` | well_id, date_stimulated, stimulated_formation, top_ft, bottom_ft, stimulation_stages, volume, volume_units, type_treatment, acid_pct, lbs_proppant, max_treatment_pressure_psi, max_treatment_rate_bbls_min, proppant_details (JSON) |
| **scraped_wells** | `scraped_id` | well_id (FK), well_name, api_number, scraped_url, api_no, closest_city, county, latitude, longitude, gas_mcf, oil_bbl, operator, production_dates_on_file, well_status, well_type |

## Notes

- **Unsupported encoding**: PDFs using encodings pypdf cannot handle (e.g. 90ms-RKSJ) will raise an error; the script does not use OCR.
- **Existing DB**: If the schema was created earlier with shorter columns, run the `ALTER TABLE` lines at the bottom of `schema.sql` to avoid "Data too long" errors. For an existing `scraped_wells` table, add new columns or drop and recreate the table.
- Parsing uses regex keyed to the assignment figures; different layouts may need pattern changes in `extract_pdf_wells.py`.
