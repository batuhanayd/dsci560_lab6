import io
import json
import os
import re
import sys
from contextlib import redirect_stderr
from datetime import datetime, date
from pathlib import Path

try:
    from pypdf import PdfReader
except ImportError:
    from PyPDF2 import PdfReader

import mysql.connector

DEBUG = True
LOG_FILE = None


def _debug(label: str, data, max_chars: int = 80):
    if not DEBUG or data is None:
        return
    s = str(data) if not isinstance(data, str) else data
    if isinstance(data, (dict, list)) or len(s) > max_chars:
        return
    line = f"{datetime.now().isoformat()} [DEBUG] {label}: {s}\n"
    if LOG_FILE is not None:
        try:
            LOG_FILE.write(line)
            LOG_FILE.flush()
        except Exception:
            pass
    else:
        print(line.rstrip())


def load_config():
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / "config.py"
    _debug("load_config config_path", str(config_path))
    if not config_path.exists():
        raise FileNotFoundError("Create config.py with PDF_FOLDER and MYSQL_CONFIG.")
    import importlib.util
    spec = importlib.util.spec_from_file_location("config", config_path)
    cfg = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cfg)
    _debug("load_config PDF_FOLDER", getattr(cfg, "PDF_FOLDER", None))
    _debug("load_config MYSQL_CONFIG host", cfg.MYSQL_CONFIG.get("host") if getattr(cfg, "MYSQL_CONFIG", None) else None)
    return cfg


def get_pdf_text(pdf_path: str) -> str:
    _debug("get_pdf_text pdf_path", pdf_path)
    with redirect_stderr(io.StringIO()):
        reader = PdfReader(pdf_path)
        parts = []
        for page in reader.pages:
            parts.append(page.extract_text() or "")
    text = "\n".join(parts)
    _debug("get_pdf_text len", len(text))
    return text or ""


WELL_COLUMN_MAX = {"api_number": 32, "enseco_job_number": 64, "job_type": 64, "latitude": 32, "longitude": 32, "datum": 32, "source_pdf": 512}
STIM_COLUMN_MAX = {"volume_units": 32}


def _trunc(s: str, max_len: int):
    if s is None:
        return None
    s = (s if isinstance(s, str) else str(s)).strip()
    if not s or max_len <= 0:
        return s if s else None
    return s[:max_len] if len(s) > max_len else s


def _parse_int(s: str):
    if not s:
        return None
    s = re.sub(r"[,'\s]", "", str(s))
    try:
        return int(s)
    except ValueError:
        return None


def _parse_decimal(s: str):
    if not s:
        return None
    s = re.sub(r",", "", str(s).strip())
    try:
        return float(s)
    except ValueError:
        return None


def _parse_date(s: str):
    if not s:
        return None
    s = (s or "").strip()
    m = re.match(r"(\d{1,2})[/\-](\d{1,2})[/\-](\d{2,4})", s)
    if m:
        mo, d, y = m.groups()
        y = int(y)
        if y < 100:
            y += 2000 if y < 50 else 1900
        try:
            return date(y, int(mo), int(d))
        except ValueError:
            pass
    return None


def _first_group(text: str, pattern: str, flags=0) -> str:
    m = re.search(pattern, text, flags)
    return (m.group(1) or "").strip() if m else ""


def parse_well_fields(text: str) -> dict:
    patterns = [
        (r"Operator\s*:?\s*(.+?)(?=\n|Well Name|API#|Address|$)", "operator"),
        (r"Well\s+Name\s*(?:and\s+Number)?\s*:?\s*(.+?)(?=\n\n|Operator|API#|Enseco|Address|$)", "well_name"),
        (r"API\s*#?\s*:?\s*(\d{2}-\d{3}-\d{5}(?:-\d{2})?)", "api_number"),
        (r"Enseco\s+Job\s*#?\s*:?\s*(\S+)", "enseco_job_number"),
        (r"Job\s+Type\s*:?\s*(.+?)(?=\n|County|$)", "job_type"),
        (r"County\s*,?\s*State\s*:?\s*(.+?)(?=\n|Well Surface|SHL|Section|$)", "county_state"),
        (r"County\s*:?\s*(\w+)(?=\n|State|$)", "county_state"),
        (r"Well\s+Surface\s+Hole\s+Location\s*\(SHL\)\s*:?\s*(.+?)(?=\n\n|Latitude|$)", "surface_hole_location"),
        (r"SHL\s*:?\s*(.+?)(?=\n\n|Latitude|$)", "surface_hole_location"),
        (r"Latitude\s*:?\s*([\d°°\'\"\.\s]+[NS]?)", "latitude"),
        (r"Longitude\s*:?\s*([\d°°\'\"\.\s]+[EW]?)", "longitude"),
        (r"Datum\s*:?\s*(\S+(?:\s+\d+)?)", "datum"),
        (r"Well\s+File\s+No\.?\s*:?\s*(\d+)", "well_file_no"),
        (r"Section\s+Township\s+(\d+\s+\d+\s*[NnSs])\s+Range\s+(\d+\s*[EeWw])", "section_township_range"),
    ]
    out = {}
    for pattern, key in patterns:
        val = _first_group(text, pattern, re.IGNORECASE | re.DOTALL)
        if val and key not in out:
            out[key] = (re.sub(r"\s+", " ", val) or "").strip()
    _debug("parse_well_fields", f"{len(out)} fields")
    return out


def parse_stimulation_fields(text: str) -> dict:
    patterns = [
        (r"Date\s+Stimulated\s*:?\s*(\d{1,2}/\d{1,2}/\d{2,4})", "date_stimulated"),
        (r"Stimulation\s+Date\s*:?\s*(\d{1,2}/\d{1,2}/\d{2,4})", "date_stimulated"),
        (r"Stimulated\s+Formation\s*:?\s*(\w+)", "stimulated_formation"),
        (r"(?:Stimulated\s+)?Formation\s*:?\s*(\w+)", "stimulated_formation"),
        (r"Top\s*\(Ft\)\s*:?\s*([\d,]+)", "top_ft"),
        (r"Top\s*\(ft\)\s*:?\s*([\d,]+)", "top_ft"),
        (r"Bottom\s*\(Ft\)\s*:?\s*([\d,]+)", "bottom_ft"),
        (r"Bottom\s*\(ft\)\s*:?\s*([\d,]+)", "bottom_ft"),
        (r"Stimulation\s+Stages\s*:?\s*(\d+)", "stimulation_stages"),
        (r"Stages\s*:?\s*(\d+)", "stimulation_stages"),
        (r"Volume\s+Units\s*:?\s*(\w+)", "volume_units"),
        (r"Volume\s*:?\s*([\d,\.]+)", "volume"),
        (r"Type\s+Treatment\s*:?\s*([^\n]+?)(?=\s*\n|Acid|Lbs\s+Proppant|$)", "type_treatment"),
        (r"Treatment\s+Type\s*:?\s*([^\n]+?)(?=\s*\n|Acid|$)", "type_treatment"),
        (r"Acid\s*%?\s*:?\s*([\d\.]+)", "acid_pct"),
        (r"Lbs\s+Proppant\s*:?\s*([\d,]+)", "lbs_proppant"),
        (r"Proppant\s*\(?\s*[Ll]bs?\.?\s*\)?\s*:?\s*([\d,]+)", "lbs_proppant"),
        (r"Maximum\s+Treatment\s+Pressure\s*\(PSI\)\s*:?\s*([\d,]+)", "max_treatment_pressure_psi"),
        (r"Max\.?\s+Treatment\s+Pressure\s*:?\s*([\d,]+)", "max_treatment_pressure_psi"),
        (r"Maximum\s+Treatment\s+Rate\s*\(BBLS/Min\)\s*:?\s*([\d\.]+)", "max_treatment_rate_bbls_min"),
        (r"Max\.?\s+Treatment\s+Rate\s*:?\s*([\d\.]+)", "max_treatment_rate_bbls_min"),
    ]
    out = {}
    for pattern, key in patterns:
        val = _first_group(text, pattern, re.IGNORECASE | re.DOTALL)
        if val is not None and val.strip() and (key not in out or not out[key].strip()):
            out[key] = (val or "").strip()
    _debug("parse_stimulation_fields", f"{len(out)} fields")
    return out


def parse_proppant_details(text: str) -> list:
    out = []
    for m in re.finditer(r"(\d+(?:/\d+)?\s*Mesh\s+\w+)\s*:?\s*([\d,]+)", text, re.IGNORECASE):
        ptype = (m.group(1) or "").strip()
        lbs = _parse_int(m.group(2))
        if ptype and lbs is not None:
            out.append({"proppant_type": ptype, "lbs": lbs})
    for m in re.finditer(r"(\d+/\d+\s+\w+)\s*:?\s*([\d,]+)", text, re.IGNORECASE):
        ptype = (m.group(1) or "").strip()
        lbs = _parse_int(m.group(2))
        if ptype and lbs is not None and not any(p["proppant_type"] == ptype for p in out):
            out.append({"proppant_type": ptype, "lbs": lbs})
    for m in re.finditer(r"(\d+\s+White)\s*:?\s*([\d,]+)", text, re.IGNORECASE):
        ptype = (m.group(1) or "").strip()
        lbs = _parse_int(m.group(2))
        if ptype and lbs is not None and not any(p["proppant_type"] == ptype for p in out):
            out.append({"proppant_type": ptype, "lbs": lbs})
    _debug("parse_proppant_details", f"{len(out)} items")
    return out


def ensure_well(cursor, config: dict, source_pdf: str) -> int:
    _debug("ensure_well source_pdf", source_pdf)
    api = (config.get("api_number") or "").strip()
    if not api and not source_pdf:
        cursor.execute("INSERT INTO wells (source_pdf) VALUES (%s)", (source_pdf,))
        _debug("ensure_well inserted (no api) well_id", cursor.lastrowid)
        return cursor.lastrowid
    if api:
        cursor.execute("SELECT well_id FROM wells WHERE api_number = %s", (api,))
        row = cursor.fetchone()
        if row:
            _debug("ensure_well existing well_id", row[0])
            return row[0]
    def _w(k):
        return _trunc(config.get(k), WELL_COLUMN_MAX.get(k, 512))
    cursor.execute(
        """INSERT INTO wells (api_number, well_name, operator, enseco_job_number, job_type,
            county_state, surface_hole_location, latitude, longitude, datum, source_pdf)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            _w("api_number"),
            (config.get("well_name") or "").strip() or None,
            (config.get("operator") or "").strip() or None,
            _w("enseco_job_number"),
            _w("job_type"),
            (config.get("county_state") or "").strip() or None,
            (config.get("surface_hole_location") or "").strip() or None,
            _w("latitude"),
            _w("longitude"),
            _w("datum"),
            _trunc(source_pdf, WELL_COLUMN_MAX["source_pdf"]) if source_pdf else None,
        ),
    )
    _debug("ensure_well inserted well_id", cursor.lastrowid)
    return cursor.lastrowid


def insert_stimulation(cursor, well_id: int, stim: dict, proppant_rows: list) -> None:
    _debug("insert_stimulation well_id", well_id)
    date_val = stim.get("date_stimulated")
    if isinstance(date_val, str):
        date_val = _parse_date(date_val)
    _debug("insert_stimulation date_val", date_val)
    proppant_json = json.dumps(proppant_rows) if proppant_rows else None
    stim_formation = (stim.get("stimulated_formation") or "").strip() or None
    stim_vol_units = _trunc(stim.get("volume_units"), STIM_COLUMN_MAX["volume_units"])
    stim_type_treat = (stim.get("type_treatment") or "").strip() or None
    cursor.execute(
        """INSERT INTO stimulations (well_id, date_stimulated, stimulated_formation, top_ft, bottom_ft,
            stimulation_stages, volume, volume_units, type_treatment, acid_pct, lbs_proppant,
            max_treatment_pressure_psi, max_treatment_rate_bbls_min, proppant_details)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            well_id, date_val, stim_formation,
            _parse_int(stim.get("top_ft")), _parse_int(stim.get("bottom_ft")),
            _parse_int(stim.get("stimulation_stages")),
            _parse_decimal(stim.get("volume")), stim_vol_units, stim_type_treat or None,
            _parse_decimal(stim.get("acid_pct")), _parse_int(stim.get("lbs_proppant")),
            _parse_int(stim.get("max_treatment_pressure_psi")),
            _parse_decimal(stim.get("max_treatment_rate_bbls_min")),
            proppant_json,
        ),
    )
    _debug("insert_stimulation stimulation_id", cursor.lastrowid)


def well_exists_for_source_pdf(cursor, source_pdf: str) -> bool:
    if not source_pdf:
        return False
    cursor.execute("SELECT well_id FROM wells WHERE source_pdf = %s LIMIT 1", (source_pdf,))
    return cursor.fetchone() is not None


def process_pdf(pdf_path: str, cursor) -> bool:
    _debug("process_pdf pdf_path", pdf_path)
    text = get_pdf_text(pdf_path)
    _debug("process_pdf text len", len(text))
    temp_dir = Path(__file__).resolve().parent / "temp"
    temp_dir.mkdir(parents=True, exist_ok=True)
    (temp_dir / f"raw_{Path(pdf_path).stem}.txt").write_text(text or "", encoding="utf-8")
    if not text.strip():
        raise ValueError(f"No text extracted from PDF (unsupported encoding e.g. 90ms-RKSJ, or empty file): {pdf_path}")
    well_data = parse_well_fields(text)
    stim_data = parse_stimulation_fields(text)
    proppant = parse_proppant_details(text)
    source_pdf = os.path.basename(pdf_path)
    print(f"Inserting: {source_pdf}")
    well_id = ensure_well(cursor, well_data, source_pdf)
    _debug("process_pdf well_id", well_id)
    has_stim = any(
        stim_data.get(k) is not None and str(stim_data.get(k)).strip()
        for k in ("date_stimulated", "stimulated_formation", "lbs_proppant")
    )
    if has_stim:
        insert_stimulation(cursor, well_id, stim_data, proppant)
    return True


if __name__ == "__main__":
    dry_run = "--dry-run" in sys.argv or "-n" in sys.argv
    script_dir = Path(__file__).resolve().parent
    script_dir.joinpath("temp").mkdir(parents=True, exist_ok=True)

    cfg = load_config()
    pdf_folder = Path(getattr(cfg, "PDF_FOLDER", "."))
    if not pdf_folder.is_absolute():
        pdf_folder = script_dir / pdf_folder
    pdfs = sorted(pdf_folder.glob("**/*.pdf"))
    if not pdfs:
        print(f"No PDFs found in {pdf_folder}", file=sys.stderr)
        sys.exit(1)

    if dry_run:
        for pdf_path in pdfs[:3]:
            text = get_pdf_text(str(pdf_path))
            print(f"\n--- {pdf_path.name} ---")
            print("Well:", parse_well_fields(text))
            print("Stimulation:", parse_stimulation_fields(text))
            print("Proppant:", parse_proppant_details(text))
        sys.exit(0)

    global LOG_FILE
    LOG_FILE = open(script_dir / "extract_wells.log", "a", encoding="utf-8")
    LOG_FILE.write(f"\n--- Run started {datetime.now().isoformat()} ---\n")
    LOG_FILE.flush()
    try:
        conn = mysql.connector.connect(**cfg.MYSQL_CONFIG)
        cursor = conn.cursor()
        for pdf_path in pdfs:
            try:
                source_pdf = pdf_path.name
                if well_exists_for_source_pdf(cursor, source_pdf):
                    print(f"Skip (already in DB): {source_pdf}")
                    continue
                process_pdf(str(pdf_path), cursor)
                print(f"OK: {source_pdf}")
                conn.commit()
            except Exception as e:
                print(f"Error {source_pdf}: {e}", file=sys.stderr)
        conn.commit()
        cursor.close()
        conn.close()
    finally:
        LOG_FILE.close()
        LOG_FILE = None
