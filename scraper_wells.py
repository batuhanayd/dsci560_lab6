import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from urllib.parse import urljoin

import mysql.connector
import requests
from bs4 import BeautifulSoup

DEBUG = True
LOG_FILE = None

BASE = "https://www.drillingedge.com"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

API_NUMBER_MAX = 32
VARCHAR_64 = 64
VARCHAR_128 = 128
VARCHAR_255 = 255


def _debug(label: str, data, max_chars: int = 80) -> None:
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


def _log_error(msg: str) -> None:
    line = f"{datetime.now().isoformat()} [ERROR] {msg}\n"
    if LOG_FILE is not None:
        try:
            LOG_FILE.write(line)
            LOG_FILE.flush()
        except Exception:
            pass
    print(line.rstrip(), file=sys.stderr)


def load_config():
    script_dir = Path(__file__).resolve().parent
    config_path = script_dir / "config.py"
    _debug("load_config config_path", str(config_path))
    if not config_path.exists():
        raise FileNotFoundError("Create config.py with MYSQL_CONFIG.")
    import importlib.util
    spec = importlib.util.spec_from_file_location("config", config_path)
    cfg = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cfg)
    _debug("load_config MYSQL_CONFIG host", cfg.MYSQL_CONFIG.get("host") if getattr(cfg, "MYSQL_CONFIG", None) else None)
    return cfg


def _norm_api(v) -> Optional[str]:
    if v is None or (isinstance(v, float) and str(v) == "nan"):
        return None
    s = (v if isinstance(v, str) else str(v)).strip()
    if not s or s.upper() == "NULL":
        return None
    return s.rstrip("-00") if s.endswith("-00") else s


def _trunc(s: Optional[str], max_len: int) -> Optional[str]:
    if s is None:
        return None
    s = (s if isinstance(s, str) else str(s)).strip()
    if not s or s.upper() == "NULL" or max_len <= 0:
        return None
    return s[:max_len] if len(s) > max_len else s


def load_wells_from_db(cursor) -> List[dict]:
    cursor.execute("SELECT well_id, well_name, api_number FROM wells")
    rows = cursor.fetchall()
    out = []
    for well_id, well_name, api_number in rows:
        name = (well_name or "").strip() if well_name else None
        api = _norm_api(api_number)
        out.append({"well_id": well_id, "name": name, "api": api})
    return out


def search_well_url(session: requests.Session, well_name: Optional[str], api: Optional[str]) -> Optional[str]:
    if api:
        api = api.strip()
        params = {"type": "wells", "operator_name": "", "well_name": "", "api_no": api}
    elif well_name:
        well_name = well_name.lower().strip().replace(" ", "-").replace("&", "and")
        params = {"type": "wells", "operator_name": "", "well_name": well_name}
    else:
        return None

    try:
        r = session.get(f"{BASE}/search", params=params, timeout=15)
        r.raise_for_status()
    except requests.RequestException as e:
        _debug("search_well_url request error", str(e))
        return None

    soup = BeautifulSoup(r.text, "html.parser")
    for a in soup.select('a[href*="/wells/"]'):
        href = a.get("href", "")
        full = urljoin(BASE, href)
        if api:
            if api in full:
                return full
        else:
            if well_name in full:
                return full
            return full
    return None


def _parse_number(s: Optional[str]) -> Optional[int]:
    if not s or "members only" in (s or "").lower():
        return None
    m = re.match(r"\s*([\d,.]+)\s*([kKmM]?)\s*$", (s or "").strip())
    if not m:
        return None
    val = float(m.group(1).replace(",", ""))
    suf = (m.group(2) or "").lower()
    if suf == "k":
        val *= 1000
    elif suf == "m":
        val *= 1000000
    return int(val)


def _text(elem) -> Optional[str]:
    if elem is None:
        return None
    t = elem.get_text(strip=True)
    return t if t and "members only" not in t.lower() else None


def scrape_well_detail(session: requests.Session, url: str) -> dict:
    out = {
        "api_no": None, "well_name": None, "operator": None, "county": None,
        "well_status": None, "well_type": None, "closest_city": None,
        "oil_bbl": None, "gas_mcf": None, "production_dates_on_file": None,
    }
    try:
        r = session.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
    except requests.RequestException as e:
        _debug("scrape_well_detail request", str(e))
        return out

    soup = BeautifulSoup(r.text, "html.parser")

    meta = soup.select_one("section.meta_info")
    if meta:
        for div in meta.select("div"):
            label = div.get_text(" ", strip=True).split(":")[0].strip()
            span = div.select_one("span.detail_point")
            if not span:
                continue
            val = (span.find("a") or span).get_text(strip=True)
            if "Well Name" in label or label == "Well Name":
                out["well_name"] = val
            elif "API #" in label or label == "API #":
                out["api_no"] = val
            elif "Operator" in label or label == "Operator":
                out["operator"] = val
            elif "County" in label or label == "County":
                out["county"] = val
            elif "Production Dates on File" in label:
                out["production_dates_on_file"] = val

    table = soup.select_one("table.skinny")
    if table:
        for tr in table.select("tr"):
            for th in tr.select("th"):
                key = th.get_text(strip=True)
                td = th.find_next_sibling("td")
                val = _text(td) if td else None
                if key == "Well Status":
                    out["well_status"] = val
                elif key == "Well Type":
                    out["well_type"] = val
                elif key == "Closest City":
                    out["closest_city"] = val
                elif key == "County" and out["county"] is None:
                    out["county"] = val
                elif key == "API No." and out["api_no"] is None:
                    out["api_no"] = val
                elif key == "Well Name" and out["well_name"] is None:
                    out["well_name"] = val
                elif key == "Operator" and out["operator"] is None:
                    out["operator"] = val

    for p in soup.select("p.block_stat"):
        num_span = p.select_one("span.dropcap")
        if not num_span:
            continue
        num = _parse_number(num_span.get_text(strip=True))
        num_span.decompose()
        desc = p.get_text(" ", strip=True).lower()
        if "oil" in desc and ("barrel" in desc or "bbl" in desc):
            out["oil_bbl"] = num
        elif "gas" in desc and ("mcf" in desc or "mmcf" in desc):
            out["gas_mcf"] = num

    return out


def scraped_exists(cursor, well_id: Optional[int]) -> bool:
    if well_id is None:
        return False
    cursor.execute("SELECT 1 FROM scraped_wells WHERE well_id = %s LIMIT 1", (well_id,))
    return cursor.fetchone() is not None


def insert_scraped(cursor, well_id: Optional[int], data: dict) -> None:
    def v(k, max_len: int = 0):
        x = data.get(k)
        if x is None:
            return None
        if isinstance(x, int):
            return x
        s = (x if isinstance(x, str) else str(x)).strip()
        if not s or s.upper() == "NULL":
            return None
        return s[:max_len] if max_len and len(s) > max_len else s

    oil_bbl = data.get("oil_bbl") if isinstance(data.get("oil_bbl"), int) else None
    gas_mcf = data.get("gas_mcf") if isinstance(data.get("gas_mcf"), int) else None

    cursor.execute(
        """INSERT INTO scraped_wells (well_id, well_name, api_number, scraped_url, api_no,
           closest_city, county, gas_mcf, oil_bbl, operator, production_dates_on_file, well_status, well_type)
           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
        (
            well_id,
            v("well_name") or v("name"),
            _trunc(v("api_no") or v("api"), API_NUMBER_MAX),
            v("url"),
            _trunc(v("api_no"), API_NUMBER_MAX),
            _trunc(v("closest_city"), VARCHAR_128),
            v("county"),
            gas_mcf,
            oil_bbl,
            v("operator"),
            _trunc(v("production_dates_on_file"), VARCHAR_255),
            _trunc(v("well_status"), VARCHAR_64),
            _trunc(v("well_type"), VARCHAR_64),
        ),
    )


def main() -> None:
    global LOG_FILE
    dry_run = "--dry-run" in sys.argv or "-n" in sys.argv
    script_dir = Path(__file__).resolve().parent

    cfg = load_config()

    if dry_run:
        try:
            conn = mysql.connector.connect(**cfg.MYSQL_CONFIG)
            cursor = conn.cursor()
            wells = load_wells_from_db(cursor)
            for w in wells[:5]:
                print("  well_id=%s name=%r api=%r" % (w.get("well_id"), w.get("name"), w.get("api")))
            print("... and", max(0, len(wells) - 5), "more.")
            cursor.close()
            conn.close()
        except Exception as e:
            _log_error(str(e))
            sys.exit(1)
        sys.exit(0)

    LOG_FILE = open(script_dir / "scraper_wells.log", "a", encoding="utf-8")
    LOG_FILE.write("\n--- Run started " + datetime.now().isoformat() + " ---\n")
    LOG_FILE.flush()

    session = requests.Session()
    session.headers.update(HEADERS)
    try:
        session.get(BASE, timeout=10)
    except requests.RequestException as e:
        _log_error("Session init: " + str(e))
        LOG_FILE.close()
        LOG_FILE = None
        sys.exit(1)

    try:
        conn = mysql.connector.connect(**cfg.MYSQL_CONFIG)
        cursor = conn.cursor()
        wells = load_wells_from_db(cursor)
        _debug("load_wells_from_db count", len(wells))
        if not wells:
            print("No wells in DB.", file=sys.stderr)
            cursor.close()
            conn.close()
            return

        inserted = 0
        skipped = 0
        errors = 0
        for i, well in enumerate(wells):
            well_id = well.get("well_id")
            name = well.get("name")
            api = well.get("api")
            try:
                url = search_well_url(session, name, api)
                well["url"] = url
                if scraped_exists(cursor, well_id):
                    skipped += 1
                    _debug("skip existing well_id", well_id)
                    continue
                if url:
                    detail = scrape_well_detail(session, url)
                    well.update(detail)
                    well["well_name"] = well.get("well_name") or name
                    well["api_no"] = well.get("api_no") or api
                else:
                    well["well_name"] = name
                    well["api_no"] = api
                insert_scraped(cursor, well_id, well)
                inserted += 1
                conn.commit()
                disp = (well.get("well_name") or name or "")[:40]
                print(disp.ljust(40), "->", url or "NOT FOUND")
            except Exception as e:
                errors += 1
                _log_error("Well %s (well_id=%s): %s" % (i + 1, well_id, e))
                try:
                    conn.rollback()
                except Exception:
                    pass
        cursor.close()
        conn.close()
        print("Done. Inserted:", inserted, ", skipped (existing):", skipped, ", errors:", errors)
    finally:
        if LOG_FILE is not None:
            LOG_FILE.close()
            LOG_FILE = None


if __name__ == "__main__":
    main()
