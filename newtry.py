import os
import json
import time
from datetime import datetime
from urllib.parse import quote_plus

import pandas as pd
import requests
from sqlalchemy import create_engine, text

# ---------- CONFIG ----------
ROOT = os.path.dirname(os.path.dirname(__file__))
CONFIG_DIR = os.path.join(ROOT, "config")
CRED2_PATH = os.path.join(CONFIG_DIR, "credentials.json")

DB_CONFIG = {
    "user": "root",
    "password": "root",
    "host": "localhost",
    "port": 3306,
    "database": "weather_project"
}

# Put your two-month window here (YYYY-MM-DD)
START_DATE = "2025-09-01"
END_DATE   = "2025-09-30"

# All tourist places you asked for
CITIES = [
    "Manali,IN", "Shimla,IN", "Auli,IN", "Gulmarg,IN", "Leh,IN",
    "Udaipur,IN", "Mount Abu,IN", "Rishikesh,IN", "Nainital,IN", "Kutch,IN",
    "Ooty,IN", "Coorg,IN", "Munnar,IN", "Kodaikanal,IN", "Darjeeling,IN",
    "Mahabaleshwar,IN", "Gangtok,IN", "Shillong,IN", "Tawang,IN",
    "Kerala,IN", "Goa,IN", "Lonavala,IN", "Cherrapunji,IN", "Wayanad,IN",
    "Konkan,IN", "Mussoorie,IN", "Panchgani,IN"
]

# Sleep between city requests (seconds)
SLEEP_BETWEEN_CALLS = 1.0

# Cache folder
CACHE_DIR = os.path.join(os.path.dirname(__file__), "cache")
os.makedirs(CACHE_DIR, exist_ok=True)

VC_BASE = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"

# ---------- load VC key ----------
if not os.path.exists(CRED2_PATH):
    raise RuntimeError(f"Missing {CRED2_PATH} — add your credential2.json with your api_key")

with open(CRED2_PATH, "r", encoding="utf-8") as f:
    cfg2 = json.load(f)
VC_KEY = cfg2.get("api_key")
if not VC_KEY:
    raise RuntimeError("No api_key found in credential2.json")

# ---------- DB engine ----------
ENGINE_URL = f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
engine = create_engine(ENGINE_URL, pool_pre_ping=True)

# ---------- ensure table schema ----------
def ensure_weather_table():
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS weather_data (
            id BIGINT AUTO_INCREMENT PRIMARY KEY,
            name TEXT,
            datetime DATE,
            temp DOUBLE,
            tempmax DOUBLE,
            tempmin DOUBLE,
            feelslike DOUBLE,
            feelslikemax DOUBLE,
            feelslikemin DOUBLE,
            dew DOUBLE,
            humidity DOUBLE,
            precip BIGINT,
            precipprob BIGINT,
            precipcover BIGINT,
            preciptype VARCHAR(100),
            sealevelpressure DOUBLE,
            severerisk DOUBLE,
            snow BIGINT,
            snowdepth BIGINT,
            cloudcover DOUBLE,
            conditions TEXT,
            description TEXT,
            icon TEXT,
            stations TEXT,
            solarradiation DOUBLE,
            solarenergy DOUBLE,
            uvindex BIGINT,
            visibility DOUBLE,
            winddir DOUBLE,
            windgust DOUBLE,
            windspeed DOUBLE,
            sunrise TEXT,
            sunset TEXT,
            moonphase DOUBLE,
            source VARCHAR(80),
            fetched_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY city_date_unique (name, datetime)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """))

# ---------- helpers ----------
def cache_filename(city, start, end):
    # deterministic filename for the request
    key = f"{city}|{start}|{end}"
    fname = f"{abs(hash(key))}.json"
    return os.path.join(CACHE_DIR, fname)

def fetch_vc_json_single(city, start, end):
    params = {"unitGroup": "metric", "key": VC_KEY, "contentType": "json", "include": "days"}
    city_enc = quote_plus(city)
    url = f"{VC_BASE}/{city_enc}/{start}/{end}"
    try:
        r = requests.get(url, params=params, timeout=40)
    except Exception as e:
        print(f"[ERR] Network error for {city} {start}->{end}: {e}")
        return None
    print(f"   ▶ {city} {start}->{end} -> status {r.status_code}")
    if r.status_code != 200:
        print("   ⚠️ VC response snippet:", r.text[:300])
        return None
    try:
        j = r.json()
    except Exception as e:
        print("[ERR] JSON decode:", e)
        return None
    # cache
    try:
        with open(cache_filename(city, start, end), "w", encoding="utf-8") as f:
            json.dump(j, f)
    except Exception:
        pass
    return j

def normalize_day_to_row(city, day):
    import json  # local import is fine here
    get = lambda k, default=None: day.get(k, default)
    dt = get("datetime")
    try:
        dt_val = pd.to_datetime(dt).date()
    except Exception:
        dt_val = dt

    # helper to safely stringify lists (use JSON to preserve structure)
    def _maybe_json(x):
        if x is None:
            return None
        if isinstance(x, (list, dict)):
            try:
                return json.dumps(x, ensure_ascii=False)
            except Exception:
                return str(x)
        return x

    row = {
        "name": city,
        "datetime": dt_val,
        "temp": get("temp"),
        "tempmax": get("tempmax"),
        "tempmin": get("tempmin"),
        "feelslike": get("feelslike"),
        "feelslikemax": get("feelslikemax"),
        "feelslikemin": get("feelslikemin"),
        "dew": get("dew"),
        "humidity": get("humidity"),
        "precip": get("precip"),
        "precipprob": get("precipprob"),
        "precipcover": get("precipcover"),
        # preciptype may be list or string -> normalize to CSV or JSON
        "preciptype": _maybe_json(get("preciptype")),
        "sealevelpressure": get("sealevelpressure"),
        "severerisk": get("severerisk"),
        "snow": get("snow"),
        "snowdepth": get("snowdepth"),
        "cloudcover": get("cloudcover"),
        "conditions": get("conditions"),
        "description": get("description"),
        "icon": get("icon"),
        # stations is often a list -> JSON-stringify it
        "stations": _maybe_json(get("stations")),
        "solarradiation": get("solarradiation"),
        "solarenergy": get("solarenergy"),
        "uvindex": get("uvindex"),
        "visibility": get("visibility"),
        "winddir": get("winddir"),
        "windgust": get("windgust"),
        "windspeed": get("windspeed"),
        "sunrise": get("sunrise"),
        "sunset": get("sunset"),
        "moonphase": get("moonphase"),
        "source": "visualcrossing"
    }
    return row


def upsert_rows(rows):
    if not rows:
        return 0
    sql = """
    INSERT INTO weather_data
      (name, datetime, temp, tempmax, tempmin, feelslike, feelslikemax, feelslikemin,
       dew, humidity, precip, precipprob, precipcover, preciptype, sealevelpressure, severerisk,
       snow, snowdepth, cloudcover, conditions, description, icon, stations,
       solarradiation, solarenergy, uvindex, visibility, winddir, windgust, windspeed,
       sunrise, sunset, moonphase, source)
    VALUES
      (:name, :datetime, :temp, :tempmax, :tempmin, :feelslike, :feelslikemax, :feelslikemin,
       :dew, :humidity, :precip, :precipprob, :precipcover, :preciptype, :sealevelpressure, :severerisk,
       :snow, :snowdepth, :cloudcover, :conditions, :description, :icon, :stations,
       :solarradiation, :solarenergy, :uvindex, :visibility, :winddir, :windgust, :windspeed,
       :sunrise, :sunset, :moonphase, :source)
    ON DUPLICATE KEY UPDATE
      temp = VALUES(temp), tempmax = VALUES(tempmax), tempmin = VALUES(tempmin),
      feelslike = VALUES(feelslike), feelslikemax = VALUES(feelslikemax), feelslikemin = VALUES(feelslikemin),
      dew = VALUES(dew), humidity = VALUES(humidity), precip = VALUES(precip), precipprob = VALUES(precipprob),
      precipcover = VALUES(precipcover), preciptype = VALUES(preciptype), sealevelpressure = VALUES(sealevelpressure),
      severerisk = VALUES(severerisk), snow = VALUES(snow), snowdepth = VALUES(snowdepth),
      cloudcover = VALUES(cloudcover), conditions = VALUES(conditions), description = VALUES(description),
      icon = VALUES(icon), stations = VALUES(stations), solarradiation = VALUES(solarradiation),
      solarenergy = VALUES(solarenergy), uvindex = VALUES(uvindex), visibility = VALUES(visibility),
      winddir = VALUES(winddir), windgust = VALUES(windgust), windspeed = VALUES(windspeed),
      sunrise = VALUES(sunrise), sunset = VALUES(sunset), moonphase = VALUES(moonphase),
      source = VALUES(source), fetched_at = CURRENT_TIMESTAMP
    """
    with engine.begin() as conn:
        conn.execute(text(sql), rows)
    return len(rows)

# ---------- main workflow ----------
def main():
    print("Starting fetch_history_2months.py")
    ensure_weather_table()

    for city in CITIES:
        print(f"\n=== Fetching {city} for {START_DATE} -> {END_DATE} ===")
        payload = fetch_vc_json_single(city, START_DATE, END_DATE)
        if not payload or "days" not in payload:
            print(f"  [WARN] No data returned for {city} in range {START_DATE}->{END_DATE}")
            time.sleep(SLEEP_BETWEEN_CALLS)
            continue

        rows = []
        for day in payload["days"]:
            row = normalize_day_to_row(city, day)
            if row:
                rows.append(row)

        n = upsert_rows(rows)
        print(f"  ✅ Upserted {n} rows for {city}")
        time.sleep(SLEEP_BETWEEN_CALLS)

    print("\nAll cities processed. Raw JSON cache: " + CACHE_DIR)

if __name__ == "__main__":
    main()
