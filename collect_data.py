"""
Del Mar Surf Data Collector
Stable schema v2 — DO NOT change field names or types.

Runs every 30 minutes via GitHub Actions or Windows Task Scheduler.
Uses plain requests — no supabase SDK, no C++ build tools.

Schema contract:
  - All numeric fields: float or None (never strings)
  - All timestamps: ISO 8601 string via datetime.isoformat()
  - All text fields: string or None
  - spot: always "8th-15th St Del Mar"
"""

import requests
import math
import logging
from datetime import date, datetime, timezone, timedelta

# ── Config ────────────────────────────────────────────────────────────────────
SUPABASE_URL = "https://ipmppcdkrdjzsnxaehsm.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImlwbXBwY2RrcmRqenNueGFlaHNtIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzQ0NTE2MTYsImV4cCI6MjA5MDAyNzYxNn0.kKr_zA1PY4ODPWDaPlUEt7zRc7-ZCXsNHOkre5b3Fks"

LAT  = 32.9595
LON  = -117.2653
TIDE_STATION = "9410230"
LOCAL_TZ_OFFSET = -7  # PDT (UTC-7). Change to -8 in winter (PST)

BUOYS = [
    {"id": "46047", "key": "b47"},  # Tanner Banks      ~130mi offshore
    {"id": "46086", "key": "b86"},  # San Clemente       ~60mi offshore
    {"id": "46225", "key": "b25"},  # Torrey Pines Outer ~7mi offshore
]

COMPASS = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
           "S","SSW","SW","WSW","W","WNW","NW","NNW"]

logging.basicConfig(
    filename="surf_collector.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)

# ── Type helpers — these enforce the schema contract ─────────────────────────

def to_float(v):
    """Any numeric-ish value → float, or None. Never returns a string."""
    if v is None:
        return None
    try:
        return float(v)
    except (ValueError, TypeError):
        return None

def to_str(v):
    """Any value → stripped string, or None."""
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None

def deg2comp(d):
    if d is None:
        return None
    return COMPASS[round((float(d) % 360) / 22.5) % 16]

def compass_to_deg(c):
    mapping = {
        "N":0,"NNE":22,"NE":45,"ENE":67,"E":90,"ESE":112,
        "SE":135,"SSE":157,"S":180,"SSW":202,"SW":225,
        "WSW":247,"W":270,"WNW":292,"NW":315,"NNW":337
    }
    return to_float(mapping.get(str(c).upper())) if c else None

def m2ft(m):
    v = to_float(m)
    return round(v * 3.28084, 1) if v is not None else None

def ms2kts(ms):
    v = to_float(ms)
    return round(v * 1.944, 1) if v is not None else None

def mph2kts(mph):
    v = to_float(mph)
    return round(v * 0.868976, 1) if v is not None else None

def is_missing(val):
    """NDBC uses 'MM' and sentinel values for missing data."""
    return str(val).strip() in ("MM","999","9999","99.0","999.0","9999.0","99","9999.9")

def now_utc():
    return datetime.now(timezone.utc)

def now_local():
    tz = timezone(timedelta(hours=LOCAL_TZ_OFFSET))
    return datetime.now(tz)

# ── Supabase REST ─────────────────────────────────────────────────────────────

def supabase_insert(table, row):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }
    r = requests.post(url, headers=headers, json=row, timeout=15)
    r.raise_for_status()
    return r.json()

# ── Data fetchers ─────────────────────────────────────────────────────────────

def fetch_buoy(buoy_id):
    """
    Returns dict with consistent float/str/None types.
    obs_time is ISO 8601 UTC string.
    """
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{buoy_id}.txt"
    text = requests.get(url, timeout=10).text.strip()
    lines = text.split("\n")
    if len(lines) < 3:
        return {}

    # cols: YY MM DD hh mm WDIR WSPD GST WVHT DPD APD MWD PRES ATMP WTMP ...
    c = lines[2].split()

    def sf(i):
        if i >= len(c) or is_missing(c[i]):
            return None
        return to_float(c[i])

    wvht = sf(8)
    dpd  = sf(9)
    mwd  = sf(11)
    wtmp = sf(14)

    # Build ISO timestamp from buoy obs time (always UTC)
    try:
        yr   = int("20" + c[0]) if len(c[0]) == 2 else int(c[0])
        mo   = int(c[1]); dd = int(c[2])
        hh   = int(c[3]); mn = int(c[4])
        obs_dt = datetime(yr, mo, dd, hh, mn, tzinfo=timezone.utc)
        obs_time_iso = obs_dt.isoformat()
    except Exception:
        obs_time_iso = None

    return {
        "wave_ft":      m2ft(wvht),
        "period_s":     to_float(dpd),
        "dir_deg":      to_float(mwd),
        "dir_comp":     deg2comp(mwd),
        "water_f":      round(float(wtmp) * 9/5 + 32, 1) if wtmp is not None else None,
        "obs_time_iso": obs_time_iso,
    }


def fetch_marine():
    """Open-Meteo current swell — nearshore model."""
    url = (
        f"https://marine-api.open-meteo.com/v1/marine"
        f"?latitude={LAT}&longitude={LON}"
        f"&current=swell_wave_height,swell_wave_period,swell_wave_direction"
        f"&timezone=America/Los_Angeles"
    )
    cur = requests.get(url, timeout=10).json().get("current", {})
    return {
        "swell_ft":       m2ft(cur.get("swell_wave_height")),
        "swell_period_s": to_float(cur.get("swell_wave_period")),
        "swell_dir_deg":  to_float(cur.get("swell_wave_direction")),
        "swell_dir_comp": deg2comp(cur.get("swell_wave_direction")),
    }


def fetch_wind():
    """
    NWS hourly forecast (best for coastal accuracy).
    Falls back to Open-Meteo if NWS is unavailable.
    Returns wind_kts as float.
    """
    import re
    try:
        headers = {"User-Agent": "DelMarSurfDashboard/2.0 surf@delmar.local"}
        points  = requests.get(
            f"https://api.weather.gov/points/{LAT},{LON}",
            headers=headers, timeout=8
        ).json()
        hourly_url = points["properties"]["forecastHourly"]
        periods    = requests.get(hourly_url, headers=headers, timeout=10).json()["properties"]["periods"]
        now = datetime.now()
        for p in periods:
            dt = datetime.fromisoformat(p["startTime"][:19])
            if abs((dt - now).total_seconds()) < 3600:
                nums = re.findall(r"\d+", p.get("windSpeed", "0"))
                mph  = float(max(nums)) if nums else 0.0
                comp = to_str(p.get("windDirection", "N"))
                return {
                    "wind_kts":      mph2kts(mph),
                    "wind_dir_comp": comp,
                    "wind_dir_deg":  compass_to_deg(comp),
                    "wind_source":   "NWS",
                }
    except Exception as e:
        log.warning(f"NWS wind failed, falling back to Open-Meteo: {e}")

    cur = requests.get(
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={LAT}&longitude={LON}"
        f"&current=wind_speed_10m,wind_direction_10m"
        f"&timezone=America/Los_Angeles",
        timeout=8
    ).json().get("current", {})
    dir_deg = to_float(cur.get("wind_direction_10m"))
    return {
        "wind_kts":      ms2kts(cur.get("wind_speed_10m")),
        "wind_dir_comp": deg2comp(dir_deg),
        "wind_dir_deg":  dir_deg,
        "wind_source":   "Open-Meteo",
    }


def fetch_tides():
    """
    NOAA predicted hi/lo tides.
    Interpolates current tide height via cosine curve.
    next_high_time / next_low_time are ISO 8601 local strings.
    """
    today = date.today().strftime("%Y%m%d")
    url   = (
        f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
        f"?begin_date={today}&end_date={today}"
        f"&station={TIDE_STATION}&product=predictions&datum=MLLW"
        f"&time_zone=lst_ldt&interval=hilo&units=english&format=json"
    )
    predictions = requests.get(url, timeout=10).json().get("predictions", [])
    if not predictions:
        return {}

    def parse_t(t):
        return datetime.strptime(t, "%Y-%m-%d %H:%M")

    hilo = sorted(
        [{"t": parse_t(p["t"]), "v": float(p["v"]), "type": p["type"]}
         for p in predictions],
        key=lambda x: x["t"]
    )

    now  = datetime.now().replace(second=0, microsecond=0)
    prev = next((p for p in reversed(hilo) if p["t"] <= now), hilo[0])
    nxt  = next((p for p in hilo if p["t"] > now), hilo[-1])

    if prev != nxt:
        total   = (nxt["t"] - prev["t"]).total_seconds()
        elapsed = (now - prev["t"]).total_seconds()
        frac    = max(0.0, min(1.0, elapsed / total))
        v       = prev["v"] + (nxt["v"] - prev["v"]) * (1 - math.cos(frac * math.pi)) / 2
        phase   = "rising" if nxt["v"] > prev["v"] else "falling"
    else:
        v, phase = prev["v"], "unknown"

    future    = [p for p in hilo if p["t"] > now]
    next_high = next((p for p in future if p["type"] == "H"), None)
    next_low  = next((p for p in future if p["type"] == "L"), None)

    def tide_iso(p):
        if p is None:
            return None
        # Attach local tz offset so it's valid ISO 8601
        tz = timezone(timedelta(hours=LOCAL_TZ_OFFSET))
        return p["t"].replace(tzinfo=tz).isoformat()

    return {
        "tide_ft":        to_float(round(v, 2)),
        "tide_phase":     to_str(phase),
        "next_high_ft":   to_float(next_high["v"]) if next_high else None,
        "next_high_time": tide_iso(next_high),
        "next_low_ft":    to_float(next_low["v"]) if next_low else None,
        "next_low_time":  tide_iso(next_low),
    }

# ── Build row with locked schema ──────────────────────────────────────────────

def build_row(marine, buoy_data, wind, tides, now_u, now_l):
    """
    Assemble the final row. All fields are explicitly typed here.
    This is the single source of truth for the schema.
    """
    row = {
        # Timestamps — ISO 8601
        "collected_at":       now_u.isoformat(),
        "collected_at_local": now_l.isoformat(),

        # Open-Meteo swell
        "swell_ft":       marine.get("swell_ft"),
        "swell_period_s": marine.get("swell_period_s"),
        "swell_dir_deg":  marine.get("swell_dir_deg"),
        "swell_dir_comp": marine.get("swell_dir_comp"),

        # Wind
        "wind_kts":      wind.get("wind_kts"),
        "wind_dir_deg":  wind.get("wind_dir_deg"),
        "wind_dir_comp": wind.get("wind_dir_comp"),
        "wind_source":   wind.get("wind_source"),

        # Tides
        "tide_ft":        tides.get("tide_ft"),
        "tide_phase":     tides.get("tide_phase"),
        "next_high_ft":   tides.get("next_high_ft"),
        "next_high_time": tides.get("next_high_time"),
        "next_low_ft":    tides.get("next_low_ft"),
        "next_low_time":  tides.get("next_low_time"),

        # Time features for ML
        "hour_of_day": now_l.hour,
        "month":       now_l.month,
        "day_of_week": now_l.weekday(),

        # Metadata
        "spot": "8th-15th St Del Mar",
    }

    # Buoys — each key prefixed by buoy shortcode
    for b in BUOYS:
        k = b["key"]
        d = buoy_data.get(k, {})
        row[f"{k}_wave_ft"]  = d.get("wave_ft")
        row[f"{k}_period_s"] = d.get("period_s")
        row[f"{k}_dir_deg"]  = d.get("dir_deg")
        row[f"{k}_dir_comp"] = d.get("dir_comp")
        row[f"{k}_water_f"]  = d.get("water_f")
        row[f"{k}_obs_time"] = d.get("obs_time_iso")  # ISO 8601 UTC

    return row

# ── Main ──────────────────────────────────────────────────────────────────────

def collect():
    now_u = now_utc()
    now_l = now_local()
    log.info(f"Collection started {now_u.isoformat()}")
    errors = []

    marine = {}
    try:
        marine = fetch_marine()
    except Exception as e:
        errors.append(f"marine: {e}"); log.error(f"Marine: {e}")

    buoy_data = {}
    for b in BUOYS:
        try:
            buoy_data[b["key"]] = fetch_buoy(b["id"])
        except Exception as e:
            errors.append(f"buoy_{b['id']}: {e}"); log.error(f"Buoy {b['id']}: {e}")

    wind = {}
    try:
        wind = fetch_wind()
    except Exception as e:
        errors.append(f"wind: {e}"); log.error(f"Wind: {e}")

    tides = {}
    try:
        tides = fetch_tides()
    except Exception as e:
        errors.append(f"tides: {e}"); log.error(f"Tides: {e}")

    row = build_row(marine, buoy_data, wind, tides, now_u, now_l)

    try:
        result = supabase_insert("surf_observations", row)
        row_id = result[0]["id"] if result else "?"
        log.info(f"Inserted id={row_id} errors={errors or 'none'}")
        print(f"✓ {now_l.strftime('%H:%M')} local · id={row_id} · errors={errors or 'none'}")
    except Exception as e:
        log.error(f"Insert failed: {e}")
        print(f"✗ Insert failed: {e}")
        raise

if __name__ == "__main__":
    collect()
