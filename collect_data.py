"""
Del Mar Surf Data Collector v4
Schema locked — DO NOT change field names or types.
 
Buoys:
  b54 = 46054 West Santa Barbara  (~200mi NW, NW swell 8-10hr lead)
  b11 = 46011 Santa Maria         (~170mi NW, NW swell 10-12hr lead)
  b47 = 46047 Tanner Banks        (~121mi W,  NW/SW intermediate, ~4-6hr lead)
  b86 = 46086 San Clemente Basin  (~60mi W,   SW/SSW indicator)
  b25 = 46225 Torrey Pines Outer  (~7mi W,    local nearshore)
 
Changes in v4:
  - b47 (46047 Tanner Banks) re-added — went adrift Mar 2025, redeployed Mar 2026
 
Fixes in v3:
  - b47 replaced with b54 (46054) and b11 (46011) — 46047 adrift since Mar 2025
  - collected_at_local now correctly applies PDT/PST offset
  - tide fetcher pulls today + tomorrow so next_high/next_low never null late in day
  - all numeric fields explicitly cast to float before insert
  - wind_dir_comp/deg populated even on calm/variable conditions
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
TIDE_STATION = "9410230"  # La Jolla
 
# Pacific timezone — update manually on DST change or use pytz if preferred
# PDT = UTC-7 (Mar-Nov), PST = UTC-8 (Nov-Mar)
LOCAL_TZ = timezone(timedelta(hours=-7))  # currently PDT
 
BUOYS = [
    {"id": "46054", "key": "b54", "name": "West Santa Barbara"},  # NW lead ~8-10hr
    {"id": "46011", "key": "b11", "name": "Santa Maria"},         # NW lead ~10-12hr
    {"id": "46047", "key": "b47", "name": "Tanner Banks"},        # NW/SW intermediate ~4-6hr lead
    {"id": "46086", "key": "b86", "name": "San Clemente Basin"},  # SW indicator
    {"id": "46225", "key": "b25", "name": "Torrey Pines Outer"},  # local nearshore
]
 
COMPASS = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
           "S","SSW","SW","WSW","W","WNW","NW","NNW"]
 
logging.basicConfig(
    filename="surf_collector.log",
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)
log = logging.getLogger(__name__)
 
# ── Type helpers ──────────────────────────────────────────────────────────────
 
def to_float(v):
    """Numeric value → Python float, or None. Never a string."""
    if v is None:
        return None
    try:
        f = float(v)
        return None if math.isnan(f) or math.isinf(f) else f
    except (ValueError, TypeError):
        return None
 
def to_str(v):
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
    return round(v * 3.28084, 2) if v is not None else None
 
def ms2kts(ms):
    v = to_float(ms)
    return round(v * 1.944, 2) if v is not None else None
 
def mph2kts(mph):
    v = to_float(mph)
    return round(v * 0.868976, 2) if v is not None else None
 
def is_missing(val):
    return str(val).strip() in ("MM","999","9999","99.0","999.0","9999.0","99","9999.9")
 
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
    if not r.ok:
        raise Exception(f"Supabase {r.status_code}: {r.text}")
    return r.json()
 
# ── Fetchers ──────────────────────────────────────────────────────────────────
 
def fetch_buoy(buoy_id):
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{buoy_id}.txt"
    text = requests.get(url, timeout=10).text.strip()
    lines = text.split("\n")
    if len(lines) < 3:
        return {}
 
    def sf(cols, i):
        if i >= len(cols) or is_missing(cols[i]):
            return None
        return to_float(cols[i])
 
    # Scan up to 10 recent rows to find best wave data
    # Buoys transmit every 10min but wave sensor doesn't fire every cycle
    data_lines = [l for l in lines[2:12] if not l.startswith("#")]
 
    # Pick row with valid WVHT (col 8) — fall back to first row for non-wave fields
    best_wave = None
    for line in data_lines:
        cols = line.split()
        if len(cols) > 8 and not is_missing(cols[8]):
            best_wave = cols
            break
 
    # Use first row for met/water temp fields (more frequently updated)
    first = data_lines[0].split() if data_lines else []
    wave_row = best_wave if best_wave else first
 
    wvht = sf(wave_row, 8)
    dpd  = sf(wave_row, 9)
    mwd  = sf(wave_row, 11)
    wtmp = sf(first, 14) if first else None
    c    = first  # use first row for timestamp
 
    try:
        yr  = int("20" + c[0]) if len(c[0]) == 2 else int(c[0])
        obs_dt = datetime(yr, int(c[1]), int(c[2]),
                          int(c[3]), int(c[4]), tzinfo=timezone.utc)
        obs_time_iso = obs_dt.isoformat()
    except Exception:
        obs_time_iso = None
 
    water_f = None
    if wtmp is not None:
        water_f = round(wtmp * 9/5 + 32, 2)
 
    return {
        "wave_ft":      m2ft(wvht),
        "period_s":     to_float(dpd),
        "dir_deg":      to_float(mwd),
        "dir_comp":     deg2comp(mwd),
        "water_f":      water_f,
        "obs_time_iso": obs_time_iso,
    }
 
 
def fetch_marine():
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
    import re
    try:
        headers = {"User-Agent": "DelMarSurfDashboard/3.0 surf@delmar.local"}
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
                comp = to_str(p.get("windDirection")) or "VRB"
                # VRB = variable, treat as no dominant direction
                dir_deg = compass_to_deg(comp) if comp != "VRB" else None
                return {
                    "wind_kts":      mph2kts(mph),
                    "wind_dir_comp": comp,
                    "wind_dir_deg":  dir_deg,
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
    Fetch today + tomorrow so next_high/next_low are never null late in day.
    Returns cosine-interpolated current height plus next hi/lo with ISO timestamps.
    """
    today    = date.today()
    tomorrow = today + timedelta(days=1)
    url = (
        f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
        f"?begin_date={today.strftime('%Y%m%d')}"
        f"&end_date={tomorrow.strftime('%Y%m%d')}"
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
        if nxt["v"] > prev["v"]:
            phase = "rising"
        elif nxt["v"] < prev["v"]:
            phase = "falling"
        else:
            phase = "at_peak"
    else:
        phase = "at_high" if prev["type"] == "H" else "at_low"
 
    future    = [p for p in hilo if p["t"] > now]
    next_high = next((p for p in future if p["type"] == "H"), None)
    next_low  = next((p for p in future if p["type"] == "L"), None)
 
    def tide_iso(p):
        if p is None:
            return None
        return p["t"].replace(tzinfo=LOCAL_TZ).isoformat()
 
    return {
        "tide_ft":        to_float(round(v, 2)),
        "tide_phase":     to_str(phase),
        "next_high_ft":   to_float(next_high["v"]) if next_high else None,
        "next_high_time": tide_iso(next_high),
        "next_low_ft":    to_float(next_low["v"]) if next_low else None,
        "next_low_time":  tide_iso(next_low),
    }
 
# ── Build row ─────────────────────────────────────────────────────────────────
 
def build_row(marine, buoy_data, wind, tides, now_utc, now_local):
    row = {
        # Timestamps
        "collected_at":       now_utc.isoformat(),
        "collected_at_local": now_local.isoformat(),
 
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
 
        # ML time features
        "hour_of_day": now_local.hour,
        "month":       now_local.month,
        "day_of_week": now_local.weekday(),
 
        # Metadata
        "spot": "8th-15th St Del Mar",
    }
 
    # Buoys — loops over BUOYS list, so b47 is picked up automatically
    for b in BUOYS:
        k = b["key"]
        d = buoy_data.get(k, {})
        row[f"{k}_wave_ft"]  = d.get("wave_ft")
        row[f"{k}_period_s"] = d.get("period_s")
        row[f"{k}_dir_deg"]  = d.get("dir_deg")
        row[f"{k}_dir_comp"] = d.get("dir_comp")
        row[f"{k}_water_f"]  = d.get("water_f")
        row[f"{k}_obs_time"] = d.get("obs_time_iso")
 
    return row
 
# ── Main ──────────────────────────────────────────────────────────────────────
 
def collect():
    now_utc   = datetime.now(timezone.utc)
    now_local = now_utc.replace(tzinfo=timezone.utc).astimezone(LOCAL_TZ)
    log.info(f"Collection started {now_utc.isoformat()}")
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
            log.info(f"Buoy {b['id']} ok: {buoy_data[b['key']]}")
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
 
    row = build_row(marine, buoy_data, wind, tides, now_utc, now_local)
 
    try:
        result = supabase_insert("surf_observations", row)
        row_id = result[0]["id"] if result else "?"
        log.info(f"Inserted id={row_id} errors={errors or 'none'}")
        print(f"✓ {now_local.strftime('%H:%M')} PDT · id={row_id} · errors={errors or 'none'}")
    except Exception as e:
        log.error(f"Insert failed: {e}")
        print(f"✗ Insert failed: {e}")
        raise
 
if __name__ == "__main__":
    collect()
