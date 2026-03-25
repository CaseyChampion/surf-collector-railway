import logging
import math
import os
import sys
from datetime import date, datetime, timezone
from zoneinfo import ZoneInfo

import requests

PACIFIC_TZ = ZoneInfo("America/Los_Angeles")
UTC = timezone.utc

SUPABASE_URL = os.environ["SUPABASE_URL"].rstrip("/")
SUPABASE_KEY = os.environ["SUPABASE_KEY"]

LAT = float(os.getenv("SURF_LAT", "32.9595"))
LON = float(os.getenv("SURF_LON", "-117.2653"))
SPOT_NAME = os.getenv("SURF_SPOT_NAME", "8th-15th St Del Mar")
TIDE_STATION = os.getenv("TIDE_STATION", "9410230")
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT_SECONDS", "12"))
USER_AGENT = os.getenv("SURF_USER_AGENT", "DelMarSurfCollector/1.0 (casey prototype)")

BUOYS = [
    {"id": "46047", "key": "b47", "name": "Tanner Banks"},
    {"id": "46086", "key": "b86", "name": "San Clemente Basin"},
    {"id": "46225", "key": "b25", "name": "Torrey Pines Outer"},
]
COMPASS = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE", "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("surf_collector")

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": USER_AGENT})


def supabase_insert(table, row):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

    r = requests.post(url, headers=headers, json=row, timeout=10)

    print("STATUS:", r.status_code)
    print("RESPONSE:", r.text)

    r.raise_for_status()
    return r.json()

def deg2comp(direction_deg):
    if direction_deg is None:
        return None
    return COMPASS[round((float(direction_deg) % 360) / 22.5) % 16]


def m2ft(meters):
    return round(float(meters) * 3.28084, 1) if meters is not None else None


def ms2kts(ms):
    return round(float(ms) * 1.94384, 1) if ms is not None else None


def safe(val):
    return None if str(val).strip() in ("MM", "999", "9999", "99.0", "999.0", "9999.0") else val


def compass_to_deg(compass):
    if not compass:
        return None
    mapping = {
        "N": 0, "NNE": 22, "NE": 45, "ENE": 67, "E": 90, "ESE": 112, "SE": 135,
        "SSE": 157, "S": 180, "SSW": 202, "SW": 225, "WSW": 247, "W": 270,
        "WNW": 292, "NW": 315, "NNW": 337,
    }
    return mapping.get(str(compass).upper())


def fetch_buoy(buoy_id: str) -> dict:
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{buoy_id}.txt"
    response = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    lines = response.text.strip().splitlines()
    if len(lines) < 3:
        return {}
    columns = lines[2].split()

    def sf(index: int):
        if index >= len(columns):
            return None
        value = safe(columns[index])
        return round(float(value), 1) if value is not None else None

    wvht = sf(8)
    dpd = sf(9)
    mwd = sf(11)
    wtmp = sf(14)
    obs_time = None
    if len(columns) > 4:
        year, month, day, hour, minute = map(int, columns[:5])
        obs_dt = datetime(year, month, day, hour, minute, tzinfo=UTC)
        obs_time = obs_dt.isoformat()

    return {
        "wave_ft": m2ft(wvht),
        "period_s": dpd,
        "dir_deg": mwd,
        "dir_comp": deg2comp(mwd),
        "water_f": round(float(wtmp) * 9 / 5 + 32, 1) if wtmp is not None else None,
        "obs_time": obs_time,
    }


def fetch_marine() -> dict:
    url = (
        f"https://marine-api.open-meteo.com/v1/marine"
        f"?latitude={LAT}&longitude={LON}"
        f"&current=swell_wave_height,swell_wave_period,swell_wave_direction"
        f"&timezone=America/Los_Angeles"
    )
    response = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    current = response.json().get("current", {})
    return {
        "swell_ft": m2ft(current.get("swell_wave_height")),
        "swell_period_s": round(float(current["swell_wave_period"]), 1) if current.get("swell_wave_period") is not None else None,
        "swell_dir_deg": round(float(current["swell_wave_direction"])) if current.get("swell_wave_direction") is not None else None,
        "swell_dir_comp": deg2comp(current.get("swell_wave_direction")),
    }


def fetch_wind() -> dict:
    import re

    try:
        points = SESSION.get(
            f"https://api.weather.gov/points/{LAT},{LON}", timeout=REQUEST_TIMEOUT
        ).json()
        hourly_url = points["properties"]["forecastHourly"]
        periods = SESSION.get(hourly_url, timeout=REQUEST_TIMEOUT).json()["properties"]["periods"]
        now_local = datetime.now(PACIFIC_TZ)
        for period in periods:
            dt = datetime.fromisoformat(period["startTime"])
            if abs((dt.astimezone(PACIFIC_TZ) - now_local).total_seconds()) < 3600:
                nums = re.findall(r"\d+", period.get("windSpeed", "0"))
                mph = float(max(nums)) if nums else 0.0
                comp = period.get("windDirection", "N")
                return {
                    "wind_kts": round(mph * 0.868976, 1),
                    "wind_dir_comp": comp,
                    "wind_dir_deg": compass_to_deg(comp),
                    "wind_source": "weather.gov",
                }
    except Exception as exc:
        log.warning("weather.gov wind fetch failed: %s", exc)

    fallback_url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={LAT}&longitude={LON}"
        f"&current=wind_speed_10m,wind_direction_10m&wind_speed_unit=ms"
        f"&timezone=America/Los_Angeles"
    )
    response = SESSION.get(fallback_url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    current = response.json().get("current", {})
    direction = current.get("wind_direction_10m")
    return {
        "wind_kts": ms2kts(current.get("wind_speed_10m")),
        "wind_dir_deg": round(float(direction)) if direction is not None else None,
        "wind_dir_comp": deg2comp(direction),
        "wind_source": "open-meteo",
    }


def fetch_tides() -> dict:
    today = date.today().strftime("%Y%m%d")
    url = (
        "https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
        f"?begin_date={today}&end_date={today}"
        f"&station={TIDE_STATION}&product=predictions&datum=MLLW"
        "&time_zone=lst_ldt&interval=hilo&units=english&format=json"
    )
    response = SESSION.get(url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    predictions = response.json().get("predictions", [])
    if not predictions:
        return {}

    def parse_t(ts: str) -> datetime:
        return datetime.strptime(ts, "%Y-%m-%d %H:%M").replace(tzinfo=PACIFIC_TZ)

    hilo = sorted(
        [{"t": parse_t(p["t"]), "v": float(p["v"]), "type": p["type"]} for p in predictions],
        key=lambda x: x["t"],
    )

    now = datetime.now(PACIFIC_TZ).replace(second=0, microsecond=0)
    prev_tide = next((p for p in reversed(hilo) if p["t"] <= now), hilo[0])
    next_tide = next((p for p in hilo if p["t"] > now), hilo[-1])

    if prev_tide != next_tide:
        fraction = max(
            0,
            min(1, (now - prev_tide["t"]).total_seconds() / (next_tide["t"] - prev_tide["t"]).total_seconds()),
        )
        tide_ft = prev_tide["v"] + (next_tide["v"] - prev_tide["v"]) * (1 - math.cos(fraction * math.pi)) / 2
        phase = "rising" if next_tide["v"] > prev_tide["v"] else "falling"
    else:
        tide_ft, phase = prev_tide["v"], "unknown"

    future = [p for p in hilo if p["t"] > now]
    next_high = next((p for p in future if p["type"] == "H"), None)
    next_low = next((p for p in future if p["type"] == "L"), None)

    return {
        "tide_ft": round(tide_ft, 2),
        "tide_phase": phase,
        "next_high_ft": next_high["v"] if next_high else None,
        "next_high_time": next_high["t"].isoformat() if next_high else None,
        "next_low_ft": next_low["v"] if next_low else None,
        "next_low_time": next_low["t"].isoformat() if next_low else None,
    }


def build_row() -> tuple[dict, list[str]]:
    errors: list[str] = []
    row: dict = {}

    collectors = [
        ("marine", fetch_marine),
        ("wind", fetch_wind),
        ("tides", fetch_tides),
    ]

    try:
        row.update(fetch_marine())
    except Exception as exc:
        errors.append(f"marine: {exc}")
        log.exception("Marine fetch failed")

    for buoy in BUOYS:
        try:
            data = fetch_buoy(buoy["id"])
            key = buoy["key"]
            row.update({
                f"{key}_wave_ft": data.get("wave_ft"),
                f"{key}_period_s": data.get("period_s"),
                f"{key}_dir_deg": data.get("dir_deg"),
                f"{key}_dir_comp": data.get("dir_comp"),
                f"{key}_water_f": data.get("water_f"),
                f"{key}_obs_time": data.get("obs_time"),
            })
        except Exception as exc:
            errors.append(f"buoy_{buoy['id']}: {exc}")
            log.exception("Buoy fetch failed for %s", buoy["id"])

    for name, fn in collectors[1:]:
        try:
            row.update(fn())
        except Exception as exc:
            errors.append(f"{name}: {exc}")
            log.exception("%s fetch failed", name.capitalize())

    now_local = datetime.now(PACIFIC_TZ)
    now_utc = datetime.now(UTC)
    row.update({
        "spot": SPOT_NAME,
        "collected_at": now_utc.isoformat(),
        "collected_at_local": now_local.isoformat(),
        "hour_of_day": now_local.hour,
        "month": now_local.month,
        "day_of_week": now_local.weekday(),
    })
    if errors:
        row["partial_errors"] = "; ".join(errors)[:1000]
    return row, errors


def collect() -> int:
    log.info("Starting collection for %s", SPOT_NAME)
    row, errors = build_row()
    result = supabase_insert("surf_observations", row)
    row_id = result[0].get("id") if result else "?"
    print(f"✓ Collected {row['collected_at_local']} · id={row_id}")
    if errors:
        print(f"  Partial errors: {errors}")
        log.warning("Inserted partial row id=%s with errors=%s", row_id, errors)
    else:
        log.info("Inserted complete row id=%s", row_id)
    return 0


def main() -> int:
    try:
        return collect()
    except KeyError as exc:
        missing = exc.args[0]
        print(f"Missing required environment variable: {missing}", file=sys.stderr)
        return 2
    except requests.HTTPError as exc:
        body = exc.response.text[:500] if exc.response is not None else ""
        print(f"HTTP error: {exc} {body}", file=sys.stderr)
        log.exception("HTTP error during collection")
        return 1
    except Exception:
        log.exception("Unhandled collector failure")
        raise


if __name__ == "__main__":
    raise SystemExit(main())
