"""
Base Assignment System with ADSB.lol Integration
===============================================
Automatically assigns aircraft to bases based on ADSB.lol position data.
Generates JSON for the dashboard's "Bases" tab.

Notes:
- ADSB.lol tracks aircraft by ICAO 24-bit hex (aka "hex"/"icao24"), not tail number.
- This script uses a tail->ICAO mapping you maintain in AIRCRAFT.
- If you already have another source for tail<->icao mapping, swap out AIRCRAFT loading.

Requires:
  pip install requests
"""

import json
import math
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, Optional, Tuple

import requests

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

# Where you want outputs written (same folder you were using for SkyRouter/OneDrive)
# Example Windows OneDrive path:
# ONEDRIVE_FOLDER = r"C:\Users\YourUser\OneDrive\407-Fleet-Tracker"
ONEDRIVE_FOLDER = r"."  # <- change me

OUTPUT_FOLDER = "data"
BASE_ASSIGNMENTS_FILE = "base_assignments.json"

# ADSB.lol settings
ADSBLOL_BASE_URL = "https://api.adsb.lol"  # base host
REQUEST_TIMEOUT_SEC = 15
USER_AGENT = "407-Fleet-Tracker/1.0 (base-assignments)"

# Tail -> ICAO hex mapping
# ICAO should be a 6-hex-char string, case-insensitive (e.g., "A1B2C3").
# You can also move this into a json file if you prefer.
AIRCRAFT: Dict[str, Dict[str, str]] = {
    # "N291HC": {"icao": "A1B2C3"},
    # "N271HC": {"icao": "D4E5F6"},
}

# Define your bases with coordinates and radius
BASES = {
    "LOGAN": {"name": "Logan", "lat": 41.7912, "lon": -111.8522, "radius_miles": 5},
    "MCKAY": {"name": "McKay", "lat": 41.2545, "lon": -112.0126, "radius_miles": 5},
    "IMED": {"name": "IMed", "lat": 40.2338, "lon": -111.6585, "radius_miles": 5},
    "PROVO": {"name": "Provo", "lat": 40.2192, "lon": -111.7233, "radius_miles": 5},
    "ROOSEVELT": {"name": "Roosevelt", "lat": 40.2765, "lon": -110.0518, "radius_miles": 5},
    "CEDAR_CITY": {"name": "Cedar City", "lat": 37.7010, "lon": -113.0989, "radius_miles": 5},
    "ST_GEORGE": {"name": "St George", "lat": 37.0365, "lon": -113.5101, "radius_miles": 5},
    "KSLC": {"name": "KSLC", "lat": 40.7884, "lon": -111.9778, "radius_miles": 10},
}

# ── HELPERS ───────────────────────────────────────────────────────────────────


def haversine_distance(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Calculate distance in miles between two lat/lon points."""
    R = 3959.0  # Earth radius in miles

    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)

    a = (
        math.sin(delta_lat / 2) ** 2
        + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon / 2) ** 2
    )
    c = 2 * math.asin(math.sqrt(a))
    return R * c


def load_previous_assignments(assignments_path: Path) -> Dict[str, Any]:
    """Load previous base assignments (for last-known-base fallback)."""
    if not assignments_path.exists():
        return {}

    try:
        with open(assignments_path, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("assignments", {})
    except Exception:
        return {}


# ── ADSB.lol INTEGRATION ──────────────────────────────────────────────────────
# ADSB.lol APIs can vary slightly by endpoint.
# This implementation tries common patterns and normalizes results to:
#   {"latitude": float, "longitude": float, "seen": seconds_ago, "altitude": ..., "ground_speed": ...}
#
# If your ADSB.lol response shape differs, adjust `normalize_adsblol_aircraft()`.


def _requests_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        }
    )
    return s


def fetch_adsblol_by_icao(session: requests.Session, icao_hex: str) -> Optional[Dict[str, Any]]:
    """
    Fetch current state for one aircraft by ICAO hex.

    Tries a couple of likely endpoints; keep the first that works.
    """
    icao_hex = icao_hex.strip().lower()

    candidate_urls = [
        # Common "v2/icao/<hex>" style (seen on ADS-B aggregator APIs)
        f"{ADSBLOL_BASE_URL}/v2/icao/{icao_hex}",
        # Alternate patterns some services use
        f"{ADSBLOL_BASE_URL}/api/v2/icao/{icao_hex}",
        f"{ADSBLOL_BASE_URL}/api/icao/{icao_hex}",
    ]

    last_err = None
    for url in candidate_urls:
        try:
            resp = session.get(url, timeout=REQUEST_TIMEOUT_SEC)
            if resp.status_code == 404:
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            last_err = e
            continue

    # If all candidates fail, return None (caller logs)
    return None


def normalize_adsblol_aircraft(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize ADSB.lol payload to a simple dict with lat/lon.

    Supported shapes (examples):
      A) {"aircraft":[{"hex":"...","lat":..,"lon":..,"seen":.., ...}], ...}
      B) {"ac":[{"hex":"...","lat":..,"lon":.., ...}], ...}
      C) {"lat":..,"lon":.., ...}  # already single-aircraft
      D) {"aircraft":{"lat":..,"lon":..}}  # nested

    Returns None if lat/lon can't be found.
    """
    if not isinstance(payload, dict):
        return None

    # Case C: direct
    if "lat" in payload and "lon" in payload:
        return {
            "latitude": payload.get("lat"),
            "longitude": payload.get("lon"),
            "seen": payload.get("seen"),
            "altitude": payload.get("alt_baro") or payload.get("altitude"),
            "ground_speed": payload.get("gs") or payload.get("ground_speed"),
            "track": payload.get("track"),
        }

    # Case D: nested single
    if "aircraft" in payload and isinstance(payload["aircraft"], dict):
        ac = payload["aircraft"]
        if "lat" in ac and "lon" in ac:
            return {
                "latitude": ac.get("lat"),
                "longitude": ac.get("lon"),
                "seen": ac.get("seen"),
                "altitude": ac.get("alt_baro") or ac.get("altitude"),
                "ground_speed": ac.get("gs") or ac.get("ground_speed"),
                "track": ac.get("track"),
            }

    # Case A/B: list under aircraft / ac
    for key in ("aircraft", "ac"):
        if key in payload and isinstance(payload[key], list) and payload[key]:
            ac0 = payload[key][0]
            if isinstance(ac0, dict) and "lat" in ac0 and "lon" in ac0:
                return {
                    "latitude": ac0.get("lat"),
                    "longitude": ac0.get("lon"),
                    "seen": ac0.get("seen"),
                    "altitude": ac0.get("alt_baro") or ac0.get("altitude"),
                    "ground_speed": ac0.get("gs") or ac0.get("ground_speed"),
                    "track": ac0.get("track"),
                }

    return None


def load_adsblol_status() -> Dict[str, Dict[str, Any]]:
    """
    Build a SkyRouter-like dict keyed by tail number:
      {
        "N291HC": {"latitude":..., "longitude":..., "seen":..., ...},
        ...
      }
    """
    status: Dict[str, Dict[str, Any]] = {}
    session = _requests_session()

    for tail, info in AIRCRAFT.items():
        icao_hex = (info.get("icao") or "").strip()
        if not icao_hex:
            continue

        payload = fetch_adsblol_by_icao(session, icao_hex)
        if not payload:
            continue

        norm = normalize_adsblol_aircraft(payload)
        if not norm:
            continue

        status[tail] = norm

    return status


# ── BASE ASSIGNMENT LOGIC ──────────────────────────────────────────────────────


def find_base_for_aircraft(aircraft_data: Dict[str, Any]) -> Tuple[Optional[str], Optional[float], bool]:
    """
    Find which base an aircraft is at based on position.

    Returns: (base_id, distance, at_base) or (None, None, False)
    """
    if aircraft_data.get("latitude") is None or aircraft_data.get("longitude") is None:
        return None, None, False

    lat = float(aircraft_data["latitude"])
    lon = float(aircraft_data["longitude"])

    closest_base = None
    closest_distance = float("inf")

    for base_id, base_info in BASES.items():
        distance = haversine_distance(lat, lon, base_info["lat"], base_info["lon"])
        if distance < closest_distance:
            closest_distance = distance
            closest_base = base_id

    if closest_base and closest_distance <= BASES[closest_base]["radius_miles"]:
        return closest_base, closest_distance, True

    return closest_base, closest_distance, False


def assign_aircraft_to_bases(
    aircraft_status: Dict[str, Dict[str, Any]],
    previous_assignments: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Assign aircraft to bases based on current position.

    Returns dict:
    {
      "LOGAN": {"aircraft":[{...}], "status":"occupied"},
      ...
      "unassigned":[{...}]
    }
    """
    assignments: Dict[str, Any] = {
        base_id: {"aircraft": [], "status": "available"} for base_id in BASES.keys()
    }
    assignments["unassigned"] = []

    for tail, aircraft_data in aircraft_status.items():
        base_id, distance, at_base = find_base_for_aircraft(aircraft_data)

        if at_base and base_id:
            assignments[base_id]["aircraft"].append(
                {
                    "tail": tail,
                    # ADSB doesn't know your maintenance hours; keep field for dashboard compatibility
                    "hours": None,
                    "distance": round(float(distance), 2) if distance is not None else None,
                    "at_base": True,
                    "seen_seconds_ago": aircraft_data.get("seen"),
                    "altitude": aircraft_data.get("altitude"),
                    "ground_speed": aircraft_data.get("ground_speed"),
                    "track": aircraft_data.get("track"),
                }
            )
            assignments[base_id]["status"] = "occupied"
        else:
            assignments["unassigned"].append(
                {
                    "tail": tail,
                    "hours": None,
                    "last_known_base": previous_assignments.get(tail, {}).get("base"),
                    "distance_from_closest": round(float(distance), 2) if distance is not None else None,
                    "closest_base": BASES[base_id]["name"] if base_id else None,
                    "at_base": False,
                    "seen_seconds_ago": aircraft_data.get("seen"),
                    "altitude": aircraft_data.get("altitude"),
                    "ground_speed": aircraft_data.get("ground_speed"),
                    "track": aircraft_data.get("track"),
                }
            )

    return assignments


# ── MAIN ──────────────────────────────────────────────────────────────────────


def generate_base_assignments() -> bool:
    """Main function to generate base assignments."""
    base_folder = Path(ONEDRIVE_FOLDER)
    assignments_path = base_folder / BASE_ASSIGNMENTS_FILE
    log_path = base_folder / "base_assignment_log.txt"

    def log(msg: str) -> None:
        timestamp = datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S %z")
        line = f"[{timestamp}] {msg}"
        print(line)
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(line + "\n")
        except Exception:
            pass

    try:
        log("Base assignment generation started (ADSB.lol)")

        if not AIRCRAFT:
            log("ERROR: AIRCRAFT mapping is empty. Add tail->icao entries in AIRCRAFT.")
            return False

        log("Loading ADSB.lol positions...")
        aircraft_status = load_adsblol_status()

        if not aircraft_status:
            log("WARNING: No ADSB.lol data available (check ICAO hex values and endpoint).")
            return False

        log(f"Loaded position data for {len(aircraft_status)} aircraft")

        previous_assignments = load_previous_assignments(assignments_path)

        log("Calculating base assignments...")
        assignments = assign_aircraft_to_bases(aircraft_status, previous_assignments)

        output_data = {
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "source": "adsb.lol",
            "bases": BASES,
            "assignments": assignments,
            "summary": {
                "total_aircraft": len(aircraft_status),
                "at_bases": sum(
                    len(a["aircraft"]) for k, a in assignments.items() if k != "unassigned"
                ),
                "away_from_base": len(assignments["unassigned"]),
            },
        }

        with open(assignments_path, "w", encoding="utf-8") as f:
            json.dump(output_data, f, indent=2)

        log(f"Base assignments saved to {assignments_path}")

        for base_id, data in assignments.items():
            if base_id == "unassigned":
                log(f"  Unassigned: {len(data)} aircraft")
            else:
                count = len(data["aircraft"])
                log(f"  {BASES[base_id]['name']}: {count} aircraft")

        log("Base assignment generation completed successfully")
        return True

    except Exception as e:
        log(f"ERROR: {e}")
        import traceback

        log(traceback.format_exc())
        return False


if __name__ == "__main__":
    generate_base_assignments()