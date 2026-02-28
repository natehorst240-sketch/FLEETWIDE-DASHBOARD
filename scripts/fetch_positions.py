"""
fetch_positions.py — ADSB.lol Live Position Fetcher
====================================================
Reads aircraft tail numbers from each fleet's dashboard.json,
queries the ADSB.lol v2 API by registration, and writes a
positions_<fleet_id>.json file into data/.

Called by the GitHub Actions workflow before build_dashboards.py
so that positions are baked into the final dashboard.json output.

Usage:
    python scripts/fetch_positions.py
    python scripts/fetch_positions.py --config fleet_config.json --data data --dist dist/data

API used:
    GET https://api.adsb.lol/v2/registration/{registration}
    No API key required. Rate-limit: be polite (0.25s between calls).
"""

from __future__ import annotations

import argparse
import json
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


ADSB_BASE = "https://api.adsb.lol/v2/registration"
CALL_DELAY = 0.25   # seconds between API calls
TIMEOUT    = 10     # seconds per request


# ─── HELPERS ──────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fetch_registration(reg: str) -> Optional[dict]:
    """
    Query ADSB.lol for a single registration.
    Returns the first aircraft record dict, or None if not found / error.
    """
    url = f"{ADSB_BASE}/{reg.strip().upper()}"
    try:
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "User-Agent": "IHC-Fleet-Dashboard/1.0",
            }
        )
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            aircraft_list = data.get("ac", [])
            if aircraft_list:
                return aircraft_list[0]
            return None
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None   # aircraft not currently tracked — normal
        log(f"    HTTP {e.code} for {reg}")
        return None
    except Exception as e:
        log(f"    Error fetching {reg}: {e}")
        return None


def parse_ac(ac: dict, reg: str) -> dict:
    """
    Normalize a raw ADSB.lol aircraft record into our position schema.
    All fields are optional — None means not available.
    """
    alt = ac.get("alt_baro")
    if alt == "ground":
        alt_ft = 0
        airborne = False
    else:
        try:
            alt_ft = int(alt) if alt is not None else None
        except (ValueError, TypeError):
            alt_ft = None
        airborne = ac.get("airGround", "").lower() == "air" or (alt_ft is not None and alt_ft > 100)

    gs = ac.get("gs")
    try:
        gs = round(float(gs), 1) if gs is not None else None
    except (ValueError, TypeError):
        gs = None

    return {
        "registration": reg,
        "lat":          ac.get("lat"),
        "lon":          ac.get("lon"),
        "alt_baro_ft":  alt_ft,
        "ground_speed_kts": gs,
        "track_deg":    ac.get("track"),
        "squawk":       ac.get("squawk"),
        "flight":       ac.get("flight", "").strip() or None,
        "airborne":     airborne,
        "seen_secs_ago": ac.get("seen"),
        "rssi":         ac.get("rssi"),
        "emergency":    ac.get("emergency") or None,
        "last_updated": utcnow(),
    }


# ─── MAIN ─────────────────────────────────────────────────────────────

def fetch_fleet_positions(fleet_cfg: dict, data_root: Path, dist_root: Path) -> bool:
    """
    Fetch positions for all aircraft in a fleet.
    Reads tails from dist/data/<fleet_id>/dashboard.json (already built),
    falls back to data/<fleet_id>/Due-List_Latest_<fleet_id>.csv tail column
    if dashboard.json doesn't exist yet.
    Writes data/positions_<fleet_id>.json.
    """
    fleet_id   = fleet_cfg["id"]
    fleet_name = fleet_cfg.get("name", fleet_id)

    # ── Discover tails from dashboard.json ────────────────────────────
    dashboard_path = dist_root / fleet_id / "dashboard.json"
    tails: list[str] = []

    if dashboard_path.exists():
        try:
            with open(dashboard_path, encoding="utf-8") as f:
                dash = json.load(f)
            aircraft = dash.get("aircraft", {})
            if isinstance(aircraft, dict):
                tails = list(aircraft.keys())
            elif isinstance(aircraft, list):
                tails = [ac.get("tail") or ac.get("registration", "") for ac in aircraft]
                tails = [t for t in tails if t]
            log(f"  {fleet_name}: found {len(tails)} tails in dashboard.json")
        except Exception as e:
            log(f"  {fleet_name}: could not read dashboard.json — {e}")

    # ── Fallback: read tails from CSV directly ────────────────────────
    if not tails:
        for csv_name in [
            f"Due-List_Latest_{fleet_id}.csv",
            f"Due-List_Latest.csv",
        ]:
            csv_path = data_root / csv_name
            if not csv_path.exists():
                csv_path = data_root / fleet_id / csv_name
            if csv_path.exists():
                import csv as _csv
                seen = set()
                with open(csv_path, encoding="utf-8-sig", newline="") as f:
                    for row in _csv.reader(f):
                        if len(row) > 0 and row[0].strip() and row[0].strip() != "Registration":
                            t = row[0].strip()
                            if t not in seen:
                                seen.add(t)
                                tails.append(t)
                log(f"  {fleet_name}: found {len(tails)} tails from CSV {csv_name}")
                break

    if not tails:
        log(f"  {fleet_name}: no tails found, skipping")
        return False

    # ── Fetch each tail ────────────────────────────────────────────────
    positions: dict[str, dict] = {}
    airborne_count = 0

    for tail in sorted(tails):
        ac_raw = fetch_registration(tail)
        if ac_raw is not None:
            pos = parse_ac(ac_raw, tail)
            positions[tail] = pos
            status = "AIR" if pos["airborne"] else "GND"
            alt_str = f"{pos['alt_baro_ft']}ft" if pos["alt_baro_ft"] is not None else "—"
            gs_str  = f"{pos['ground_speed_kts']}kts" if pos["ground_speed_kts"] else "—"
            log(f"    {tail}: {status} | {alt_str} | {gs_str}")
            if pos["airborne"]:
                airborne_count += 1
        else:
            log(f"    {tail}: not tracked (on ground / no signal)")
            positions[tail] = {
                "registration": tail,
                "lat": None, "lon": None,
                "alt_baro_ft": None, "ground_speed_kts": None,
                "track_deg": None, "squawk": None, "flight": None,
                "airborne": False, "seen_secs_ago": None,
                "rssi": None, "emergency": None,
                "last_updated": utcnow(),
            }
        time.sleep(CALL_DELAY)

    # ── Write output ───────────────────────────────────────────────────
    out = {
        "fetched_at_utc": utcnow(),
        "fleet_id":       fleet_id,
        "fleet_name":     fleet_name,
        "aircraft_count": len(tails),
        "airborne_count": airborne_count,
        "positions":      positions,
    }

    out_path = data_root / f"positions_{fleet_id}.json"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    log(f"  ✓ {fleet_name}: {len(positions)} positions written → {out_path} ({airborne_count} airborne)")
    return True


def main():
    parser = argparse.ArgumentParser(
        description="Fetch live aircraft positions from ADSB.lol for all configured fleets"
    )
    parser.add_argument("--config", default="fleet_config.json")
    parser.add_argument("--data",   default="data")
    parser.add_argument("--dist",   default="dist/data")
    args = parser.parse_args()

    config_path = Path(args.config)
    data_root   = Path(args.data)
    dist_root   = Path(args.dist)

    if not config_path.exists():
        log(f"ERROR: Config not found: {config_path}")
        return

    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    fleets = config.get("fleets", [])
    log(f"Fetching positions for {len(fleets)} fleet(s)...")

    ok = 0
    for fc in fleets:
        log(f"Fleet: {fc.get('name', fc['id'])}")
        if fetch_fleet_positions(fc, data_root, dist_root):
            ok += 1

    log(f"Done: {ok}/{len(fleets)} fleets positioned.")


if __name__ == "__main__":
    main()
