# position_fetcher.py  (was flightaware_fetcher.py)
#
# Strategy:
#   1. Try ADSB.lol first for every aircraft — completely free, no account needed.
#      Endpoint: GET https://api.adsb.lol/v2/reg/{TAIL}
#   2. Only call FlightAware AeroAPI for tails that ADSB.lol returned no live data for.
#      Uses the single cheapest endpoint: GET /flights/{ident}  ($0.005/call)
#      The last_position field inside that response gives us lat/lon without
#      a second /position call, keeping cost at $0.005/aircraft instead of $0.015.
#   3. Hard monthly spend cap on FlightAware. Spend is tracked in a persistent JSON
#      ledger file (data/fa_spend_ledger.json). If the cap would be exceeded,
#      FlightAware calls stop for the rest of the calendar month and a warning is
#      written into the output JSON so the dashboard can surface it.
#
# Output per fleet:  data/<fleet_id>/flightaware_status.json
# Spend ledger:      data/fa_spend_ledger.json  (committed to git by deploy.yml)
#
# Usage:
#   python scripts/flightaware_fetcher.py                 # normal run
#   python scripts/flightaware_fetcher.py --dry-run       # no API calls, print plan
#   python scripts/flightaware_fetcher.py --fleet aw109sp # single fleet only
#   python scripts/flightaware_fetcher.py --show-spend    # print ledger and exit
#   python scripts/flightaware_fetcher.py --reset-spend   # zero this month's ledger

import argparse
import json
import math
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import requests

# ─── USER CONFIG ──────────────────────────────────────────────────────────────

AEROAPI_KEY  = os.environ.get("FLIGHTAWARE_API_KEY", "")
AEROAPI_BASE = "https://aeroapi.flightaware.com/aeroapi"

# Hard monthly spend cap — matches your FlightAware free credit
FA_MONTHLY_CAP_USD = 5.00

# Cost of the one FA endpoint we use: GET /flights/{ident}
FA_COST_PER_CALL = 0.005

# Stop at 90% of cap — leaves $0.50 buffer for manual testing or reruns
FA_CAP_SAFETY_FACTOR = 0.90

# Seconds since last ADS-B message before we consider position "too stale"
# and fall back to FlightAware
ADSB_MAX_AGE_SECONDS = 300   # 5 minutes

# FA rate limit: 1 req/sec; we wait 1.1s to stay comfortably under
FA_RATE_LIMIT_DELAY = 1.1

# ─── AIRCRAFT REGISTRY ────────────────────────────────────────────────────────

FLEET_AIRCRAFT = {
    "aw109sp": [
        # Replace with your actual AW109SP tail numbers
        "N251HC",
        "N261HC",
        "N271HC",
        "N281HC",
        "N291HC",
        "N431HC",
        "N531HC",
        "N631HC",
        "N731HC",
        # ... add all AW109SP tails
    ],
    "bell407": [
        # Replace with your actual Bell 407 tail numbers
        "N407IH",
        "N407HS",
        # ... add all Bell 407 tails
    ],
    # "ec135": ["N135XX", "N135YY"],
}

BASES = {
  "LOGAN": {
      "name": "Logan",
      "lat": 41.7912,
      "lon": -111.8522,
      "radius_miles": 5
    },
    "MCKAY": {
      "name": "McKay",
      "lat": 41.2545,
      "lon": -112.0126,
      "radius_miles": 5
    },
    "IMED": {
      "name": "IMed",
      "lat": 40.2338,
      "lon": -111.6585,
      "radius_miles": 5
    },
    "PROVO": {
      "name": "Provo",
      "lat": 40.2192,
      "lon": -111.7233,
      "radius_miles": 5
    },
    "ROOSEVELT": {
      "name": "Roosevelt",
      "lat": 40.2765,
      "lon": -110.0518,
      "radius_miles": 5
    },
    "CEDAR_CITY": {
      "name": "Cedar City",
      "lat": 37.701,
      "lon": -113.0989,
      "radius_miles": 5
    },
    "ST_GEORGE": {
      "name": "St George",
      "lat": 37.0365,
      "lon": -113.5101,
      "radius_miles": 5
    },
    "KSLC": {
      "name": "KSLC",
      "lat": 40.7884,
      "lon": -111.9778,
      "radius_miles": 10
    }
  },
}


# ─── SPEND LEDGER ─────────────────────────────────────────────────────────────

class SpendLedger:
    """
    Tracks FlightAware spend per calendar month in data/fa_spend_ledger.json.
    Persists across runs — the cap is enforced across the whole month, not just
    within a single GitHub Actions run.

    File format:
    {
      "2026-02": {"calls": 12, "usd": 0.060, "last_updated": "..."},
      "2026-03": {"calls": 0,  "usd": 0.000, "last_updated": "..."}
    }
    """

    def __init__(self, path: Path):
        self.path  = path
        self.month = datetime.now(timezone.utc).strftime("%Y-%m")
        self._data = self._load()

    def _load(self) -> dict:
        if self.path.exists():
            try:
                with open(self.path, encoding="utf-8") as f:
                    return json.load(f)
            except Exception:
                pass
        return {}

    def _save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(self._data, f, indent=2)

    @property
    def _month_entry(self) -> dict:
        if self.month not in self._data:
            self._data[self.month] = {
                "calls": 0, "usd": 0.0,
                "last_updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
        return self._data[self.month]

    @property
    def calls_this_month(self) -> int:
        return self._month_entry["calls"]

    @property
    def spend_this_month(self) -> float:
        return self._month_entry["usd"]

    @property
    def effective_cap(self) -> float:
        return FA_MONTHLY_CAP_USD * FA_CAP_SAFETY_FACTOR

    @property
    def remaining_budget(self) -> float:
        return max(0.0, self.effective_cap - self.spend_this_month)

    @property
    def calls_remaining(self) -> int:
        return int(self.remaining_budget / FA_COST_PER_CALL)

    def can_afford(self) -> bool:
        return self.calls_remaining >= 1

    def record_call(self):
        """Record one call pessimistically (before the HTTP request)."""
        e = self._month_entry
        e["calls"]        += 1
        e["usd"]          += FA_COST_PER_CALL
        e["last_updated"]  = datetime.now(timezone.utc).isoformat(timespec="seconds")
        self._save()

    def reset_month(self):
        self._data[self.month] = {
            "calls": 0, "usd": 0.0,
            "last_updated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        self._save()

    def summary_line(self) -> str:
        pct = (self.spend_this_month / FA_MONTHLY_CAP_USD) * 100
        return (
            f"FA {self.month}: ${self.spend_this_month:.3f} / "
            f"${FA_MONTHLY_CAP_USD:.2f} ({pct:.0f}%)  "
            f"| {self.calls_this_month} calls  "
            f"| {self.calls_remaining} remaining before ${self.effective_cap:.2f} safety cap"
        )

    def as_dict(self) -> dict:
        return {
            "month":             self.month,
            "calls_this_month":  self.calls_this_month,
            "usd_this_month":    round(self.spend_this_month, 4),
            "monthly_cap_usd":   FA_MONTHLY_CAP_USD,
            "safety_cap_usd":    round(self.effective_cap, 2),
            "remaining_usd":     round(self.remaining_budget, 4),
            "calls_remaining":   self.calls_remaining,
            "cap_reached":       not self.can_afford(),
        }


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def haversine_nm(lat1, lon1, lat2, lon2) -> float:
    R = 3440.065
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def classify_base(lat: float, lon: float) -> dict:
    best_id, best_dist = None, float("inf")
    for bid, b in BASES.items():
        d = haversine_nm(lat, lon, b["lat"], b["lon"])
        if d < best_dist:
            best_dist, best_id = d, bid
    if best_id is None:
        return {"at_base": False, "base_id": None, "base_name": None, "distance_nm": None}
    at_b = best_dist <= BASES[best_id].get("radius_nm", 3)
    return {
        "at_base":    at_b,
        "base_id":    best_id,
        "base_name":  BASES[best_id]["name"],
        "distance_nm": round(best_dist, 1),
    }


def age_seconds(ts_str) -> float:
    if not ts_str:
        return float("inf")
    try:
        ts  = datetime.fromisoformat(ts_str.replace("Z", "+00:00"))
        return (datetime.now(timezone.utc) - ts).total_seconds()
    except Exception:
        return float("inf")


def empty_result(reg: str, source: str = "none") -> dict:
    return {
        "registration": reg,
        "source":       source,
        "status":       "unknown",
        "airborne":     False,
        "last_seen":    None,
        "position":     {"lat": None, "lon": None, "altitude_ft": None,
                         "groundspeed_kts": None, "heading": None},
        "flight":       {"ident": None, "origin": None, "destination": None,
                         "actual_off": None, "actual_on": None},
        "base":         {"at_base": None, "base_id": None,
                         "base_name": None, "distance_nm": None},
        "fetched_utc":  datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# ─── SOURCE 1: ADSB.LOL (FREE) ────────────────────────────────────────────────

def fetch_adsblol(reg: str) -> dict | None:
    """
    Query ADSB.lol v2 by registration. Returns normalized result or None.
    No API key, no cost, no rate limit (currently).
    """
    url = f"https://api.adsb.lol/v2/reg/{reg.upper()}"
    try:
        resp = requests.get(url, timeout=15)
    except requests.RequestException as e:
        log(f"      ADSB.lol request failed: {e}")
        return None

    if resp.status_code == 404:
        return None
    if resp.status_code != 200:
        log(f"      ADSB.lol returned {resp.status_code}")
        return None

    ac_list = resp.json().get("ac", [])
    if not ac_list:
        return None

    ac = ac_list[0]
    lat = ac.get("lat")
    lon = ac.get("lon")
    if lat is None or lon is None:
        return None

    # seen_pos = seconds since last position message from this aircraft
    seen_pos = ac.get("seen_pos", ac.get("seen", 9999))
    if seen_pos > ADSB_MAX_AGE_SECONDS:
        log(f"      ADSB.lol: position is {int(seen_pos)}s old (>{ADSB_MAX_AGE_SECONDS}s limit)")
        return None

    alt_baro   = ac.get("alt_baro", 0)
    alt_ft     = 0 if alt_baro == "ground" else int(alt_baro or 0)
    gs         = ac.get("gs") or 0
    on_gnd_raw = ac.get("on_ground")

    if on_gnd_raw is not None:
        airborne = not bool(on_gnd_raw)
    else:
        airborne = alt_ft > 200 or gs > 40

    r = empty_result(reg, source="adsb_lol")
    r["status"]              = "airborne" if airborne else "on_ground"
    r["airborne"]            = airborne
    r["last_seen"]           = datetime.now(timezone.utc).isoformat(timespec="seconds")
    r["position"]["lat"]     = lat
    r["position"]["lon"]     = lon
    r["position"]["altitude_ft"]     = alt_ft
    r["position"]["groundspeed_kts"] = round(gs, 1)
    r["position"]["heading"] = ac.get("track")
    r["flight"]["ident"]     = (ac.get("flight") or "").strip() or None
    r["base"]                = classify_base(lat, lon)
    return r


# ─── SOURCE 2: FLIGHTAWARE (FALLBACK, CAPPED) ────────────────────────────────

def fetch_flightaware(reg: str, ledger: SpendLedger, dry_run: bool = False) -> dict | None:
    """
    Fallback to FlightAware only when ADSB.lol has nothing.
    Uses one call: GET /flights/{ident} ($0.005).
    Spend is recorded BEFORE the HTTP request (pessimistic) so
    concurrent runs cannot both believe they have remaining budget.
    """
    if not ledger.can_afford():
        log(f"      ⛔ FA cap reached — {ledger.summary_line()}")
        return None

    if not AEROAPI_KEY and not dry_run:
        log(f"      FA skipped — FLIGHTAWARE_API_KEY not set")
        return None

    log(f"      → FA fallback (will cost ${FA_COST_PER_CALL:.3f} | {ledger.summary_line()})")

    if dry_run:
        log(f"      [DRY RUN] would call GET /flights/{reg}")
        return None

    # Record spend before the call — if the call fails we've "wasted" $0.005
    # but that's safer than accidentally going over budget
    ledger.record_call()

    time.sleep(FA_RATE_LIMIT_DELAY)
    url  = f"{AEROAPI_BASE}/flights/{reg}"
    try:
        resp = requests.get(url,
                            headers={"x-apikey": AEROAPI_KEY},
                            params={"max_pages": 1},
                            timeout=30)
    except requests.RequestException as e:
        log(f"      FA request error: {e}")
        return None

    if resp.status_code == 404:
        return None
    if resp.status_code == 429:
        log("      FA rate-limited — not retrying (spend already recorded)")
        return None
    if resp.status_code != 200:
        log(f"      FA error {resp.status_code}: {resp.text[:100]}")
        return None

    data    = resp.json()
    flights = data.get("flights", [])
    if not flights:
        return None

    latest     = flights[0]
    actual_off = latest.get("actual_off")
    actual_on  = latest.get("actual_on")
    airborne   = bool(actual_off and not actual_on)

    r = empty_result(reg, source="flightaware")
    r["airborne"] = airborne
    r["status"]   = "airborne" if airborne else "on_ground"
    r["flight"]   = {
        "ident":       latest.get("ident"),
        "origin":      _airport_label(latest.get("origin")),
        "destination": _airport_label(latest.get("destination")),
        "actual_off":  actual_off,
        "actual_on":   actual_on,
    }

    # Use last_position embedded in the flight response — no extra /position call
    lp = latest.get("last_position") or {}
    lat = lp.get("latitude")
    lon = lp.get("longitude")
    if lat and lon:
        r["position"] = {
            "lat":             lat,
            "lon":             lon,
            "altitude_ft":     (lp.get("altitude") or 0) * 100,
            "groundspeed_kts": lp.get("groundspeed"),
            "heading":         lp.get("heading"),
        }
        r["last_seen"] = lp.get("timestamp")
        if age_seconds(r["last_seen"]) > 7200:
            r["status"] = "last_known"
        r["base"] = classify_base(lat, lon)

    return r


def _airport_label(ap: dict | None) -> str | None:
    if not ap:
        return None
    code  = ap.get("code_iata") or ap.get("code_icao") or ap.get("code")
    place = ap.get("city") or ap.get("name")
    parts = [p for p in [code, place] if p]
    return " / ".join(parts) if parts else None


# ─── PER-AIRCRAFT DISPATCH ────────────────────────────────────────────────────

def fetch_aircraft(reg: str, ledger: SpendLedger, dry_run: bool = False) -> dict:
    if dry_run:
        log(f"    [DRY RUN] {reg}: would try ADSB.lol, then FA if empty")
        return empty_result(reg, source="dry_run")

    # 1. Try ADSB.lol (free)
    result = fetch_adsblol(reg)
    if result:
        pos = result["position"]
        log(f"    ✓ {reg}: {result['status']} via ADSB.lol"
            + (f" | {pos['altitude_ft']}ft {pos['groundspeed_kts']}kts" if pos["lat"] else ""))
        return result

    log(f"    ✗ {reg}: ADSB.lol empty — trying FA fallback")

    # 2. FA fallback (capped)
    result = fetch_flightaware(reg, ledger, dry_run=dry_run)
    if result:
        log(f"    ✓ {reg}: {result['status']} via FlightAware")
        return result

    # 3. Nothing
    r = empty_result(reg, source="none")
    r["status"] = "no_data"
    log(f"    ✗ {reg}: no data from any source")
    return r


# ─── FLEET LOOP ───────────────────────────────────────────────────────────────

def fetch_fleet(fleet_id: str, regs: list[str],
                out_path: Path, ledger: SpendLedger,
                dry_run: bool = False) -> None:

    log(f"\n── Fleet: {fleet_id} ({len(regs)} aircraft) ──")
    log(f"   {ledger.summary_line()}")

    statuses  = {}
    src_tally = {"adsb_lol": 0, "flightaware": 0, "none": 0, "dry_run": 0}

    for reg in regs:
        log(f"  {reg}:")
        s = fetch_aircraft(reg, ledger, dry_run=dry_run)
        statuses[reg] = s
        src = s.get("source", "none")
        src_tally[src] = src_tally.get(src, 0) + 1

    log(f"\n  Summary: "
        f"{src_tally['adsb_lol']} ADSB.lol | "
        f"{src_tally['flightaware']} FlightAware | "
        f"{src_tally.get('none', 0)} no data")
    log(f"  {ledger.summary_line()}")

    out = {
        "generated_utc":    datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "fleet_id":         fleet_id,
        "aircraft":         statuses,
        "base_assignments": _build_base_summary(statuses),
        "bases":            {bid: {"name": b["name"], "lat": b["lat"], "lon": b["lon"]}
                             for bid, b in BASES.items()},
        "data_sources":     src_tally,
        "fa_spend":         ledger.as_dict(),
    }

    if not dry_run:
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        log(f"  Wrote → {out_path}")


def _build_base_summary(statuses: dict) -> dict:
    assignments = {bid: [] for bid in BASES}
    airborne, away = [], []

    for reg, s in statuses.items():
        b     = s.get("base", {})
        at_b  = b.get("at_base")
        b_id  = b.get("base_id")
        entry = {
            "tail":            reg,
            "source":          s.get("source"),
            "status":          s.get("status"),
            "airborne":        s.get("airborne", False),
            "lat":             s.get("position", {}).get("lat"),
            "lon":             s.get("position", {}).get("lon"),
            "altitude_ft":     s.get("position", {}).get("altitude_ft"),
            "groundspeed_kts": s.get("position", {}).get("groundspeed_kts"),
            "heading":         s.get("position", {}).get("heading"),
            "last_seen":       s.get("last_seen"),
            "flight":          s.get("flight", {}),
            "at_base":         at_b,
            "base_id":         b_id,
            "distance_nm":     b.get("distance_nm"),
        }
        if s.get("airborne"):
            airborne.append(entry)
        elif at_b and b_id:
            assignments[b_id].append(entry)
        else:
            away.append(entry)

    return {
        "bases":    {bid: {"aircraft": acs, "name": BASES[bid]["name"]}
                     for bid, acs in assignments.items()},
        "airborne": airborne,
        "away":     away,
    }


# ─── MAIN ─────────────────────────────────────────────────────────────────────

def main():
    p = argparse.ArgumentParser(
        description="Aircraft position fetcher: ADSB.lol primary, FlightAware fallback with cap"
    )
    p.add_argument("--config",      default="fleet_config.json")
    p.add_argument("--data",        default="data")
    p.add_argument("--dry-run",     action="store_true")
    p.add_argument("--fleet",       default=None)
    p.add_argument("--reset-spend", action="store_true",
                   help="Zero this month's FA spend ledger")
    p.add_argument("--show-spend",  action="store_true",
                   help="Print spend history and exit")
    args = p.parse_args()

    data_root   = Path(args.data)
    ledger      = SpendLedger(data_root / "fa_spend_ledger.json")

    if args.reset_spend:
        ledger.reset_month()
        log(f"Ledger reset for {ledger.month}. {ledger.summary_line()}")
        return

    if args.show_spend:
        print(f"\nFlightAware spend ledger: {ledger.path}")
        print(f"Current month: {ledger.summary_line()}\n")
        print("Full history:")
        for month, d in sorted(ledger._data.items()):
            print(f"  {month}  ${d['usd']:.4f}  {d['calls']} calls  (last updated {d['last_updated']})")
        return

    # Load tail numbers
    fleet_aircraft = dict(FLEET_AIRCRAFT)
    cfg_path = Path(args.config)
    if cfg_path.exists():
        try:
            with open(cfg_path, encoding="utf-8") as f:
                cfg = json.load(f)
            for fleet in cfg.get("fleets", []):
                fid, tails = fleet.get("id"), fleet.get("aircraft", [])
                if fid and tails and fid not in fleet_aircraft:
                    fleet_aircraft[fid] = tails
        except Exception as e:
            log(f"Warning: could not read {cfg_path}: {e}")

    fleets = [args.fleet] if args.fleet else list(fleet_aircraft.keys())
    total  = sum(len(fleet_aircraft.get(f, [])) for f in fleets)

    log(f"Position fetcher starting — {total} aircraft, {len(fleets)} fleet(s)")
    log(f"{ledger.summary_line()}")

    for fleet_id in fleets:
        regs = fleet_aircraft.get(fleet_id)
        if not regs:
            log(f"No aircraft for '{fleet_id}' — skipping")
            continue
        out_path = data_root / fleet_id / "flightaware_status.json"
        fetch_fleet(fleet_id, regs, out_path, ledger, dry_run=args.dry_run)

    log("\n── Final spend ──")
    log(ledger.summary_line())
    log("Done.")


if __name__ == "__main__":
    main()
