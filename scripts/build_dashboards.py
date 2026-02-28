"""
IHC Fleet Dashboard Builder — Unified Multi-Fleet Generator
===========================================================
Reads fleet_config.json (exported from the dashboard's Configure Fleets modal)
and processes each fleet's CSV exports into dashboard.json files consumed by
the unified frontend.

Usage:
    python build_dashboards.py                        # uses fleet_config.json
    python build_dashboards.py --config my_cfg.json  # custom config path

Output per fleet:
    dist/data/<fleet_id>/dashboard.json

Supports two fleet types:
  - "phase"  : Tracks specific phase-inspection intervals (e.g., AW109SP)
  - "all"    : Tracks all inspections matching tracked ATA rules (e.g., Bell 407)
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Optional

import pandas as pd


# ─── COLUMN INDEX MAP (CAMP export, 0-based) ──────────────────────────────────
# Adjust these if your CSV export column order differs.
COLS = {
    "reg":          0,
    "airframe_rpt": 2,
    "airframe_hrs": 3,
    "ata":          5,
    "equip_hrs":    7,
    "item_type":   11,
    "disposition": 13,
    "desc":        15,
    "interval_hrs":30,
    "rem_days":    50,
    "rem_months":  52,
    "rem_hrs":     54,
    "status":      63,
}

# Retirement/overhaul keywords for component detection
RETIREMENT_KW = [
    "RETIRE", "OVERHAUL", "DISCARD", "LIFE LIMIT", "TBO",
    "REPLACEMENT", "REPLACE", "CHANGE OIL", "NOZZLE",
    "BATTERY", "CARTRIDGE", "BELT", "FILTER",
]


# ─── HELPERS ──────────────────────────────────────────────────────────────────

def log(msg: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")


def safe_float(val: Any) -> Optional[float]:
    if val is None:
        return None
    s = str(val).strip().replace(",", "")
    if s == "":
        return None
    try:
        return float(s)
    except (ValueError, TypeError):
        return None


def parse_date(val: Any) -> Optional[str]:
    """Return ISO date string or None."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return None
    s = str(val).strip()
    if not s:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m/%d/%y"):
        try:
            return datetime.strptime(s, fmt).date().isoformat()
        except ValueError:
            pass
    try:
        dt = pd.to_datetime(s, errors="coerce")
        if not pd.isna(dt):
            return dt.date().isoformat()
    except Exception:
        pass
    return None


def parse_date_as_dt(val: Any) -> Optional[datetime]:
    iso = parse_date(val)
    if not iso:
        return None
    try:
        return datetime.fromisoformat(iso)
    except Exception:
        return None


def norm_ata(x: Any) -> str:
    """Normalize an ATA string for matching.
    - Uppercase, collapse whitespace
    - Remove spaces around punctuation
    - Normalize . and / to - so 24MO.INSPECTION == 24MO-INSPECTION
    """
    if x is None:
        return ""
    s = str(x).strip().upper()
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*([-./])\s*", r"\1", s)
    # Normalize word-separators to dash so dots/slashes match dashes in label names
    # e.g. "24MO.INSPECTION" -> "24MO-INSPECTION"
    s = re.sub(r"(?<=[A-Z0-9])[./](?=[A-Z])", "-", s)
    return s


def strip_ata_chapter(s: str) -> str:
    """
    Strip the leading ATA chapter number from a normalized ATA string.
    E.g. '05 12MO-INSPECTION' -> '12MO-INSPECTION'
         '72 72/300'          -> '72/300'
         '63 11-20 INTERIM'   -> '11-20 INTERIM'
    Removes a leading 1-2 digit chapter code and optional space.
    """
    return re.sub(r"^\d{2}\s+", "", s).strip()


def ata_matches(ata_val: Any, rule: dict) -> bool:
    """
    Check if an ATA value matches a configured inspection rule.
    Modes:
      contains      - target appears anywhere in the full ATA string
      exact         - target equals full ATA string or a space-split token
      strip-chapter - strip leading chapter digits+space from both sides,
                      then check target appears anywhere in remainder
    """
    a = norm_ata(ata_val)
    t = norm_ata(rule.get("ataMatch", rule.get("match", "")))
    if not a or not t:
        return False
    mode = rule.get("mode", "contains")
    if mode == "exact":
        return a == t or t in a.split()
    if mode == "strip-chapter":
        a_stripped = strip_ata_chapter(a)
        t_stripped = strip_ata_chapter(t)
        return t_stripped in a_stripped
    # default: contains
    return t in a


def classify(remaining_days: Optional[float], remaining_hours: Optional[float], thresh: dict) -> str:
    """Classify urgency preferring days over hours."""
    cd  = thresh.get("criticalDays",  7)
    cwd = thresh.get("comingDays",   30)
    ch  = thresh.get("criticalHrs",  25)
    cwh = thresh.get("comingHrs",   100)

    if remaining_days is not None:
        d = float(remaining_days)
        if d < 0:    return "OVERDUE"
        if d <= cd:  return "CRITICAL"
        if d <= cwd: return "COMING_DUE"
        return "OK"

    if remaining_hours is not None:
        h = float(remaining_hours)
        if h < 0:    return "OVERDUE"
        if h <= ch:  return "CRITICAL"
        if h <= cwh: return "COMING_DUE"
        return "OK"

    return "UNKNOWN"


def urgency_sort_key(item: dict) -> tuple:
    order = {"OVERDUE": 0, "CRITICAL": 1, "COMING_DUE": 2, "OK": 3, "UNKNOWN": 4}
    bucket = order.get(item.get("status", "UNKNOWN"), 9)
    d = item.get("remaining_days")
    h = item.get("remaining_hours")
    sub = d if d is not None else (h if h is not None else 999999)
    return (bucket, sub)


def has_retirement_kw(desc: Any) -> bool:
    d = str(desc).upper()
    return any(kw in d for kw in RETIREMENT_KW)


# ─── CSV PARSING ──────────────────────────────────────────────────────────────

def read_csv_rows(filepath: Path) -> list[list[str]]:
    with open(filepath, "r", encoding="utf-8-sig", newline="") as f:
        return list(csv.reader(f))


def _flatten_inspections(fleet_cfg: dict) -> list[dict]:
    """
    Return a flat list of inspection rules, each tagged with '_group'.
    Handles both:
      - flat:    fleet_cfg["inspections"] = [{label, ataMatch, mode}, ...]
      - grouped: fleet_cfg["inspection_groups"] = [{label, inspections:[...]}, ...]
    """
    groups = fleet_cfg.get("inspection_groups", [])
    if groups:
        flat = []
        for grp in groups:
            for rule in grp.get("inspections", []):
                flat.append({**rule, "_group": grp["label"]})
        return flat
    return [{**r, "_group": None} for r in fleet_cfg.get("inspections", [])]


def parse_fleet_csv(filepath: Path, fleet_cfg: dict) -> dict:
    """
    Parse a single CSV file for a fleet.
    Returns a dict keyed by tail with aircraft meta and items list.
    Works for both 'phase' and 'all' fleet types.

    For 'phase' fleets, each item carries a 'group' key (str or None) so the
    frontend can render multiple tables (e.g. Phase Inspections + Interim).
    """
    rows = read_csv_rows(filepath)
    if len(rows) < 2:
        raise ValueError(f"CSV appears empty: {filepath}")

    inspections = _flatten_inspections(fleet_cfg)
    thresh      = fleet_cfg.get("thresholds", {})
    comp_win    = thresh.get("componentWindow", 200)
    fleet_type  = fleet_cfg.get("type", "all")

    aircraft: dict[str, dict] = {}

    for row in rows[1:]:
        if len(row) <= max(COLS.values()):
            continue

        reg = row[COLS["reg"]].strip() if row[COLS["reg"]] else ""
        if not reg:
            continue

        # ── Aircraft metadata ──────────────────────────────────────────────
        if reg not in aircraft:
            ah  = safe_float(row[COLS["airframe_hrs"]])
            rdt = parse_date(row[COLS["airframe_rpt"]])
            aircraft[reg] = {
                "airframe_hours":       ah,
                "airframe_report_date": rdt,
                "items": [],
                "_phase": {},          # temp for phase-interval dedup
            }

        ata_text  = row[COLS["ata"]].strip()  if row[COLS["ata"]]  else ""
        item_type = row[COLS["item_type"]].strip().upper() if row[COLS["item_type"]] else ""
        desc      = row[COLS["desc"]].strip() if row[COLS["desc"]] else ""
        rem_hrs   = safe_float(row[COLS["rem_hrs"]])
        rem_days  = safe_float(row[COLS["rem_days"]])
        status_raw= row[COLS["status"]].strip() if row[COLS["status"]] else ""
        disp      = row[COLS["disposition"]].strip() if row[COLS["disposition"]] else ""
        rii_flag  = "RII" in disp.upper() or "RII" in desc.upper()

        if item_type not in ("INSPECTION", "PART"):
            continue

        status = classify(rem_days, rem_hrs, thresh)

        # ── Phase type: match tracked intervals ────────────────────────────
        if fleet_type == "phase" and item_type == "INSPECTION":
            for rule in inspections:
                if ata_matches(ata_text, rule):
                    key = rule["label"]
                    existing = aircraft[reg]["_phase"].get(key)
                    # Keep most urgent (lowest rem_hrs first, then rem_days)
                    if existing is None or _more_urgent(rem_hrs, rem_days, existing):
                        aircraft[reg]["_phase"][key] = {
                            "label":           rule["label"],
                            "group":           rule.get("_group"),
                            "ata":             ata_text,
                            "description":     desc,
                            "remaining_hours": rem_hrs,
                            "remaining_days":  rem_days,
                            "next_due_status": status_raw,
                            "status":          status,
                            "tracked":         True,
                            "tracked_label":   rule["label"],
                            "rii":             rii_flag,
                        }

        # ── All type: include inspections matching any tracked rule ────────
        elif fleet_type == "all" and item_type == "INSPECTION":
            tracked_label = None
            tracked_group = None
            for rule in inspections:
                if ata_matches(ata_text, rule):
                    tracked_label = rule["label"]
                    tracked_group = rule.get("_group")
                    break

            # Include if tracked OR if it's a component-style item within window
            is_comp = has_retirement_kw(desc)
            in_window = (
                (rem_hrs is not None and rem_hrs <= comp_win)
                or (rem_hrs is None and rem_days is not None and rem_days <= 60)
                or status_raw.upper() == "PAST DUE"
            )

            if tracked_label is not None or (is_comp and in_window):
                clean_desc = re.sub(r"^\(RII\)\s*", "", desc, flags=re.IGNORECASE)
                clean_desc = re.sub(r"^RII\s+", "", clean_desc, flags=re.IGNORECASE).strip()

                aircraft[reg]["items"].append({
                    "label":           clean_desc or desc,
                    "group":           tracked_group,
                    "ata":             ata_text,
                    "description":     clean_desc or desc,
                    "next_due_date":   None,
                    "remaining_hours": rem_hrs,
                    "remaining_days":  rem_days,
                    "next_due_status": status_raw,
                    "status":          status,
                    "tracked":         tracked_label is not None,
                    "tracked_label":   tracked_label,
                    "rii":             rii_flag,
                })

        # ── PART items: always include if within window ────────────────────
        elif item_type == "PART":
            in_window = (
                (rem_hrs is not None and rem_hrs <= comp_win)
                or (rem_hrs is None and rem_days is not None and rem_days <= 60)
                or status_raw.upper() == "PAST DUE"
            )
            if in_window:
                clean_desc = re.sub(r"^\(RII\)\s*", "", desc, flags=re.IGNORECASE).strip()
                aircraft[reg]["items"].append({
                    "label":           clean_desc or desc,
                    "group":           None,
                    "ata":             ata_text,
                    "description":     clean_desc or desc,
                    "next_due_date":   None,
                    "remaining_hours": rem_hrs,
                    "remaining_days":  rem_days,
                    "next_due_status": status_raw,
                    "status":          status,
                    "tracked":         False,
                    "tracked_label":   None,
                    "rii":             rii_flag,
                })

    # ── Finalize: merge phase intervals into ordered items list ────────────
    if fleet_type == "phase":
        for reg, ac_data in aircraft.items():
            phase_items = []
            for rule in inspections:
                key = rule["label"]
                it  = ac_data["_phase"].get(key)
                if it:
                    phase_items.append(it)
            ac_data["items"] = phase_items

    # Clean up temp key; for 'all' type also sort by urgency
    for reg, ac_data in aircraft.items():
        ac_data.pop("_phase", None)
        if fleet_type != "phase":
            ac_data["items"].sort(key=urgency_sort_key)

    return aircraft


def _more_urgent(rem_hrs, rem_days, existing: dict) -> bool:
    """Returns True if new entry is more urgent than existing."""
    eh = existing.get("remaining_hours")
    ed = existing.get("remaining_days")
    if rem_hrs is not None and eh is not None:
        return rem_hrs < eh
    if rem_hrs is not None and eh is None:
        return True
    if rem_days is not None and ed is not None:
        return rem_days < ed
    return False


# ─── FLIGHT HOURS HISTORY ─────────────────────────────────────────────────────

def load_history(history_path: Path) -> dict:
    if not history_path.exists():
        return {}
    try:
        with open(history_path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


def save_history(history_path: Path, history: dict) -> None:
    history_path.parent.mkdir(parents=True, exist_ok=True)
    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(history, f, indent=2)


def update_history(history: dict, aircraft: dict, report_dt: Optional[datetime]) -> dict:
    date_key = (report_dt or datetime.today()).strftime("%Y-%m-%d")
    cutoff   = (datetime.today() - timedelta(days=90)).strftime("%Y-%m-%d")

    for tail, ac_data in aircraft.items():
        hours = ac_data.get("airframe_hours")
        if hours is None:
            continue
        if tail not in history:
            history[tail] = {}
        history[tail][date_key] = {"hours": hours, "date": date_key}

    # Prune old entries
    for tail in list(history.keys()):
        history[tail] = {d: v for d, v in history[tail].items() if d >= cutoff}

    return history


# ─── BASE ASSIGNMENTS ─────────────────────────────────────────────────────────

def load_base_assignments(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        with open(path, encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


# ─── MAIN BUILD ───────────────────────────────────────────────────────────────

def build_fleet(fleet_cfg: dict, data_root: Path, dist_root: Path) -> bool:
    """Build dashboard.json for a single fleet. Returns True on success."""
    fleet_id   = fleet_cfg["id"]
    fleet_name = fleet_cfg.get("name", fleet_id)
    fleet_type = fleet_cfg.get("type", "all")
    thresh     = fleet_cfg.get("thresholds", {})
    log(f"Building fleet: {fleet_name} ({fleet_type})")

    # ── Input paths ───────────────────────────────────────────────────────
    fleet_data_dir = data_root / fleet_id
    # Support both flat data/ layout (Due-List_Latest_<fleet_id>.csv)
    # and per-fleet subfolder layout (data/<fleet_id>/Due-List_Latest.csv)
    daily_csv  = data_root / f"Due-List_Latest_{fleet_id}.csv"
    weekly_csv = data_root / f"Due-List_BIG_WEEKLY_{fleet_id}.csv"
    # Fallback to subfolder layout
    if not daily_csv.exists():
        daily_csv  = fleet_data_dir / f"Due-List_Latest_{fleet_id}.csv"
        weekly_csv = fleet_data_dir / f"Due-List_BIG_WEEKLY_{fleet_id}.csv"
    if not daily_csv.exists():
        daily_csv  = fleet_data_dir / "Due-List_Latest.csv"
        weekly_csv = fleet_data_dir / "Due-List_BIG_WEEKLY.csv"
    history_path   = data_root / f"flight_hours_history_{fleet_id}.json"
    if not history_path.exists():
        history_path = fleet_data_dir / "flight_hours_history.json"
    skyrouter_path = data_root / f"skyrouter_status_{fleet_id}.json"
    if not skyrouter_path.exists():
        skyrouter_path = fleet_data_dir / "skyrouter_status.json"
    bases_path     = data_root / f"base_assignments_{fleet_id}.json"
    if not bases_path.exists():
        bases_path = fleet_data_dir / "base_assignments.json"

    if not daily_csv.exists():
        log(f"  ⚠ Skipping {fleet_name}: no data file at {daily_csv}")
        return None  # None = skipped, not an error

    # ── Parse CSVs ────────────────────────────────────────────────────────
    log(f"  Parsing {daily_csv.name} ...")
    aircraft = parse_fleet_csv(daily_csv, fleet_cfg)

    if weekly_csv.exists() and fleet_type == "phase":
        log(f"  Merging {weekly_csv.name} ...")
        weekly_aircraft = parse_fleet_csv(weekly_csv, fleet_cfg)
        flat_inspections = _flatten_inspections(fleet_cfg)
        # Merge: weekly provides long-range intervals not in daily
        for tail, wac in weekly_aircraft.items():
            if tail not in aircraft:
                aircraft[tail] = wac
            else:
                daily_labels = {it["tracked_label"] for it in aircraft[tail]["items"] if it.get("tracked_label")}
                for wit in wac["items"]:
                    if wit.get("tracked_label") and wit["tracked_label"] not in daily_labels:
                        aircraft[tail]["items"].append(wit)
                # Re-sort to preserve group order from config
                label_order = {r["label"]: i for i, r in enumerate(flat_inspections)}
                aircraft[tail]["items"].sort(key=lambda x: label_order.get(x.get("tracked_label", ""), 999))

    log(f"  Parsed {len(aircraft)} aircraft")

    # ── Flight hours history ───────────────────────────────────────────────
    history = load_history(history_path)
    report_dt = None
    for ac in aircraft.values():
        rdt = ac.get("airframe_report_date")
        if rdt:
            try:
                report_dt = datetime.fromisoformat(rdt)
                break
            except Exception:
                pass
    history = update_history(history, aircraft, report_dt)
    save_history(history_path, history)
    log(f"  Updated flight hours history → {history_path}")

    # ── Optional data sources ─────────────────────────────────────────────
    skyrouter = {}
    if skyrouter_path.exists():
        try:
            with open(skyrouter_path, encoding="utf-8") as f:
                skyrouter = json.load(f).get("aircraft", {})
            log(f"  Loaded SkyRouter data for {len(skyrouter)} aircraft")
        except Exception as e:
            log(f"  Warning: Could not load SkyRouter data: {e}")

    base_assignments = load_base_assignments(bases_path)
    if base_assignments:
        log(f"  Loaded base assignments")

    # ── Build summary ─────────────────────────────────────────────────────
    all_items = []
    for ac_data in aircraft.values():
        all_items.extend(ac_data["items"])

    counts = {"OVERDUE":0, "CRITICAL":0, "COMING_DUE":0, "OK":0}
    for it in all_items:
        s = it.get("status", "UNKNOWN")
        if s in counts:
            counts[s] += 1

    # ── Output JSON ───────────────────────────────────────────────────────
    out = {
        "generated_at_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "fleet":          fleet_name,
        "fleet_id":       fleet_id,
        "fleet_type":     fleet_type,
        "aircraft_count": len(aircraft),
        "config": {
            "inspections":       fleet_cfg.get("inspections", []),
            "inspection_groups": fleet_cfg.get("inspection_groups", []),
            "thresholds":        thresh,
        },
        "summary": {**counts, "total_tracked": sum(counts.values())},
        "aircraft":        aircraft,
        "flight_hours_history": history,
    }

    if base_assignments:
        out["base_assignments"] = base_assignments

    if skyrouter:
        out["skyrouter"] = skyrouter

    # ── Write output ──────────────────────────────────────────────────────
    out_dir  = dist_root / fleet_id
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "dashboard.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    log(f"  ✓ Wrote {out_path} ({len(aircraft)} aircraft, {counts['OVERDUE']} overdue, {counts['CRITICAL']} critical)")
    return True


def build_all(config_path: Path, data_root: Path, dist_root: Path) -> int:
    """Build all fleets defined in config. Returns exit code.
    Exit 0 if all fleets either built successfully or were skipped due to
    missing CSV data (no data uploaded yet is not an error in CI).
    Exit 1 only on real errors (bad config, parse failure, etc.).
    """
    if not config_path.exists():
        log(f"ERROR: Config file not found: {config_path}")
        log("Export fleet_config.json from the dashboard's Configure Fleets modal.")
        return 1

    with open(config_path, encoding="utf-8") as f:
        config = json.load(f)

    fleets = config.get("fleets", [])
    if not fleets:
        log("No fleets defined in config.")
        return 0

    log(f"Building {len(fleets)} fleet(s)...")
    ok = 0
    skipped = 0
    for fc in fleets:
        result = build_fleet(fc, data_root, dist_root)
        if result is True:
            ok += 1
        elif result is None:
            skipped += 1
        # False = real error

    total_done = ok + skipped
    log(f"Done: {ok} built, {skipped} skipped (no data), {len(fleets)-total_done} errors.")
    # Only fail if there was a real error (not just missing CSV data)
    return 0 if (len(fleets) - total_done) == 0 else 1


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="IHC Fleet Dashboard Builder — generates per-fleet dashboard.json files"
    )
    parser.add_argument(
        "--config",
        default="fleet_config.json",
        help="Path to fleet_config.json (default: fleet_config.json)",
    )
    parser.add_argument(
        "--data",
        default="data",
        help="Root folder containing per-fleet CSV and support files (default: data/)",
    )
    parser.add_argument(
        "--dist",
        default="dist/data",
        help="Output root for generated JSON files (default: dist/data/)",
    )
    args = parser.parse_args()

    sys.exit(build_all(
        config_path=Path(args.config),
        data_root=Path(args.data),
        dist_root=Path(args.dist),
    ))


if __name__ == "__main__":
    main()
