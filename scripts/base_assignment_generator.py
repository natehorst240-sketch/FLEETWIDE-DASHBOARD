"""
Base Assignment System with SkyRouter Integration
=================================================
Automatically assigns aircraft to bases based on SkyRouter position data.
Generates JSON for the dashboard's "Bases" tab.
"""

import json
from pathlib import Path
from datetime import datetime
import math

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

ONEDRIVE_FOLDER = r"C:\Users\nateh\OneDrive - Intermountain Health\VeryonExports\Veryon Exports"
SKYROUTER_STATUS_FILE = "skyrouter_status.json"
BASE_ASSIGNMENTS_FILE = "base_assignments.json"

# Define your bases with coordinates and radius
BASES = {
    "LOGAN": {
        "name": "Logan",
        "lat": 41.7555,
        "lon": -111.8214,
        "radius_miles": 5
    },
    "MCKAY": {
        "name": "McKay",
        "lat": 41.1840,
        "lon": -111.9537,
        "radius_miles": 5
    },
    "IMED": {
        "name": "IMED",
        "lat": 40.6592,
        "lon": -111.8909,
        "radius_miles": 5
    },
    "PROVO": {
        "name": "Provo",
        "lat": 40.2486,
        "lon": -111.6643,
        "radius_miles": 5
    },
    "ROOSEVELT": {
        "name": "Roosevelt",
        "lat": 40.3049,
        "lon": -109.9964,
        "radius_miles": 5
    },
    "CEDAR_CITY": {
        "name": "Cedar City",
        "lat": 37.70038,
        "lon": -113.0675,
        "radius_miles": 5
    },
    "ST_GEORGE": {
        "name": "St George",
        "lat": 37.0982,
        "lon": -113.5532,
        "radius_miles": 5
    },
    "KSLC": {
        "name": "KSLC",
        "lat": 40.7741,
        "lon": -111.9567,
        "radius_miles": 10
    }
}

# ── HELPERS ───────────────────────────────────────────────────────────────────

def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance in miles between two lat/lon points."""
    R = 3959  # Earth radius in miles
    
    lat1_rad = math.radians(lat1)
    lat2_rad = math.radians(lat2)
    delta_lat = math.radians(lat2 - lat1)
    delta_lon = math.radians(lon2 - lon1)
    
    a = math.sin(delta_lat/2)**2 + math.cos(lat1_rad) * math.cos(lat2_rad) * math.sin(delta_lon/2)**2
    c = 2 * math.asin(math.sqrt(a))
    
    return R * c


def load_skyrouter_status(skyrouter_path):
    """Load aircraft positions from SkyRouter data."""
    if not skyrouter_path.exists():
        return {}
    
    try:
        with open(skyrouter_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('aircraft', {})
    except Exception as e:
        print(f"Error loading SkyRouter status: {e}")
        return {}


def load_previous_assignments(assignments_path):
    """Load previous base assignments (for manual overrides)."""
    if not assignments_path.exists():
        return {}
    
    try:
        with open(assignments_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data.get('assignments', {})
    except Exception:
        return {}


def find_base_for_aircraft(aircraft_data):
    """
    Find which base an aircraft is at based on position.
    
    Returns: (base_id, distance, at_base) or (None, None, False)
    """
    if 'latitude' not in aircraft_data or 'longitude' not in aircraft_data:
        return None, None, False
    
    lat = aircraft_data['latitude']
    lon = aircraft_data['longitude']
    
    # Check each base
    closest_base = None
    closest_distance = float('inf')
    
    for base_id, base_info in BASES.items():
        distance = haversine_distance(lat, lon, base_info['lat'], base_info['lon'])
        
        if distance < closest_distance:
            closest_distance = distance
            closest_base = base_id
    
    # Check if within radius
    if closest_base and closest_distance <= BASES[closest_base]['radius_miles']:
        return closest_base, closest_distance, True
    
    return closest_base, closest_distance, False


def assign_aircraft_to_bases(skyrouter_status, previous_assignments):
    """
    Assign aircraft to bases based on current position.
    
    Returns dict:
    {
        "LOGAN": {
            "aircraft": ["N291HC"],
            "status": "occupied",  # or "available"
        },
        ...
        "unassigned": ["N271HC", ...]  # Aircraft not at any base
    }
    """
    assignments = {base_id: {"aircraft": [], "status": "available"} for base_id in BASES.keys()}
    assignments["unassigned"] = []
    
    for tail, aircraft_data in skyrouter_status.items():
        base_id, distance, at_base = find_base_for_aircraft(aircraft_data)
        
        if at_base and base_id:
            # Aircraft is at a base
            assignments[base_id]["aircraft"].append({
                "tail": tail,
                "hours": aircraft_data.get('current_hours'),
                "distance": round(distance, 2),
                "at_base": True
            })
            assignments[base_id]["status"] = "occupied"
        else:
            # Aircraft is not at any base
            assignments["unassigned"].append({
                "tail": tail,
                "hours": aircraft_data.get('current_hours'),
                "last_known_base": previous_assignments.get(tail, {}).get('base'),
                "distance_from_closest": round(distance, 2) if distance else None,
                "closest_base": BASES[base_id]["name"] if base_id else None,
                "at_base": False
            })
    
    return assignments


def generate_base_assignments():
    """Main function to generate base assignments."""
    log_path = Path(__file__).with_name("base_assignment_log.txt")
    
    def log(msg):
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        line = f"[{timestamp}] {msg}"
        print(line)
        try:
            with open(log_path, 'a', encoding='utf-8') as f:
                f.write(line + '\n')
        except Exception:
            pass
    
    try:
        log("Base assignment generation started")
        
        # Load data
        skyrouter_path = Path(ONEDRIVE_FOLDER) / SKYROUTER_STATUS_FILE
        assignments_path = Path(ONEDRIVE_FOLDER) / BASE_ASSIGNMENTS_FILE
        
        log("Loading SkyRouter status...")
        skyrouter_status = load_skyrouter_status(skyrouter_path)
        
        if not skyrouter_status:
            log("WARNING: No SkyRouter data available")
            return False
        
        log(f"Loaded position data for {len(skyrouter_status)} aircraft")
        
        # Load previous assignments
        previous_assignments = load_previous_assignments(assignments_path)
        
        # Generate new assignments
        log("Calculating base assignments...")
        assignments = assign_aircraft_to_bases(skyrouter_status, previous_assignments)
        
        # Create output data
        output_data = {
            "last_updated": datetime.now().isoformat(),
            "bases": BASES,
            "assignments": assignments,
            "summary": {
                "total_aircraft": len(skyrouter_status),
                "at_bases": sum(len(a["aircraft"]) for k, a in assignments.items() if k != "unassigned"),
                "away_from_base": len(assignments["unassigned"])
            }
        }
        
        # Save to JSON
        with open(assignments_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2)
        
        log(f"Base assignments saved to {assignments_path}")
        
        # Log summary
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


if __name__ == '__main__':
    generate_base_assignments()