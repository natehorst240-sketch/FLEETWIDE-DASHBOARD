"""
SkyRouter API Data Fetcher
===========================
Uses SkyRouter Data Exchange API to get aircraft position data.


Based on SkyRouter DataExchange API documentation.
"""

import json
import requests
from datetime import datetime, timedelta
from pathlib import Path
import math

# ── CONFIGURATION ─────────────────────────────────────────────────────────────

SKYROUTER_USERNAME = "HSU"
SKYROUTER_PASSWORD = "Hsu2018!"

# Base API URL 
SKYROUTER_API_BASE = "https://new.skyrouter.com/Bsn.Skyrouter.DataExchange/"

OUTPUT_FOLDER = r"C:\Users\nateh\OneDrive - Intermountain Health\VeryonExports\Veryon Exports"
OUTPUT_FILENAME = "skyrouter_status.json"

# Your base location (Provo Airport example)
BASE_LAT = 40.7884
BASE_LON = -111.7233
BASE_RADIUS_MILES = 5  # Consider "at base" if within this radius

# How far back to look for position data (in hours)
LOOKBACK_HOURS = 6

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


def parse_skyrouter_position(pos_data):
    """
    Parse position data from SkyRouter API response.
    
    Based on API format from documentation:
    - Latitude: Field 10
    - Longitude: Field 11
    - Altitude: Field 12
    - Report Type: Field 3 (POS, TOF, LAN, etc.)
    """
    # Split by commas (CSV format from API)
    fields = pos_data.split(',')
    
    if len(fields) < 18:
        return None
    
    try:
        # Extract key fields based on API documentation
        report_type = fields[2].strip()  # Field 3: Report Type
        registration = fields[6].strip()  # Field 7: Registration (tail number)
        
        # Parse latitude/longitude (Fields 10, 11)
        lat_str = fields[9].strip()  # Field 10: Latitude
        lon_str = fields[10].strip()  # Field 11: Longitude
        
        # Convert lat/lon from degrees-minutes-seconds format if needed
        # Format from docs: [+/-]NNN.NNNNNNNNNNNNN
        latitude = float(lat_str) if lat_str else None
        longitude = float(lon_str) if lon_str else None
        
        # Parse other useful fields
        altitude = fields[11].strip() if len(fields) > 11 else ""  # Field 12
        velocity = fields[12].strip() if len(fields) > 12 else ""  # Field 13
        heading = fields[13].strip() if len(fields) > 13 else ""   # Field 14
        
        # Date and time
        date_str = fields[7].strip()  # Field 8: Date acquisition
        time_str = fields[8].strip()  # Field 9: Time acquisition
        
        return {
            'registration': registration,
            'report_type': report_type,
            'latitude': latitude,
            'longitude': longitude,
            'altitude': altitude,
            'velocity': velocity,
            'heading': heading,
            'date': date_str,
            'time': time_str
        }
    except (ValueError, IndexError) as e:
        print(f"Error parsing position data: {e}")
        return None


def fetch_flight_tracking_data():
    """
    Fetch flight tracking data from SkyRouter API.
    
    Returns raw text response from API.
    """
    # Calculate time range (last N hours)
    now = datetime.utcnow()
    from_time = now - timedelta(hours=LOOKBACK_HOURS)
    
    # Build API URL based on Data Exchange format
    # Format: .../?username=X&password=Y&datatype=FlightTracking&option=EverythingSinceLastRequest
    # OR: .../?username=X&password=Y&datatype=FlightTracking&option=SinceUTC&sinceutc=YYYY-MM-DD HH:MM:SS
    
    # Using "Everything since last request" is simpler
    url = f"{SKYROUTER_API_BASE}?username={SKYROUTER_USERNAME}&password={SKYROUTER_PASSWORD}&datatype=FlightTracking&option=EverythingSinceLastRequest"
    
    # Alternative: specify exact time range
    # from_time_str = from_time.strftime("%Y-%m-%d %H:%M:%S")
    # url = f"{SKYROUTER_API_BASE}?username={SKYROUTER_USERNAME}&password={SKYROUTER_PASSWORD}&datatype=FlightTracking&option=SinceUTC&sinceutc={from_time_str}"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from SkyRouter: {e}")
        return None


def process_skyrouter_data(raw_data):
    """
    Process raw SkyRouter API response into aircraft status dict.
    
    Returns:
    {
        "N291HC": {
            "status": "ACTIVE",
            "at_base": False,
            "last_update": "2026-02-25 22:40:52",
            "timestamp": "2026-02-25T22:40:52",
            "latitude": 40.123,
            "longitude": -111.456,
            "altitude": "5000",
            "distance_from_base": 12.5
        },
        ...
    }
    """
    if not raw_data:
        return {}
    
    aircraft_data = {}
    lines = raw_data.strip().split('\n')
    
    for line in lines:
        line = line.strip()
        if not line:
            continue
        
        # Parse the position data
        pos = parse_skyrouter_position(line)
        if not pos or not pos['registration']:
            continue
        
        tail = pos['registration']
        
        # Skip if we don't have lat/lon
        if pos['latitude'] is None or pos['longitude'] is None:
            continue
        
        # Calculate distance from base
        distance = haversine_distance(
            BASE_LAT, BASE_LON,
            pos['latitude'], pos['longitude']
        )
        
        # Determine if at base
        at_base = distance <= BASE_RADIUS_MILES
        
        # Determine status from report type
        report_type = pos['report_type'].upper()
        status_map = {
            'POS': 'ACTIVE',      # Regular Position
            'TOF': 'TAKE-OFF',    # Take-Off
            'LAN': 'LANDING',     # Landing
            'OGA': 'DEPARTED',    # Off-Gate
            'IGA': 'ARRIVED',     # In-Gate
            'QPS': 'ACTIVE',      # Quick Position Report
            'HBT': 'ACTIVE',      # Heartbeat
            'BEA': 'ACTIVE',      # Beacon
        }
        status = status_map.get(report_type, report_type)
        
        # If at base and not taking off, mark as AT BASE
        if at_base and report_type not in ['TOF', 'OGA']:
            status = 'AT BASE'
        
        # Combine date and time
        timestamp_str = f"{pos['date']} {pos['time']}"
        try:
            dt = datetime.strptime(timestamp_str, "%Y%m%d %H%M%S")
            iso_timestamp = dt.isoformat()
        except:
            iso_timestamp = datetime.now().isoformat()
        
        # Keep the most recent position for each aircraft
        if tail not in aircraft_data:
            aircraft_data[tail] = {
                'status': status,
                'at_base': at_base,
                'last_update': timestamp_str,
                'timestamp': iso_timestamp,
                'latitude': pos['latitude'],
                'longitude': pos['longitude'],
                'altitude': pos['altitude'],
                'velocity': pos['velocity'],
                'heading': pos['heading'],
                'distance_from_base': round(distance, 2)
            }
        else:
            # Keep newer position
            existing_dt = datetime.fromisoformat(aircraft_data[tail]['timestamp'])
            new_dt = datetime.fromisoformat(iso_timestamp)
            if new_dt > existing_dt:
                aircraft_data[tail] = {
                    'status': status,
                    'at_base': at_base,
                    'last_update': timestamp_str,
                    'timestamp': iso_timestamp,
                    'latitude': pos['latitude'],
                    'longitude': pos['longitude'],
                    'altitude': pos['altitude'],
                    'velocity': pos['velocity'],
                    'heading': pos['heading'],
                    'distance_from_base': round(distance, 2)
                }
    
    return aircraft_data


def fetch_and_save():
    """Main function to fetch data and save to JSON."""
    log_path = Path(__file__).with_name("skyrouter_api_log.txt")
    
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
        log("SkyRouter API fetch started")
        
        # Fetch data
        log("Fetching flight tracking data from API...")
        raw_data = fetch_flight_tracking_data()
        
        if not raw_data:
            log("WARNING: No data received from API")
            return False
        
        log(f"Received {len(raw_data)} bytes of data")
        
        # Process data
        log("Processing position data...")
        aircraft_data = process_skyrouter_data(raw_data)
        log(f"Processed status for {len(aircraft_data)} aircraft")
        
        # Save to JSON
        output_path = Path(OUTPUT_FOLDER) / OUTPUT_FILENAME
        output_data = {
            "last_updated": datetime.now().isoformat(),
            "aircraft": aircraft_data
        }
        
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2)
        
        log(f"Data saved to {output_path}")
        
        # Log summary
        at_base = sum(1 for a in aircraft_data.values() if a['at_base'])
        away = len(aircraft_data) - at_base
        log(f"Summary: {at_base} at base, {away} away")
        
        log("SkyRouter API fetch completed successfully")
        return True
        
    except Exception as e:
        log(f"ERROR: {e}")
        import traceback
        log(traceback.format_exc())
        return False


if __name__ == '__main__':
    fetch_and_save()