from src.utils import setup_logger
import numpy as np
import pandas as pd
from math import radians, sin, cos, sqrt, atan2
from datetime import datetime

logger = setup_logger()

# === Compute GPS distance between points ===
def haversine(lat1, lon1, lat2, lon2):
    R = 6371000  # radius of Earth in meters
    phi1, phi2 = radians(lat1), radians(lat2)
    d_phi = radians(lat2 - lat1)
    d_lambda = radians(lon2 - lon1)
    a = sin(d_phi / 2) ** 2 + cos(phi1) * cos(phi2) * sin(d_lambda / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

# === Direction inference ===
def infer_direction(accX, accY, accZ):
    max_axis = max(abs(accX), abs(accY), abs(accZ))
    if max_axis == abs(accX):
        return "Right" if accX > 0 else "Left"
    elif max_axis == abs(accY):
        return "Forward" if accY > 0 else "Backward"
    else:
        return "Up" if accZ > 0 else "Down"

# === Enhanced Event Detection ===    
def detect_event(acc_mag, jerk, accZ, direction):
    if jerk is None or np.isnan(jerk):
        return "Normal"
    if acc_mag < 1.5:
        return f"Hard Fall - {direction}"
    elif 1.5 <= acc_mag < 2:
        return f"Soft Fall - {direction}"
    elif acc_mag > 20 and abs(jerk) > 10:
        return f"Severe Crash - {direction}"
    elif acc_mag > 15 and abs(jerk) > 7:
        return f"Moderate Crash - {direction}"
    elif abs(jerk) > 8:
        return f"Hard Push - {direction}"
    elif abs(jerk) > 5:
        return f"Soft Push - {direction}"
    elif accZ > 7:
        return f"Lift Up - {direction}"
    elif accZ < -7:
        return f"Put Down - {direction}"
    return "Normal"

def preprocess(record, prev_record=None):
    # Parse timestamp
    ts = pd.to_datetime(record['timestamp'])

    # Acceleration magnitude
    acc_mag = np.sqrt(record['accX'] ** 2 + record['accY'] ** 2 + record['accZ'] ** 2)

    # Jerk and GPS distance (need previous record)
    jerk = None
    distance = 0
    delta_time = None
    speed = None
    rel_x = None
    rel_y = None
    if prev_record:
        prev_ts = pd.to_datetime(prev_record['timestamp'])
        delta_time = (ts - prev_ts).total_seconds()
        prev_acc_mag = np.sqrt(prev_record['accX'] ** 2 + prev_record['accY'] ** 2 + prev_record['accZ'] ** 2)
        jerk = (acc_mag - prev_acc_mag) / delta_time if delta_time != 0 else None
        distance = haversine(prev_record['gpsLat'], prev_record['gpsLon'], record['gpsLat'], record['gpsLon'])
        speed = distance / delta_time if delta_time != 0 else 0
        # Relative GPS coordinates (origin: first record)
        origin_lat = prev_record.get('origin_lat', prev_record['gpsLat'])
        origin_lon = prev_record.get('origin_lon', prev_record['gpsLon'])
        rel_x = radians(record['gpsLon'] - origin_lon) * 6371000 * cos(radians(origin_lat))
        rel_y = radians(record['gpsLat'] - origin_lat) * 6371000
    else:
        jerk = np.nan
        distance = 0
        delta_time = np.nan
        speed = 0
        rel_x = 0
        rel_y = 0
        # Store initial GPS as origin for future relative coordinates
        record['origin_lat'] = record['gpsLat']
        record['origin_lon'] = record['gpsLon']

    direction = infer_direction(record['accX'], record['accY'], record['accZ'])
    event = detect_event(acc_mag, jerk, record['accZ'], direction)

    processed = {
        **record,  # original fields
        'timestamp': ts.isoformat(),  # standardize timestamp
        'acc_mag': acc_mag,
        'jerk': jerk,
        'distance': distance,
        'speed': speed,
        'direction': direction,
        'event': event,
        'rel_x': rel_x,
        'rel_y': rel_y,
    }
    return processed