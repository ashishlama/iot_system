import sqlite3
from src.utils import setup_logger
from datetime import datetime

logger = setup_logger()

SENSOR_COLUMNS = [
    'timestamp',   
    'accX', 'accY', 'accZ',
    'gyroX', 'gyroY', 'gyroZ',
    'gpsLat', 'gpsLon', 'gpsZ'
]

PROCESSED_COLUMNS = [
    'sensor_data_id',         
    'timestamp', 'acc_mag', 'jerk', 'distance', 'speed',
    'direction', 'event', 'rel_x', 'rel_y', 'delta_time'
]

def init_db(db_name):
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS sensor_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp TEXT,
            accX REAL,
            accY REAL,
            accZ REAL,
            gyroX REAL,
            gyroY REAL,
            gyroZ REAL,
            gpsLat REAL,
            gpsLon REAL,
            gpsZ REAL
        )
    ''')
    # Processed data table with link to raw data
    c.execute('''
        CREATE TABLE IF NOT EXISTS processed_data (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_data_id INTEGER,
            timestamp TEXT,
            acc_mag REAL,
            jerk REAL,
            distance REAL,
            speed REAL,
            direction TEXT,
            event TEXT,
            rel_x REAL,
            rel_y REAL,
            delta_time REAL,
            FOREIGN KEY (sensor_data_id) REFERENCES sensor_data(id)
        )
    ''')
    conn.commit()
    logger.info("Database initialized with sensor_data and processed_data tables.")
    return conn

def insert_data(conn, data):
    c = conn.cursor()
    values = [data.get(col) for col in SENSOR_COLUMNS]
    c.execute(f'''
        INSERT INTO sensor_data (
            timestamp, accX, accY, accZ,
            gyroX, gyroY, gyroZ,
            gpsLat, gpsLon, gpsZ
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', values)
    conn.commit()
    sensor_data_id = c.lastrowid
    logger.debug(f"Inserted raw data row with id {sensor_data_id}: {values}")
    return sensor_data_id

def insert_processed_data(conn, sensor_data_id, processed):
    c = conn.cursor()
    # Example: processed is a dict with at least 'processed_value' key
    values = [sensor_data_id] + [processed.get(col) for col in PROCESSED_COLUMNS[1:]]
    c.execute(f'''
        INSERT INTO processed_data (
            {', '.join(PROCESSED_COLUMNS)}
        ) VALUES ({', '.join(['?']*len(PROCESSED_COLUMNS))})
    ''', values)
    conn.commit()
    logger.debug(f"Inserted processed data linked to sensor_data_id {sensor_data_id}: {values}")

def get_joined_data(conn, limit=50):
    c = conn.cursor()
    c.execute('''
        SELECT s.*, p.processed_value
        FROM sensor_data s
        LEFT JOIN processed_data p ON s.id = p.sensor_data_id
        ORDER BY s.id DESC
        LIMIT ?
    ''', (limit,))
    return c.fetchall()

def get_data(conn, limit=50):
    c = conn.cursor()
    c.execute('''
        SELECT s.*, p.*
        FROM sensor_data s
        LEFT JOIN processed_data p ON s.id = p.sensor_data_id
        ORDER BY s.id DESC
        LIMIT ?
    ''', (limit,))
    return c.fetchall()

SENSOR_TYPE_TO_COLUMNS = {
    'accelerometer': ['accX', 'accY', 'accZ'],
    'gyroscope': ['gyroX', 'gyroY', 'gyroZ'],
    'gps': ['gpsLat', 'gpsLon', 'gpsZ']
    # Add more mappings as needed
}

def get_latest_sensor_data(conn, sensor_type, limit=10):
    columns = SENSOR_TYPE_TO_COLUMNS.get(sensor_type)
    if not columns:
        return []

    c = conn.cursor()
    # Also select id and timestamp for mapping to id/time fields
    c.execute(f'''
        SELECT id, timestamp, {", ".join(columns)}
        FROM sensor_data
        ORDER BY id DESC
        LIMIT ?
    ''', (limit,))
    rows = c.fetchall()

    # Format the output per your structure
    result = []
    for row in rows:
        id_val = row[0]
        ts = row[1]
        # Convert timestamp to ISO format ending with Z if not already
        # If your timestamp is already in ISO format, you can just append Z if needed
        try:
            dt = datetime.fromisoformat(ts)
            iso_ts = dt.isoformat() + "Z" if not ts.endswith("Z") else ts
        except Exception:
            # Fallback: just append Z
            iso_ts = ts if ts.endswith("Z") else ts + "Z"
        
        if sensor_type == "gyroscope":
             result.append({
            "SensorType": sensor_type,
            "id": id_val,
            "time": iso_ts,
            "alpha": row[2],
            "beta": row[3],
            "gamma": row[4],
            })
        elif sensor_type == "gps":
            result.append({
            "SensorType": sensor_type,
            "id": id_val,
            "time": iso_ts,
            "lat": row[2],
            "lng": row[3],
            "alt": row[4],
        })
        elif sensor_type == "accelerometer":
            result.append({
            "SensorType": sensor_type,
            "id": id_val,
            "time": iso_ts,
            "x": row[2],
            "y": row[3],
            "z": row[4],
        })
        
    return result

    
def get_latest_processed_data(conn, limit=10):
    columns = PROCESSED_COLUMNS
    if not columns:
        return []

    c = conn.cursor()
    # Also select id and timestamp for mapping to id/time fields
    c.execute(f'''
        SELECT  {", ".join(columns)}
        FROM processed_data
        ORDER BY id DESC
        LIMIT ?
    ''', (limit,))
    rows = c.fetchall()

    # Format the output per your structure
    result = []
    for row in rows:
        id_val = row[0]
        ts = row[1]
        # Convert timestamp to ISO format ending with Z if not already
        # If your timestamp is already in ISO format, you can just append Z if needed
        try:
            dt = datetime.fromisoformat(ts)
            iso_ts = dt.isoformat() + "Z" if not ts.endswith("Z") else ts
        except Exception:
            # Fallback: just append Z
            iso_ts = ts if ts.endswith("Z") else ts + "Z"
            
        result.append({
            "id": id_val,
            "time": iso_ts,
            "acc_mag": row[2],
            "jerk": row[3],
            "distance": row[4],
            "speed": row[5],
            "direction": row[6],
            "event": row[7],
            "rel_x": row[8],
            "rel_y": row[9],
            "delta_time": row[10]
        })

        
    return result

