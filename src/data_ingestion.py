import time
from datetime import datetime
from src.sensor_data_json import Phyphox, ticker  # Make sure sensor_data_json.py is in your PYTHONPATH
from src.utils import setup_logger
import urllib.error
import socket

logger = setup_logger()

def sensor_data_stream(
    host, 
    interval=0.5, 
    timeout=1.0, 
    sensors=None
):
    """
    Yields sensor data from Phyphox-compatible device at specified interval.
    """
    if sensors is None:
        sensors = [
            "accX", "accY", "accZ",
            "gyroX", "gyroY", "gyroZ",
            "gpsLat", "gpsLon", "gpsZ"
        ]
    time_var = "acc_time"
    t = 0

    try:
        pp = Phyphox(host, timeout=timeout)
        logger.info(f"Connecting to Phyphox at {host}")

        # Try a basic API call to ensure device is reachable
        try:
            pp.json("/get?dummy")
        except (urllib.error.URLError, socket.error, socket.timeout) as err:
            msg = f"Could not connect to Phyphox device at {host}: {err}"
            logger.error(msg)
            yield {"error": msg}
            return

        logger.info("Clearing data buffer")
        pp.clear()
        if not pp.wait_measuring(False):
            logger.warning("Buffer not cleared")
        logger.info("Starting measurement")
        pp.start()
        if not pp.wait_measuring(True):
            logger.error("Measurement didn't start")
            return
        
        logger.info("Begin streaming sensor data...")
        for _ in ticker(interval):
            buf, mea = pp.get(time_var, t, *sensors)
            if not buf:
                continue
            if not mea or round(t, 3) > round(buf[-1][0], 3):
                logger.info("Recording stopped by device")
                break
            t = buf[-1][0]
            for row in buf:
                record = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }
                for i, sensor in enumerate(sensors):
                    record[sensor] = row[i + 1]  # +1 since row[0] is time
                logger.debug(f"Ingested: {record}")
                yield record

    except Exception as e:
        logger.error(f"Data ingestion error: {e}")
        yield {"error": f"Data ingestion error: {e}"}

    finally:
        try:
            logger.info("Stopping measurement")
            pp.stop(no_sleep=True)
        except Exception as stop_err:
            logger.warning(f"Error while stopping measurement: {stop_err}")

# Example for running this directly (for debug/testing)
if __name__ == "__main__":
    HOST = "192.168.2.137"  # Replace with your Phyphox device address
    for data in sensor_data_stream(HOST):
        print(data)
