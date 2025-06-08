from src.data_ingestion import sensor_data_stream
from src.preprocessing import preprocess
from src.database import init_db, insert_data, insert_processed_data
from src.utils import setup_logger
import time
from src.config import PhyPhox_HOST, RETRY_INTERVAL, DB_NAME

logger = setup_logger()

def main():
    logger.info(f"Running pipeline")

    logger.info(f"Creating DB if not exists")
    conn = init_db(DB_NAME)
    logger.info(f"DB initialization complete")

    host = PhyPhox_HOST
    retry_interval = RETRY_INTERVAL

    while True:
        had_error = False
        prev_record = None
        for data in sensor_data_stream(host):
            if 'error' in data:
                logger.error(f"Data ingestion failed: {data['error']}")
                print(f"Data ingestion failed: {data['error']}")
                had_error = True
                break  # Break the inner for-loop and go to retry
            # Valid data
            logger.info(f"Preprocessing data started")
            sensor_data_id = insert_data(conn, data)
            processed_data = preprocess(data, prev_record)
            prev_record = data
            print(processed_data)
            logger.info(f"Preprocessing data completed")
            insert_processed_data(conn, sensor_data_id, processed_data)

        if had_error:
            logger.info(f"Retrying data ingestion in {retry_interval} seconds...")
            print(f"Retrying data ingestion in {retry_interval} seconds...")
            time.sleep(retry_interval)
        else:
            # If the inner loop exited normally (not by error), break out of while True
            break

if __name__ == "__main__":
    main()