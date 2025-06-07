from src.data_ingestion import get_data
from src.preprocessing import preprocess
from src.database import init_db, insert_data

def main():
    print(f"Running pipeline")
    print(f"Creating DB if not exists")
    conn = init_db()
    print(f"DB initialization complete")
    for data in get_data():
        print(f"Preprocessing data started")
        clean = preprocess(data)
        print(f"Preprocessing data completed")
        print(f"Inserting data into DB")
        insert_data(conn, clean)
        print(f"Inserted: {clean}")

if __name__ == "__main__":
    main()