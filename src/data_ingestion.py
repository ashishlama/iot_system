import time
import random

def get_data():
    while True:
        data = {"timestamp": time.time(), "value": random.random()}
        yield data
        time.sleep(1)