#!/usr/bin/env python3
import json
from datetime import datetime
import socket
import time
import urllib.error
import urllib.request
import urllib.parse

class Phyphox:
    def __init__(self, url, timeout=1.000, socket_timeout_retries=4):
        u = urllib.parse.urlparse(url)
        if not u.netloc:
            u = urllib.parse.urlparse("//" + url)
        if not u.scheme:
            u = u._replace(scheme="http")
        u = u._replace(path="", params="", query="", fragment="")
        self.__addr = u.geturl()
        self.timeout = timeout
        self.socket_timeout_retries = socket_timeout_retries

    def url(self):
        return self.__addr

    def json(self, path):
        tries = 0
        while True:
            try:
                tries += 1
                with urllib.request.urlopen(self.__addr + path, timeout=self.timeout) as r:
                    if r.status != 200:
                        raise Exception("invalid phyphox response: status {r.status}")
                    return json.loads(r.read())
            except urllib.error.URLError as err:
                if isinstance(err.reason, socket.timeout):
                    if tries < self.socket_timeout_retries:
                        continue
                raise err
            except socket.timeout as err:
                if tries < self.socket_timeout_retries:
                    continue
                raise err

    def control(self, cmd):
        obj = self.json(f"/control?cmd={cmd}")
        if "result" not in obj:
            raise Exception("invalid phyphox response: does not contain result")
        if not obj["result"]:
            raise Exception(f"phyphox error: failed to execute {cmd}")

    def get(self, time_var, since=None, *vars):
        vars = [time_var, *vars]
        prt = since is not None and since >= 0
        qry = "&".join(
            [f"{time_var}={since:.4f}" if prt else time_var] +
            [f"{var}={since:.4f}|{time_var}" if prt else var for var in vars])
        obj = self.json(f"/get?{qry}")
        if "status" not in obj or "measuring" not in obj["status"] or "buffer" not in obj:
            raise Exception("invalid phyphox response")
        mea = obj["status"]["measuring"]
        dat = dict()
        for var in vars:
            if var not in obj["buffer"]:
                raise Exception(f"phyphox error: variable {var} does not exist for the current experiment")
            buf = obj["buffer"][var]["buffer"]
            if len(buf) == 0 or (len(buf) == 1 and buf[0] is None):
                buf = []
            dat[var] = buf
        dat = list(zip(*[dat[var] for var in vars]))
        return dat, mea

    def is_measuring(self):
        obj = self.json("/get?dummy")
        return obj["status"].get("measuring", False)

    def wait_measuring(self, measuring=True, tries=4, interval=0.25):
        for _ in range(tries):
            if self.is_measuring() == measuring:
                return True
            time.sleep(interval)
        return False

    def clear(self, no_sleep=False):
        self.control("clear")
        if not no_sleep:
            time.sleep(0.25)

    def stop(self, no_sleep=False):
        self.control("stop")
        if not no_sleep:
            time.sleep(0.25)

    def start(self, no_sleep=False):
        self.control("start")
        if not no_sleep:
            time.sleep(0.5)

def ticker(interval, resolution=0.01):
    ts = time.monotonic()
    if interval <= 0:
        raise ValueError("interval must be positive")
    while True:
        tc = ts
        while True:
            cur = time.monotonic()
            if ts <= cur:
                while ts <= cur:
                    ts += interval
                break
            time.sleep(resolution)
        yield tc

if __name__ == "__main__":
    import argparse
    import sys
    import signal

    parser = argparse.ArgumentParser("phyphox_live")
    parser.add_argument("-i", "--interval", type=float, default=0.500)
    parser.add_argument("-t", "--timeout", type=float, default=1.000)
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("host", type=str)
    args = parser.parse_args()

    pp = Phyphox(args.host, timeout=args.timeout)

    print("clearing data", file=sys.stderr)
    pp.clear()
    if not pp.wait_measuring(False):
        raise Exception("phyphox didn't clear the buffer")

    print("starting recording", file=sys.stderr)
    pp.start()
    if not pp.wait_measuring(True):
        raise Exception("phyphox didn't start recording")

    def interrupt(*a):
        print("stopping recording", file=sys.stderr)
        pp.stop(no_sleep=True)
        exit(0)
    signal.signal(signal.SIGINT, interrupt)

    print("getting data", file=sys.stderr)
    t = 0
    records = []

    for _ in ticker(args.interval):
        buf, mea = pp.get(
            "acc_time", t,
            "accX", "accY", "accZ",
            "gyroX", "gyroY", "gyroZ",
            "gpsLat", "gpsLon", "gpsZ"
        )

        if not buf:
            continue

        if not mea or round(t, 3) > round(buf[-1][0], 3):
            print("recording stopped", file=sys.stderr)
            break

        t = buf[-1][0]

        for row in buf:
            record = {
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "accX": row[1],
                "accY": row[2],
                "accZ": row[3],
                "gyroX": row[4],
                "gyroY": row[5],
                "gyroZ": row[6],
                "gpsLat": row[7],
                "gpsLon": row[8],
                "gpsZ": row[9]
            }
            print(record)
            records.append(record)

    with open("sensor_data.json", "w") as f:
        json.dump(records, f, indent=2)

    print("Saved sensor data to sensor_data.json")
