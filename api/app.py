# from flask import Response
# import time
# import sqlite3


# @app.route('/api/stream')
# def stream():
#     conn = sqlite3.connect("../data.db")
#     c = conn.cursor()
#     last_ts = 0
#     while True:
#         c.execute('SELECT * from stream WHERE timestamp > ? ORDER BY timestamp ASC', (last_ts))
#         rows = c.fetchall()
#         if rows:
#             for r in rows:
#                 last_ts = r[0]
#                 yield f"data: {r[1]}\n\n"
#         time.sleep(1)
# return Response(event_stream(), mimetype="text/event-stream")

from flask import Flask, jsonify
import sqlite3

app = Flask(__name__)

def get_latest_data():
    conn = sqlite3.connect("../data.db")   # Adjust path as needed
    c = conn.cursor()
    c.execute('SELECT * FROM stream ORDER BY timestamp DESC LIMIT 50')
    rows = c.fetchall()
    conn.close()
    # Format as list of dicts
    data = [{"timestamp": r[0], "value": r[1]} for r in rows]
    return data

@app.route('/api/data', methods=['GET'])
def api_data():
    return jsonify(get_latest_data())

if __name__ == '__main__':
    app.run(debug=True)