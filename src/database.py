import sqlite3

def init_db():
    conn = sqlite3.connect("data.db")
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS stream (timestamp REAL, value REAL)''')
    conn.commit()
    return conn

def insert_data(conn, data):
    c = conn.cursor()
    c.execute('INSERT INTO stream (timestamp, value) VALUES (?, ?)', (data['timestamp'], data['value']))
    conn.commit()