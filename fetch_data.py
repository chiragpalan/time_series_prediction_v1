import yfinance as yf
import sqlite3
from datetime import datetime

# Create or connect to an SQLite database
conn = sqlite3.connect('stocks_data.db')
cursor = conn.cursor()

# Create tables if they don't exist
cursor.execute('''CREATE TABLE IF NOT EXISTS Reliance (
                    Date TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, Volume INTEGER)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS TCS (
                    Date TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, Volume INTEGER)''')

cursor.execute('''CREATE TABLE IF NOT EXISTS Infosys (
                    Date TEXT PRIMARY KEY, Open REAL, High REAL, Low REAL, Close REAL, Volume INTEGER)''')

# Function to fetch and insert stock data
def fetch_and_store(ticker, table_name):
    data = yf.download(ticker, period='1d', interval='1d')
    data.reset_index(inplace=True)
    for _, row in data.iterrows():
        cursor.execute(f'''INSERT OR REPLACE INTO {table_name} (Date, Open, High, Low, Close, Volume) 
                           VALUES (?, ?, ?, ?, ?, ?)''',
                       (row['Date'].strftime('%Y-%m-%d'), row['Open'], row['High'], 
                        row['Low'], row['Close'], row['Volume']))
    conn.commit()

# Fetch and store data for Reliance, TCS, and Infosys
fetch_and_store('RELIANCE.NS', 'Reliance')
fetch_and_store('TCS.NS', 'TCS')
fetch_and_store('INFY.NS', 'Infosys')

# Close the database connection
conn.close()
