import sqlite3
import pandas as pd
import requests
import io

# Step 1: Download the stock_data.db from GitHub
url = "https://raw.githubusercontent.com/chiragpalan/time_series_prediction_v1/main/stock_data.db"
response = requests.get(url)
if response.status_code != 200:
    raise Exception(f"Failed to download database. Status code: {response.status_code}")

# Step 2: Load the stock_data.db into an in-memory SQLite database
db_file = io.BytesIO(response.content)
conn_stock = sqlite3.connect(':memory:')
with sqlite3.connect(db_file) as f_conn:
    f_conn.backup(conn_stock)

# Step 3: Create a new SQLite database to store technical features
conn_tech = sqlite3.connect('technical_features.db')

# Helper function to calculate pivot points and support/resistance levels
def calculate_pivot_points(df):
    df['Pivot'] = (df['High'] + df['Low'] + df['Close']) / 3
    df['Resistance1'] = 2 * df['Pivot'] - df['Low']
    df['Support1'] = 2 * df['Pivot'] - df['High']
    df['Resistance2'] = df['Pivot'] + (df['High'] - df['Low'])
    df['Support2'] = df['Pivot'] - (df['High'] - df['Low'])
    return df

# Step 4: Process each table and store results in the new database
cursor = conn_stock.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [t[0] for t in cursor.fetchall()]

for table in tables:
    print(f"Processing: {table}")

    # Load the data from the original database
    df = pd.read_sql_query(f"SELECT * FROM {table}", conn_stock)

    # Calculate SMAs (7, 14, 21, 30 days)
    df['SMA_7'] = df['Close'].rolling(window=7).mean()
    df['SMA_14'] = df['Close'].rolling(window=14).mean()
    df['SMA_21'] = df['Close'].rolling(window=21).mean()
    df['SMA_30'] = df['Close'].rolling(window=30).mean()

    # Calculate EMAs (7, 14, 21, 30 days)
    df['EMA_7'] = df['Close'].ewm(span=7, adjust=False).mean()
    df['EMA_14'] = df['Close'].ewm(span=14, adjust=False).mean()
    df['EMA_21'] = df['Close'].ewm(span=21, adjust=False).mean()
    df['EMA_30'] = df['Close'].ewm(span=30, adjust=False).mean()

    # Calculate Pivot Points, Support, and Resistance levels
    df = calculate_pivot_points(df)

    # Store the new data in the technical_features database
    new_table_name = f"{table}_technical"
    df.to_sql(new_table_name, conn_tech, if_exists='replace', index=False)

# Close both connections
conn_stock.close()
conn_tech.close()

print("Processing completed and technical_features.db created.")
