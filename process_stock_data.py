import sqlite3
import pandas as pd
import requests
import tempfile
import os

# Step 1: Download the stock_data.db from GitHub
url = "https://raw.githubusercontent.com/chiragpalan/time_series_prediction_v1/main/stock_data.db"
response = requests.get(url)
if response.status_code != 200:
    raise Exception(f"Failed to download database. Status code: {response.status_code}")

# Step 2: Write the downloaded data to a temporary file
with tempfile.NamedTemporaryFile(delete=False) as temp_db_file:
    temp_db_file.write(response.content)
    temp_db_file_path = temp_db_file.name  # Save the path of the temporary file

# Step 3: Load the stock_data.db into an in-memory SQLite database
conn_stock = sqlite3.connect(':memory:')  # Create an in-memory database
with sqlite3.connect(temp_db_file_path) as source_conn:
    source_conn.backup(conn_stock)  # Backup from downloaded db to in-memory db

# Step 4: Create a new SQLite database to store technical features
conn_tech = sqlite3.connect('technical_features.db')

# Helper function to calculate pivot points and support/resistance levels
def calculate_pivot_points(df):
    df['Pivot'] = (df['High'] + df['Low'] + df['Close']) / 3
    df['Resistance1'] = 2 * df['Pivot'] - df['Low']
    df['Support1'] = 2 * df['Pivot'] - df['High']
    df['Resistance2'] = df['Pivot'] + (df['High'] - df['Low'])
    df['Support2'] = df['Pivot'] - (df['High'] - df['Low'])

    # Calculate price_wrt_vol
    df['price_wrt_vol'] = (df['Volume'] - df['Volume'].shift(1)) / (df['Close'] - df['Open'].shift(1))

    # Calculate diff between high and low
    df["change"] = df["High"] - df["Low"]

    #creating target variable - as avg of O, H, L, C of next 7 days
    df["target_n7d"] = (
                        df[['Open', 'High', 'Low', 'Close']]
                       .shift(-7)
                       .mean(axis = 1)
                        )
    return df

# Step 5: Process each table and store results in the new database
cursor = conn_stock.cursor()
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [t[0] for t in cursor.fetchall()]

for table in tables:
    print(f"Processing: {table}")

    # Load the data from the original in-memory database
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

# Step 6: Clean up the temporary file
os.remove(temp_db_file_path)

print("Processing completed and technical_features.db created.")
