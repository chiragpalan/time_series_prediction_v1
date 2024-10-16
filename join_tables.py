import sqlite3
import pandas as pd
import requests
import tempfile
import os

# GitHub URLs for the databases
URL_DB_A = "https://raw.githubusercontent.com/chiragpalan/time_series_prediction_v1/main/stock_data.db"
URL_DB_B = "https://raw.githubusercontent.com/chiragpalan/time_series_prediction_v1/main/technical_features.db"

# Helper function to download a file and save it to a temporary location
def download_db(url):
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download {url}. Status code: {response.status_code}")
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(response.content)
        return temp_file.name

# Download both databases
db_a_path = download_db(URL_DB_A)
db_b_path = download_db(URL_DB_B)

# Open connections to both databases
conn_a = sqlite3.connect(db_a_path)
conn_b = sqlite3.connect(db_b_path)

# Create a new SQLite database to store the joined results
conn_result = sqlite3.connect("joined_data.db")

# Fetch the table names from database A
cursor_a = conn_a.cursor()
cursor_a.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = [t[0] for t in cursor_a.fetchall()]

# Iterate over tables and perform the join
for table in tables:
    technical_table = f"{table}_technical"

    # Check if the corresponding technical table exists in database B
    cursor_b = conn_b.cursor()
    cursor_b.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{technical_table}';")
    if cursor_b.fetchone() is None:
        print(f"Skipping: {technical_table} not found.")
        continue

    # Load data from both tables
    df_a = pd.read_sql_query(f"SELECT * FROM {table}", conn_a)
    df_a.drop_duplicates(subset="Date", inplace=True)
    df_b = pd.read_sql_query(f"SELECT * FROM {technical_table}", conn_b)
    df_b.drop_duplicates(subset="Date", inplace=True)

    # Perform an inner join on the 'Date' column (assuming both tables have a 'Date' column)
    joined_df = pd.merge(
        df_a, df_b, on='Date', suffixes=('_A', '_B'), how = "left")

    # Optional: Drop duplicate columns if needed
    columns_to_drop = [col for col in joined_df.columns if col.endswith('_A') and col[:-2] in df_b.columns]
    joined_df.drop(columns=columns_to_drop, inplace=True)

    # Save the joined table to the new database
    joined_df.to_sql(table, conn_result, if_exists='replace', index=False)
    

# Close all connections
conn_a.close()
conn_b.close()
conn_result.close()

# Clean up temporary files
os.remove(db_a_path)
os.remove(db_b_path)

print("All tables joined successfully and saved to joined_data.db.")
