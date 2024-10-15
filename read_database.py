import sqlite3
import pandas as pd
import os

# URLs to your GitHub-hosted databases
databases = [
    "https://raw.githubusercontent.com/chiragpalan/time_series_prediction_v1/main/stock_data.db",
    "https://raw.githubusercontent.com/chiragpalan/time_series_prediction_v1/main/technical_features.db",
    "https://raw.githubusercontent.com/chiragpalan/time_series_prediction_v1/main/another_database.db"
]

def fetch_and_store(db_url, db_name):
    db_path = f"{db_name}.db"
    # Download the database locally
    os.system(f"wget {db_url} -O {db_path}")
    return db_path

# Initialize markdown content
markdown_content = "# Database Content\n\n"

for db_url in databases:
    db_name = db_url.split('/')[-1].replace('.db', '')
    db_path = fetch_and_store(db_url, db_name)

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Fetch all table names
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]

    markdown_content += f"## {db_name}\n\n"
    
    # Read and display each table's data (first 5 rows)
    for table in tables:
        df = pd.read_sql_query(f"SELECT * FROM {table} ORDER BY date DESC;", conn)
        markdown_content += f"### {table}\n\n```\n{df.to_string(index=False)}\n```\n\n"

    conn.close()
    os.remove(db_path)  # Clean up

# Write to README.md
with open("README.md", "w") as f:
    f.write(markdown_content)

print("README.md updated successfully.")
