import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine
import os

# Database credentials from environment variables
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")

# Create the engine for PostgreSQL connection
DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URL)

TABLE_NAME = "nifty100_data"

def download_and_store_data(ticker="^NSEI", period="1d"):
    # Download the data
    data = yf.download(ticker, period=period)
    data.reset_index(inplace=True)  # Ensure 'Date' is a column

    # Append the data to the database table
    data.to_sql(TABLE_NAME, engine, if_exists='append', index=False)
    print(f"Data for {ticker} stored successfully.")

if __name__ == "__main__":
    download_and_store_data()

