import sys
import subprocess
import importlib

# Ensure required packages are installed
def install_and_import(package_name):
    try:
        importlib.import_module(package_name)
    except ImportError:
        print(f"Installing package: {package_name}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
        print(f"Package {package_name} installed successfully.")

        importlib.invalidate_caches()

install_and_import("yfinance")

import yfinance as yf
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, to_date

# Config
# Mag 7 + OSEBX
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "^OSEBX"]
START_DATE = "2020-01-01"
END_DATE = "2024-12-31"

def get_spark_session():
    return SparkSession.builder \
        .appName("StockDataIngestion") \
        .getOrCreate()

def fetch_stock_data(tickers):
    print(f"Fetching data for tickers: {tickers}")
    data = yf.download(tickers, start=START_DATE, end=END_DATE, group_by='ticker')
    all_stocks = []

    for ticker in tickers:
        try:
            df_ticker = data[ticker].copy()

            if df_ticker.empty:
                print(f"No data found for ticker: {ticker}")
                continue
            
            if 'Date' in df_ticker.columns:
                df_ticker = df_ticker.rename(columns={'Date': 'date'})
            else:
                df_ticker.columns_values[0] = 'date'
            
            df_ticker["ticker"] = ticker

            all_stocks.append(df_ticker)
        
        except Exception as e:
            print(f"Error processing ticker {ticker}: {e}")
    
    return all_stocks

# Main ingestion function
pdf = fetch_stock_data(TICKERS)
print(f"Fetched {len(pdf)} rows of stock data.")

# Convert to Spark DataFrame
spark = get_spark_session()
df_spark = spark.createDataFrame(pdf)

# Fix column names
for col_name in df_spark.columns:
    new_name = col_name.lower().replace(" ", "_")
    df_spark = df_spark.withColumnRenamed(col_name, new_name)

# Add ingestion metadata
df_final = df_spark.withColumn("ingestion_timestamp", current_timestamp())

# Save to Databricks
target_table = "wallstreet_bronze.raw_stocks"

print(f"Writing data to table: {target_table}")
df_final.write.mode("overwrite").format("delta").saveAsTable(target_table)

print("Data ingestion completed successfully.")