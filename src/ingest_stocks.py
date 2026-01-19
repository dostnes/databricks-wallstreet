import importlib
import subprocess
import sys


# Ensure required packages are installed
def install_and_import(package_name):
    try:
        importlib.import_module(package_name)
    except ImportError:
        print(f"📦 Installing package: {package_name}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
        print(f"✅ Package {package_name} installed successfully.")
        importlib.invalidate_caches()


install_and_import("yfinance")

import pandas as pd
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp

# Config
# Mag 7 + OSEBX
TICKERS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "^OSEBX"]
START_DATE = "2020-01-01"
END_DATE = "2024-12-31"


def get_spark_session():
    return SparkSession.builder.appName("StockDataIngestion").getOrCreate()


def fetch_stock_data(tickers):
    print(f"Fetching data for tickers: {tickers}")

    # Last ned data
    data = yf.download(tickers, start=START_DATE, end=END_DATE, group_by="ticker")
    all_stocks = []

    for ticker in tickers:
        try:
            # yfinance returnerer noen ganger tomme dataframes
            # vi må sjekke om tickeren finnes i data
            if ticker not in data:
                print(f"⚠️ Warning: Ticker {ticker} not found in response.")
                continue

            df_ticker = data[ticker].copy()

            if df_ticker.empty:
                print(f"⚠️ Warning: No data rows for ticker: {ticker}")
                continue

            # --- FIX 1: Håndter dato (flytt fra Index til Kolonne) ---
            # Vi resetter index først, da blir 'Date' (eller 'Datetime')
            # en vanlig kolonne
            df_ticker = df_ticker.reset_index()

            # Vi standardiserer navnet til 'transaction_date'
            # Vi sjekker første kolonne, da yfinance av og til bytter
            if "Date" in df_ticker.columns:
                df_ticker = df_ticker.rename(columns={"Date": "transaction_date"})
            else:
                # Fallback: Døp om den første kolonnen (som var indeksen)
                first_col = df_ticker.columns[0]
                df_ticker = df_ticker.rename(columns={first_col: "transaction_date"})

            # Legg til ticker-navn
            df_ticker["ticker"] = ticker

            all_stocks.append(df_ticker)

        except Exception as e:
            print(f"❌ Error processing ticker {ticker}: {e}")

    # --- FIX 2: Slå sammen listen til én stor DataFrame ---
    if all_stocks:
        return pd.concat(all_stocks)
    else:
        return pd.DataFrame()  # Returner tom tabell hvis alt feilet


# Main ingestion function
print("🚀 Starting ingestion...")
pdf = fetch_stock_data(TICKERS)

if not pdf.empty:
    print(f"✅ Fetched {len(pdf)} rows of stock data.")

    # Convert to Spark DataFrame
    spark = get_spark_session()

    # Pandas er litt løs på datatyper, Spark er streng.
    # Vi sikrer at dato er datetime før vi sender til Spark
    pdf["transaction_date"] = pd.to_datetime(pdf["transaction_date"])

    df_spark = spark.createDataFrame(pdf)

    # Fix column names (replace spaces and convert to lower case)
    for col_name in df_spark.columns:
        new_name = col_name.lower().replace(" ", "_")
        df_spark = df_spark.withColumnRenamed(col_name, new_name)

    # Add ingestion metadata
    df_final = df_spark.withColumn("ingestion_timestamp", current_timestamp())

    # Save to Databricks
    target_table = "wallstreet_bronze.raw_stocks"

    print(f"💾 Writing data to table: {target_table}")
    df_final.write.mode("overwrite").format("delta").saveAsTable(target_table)

    print("🎉 Data ingestion completed successfully.")
    if "display" not in globals():

        def display(*args):
            print(*args)

else:
    print("❌ No data fetched. Check internet connection or tickers.")
