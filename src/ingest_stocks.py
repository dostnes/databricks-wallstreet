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
            df_ticker.reset_index()
            df_ticker['Ticker'] = ticker
            all_stocks.append(df_ticker)
        except KeyError:
            print(f"Data for ticker {ticker} not found in the downloaded data.")
        
    if all_stocks:
        return pd.concat(all_stocks)
    else:
        return pd.DataFrame()

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
df_final = df_spark.withColumn("ingestion_timestamp", current_timestamp)

# Save to Databricks
target_table = "wallstreet_bronze.raw_stocks"

print(f"Writing data to table: {target_table}")
df_final.write.mode("overwrite").format("delta").saveAsTable(target_table)

print("Data ingestion completed successfully.")