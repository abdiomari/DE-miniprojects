"""
setup_database.py - Create SQLite database and load cleaned shipments.
Run this once to set up your Day 3 environment.
"""
import pandas as pd
import sqlite3

# Read the cleaned CSV you already created
df = pd.read_csv("data/clean/shipments.csv", parse_dates=['scheduled_delivery', 'actual_delivery'])

# Create SQLite database
conn = sqlite3.connect("omnifreight.db")

# Load shipments
df.to_sql("raw_shipments", conn, if_exists="replace", index=False)

# Load carrier directory
carriers = pd.read_csv("data/raw/carrier_directory.csv")
carriers.to_sql("carrier_directory", conn, if_exists="replace", index=False)

# Load lane rates
rates = pd.read_csv("data/raw/lane_rates.csv")
rates.to_sql("lane_rates", conn, if_exists="replace", index=False)

print("Database setup complete.")
print(f"raw_shipments: {len(df)} rows")
print(f"carrier_directory: {len(carriers)} rows")
print(f"lane_rates: {len(rates)} rows")

conn.close()