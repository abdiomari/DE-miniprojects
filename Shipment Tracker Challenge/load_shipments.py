"""
python script to clean data 
"""

import pandas as pd

cols = ['carrier_name', 'pickup_city', 'pickup_state', 
             'delivery_city', 'delivery_state']

df = pd.read_csv(
    "data/raw/shipments_YYYYMMDD.csv",
    parse_dates=['scheduled_delivery', 'actual_delivery'], 
    na_values=['', ' ']
    )
df[cols] = df[cols].apply(lambda x: x.str.lower().str.strip())
# df[cols] = df[cols].apply(lambda x: x.str.strip())

print(df.head())

df.to_csv('data/clean/shipments.csv', index=False)

