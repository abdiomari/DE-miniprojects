"""
python script to clean data 
"""

import pandas as pd
import sys
from pathlib import Path

cols = ['carrier_name', 'pickup_city', 'delivery_city', 'pickup_state', 'delivery_state']


def extract(filepath):
    if not filepath.exists():
        sys.exit(1)
    
    df = pd.read_csv( filepath, na_values=['', ' '])
    return df


    # ,
    # parse_dates=['scheduled_delivery', 'actual_delivery'], 
    # na_values=['', ' ']
def clean_text(df):
    df[cols] = df[cols].apply(lambda x: x.str.strip())
    return df 

def parse_dates_safely(df):
    for col in ['scheduled_delivery', 'actual_delivery']:
        df[ col ] = pd.to_datetime(df[col], errors='coerce')
        n_null = df[col].isna().sum()
    return df 

# df[cols] = df[cols].apply(lambda x: x.str.lower())


# print(df.head())

# df.to_csv('data/clean/shipments.csv')

def main():
    input_path = Path("data/raw/shipments_YYYYMMDD.csv")
    output_path  = Path('data/clean/shipments_2.csv')

    df = extract(input_path)
    df = clean_text(df)
    df = parse_dates_safely(df)

    # if validate(df):
    #     output_path.parent.mkdir(exist_ok=True)
    #     df.to_csv(output_path, index=False)
    # else:
    #     sys.exit(1)
    df.to_csv(output_path, index=False)
    
if __name__ == "__name__":
    main()

