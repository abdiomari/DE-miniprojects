### The Scenario (Simplified but Still Real)   

You've just joined OmniFreight's small data team. The operations manager, Sarah, comes to you with a USB drive and says:

```
"Every morning I download three spreadsheets from our old TMS system and try to figure out which carriers were late yesterday. It takes me 2 hours of VLOOKUPs. Can you just... automate this?"
```

This is Classic Junior DE Work: Taking messy, manual Excel processes and turning them into reliable, queryable data.

### Deliverables

#### Task 1 : Python Scripting & Data Cleaning

Task: Write a single Python script called load_shipments.py that:

 - Reads the daily CSV file.

 - Cleans the data (lowercase all strings, strip whitespace, parse dates safely).

 - Handles the missing actual_delivery (should be NULL in database, not empty string).

 - Outputs a clean CSV ready for database import.

#### Learning Objectives Tested:

 - File I/O with pathlib

 - String methods (.lower(), .strip(), .replace())

 - pandas basics (or csv module)

 - datetime parsing with error handling (try/except)


```python
 # Write a function that takes this raw row:
raw_row = {
    'shipment_id': 'S003',
    'carrier_name': 'FAST TRUCKING  ',  # Note trailing spaces
    'pickup_city': 'chicago',
    'scheduled_delivery': '2024-01-15 16:00',  # Missing seconds
    'actual_delivery': ''  # Empty string, not NULL
}

# And returns this:
cleaned_row = {
    'shipment_id': 'S003',
    'carrier_name': 'fast trucking',  # normalized for joining
    'pickup_city': 'chicago',
    'scheduled_delivery': datetime(2024, 1, 15, 16, 0, 0),
    'actual_delivery': None  # Python None for SQL NULL
}
```

### Task 2: Basic SQL - Building the Foundation

Task: Sarah needs answers. The cleaned data is now in a PostgreSQL table called raw_shipments. Write SQL queries to answer these business questions.

SQL Challenge 1: The Morning Report
"Sarah wants to see yesterday's shipments that were late (actual > scheduled) OR still missing delivery confirmation."

```sql
-- Write this query
SELECT 
    shipment_id,
    carrier_name,
    pickup_city || ', ' || pickup_state as origin,
    delivery_city || ', ' || delivery_state as destination,
    scheduled_delivery,
    actual_delivery,
    CASE 
        WHEN actual_delivery IS NULL THEN 'MISSING CONFIRMATION'
        WHEN actual_delivery > scheduled_delivery THEN 'LATE'
        ELSE 'ON TIME'
    END as status
FROM raw_shipments
WHERE 
    DATE(scheduled_delivery) = CURRENT_DATE - INTERVAL '1 day'
    AND (actual_delivery IS NULL OR actual_delivery > scheduled_delivery)
ORDER BY status, carrier_name;
```


### SQL Challenge 2: Carrier Performance Summary
"Which carriers had the most late shipments last month?"

```sql
-- Write this query (tests GROUP BY and JOIN)
SELECT 
    c.scac_code,
    c.carrier_legal_name,
    COUNT(s.shipment_id) as total_shipments,
    COUNT(CASE WHEN s.actual_delivery > s.scheduled_delivery THEN 1 END) as late_shipments,
    ROUND(
        100.0 * COUNT(CASE WHEN s.actual_delivery > s.scheduled_delivery THEN 1 END) / COUNT(s.shipment_id),
        1
    ) as late_percentage
FROM raw_shipments s
JOIN carrier_directory c ON LOWER(s.carrier_name) LIKE '%' || LOWER(c.carrier_legal_name) || '%'  -- Fuzzy join! (we'll fix this)
WHERE s.scheduled_delivery >= DATE_TRUNC('month', CURRENT_DATE - INTERVAL '1 month')
GROUP BY c.scac_code, c.carrier_legal_name
HAVING COUNT(s.shipment_id) >= 10  -- Only carriers with meaningful volume
ORDER BY late_percentage DESC;
```

### Task 3: Data Modeling - From Messy to Structured

Task: You realize the fuzzy join is unreliable. You need to create proper dimension and fact tables.

#### Data Modeling Exercise (Pen and Paper + DDL):

 - Design these three tables:

    - dim_carrier (Surrogate Key: carrier_id)

    - dim_date (Calendar dimension - *hint: pre-populate with dates for 5 years*)

    - fct_shipment (Contains foreign keys to dimensions, plus metrics)

#### SQL DDL Challenge:

Write the CREATE TABLE statements.

```sql
-- Example of what you should write:
CREATE TABLE dim_carrier (
    carrier_id INTEGER PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
    carrier_name VARCHAR(100) NOT NULL,
    scac_code VARCHAR(4) UNIQUE,
    contact_phone VARCHAR(20),
    insurance_status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE fct_shipment (
    shipment_id VARCHAR(20) PRIMARY KEY,
    carrier_id INTEGER REFERENCES dim_carrier(carrier_id),
    scheduled_delivery_date_id INTEGER REFERENCES dim_date(date_id),
    actual_delivery_date_id INTEGER REFERENCES dim_date(date_id),
    pickup_city VARCHAR(50),
    pickup_state CHAR(2),
    delivery_city VARCHAR(50),
    delivery_state CHAR(2),
    is_late BOOLEAN GENERATED ALWAYS AS (actual_delivery_date_id > scheduled_delivery_date_id) STORED,
    -- ^ This is a computed column - saves you writing CASE statements everywhere!
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### The Transformation Challenge:

Write the Python code that takes the cleaned raw_shipments DataFrame and:

1. Looks up or inserts the correct carrier_id (handling the duplicate spelling issue from earlier).

2. Looks up the date_id from the pre-loaded dim_date table.

3. Inserts into fct_shipment.

### Task 4: Putting It All Together - A Simple ETL Pipeline

Task: Create a single runnable Python script daily_etl.py that Sarah (or a cron job) can execute each morning.

```python
import pandas as pd
import psycopg2  # PostgreSQL adapter
from datetime import datetime
import logging

# Setup logging so you know if something broke
logging.basicConfig(level=logging.INFO)

def extract(filepath):
    """Read CSV and handle encoding issues."""
    pass

def transform(raw_df, conn):
    """Clean data and resolve carrier_id keys."""
    pass

def load(clean_df, conn):
    """Insert into fct_shipment, handle duplicates gracefully."""
    pass

def main():
    today = datetime.now().strftime('%Y%m%d')
    filepath = f'/data/daily_feeds/shipments_{today}.csv'
    
    conn = psycopg2.connect("dbname=omnifreight user=etl_user")
    
    try:
        raw_df = extract(filepath)
        clean_df = transform(raw_df, conn)
        rows_inserted = load(clean_df, conn)
        conn.commit()
        logging.info(f"Success! Inserted {rows_inserted} records.")
    except Exception as e:
        logging.error(f"ETL Failed: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    main()

```

### The "Real World" Twist - Error Handling:
On Task 4, imagine this happens:

 - Sarah renames the CSV file because she's "organizing."

 - A carrier is in the shipment file but missing from dim_carrier.

Your script needs to:

- Exit gracefully with a clear error message (e.g., "ERROR: Missing carrier 'Fast Trucking' in dim_carrier table. Please add them first.").

 - Write the bad rows to an error_log.csv for manual review.


 | Skill Area |	What You Actually Do |
 | ----------- |   --------------  | 
| Python      | 	File handling, string cleaning, datetime parsing, database connections, error handling.|
| SQL |	SELECT, WHERE, JOIN, GROUP BY, CASE, date functions, CREATE TABLE, foreign keys. |
| Data Modeling |	Understanding why we use surrogate keys (carrier_id vs carrier_name). |
| ETL Fundamentals |	Extract → Transform → Load pattern. Handling bad data without crashing. |
| Business Context	| Translating "Who was late?" into a SQL query that management can trust. |