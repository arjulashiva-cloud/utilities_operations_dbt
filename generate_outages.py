# generate_outages.py
# Purpose: Generate realistic simulated outage data correlated with weather

import os
import pandas as pd
import numpy as np
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import random

# ── Seed for reproducibility ──────────────────────────────────────────────────
random.seed(42)
np.random.seed(42)

# ── Connect to Snowflake ──────────────────────────────────────────────────────
conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "UTILITIES_RAW"),
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
)

# ── Load weather data ─────────────────────────────────────────────────────────
weather_df = pd.read_sql("""
    SELECT 
        weather_date,
        city_name,
        is_high_wind,
        is_freeze_risk,
        is_heat_alert,
        is_heavy_precip,
        ops_risk_score
    FROM UTILITIES_RAW.PUBLIC_MARTS.MART_WEATHER_OPERATIONS
    ORDER BY weather_date, city_name
""", conn)

print(f"Loaded {len(weather_df)} weather rows ✅")

weather_df.columns = [c.lower() for c in weather_df.columns]

# ── City circuits (neighborhoods/districts within each city) ──────────────────
# Real utilities track outages at circuit level, not just city level
city_circuits = {
    'Denver':            ['Circuit-D1', 'Circuit-D2', 'Circuit-D3', 'Circuit-D4',
                          'Circuit-D5', 'Circuit-D6', 'Circuit-D7', 'Circuit-D8'],
    'Colorado_Springs':  ['Circuit-CS1', 'Circuit-CS2', 'Circuit-CS3',
                          'Circuit-CS4', 'Circuit-CS5'],
    'Boulder':           ['Circuit-B1', 'Circuit-B2', 'Circuit-B3'],
    'Fort_Collins':      ['Circuit-FC1', 'Circuit-FC2', 'Circuit-FC3'],
    'Pueblo':            ['Circuit-P1', 'Circuit-P2']
}

# ── Customers per circuit ─────────────────────────────────────────────────────
circuit_customers = {
    'Denver':           (2000, 12000),
    'Colorado_Springs': (1000, 6000),
    'Boulder':          (800,  4000),
    'Fort_Collins':     (600,  3500),
    'Pueblo':           (400,  2500)
}

# ── Outage cause logic ────────────────────────────────────────────────────────
def get_cause(row):
    if row['is_high_wind'] == 1:
        return random.choices(
            ['Wind Damage', 'Tree Contact', 'Equipment Failure', 'Downed Line'],
            weights=[35, 30, 20, 15]
        )[0]
    elif row['is_freeze_risk'] == 1:
        return random.choices(
            ['Ice on Lines', 'Frozen Equipment', 'Equipment Failure', 'Pipe Burst'],
            weights=[40, 25, 25, 10]
        )[0]
    elif row['is_heavy_precip'] == 1:
        return random.choices(
            ['Lightning Strike', 'Flooding', 'Tree Contact', 'Equipment Failure'],
            weights=[35, 30, 20, 15]
        )[0]
    elif row['is_heat_alert'] == 1:
        return random.choices(
            ['Overload', 'Equipment Failure', 'Transformer Failure', 'Planned Outage'],
            weights=[40, 30, 20, 10]
        )[0]
    else:
        return random.choices(
            ['Equipment Failure', 'Planned Maintenance', 'Animal Contact',
             'Vehicle Accident', 'Unknown'],
            weights=[35, 25, 20, 10, 10]
        )[0]

# ── Generate outages ──────────────────────────────────────────────────────────
# Number of outages per city per day uses Poisson distribution
# Lambda (avg outages) scales with risk score
risk_to_lambda = {0: 1.5, 1: 3.0, 2: 6.0, 3: 10.0, 4: 15.0}

outages = []
outage_id = 1

for _, row in weather_df.iterrows():
    city = row['city_name']
    risk = int(row['ops_risk_score'])
    circuits = city_circuits[city]
    cust_min, cust_max = circuit_customers[city]

    # How many outage events today for this city?
    lam = risk_to_lambda[risk]
    num_outages = np.random.poisson(lam)
    num_outages = max(1, num_outages)  # at least 1 per city per day

    for _ in range(num_outages):
        cause = get_cause(row)

        # Duration based on cause and risk
        if 'Planned' in cause:
            duration = round(random.uniform(2, 6), 1)
        elif risk >= 3:
            duration = round(random.uniform(4, 24), 1)
        else:
            duration = round(random.uniform(0.5, 10), 1)

        outages.append({
            'outage_id':          outage_id,
            'outage_date':        row['weather_date'],
            'city':               city,
            'circuit':            random.choice(circuits),
            'cause':              cause,
            'duration_hours':     duration,
            'customers_affected': random.randint(cust_min, cust_max),
            'weather_risk_score': risk
        })
        outage_id += 1

outages_df = pd.DataFrame(outages)
print(f"\nGenerated {len(outages_df)} outage records ✅")
print(f"\nOutages by city:\n{outages_df['city'].value_counts()}")
print(f"\nOutages by cause:\n{outages_df['cause'].value_counts()}")

# ── Load to Snowflake ─────────────────────────────────────────────────────────
cursor = conn.cursor()

# Create table
cursor.execute("""
    CREATE OR REPLACE TABLE UTILITIES_RAW.PUBLIC.RAW_OUTAGES (
        outage_id           INTEGER,
        outage_date         DATE,
        city                VARCHAR(50),
        circuit             VARCHAR(20),
        cause               VARCHAR(100),
        duration_hours      FLOAT,
        customers_affected  INTEGER,
        weather_risk_score  INTEGER
    )
""")

# Build list of tuples from dataframe
rows = [
    (
        row['outage_id'],
        str(row['outage_date']),
        row['city'],
        row['circuit'],
        row['cause'],
        row['duration_hours'],
        row['customers_affected'],
        row['weather_risk_score']
    )
    for _, row in outages_df.iterrows()
]

# Insert in batches of 200
batch_size = 200
total_inserted = 0
for i in range(0, len(rows), batch_size):
    batch = rows[i:i + batch_size]
    cursor.executemany("""
        INSERT INTO UTILITIES_RAW.PUBLIC.RAW_OUTAGES
        (outage_id, outage_date, city, circuit, cause,
         duration_hours, customers_affected, weather_risk_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, batch)
    total_inserted += len(batch)
    print(f"  Inserted {total_inserted}/{len(rows)} rows...")

print(f"\nLoaded {total_inserted} rows into RAW_OUTAGES ✅")

conn.close()
print("\nDone! 🎉")