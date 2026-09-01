# generate_om_spend.py
# Purpose: Generate realistic O&M (Operations & Maintenance) spend data

import os
import pandas as pd
import numpy as np
import snowflake.connector
import random
from datetime import date

# ── Seed ─────────────────────────────────────────────────────────────────────
random.seed(77)
np.random.seed(77)

# ── Connect to Snowflake ──────────────────────────────────────────────────────
conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "UTILITIES_RAW"),
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
)
# ── Load weather mart — aggregated at month + city level ──────────────────────
weather_df = pd.read_sql("""
    SELECT
        city_name,
        month_num,
        year,
        season,
        SUM(ops_risk_score)     AS total_risk,
        AVG(ops_risk_score)     AS avg_risk,
        SUM(is_high_wind)       AS wind_days,
        SUM(is_freeze_risk)     AS freeze_days,
        SUM(is_heat_alert)      AS heat_days,
        COUNT(*)                AS total_days
    FROM UTILITIES_RAW.PUBLIC_MARTS.MART_WEATHER_OPERATIONS
    GROUP BY city_name, month_num, year, season
    ORDER BY year, month_num, city_name
""", conn)

weather_df.columns = [c.lower() for c in weather_df.columns]
print(f"Loaded {len(weather_df)} month-city combinations ✅")

# ── Config ────────────────────────────────────────────────────────────────────
asset_types = [
    'Transformer',
    'Power Line',
    'Substation',
    'Smart Meter',
    'Distribution Panel',
    'Underground Cable'
]

cost_categories = ['Labor', 'Materials', 'Contractor', 'Equipment Rental']

# Base monthly spend ranges by city size ($)
city_base_spend = {
    'Denver':           (80000,  150000),
    'Colorado_Springs': (50000,  90000),
    'Boulder':          (30000,  60000),
    'Fort_Collins':     (25000,  55000),
    'Pueblo':           (15000,  35000)
}

# ── Generate O&M spend records ────────────────────────────────────────────────
# Multiple spend records per city per month (one per asset type per cost category)

records = []
spend_id = 1

for _, row in weather_df.iterrows():
    city      = row['city_name']
    month     = int(row['month_num'])
    year      = int(row['year'])
    avg_risk  = float(row['avg_risk'])
    wind_days = int(row['wind_days'])
    freeze_days = int(row['freeze_days'])
    heat_days = int(row['heat_days'])

    base_min, base_max = city_base_spend[city]

    # Risk multiplier — bad weather months cost more
    risk_multiplier = 1.0 + (avg_risk * 0.25)

    for asset in asset_types:
        for category in cost_categories:

            # Base amount for this asset + category combo
            base_amount = random.uniform(base_min * 0.05, base_max * 0.15)

            # Adjust by asset type — some assets cost more
            asset_multiplier = {
                'Transformer':          1.8,
                'Power Line':           1.5,
                'Substation':           2.0,
                'Smart Meter':          0.6,
                'Distribution Panel':   1.2,
                'Underground Cable':    1.7
            }[asset]

            # Adjust by cost category
            category_multiplier = {
                'Labor':            1.0,
                'Materials':        0.8,
                'Contractor':       1.3,
                'Equipment Rental': 0.6
            }[category]

            # Storm damage adds extra cost
            storm_adder = (wind_days * random.uniform(500, 2000) +
                           freeze_days * random.uniform(300, 1500) +
                           heat_days * random.uniform(200, 1000))

            # Only add storm cost to relevant asset types
            if asset in ['Power Line', 'Transformer', 'Substation']:
                extra = storm_adder * random.uniform(0.1, 0.3)
            else:
                extra = storm_adder * random.uniform(0.01, 0.05)

            amount = round(
                (base_amount * asset_multiplier * category_multiplier * risk_multiplier)
                + extra, 2
            )

            records.append({
                'spend_id':      spend_id,
                'spend_year':    year,
                'spend_month':   month,
                'city':          city,
                'asset_type':    asset,
                'cost_category': category,
                'amount_usd':    amount,
                'avg_risk_score': round(avg_risk, 2)
            })
            spend_id += 1

spend_df = pd.DataFrame(records)
print(f"\nGenerated {len(spend_df)} O&M spend records ✅")
print(f"\nSpend by city:\n{spend_df.groupby('city')['amount_usd'].sum().apply(lambda x: f'${x:,.0f}')}")
print(f"\nSpend by asset type:\n{spend_df.groupby('asset_type')['amount_usd'].sum().apply(lambda x: f'${x:,.0f}')}")
print(f"\nTotal O&M Spend: ${spend_df['amount_usd'].sum():,.0f}")

# ── Load to Snowflake ─────────────────────────────────────────────────────────
cursor = conn.cursor()

cursor.execute("""
    CREATE OR REPLACE TABLE UTILITIES_RAW.PUBLIC.RAW_OM_SPEND (
        spend_id        INTEGER,
        spend_year      INTEGER,
        spend_month     INTEGER,
        city            VARCHAR(50),
        asset_type      VARCHAR(50),
        cost_category   VARCHAR(50),
        amount_usd      FLOAT,
        avg_risk_score  FLOAT
    )
""")

rows = [
    (
        row['spend_id'],
        row['spend_year'],
        row['spend_month'],
        row['city'],
        row['asset_type'],
        row['cost_category'],
        row['amount_usd'],
        row['avg_risk_score']
    )
    for _, row in spend_df.iterrows()
]

batch_size = 200
total_inserted = 0
for i in range(0, len(rows), batch_size):
    batch = rows[i:i + batch_size]
    cursor.executemany("""
        INSERT INTO UTILITIES_RAW.PUBLIC.RAW_OM_SPEND
        (spend_id, spend_year, spend_month, city, asset_type,
         cost_category, amount_usd, avg_risk_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, batch)
    total_inserted += len(batch)
    print(f"  Inserted {total_inserted}/{len(rows)} rows...")

print(f"\nLoaded {total_inserted} rows into RAW_OM_SPEND ✅")
conn.close()
print("\nDone! 🎉")