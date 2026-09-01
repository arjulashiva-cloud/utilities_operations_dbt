# generate_workforce.py
# Purpose: Generate realistic simulated workforce data correlated with weather

import os
import pandas as pd
import numpy as np
import snowflake.connector
import random

# ── Seed ─────────────────────────────────────────────────────────────────────
random.seed(99)
np.random.seed(99)

# ── Connect to Snowflake ──────────────────────────────────────────────────────
conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "UTILITIES_RAW"),
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
)

print("Connected to Snowflake ✅")

# ── Load weather mart ─────────────────────────────────────────────────────────
weather_df = pd.read_sql("""
    SELECT
        weather_date,
        city_name,
        ops_risk_score,
        is_high_wind,
        is_freeze_risk,
        is_heat_alert,
        season
    FROM UTILITIES_RAW.PUBLIC_MARTS.MART_WEATHER_OPERATIONS
    ORDER BY weather_date, city_name
""", conn)

weather_df.columns = [c.lower() for c in weather_df.columns]
print(f"Loaded {len(weather_df)} weather rows ✅")

# ── Config ────────────────────────────────────────────────────────────────────
crew_types = ['Lineman', 'Engineer', 'Inspector', 'Safety Officer']

# Job type probabilities by weather condition
def get_job_type(risk_score, crew_type):
    if risk_score >= 3:
        return random.choices(
            ['Emergency Response', 'Storm Restoration', 'Damage Assessment'],
            weights=[50, 30, 20]
        )[0]
    elif risk_score == 2:
        return random.choices(
            ['Emergency Response', 'Maintenance', 'Inspection'],
            weights=[40, 35, 25]
        )[0]
    elif crew_type == 'Inspector':
        return random.choices(
            ['Inspection', 'Maintenance', 'Installation'],
            weights=[60, 25, 15]
        )[0]
    elif crew_type == 'Safety Officer':
        return random.choices(
            ['Safety Audit', 'Incident Investigation', 'Training'],
            weights=[50, 30, 20]
        )[0]
    else:
        return random.choices(
            ['Maintenance', 'Installation', 'Inspection', 'Emergency Response'],
            weights=[40, 30, 20, 10]
        )[0]

# Base crews per city per day (more crews in bigger cities)
base_crews = {
    'Denver':           8,
    'Colorado_Springs': 5,
    'Boulder':          3,
    'Fort_Collins':     3,
    'Pueblo':           2
}

# ── Generate workforce records ────────────────────────────────────────────────
records = []
record_id = 1

for _, row in weather_df.iterrows():
    city      = row['city_name']
    risk      = int(row['ops_risk_score'])
    base      = base_crews[city]

    # More crews deployed on risky days
    extra_crews = {0: 0, 1: 1, 2: 3, 3: 5, 4: 8}
    total_crews = base + extra_crews[risk]

    for crew_num in range(total_crews):
        crew_type = random.choice(crew_types)
        job_type  = get_job_type(risk, crew_type)

        # Hours: overtime more likely on high-risk days
        if risk >= 3:
            hours = round(random.uniform(10, 16), 1)
            is_overtime = 1
        elif risk == 2:
            hours = round(random.uniform(8, 12), 1)
            is_overtime = 1 if hours > 8 else 0
        else:
            hours = round(random.uniform(7, 9), 1)
            is_overtime = 1 if hours > 8 else 0

        # Hourly rate by crew type
        rate_map = {
            'Lineman':        random.uniform(45, 65),
            'Engineer':       random.uniform(60, 85),
            'Inspector':      random.uniform(40, 55),
            'Safety Officer': random.uniform(50, 70)
        }
        hourly_rate = round(rate_map[crew_type], 2)
        labor_cost  = round(hours * hourly_rate * (1.5 if is_overtime else 1), 2)

        records.append({
            'record_id':    record_id,
            'work_date':    row['weather_date'],
            'city':         city,
            'crew_id':      f"{city[:2].upper()}-CREW-{crew_num + 1:02d}",
            'crew_type':    crew_type,
            'job_type':     job_type,
            'hours_worked': hours,
            'is_overtime':  is_overtime,
            'hourly_rate':  hourly_rate,
            'labor_cost':   labor_cost,
            'risk_score':   risk
        })
        record_id += 1

workforce_df = pd.DataFrame(records)
print(f"\nGenerated {len(workforce_df)} workforce records ✅")
print(f"\nRecords by city:\n{workforce_df['city'].value_counts()}")
print(f"\nRecords by crew type:\n{workforce_df['crew_type'].value_counts()}")
print(f"\nTotal labor cost: ${workforce_df['labor_cost'].sum():,.0f}")

# ── Load to Snowflake ─────────────────────────────────────────────────────────
cursor = conn.cursor()

cursor.execute("""
    CREATE OR REPLACE TABLE UTILITIES_RAW.PUBLIC.RAW_WORKFORCE (
        record_id    INTEGER,
        work_date    DATE,
        city         VARCHAR(50),
        crew_id      VARCHAR(20),
        crew_type    VARCHAR(50),
        job_type     VARCHAR(50),
        hours_worked FLOAT,
        is_overtime  INTEGER,
        hourly_rate  FLOAT,
        labor_cost   FLOAT,
        risk_score   INTEGER
    )
""")

rows = [
    (
        row['record_id'],
        str(row['work_date']),
        row['city'],
        row['crew_id'],
        row['crew_type'],
        row['job_type'],
        row['hours_worked'],
        row['is_overtime'],
        row['hourly_rate'],
        row['labor_cost'],
        row['risk_score']
    )
    for _, row in workforce_df.iterrows()
]

batch_size = 200
total_inserted = 0
for i in range(0, len(rows), batch_size):
    batch = rows[i:i + batch_size]
    cursor.executemany("""
        INSERT INTO UTILITIES_RAW.PUBLIC.RAW_WORKFORCE
        (record_id, work_date, city, crew_id, crew_type, job_type,
         hours_worked, is_overtime, hourly_rate, labor_cost, risk_score)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, batch)
    total_inserted += len(batch)
    print(f"  Inserted {total_inserted}/{len(rows)} rows...")

print(f"\nLoaded {total_inserted} rows into RAW_WORKFORCE ✅")
conn.close()
print("\nDone! 🎉")