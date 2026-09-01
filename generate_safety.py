# generate_safety.py
# Purpose: Generate realistic safety observations and incidents correlated with weather

import os

import os

import pandas as pd
import numpy as np
import snowflake.connector
import random

# ── Seed ─────────────────────────────────────────────────────────────────────
random.seed(55)
np.random.seed(55)

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
        is_heavy_precip,
        season
    FROM UTILITIES_RAW.PUBLIC_MARTS.MART_WEATHER_OPERATIONS
    ORDER BY weather_date, city_name
""", conn)

weather_df.columns = [c.lower() for c in weather_df.columns]
print(f"Loaded {len(weather_df)} weather rows ✅")

# ── Config ────────────────────────────────────────────────────────────────────
observation_types = [
    'Near Miss',
    'Unsafe Condition',
    'Positive Observation',
    'Incident',
    'Property Damage'
]

# Severity levels
severity_levels = ['Low', 'Medium', 'High', 'Critical']

# Crew types (same as workforce)
crew_types = ['Lineman', 'Engineer', 'Inspector', 'Safety Officer']

# Root causes by weather condition
def get_root_cause(row, obs_type):
    if obs_type == 'Positive Observation':
        return random.choice([
            'Proper PPE Usage',
            'Hazard Identified Early',
            'Team Communication',
            'Procedure Followed Correctly'
        ])
    if row['is_high_wind'] == 1:
        return random.choices(
            ['Wind Exposure', 'Unstable Equipment', 'Falling Object', 'Slippery Surface'],
            weights=[40, 25, 20, 15]
        )[0]
    elif row['is_freeze_risk'] == 1:
        return random.choices(
            ['Slippery Surface', 'Cold Stress', 'Equipment Malfunction', 'Visibility Issues'],
            weights=[40, 30, 20, 10]
        )[0]
    elif row['is_heat_alert'] == 1:
        return random.choices(
            ['Heat Stress', 'Dehydration Risk', 'Equipment Overheating', 'Fatigue'],
            weights=[40, 30, 20, 10]
        )[0]
    elif row['is_heavy_precip'] == 1:
        return random.choices(
            ['Slippery Surface', 'Visibility Issues', 'Electrical Hazard', 'Flooding'],
            weights=[35, 25, 25, 15]
        )[0]
    else:
        return random.choices(
            ['Improper Procedure', 'Equipment Issue', 'Human Error', 'Environmental'],
            weights=[35, 30, 20, 15]
        )[0]

# Severity driven by observation type + risk
def get_severity(obs_type, risk_score):
    if obs_type == 'Positive Observation':
        return 'Low'
    elif obs_type == 'Incident' and risk_score >= 3:
        return random.choices(
            ['Medium', 'High', 'Critical'],
            weights=[30, 45, 25]
        )[0]
    elif obs_type == 'Incident':
        return random.choices(
            ['Low', 'Medium', 'High'],
            weights=[30, 50, 20]
        )[0]
    elif obs_type == 'Near Miss':
        return random.choices(
            ['Low', 'Medium', 'High'],
            weights=[20, 55, 25]
        )[0]
    else:
        return random.choices(
            ['Low', 'Medium'],
            weights=[60, 40]
        )[0]

# Observations per city per day based on risk
risk_to_obs = {0: 1, 1: 2, 2: 4, 3: 6, 4: 9}

city_crews = {
    'Denver':           8,
    'Colorado_Springs': 5,
    'Boulder':          3,
    'Fort_Collins':     3,
    'Pueblo':           2
}

# ── Generate safety records ───────────────────────────────────────────────────
records = []
obs_id = 1

for _, row in weather_df.iterrows():
    city = row['city_name']
    risk = int(row['ops_risk_score'])
    num_obs = risk_to_obs[risk]

    # More observations on high-risk days (more crews = more reporting)
    num_obs = np.random.poisson(num_obs)
    num_obs = max(1, num_obs)

    max_crew = city_crews[city]

    for _ in range(num_obs):
        # Positive observations more common on low-risk days
        if risk == 0:
            obs_type = random.choices(
                observation_types,
                weights=[10, 15, 50, 5, 20]
            )[0]
        elif risk >= 3:
            obs_type = random.choices(
                observation_types,
                weights=[30, 25, 15, 20, 10]
            )[0]
        else:
            obs_type = random.choices(
                observation_types,
                weights=[20, 25, 30, 15, 10]
            )[0]

        root_cause = get_root_cause(row, obs_type)
        severity   = get_severity(obs_type, risk)

        # Days to close — critical takes longer
        close_days = {
            'Low':      random.randint(1, 5),
            'Medium':   random.randint(3, 14),
            'High':     random.randint(7, 30),
            'Critical': random.randint(14, 60)
        }[severity]

        records.append({
            'obs_id':          obs_id,
            'obs_date':        row['weather_date'],
            'city':            city,
            'crew_id':         f"{city[:2].upper()}-CREW-{random.randint(1, max_crew):02d}",
            'crew_type':       random.choice(crew_types),
            'observation_type': obs_type,
            'severity':        severity,
            'root_cause':      root_cause,
            'days_to_close':   close_days,
            'weather_risk_score': risk,
            'season':          row['season']
        })
        obs_id += 1

safety_df = pd.DataFrame(records)
print(f"\nGenerated {len(safety_df)} safety records ✅")
print(f"\nBy observation type:\n{safety_df['observation_type'].value_counts()}")
print(f"\nBy severity:\n{safety_df['severity'].value_counts()}")
print(f"\nBy city:\n{safety_df['city'].value_counts()}")

# ── Load to Snowflake ─────────────────────────────────────────────────────────
cursor = conn.cursor()

cursor.execute("""
    CREATE OR REPLACE TABLE UTILITIES_RAW.PUBLIC.RAW_SAFETY (
        obs_id              INTEGER,
        obs_date            DATE,
        city                VARCHAR(50),
        crew_id             VARCHAR(20),
        crew_type           VARCHAR(50),
        observation_type    VARCHAR(50),
        severity            VARCHAR(20),
        root_cause          VARCHAR(100),
        days_to_close       INTEGER,
        weather_risk_score  INTEGER,
        season              VARCHAR(10)
    )
""")

rows = [
    (
        row['obs_id'],
        str(row['obs_date']),
        row['city'],
        row['crew_id'],
        row['crew_type'],
        row['observation_type'],
        row['severity'],
        row['root_cause'],
        row['days_to_close'],
        row['weather_risk_score'],
        row['season']
    )
    for _, row in safety_df.iterrows()
]

batch_size = 200
total_inserted = 0
for i in range(0, len(rows), batch_size):
    batch = rows[i:i + batch_size]
    cursor.executemany("""
        INSERT INTO UTILITIES_RAW.PUBLIC.RAW_SAFETY
        (obs_id, obs_date, city, crew_id, crew_type, observation_type,
         severity, root_cause, days_to_close, weather_risk_score, season)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, batch)
    total_inserted += len(batch)
    print(f"  Inserted {total_inserted}/{len(rows)} rows...")

print(f"\nLoaded {total_inserted} rows into RAW_SAFETY ✅")
conn.close()
print("\nDone! 🎉")