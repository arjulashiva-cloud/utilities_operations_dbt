import os
import requests
import pandas as pd
import snowflake.connector
from datetime import datetime

# ── Config ─────────────────────────────────────────────────────────────────────
EIA_API_KEY = os.environ["EIA_API_KEY"]
BASE_URL    = "https://api.eia.gov/v2"

# PSCO = Public Service Company of Colorado = Xcel Energy
RESPONDENT  = "PSCO"
START_DATE  = "2026-01-01"
END_DATE    = "2026-08-28"

# ── Step 1: Pull Hourly Electricity Demand from EIA ───────────────────────────
print("📡 Pulling real PSCO (Xcel Energy) electricity demand from EIA...")

all_rows = []
offset   = 0
length   = 5000

while True:
    url = f"{BASE_URL}/electricity/rto/region-data/data/"
    params = {
        "api_key":               EIA_API_KEY,
        "frequency":             "hourly",
        "data[0]":               "value",
        "facets[respondent][]":  RESPONDENT,
        "facets[type][]":        "D",
        "start":                 START_DATE,
        "end":                   END_DATE,
        "sort[0][column]":       "period",
        "sort[0][direction]":    "asc",
        "offset":                offset,
        "length":                length,
    }

    response = requests.get(url, params=params)
    data     = response.json()

    if "response" not in data or "data" not in data["response"]:
        print("❌ Unexpected API response:")
        print(data)
        break

    batch = data["response"]["data"]
    if not batch:
        break

    all_rows.extend(batch)
    total = data["response"].get("total", "?")
    print(f"   Fetched {len(all_rows)} / {total} records...")

    if len(all_rows) >= int(total):
        break

    offset += length

print(f"   ✅ Total records pulled: {len(all_rows)}")

# ── Step 2: Build DataFrame ────────────────────────────────────────────────────
print("\n🔧 Processing data...")

df = pd.DataFrame(all_rows)
print(f"   Raw columns: {list(df.columns)}")

df["period"]     = pd.to_datetime(df["period"])
df["date"]       = df["period"].dt.date
df["hour"]       = df["period"].dt.hour
df["value"]      = pd.to_numeric(df["value"], errors="coerce")
df = df.rename(columns={"value": "demand_mwh"})

print(f"   Date range : {df['date'].min()} → {df['date'].max()}")
print(f"   Hours      : {df['hour'].min()} – {df['hour'].max()}")
print(f"   Demand MWh : min={df['demand_mwh'].min():.0f}, max={df['demand_mwh'].max():.0f}, avg={df['demand_mwh'].mean():.0f}")

# ── Step 3: Aggregate to Daily ─────────────────────────────────────────────────
print("\n📊 Aggregating to daily level...")

daily = df.groupby("date").agg(
    total_demand_mwh = ("demand_mwh", "sum"),
    peak_demand_mwh  = ("demand_mwh", "max"),
    avg_demand_mwh   = ("demand_mwh", "mean"),
    min_demand_mwh   = ("demand_mwh", "min"),
    hours_reported   = ("demand_mwh", "count"),
).reset_index()

daily["date"]             = pd.to_datetime(daily["date"])
daily["peak_demand_mwh"]  = daily["peak_demand_mwh"].round(1)
daily["avg_demand_mwh"]   = daily["avg_demand_mwh"].round(1)
daily["min_demand_mwh"]   = daily["min_demand_mwh"].round(1)
daily["total_demand_mwh"] = daily["total_demand_mwh"].round(1)

daily["load_tier"] = daily["peak_demand_mwh"].apply(
    lambda x: "Critical Peak" if x >= 6500 else
              "High Load"     if x >= 5500 else
              "Moderate Load" if x >= 4500 else
              "Normal Load"
)

daily["day_of_week"]      = daily["date"].dt.day_name()
daily["is_weekend"]       = daily["date"].dt.dayofweek.isin([5, 6]).astype(int)
daily["month"]            = daily["date"].dt.month
daily["year"]             = daily["date"].dt.year
daily["respondent"]       = RESPONDENT
daily["respondent_name"]  = "Public Service Company of Colorado (Xcel Energy)"
daily["created_at"]       = datetime.now()

print(f"   Daily rows : {len(daily)}")
print(f"\n   Sample (first 5 days):")
print(daily[["date","total_demand_mwh","peak_demand_mwh","load_tier"]].head())

# ── Step 4: Load to Snowflake ──────────────────────────────────────────────────
print("\n📤 Loading to Snowflake...")

conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "UTILITIES_RAW"),
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
)
cursor = conn.cursor()

cursor.execute("""
CREATE OR REPLACE TABLE RAW_EIA_DEMAND (
    DATE                DATE,
    RESPONDENT          VARCHAR(20),
    RESPONDENT_NAME     VARCHAR(100),
    TOTAL_DEMAND_MWH    FLOAT,
    PEAK_DEMAND_MWH     FLOAT,
    AVG_DEMAND_MWH      FLOAT,
    MIN_DEMAND_MWH      FLOAT,
    HOURS_REPORTED      NUMBER,
    LOAD_TIER           VARCHAR(30),
    DAY_OF_WEEK         VARCHAR(15),
    IS_WEEKEND          NUMBER,
    MONTH               NUMBER,
    YEAR                NUMBER,
    CREATED_AT          TIMESTAMP_NTZ
)
""")

rows = [
    (
        str(r["date"].date()),
        r["respondent"],
        r["respondent_name"],
        float(r["total_demand_mwh"]),
        float(r["peak_demand_mwh"]),
        float(r["avg_demand_mwh"]),
        float(r["min_demand_mwh"]),
        int(r["hours_reported"]),
        r["load_tier"],
        r["day_of_week"],
        int(r["is_weekend"]),
        int(r["month"]),
        int(r["year"]),
        datetime.now(),
    )
    for _, r in daily.iterrows()
]

for i in range(0, len(rows), 200):
    cursor.executemany(
        "INSERT INTO RAW_EIA_DEMAND VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
        rows[i:i+200]
    )

conn.commit()
cursor.close()
conn.close()

print(f"   ✅ Loaded {len(rows)} rows into RAW_EIA_DEMAND")
print("\n🎉 Done! Real Xcel Energy demand data is live in Snowflake.")
print(f"   Covering: {daily['date'].min().date()} → {daily['date'].max().date()}")