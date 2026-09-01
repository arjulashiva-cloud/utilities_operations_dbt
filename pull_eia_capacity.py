import os
import requests
import pandas as pd
import snowflake.connector
from datetime import datetime

EIA_API_KEY = os.environ["EIA_API_KEY"]
STATE       = "CO"

# ════════════════════════════════════════════════════════════════════════════════
# PART 1 — State-level Annual Generating Capacity by Fuel Type
# Endpoint: /electricity/state-electricity-profiles/capability/data/
# ════════════════════════════════════════════════════════════════════════════════
print("📡 Pulling Colorado annual generating capacity by fuel type...")

all_rows = []
offset   = 0

while True:
    r = requests.get(
        "https://api.eia.gov/v2/electricity/state-electricity-profiles/capability/data/",
        params={
            "api_key":           EIA_API_KEY,
            "frequency":         "annual",
            "data[0]":           "capacity",
            "facets[stateid][]": STATE,
            "sort[0][column]":   "period",
            "sort[0][direction]":"desc",
            "offset":            offset,
            "length":            5000,
        }
    )
    data  = r.json()
    resp  = data.get("response", {})
    batch = resp.get("data", [])

    if not batch:
        print(f"   Response preview: {str(resp)[:400]}")
        break

    all_rows.extend(batch)
    total = resp.get("total", len(all_rows))
    print(f"   Fetched {len(all_rows)} / {total} records...")
    if len(all_rows) >= int(total):
        break
    offset += 5000

print(f"   ✅ Capacity records: {len(all_rows)}")

# ════════════════════════════════════════════════════════════════════════════════
# PART 2 — Individual Generator Inventory (with GPS coordinates!)
# Endpoint: /electricity/operating-generator-capacity/data/
# ════════════════════════════════════════════════════════════════════════════════
print("\n📡 Pulling Colorado individual generator inventory...")

gen_rows = []
offset   = 0

while True:
    r = requests.get(
        "https://api.eia.gov/v2/electricity/operating-generator-capacity/data/",
        params={
            "api_key":           EIA_API_KEY,
            "frequency":         "monthly",
            "data[0]":           "net-summer-capacity-mw",
            "data[1]":           "nameplate-capacity-mw",
            "data[2]":           "latitude",
            "data[3]":           "longitude",
            "facets[stateid][]": STATE,
            "sort[0][column]":   "period",
            "sort[0][direction]":"desc",
            "offset":            offset,
            "length":            5000,
        }
    )
    data  = r.json()
    resp  = data.get("response", {})
    batch = resp.get("data", [])

    if not batch:
        print(f"   Response preview: {str(resp)[:400]}")
        break

    gen_rows.extend(batch)
    total = resp.get("total", len(gen_rows))
    print(f"   Fetched {len(gen_rows)} / {total} records...")
    if len(gen_rows) >= int(total):
        break
    offset += 5000

print(f"   ✅ Generator records: {len(gen_rows)}")

# ════════════════════════════════════════════════════════════════════════════════
# Process Part 1 — State Capacity
# ════════════════════════════════════════════════════════════════════════════════
if all_rows:
    print("\n🔧 Processing state capacity data...")
    cap_df = pd.DataFrame(all_rows)
    print(f"   Columns: {list(cap_df.columns)}")
    print(f"   Sample : {cap_df.iloc[0].to_dict()}")

# ════════════════════════════════════════════════════════════════════════════════
# Process Part 2 — Generator Inventory
# ════════════════════════════════════════════════════════════════════════════════
if gen_rows:
    print("\n🔧 Processing generator inventory...")
    gen_df = pd.DataFrame(gen_rows)
    print(f"   Columns: {list(gen_df.columns)}")
    print(f"   Sample : {gen_df.iloc[0].to_dict()}")

    # Keep only latest period per generator
    gen_df["period"] = pd.to_datetime(gen_df["period"], format="%Y-%m")
    latest_period    = gen_df["period"].max()
    gen_latest       = gen_df[gen_df["period"] == latest_period].copy()
    print(f"\n   Latest period    : {latest_period.strftime('%Y-%m')}")
    print(f"   Active generators: {len(gen_latest)}")

    # Clean numeric columns
    for col in ["net-summer-capacity-mw", "nameplate-capacity-mw", "latitude", "longitude"]:
        if col in gen_latest.columns:
            gen_latest[col] = pd.to_numeric(gen_latest[col], errors="coerce")

    # Summary by energy source
    src_col = "energy_source_code" if "energy_source_code" in gen_latest.columns else None
    if src_col:
        fuel_map = {
            "COL":"Coal","NG":"Natural Gas","NUC":"Nuclear",
            "WAT":"Hydro","WND":"Wind","SUN":"Solar PV",
            "GEO":"Geothermal","PET":"Petroleum","OTH":"Other",
            "DFO":"Diesel","BIT":"Bituminous Coal","SUB":"Subbituminous Coal",
            "MWH":"Battery Storage","OG":"Other Gas",
        }
        gen_latest["fuel_type"] = gen_latest[src_col].map(fuel_map).fillna(gen_latest[src_col])
        summary = gen_latest.groupby("fuel_type")["net-summer-capacity-mw"].sum().sort_values(ascending=False)
        total   = summary.sum()
        print(f"\n🌱 Colorado Active Generation Fleet ({latest_period.strftime('%Y-%m')}):")
        for fuel, mw in summary.items():
            if mw and mw > 0:
                print(f"   {fuel:<20}: {mw:>8.0f} MW  ({mw/total*100:.1f}%)")
        print(f"   {'TOTAL':<20}: {total:>8.0f} MW")

    # ── Load generators to Snowflake ──────────────────────────────────────────
    print("\n📤 Loading to Snowflake...")
    conn   = snowflake.connector.connect(
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "UTILITIES_RAW"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
    )
    cursor = conn.cursor()

    cursor.execute("""
    CREATE OR REPLACE TABLE RAW_EIA_GENERATORS (
        PERIOD                  DATE,
        STATE                   VARCHAR(5),
        PLANT_NAME              VARCHAR(200),
        GENERATOR_ID            VARCHAR(50),
        ENERGY_SOURCE_CODE      VARCHAR(20),
        FUEL_TYPE               VARCHAR(50),
        TECHNOLOGY              VARCHAR(100),
        NET_SUMMER_CAPACITY_MW  FLOAT,
        NAMEPLATE_CAPACITY_MW   FLOAT,
        LATITUDE                FLOAT,
        LONGITUDE               FLOAT,
        CREATED_AT              TIMESTAMP_NTZ
    )
    """)

    plant_col  = next((c for c in gen_latest.columns if "plant" in c.lower() and "name" in c.lower()), None)
    gen_id_col = next((c for c in gen_latest.columns if "generatorid" in c.lower()), None)
    tech_col   = next((c for c in gen_latest.columns if "technology" in c.lower()), None)

    rows = []
    for _, r in gen_latest.iterrows():
        rows.append((
            str(latest_period.date()),
            STATE,
            str(r.get(plant_col, "")) if plant_col else "",
            str(r.get(gen_id_col, "")) if gen_id_col else "",
            str(r.get("energy_source_code", "")),
            str(r.get("fuel_type", "")),
            str(r.get(tech_col, "")) if tech_col else "",
            float(r["net-summer-capacity-mw"]) if pd.notna(r.get("net-summer-capacity-mw")) else None,
            float(r["nameplate-capacity-mw"])   if pd.notna(r.get("nameplate-capacity-mw"))  else None,
            float(r["latitude"])  if pd.notna(r.get("latitude"))  else None,
            float(r["longitude"]) if pd.notna(r.get("longitude")) else None,
            datetime.now(),
        ))

    for i in range(0, len(rows), 200):
        cursor.executemany(
            "INSERT INTO RAW_EIA_GENERATORS VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            rows[i:i+200]
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"   ✅ Loaded {len(rows)} rows into RAW_EIA_GENERATORS")

print("\n🎉 Done! Real Colorado generator data is live in Snowflake.")