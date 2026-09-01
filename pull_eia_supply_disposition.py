import os
import requests
import pandas as pd
import snowflake.connector
from datetime import datetime

EIA_API_KEY = os.environ["EIA_API_KEY"]
STATE       = "CO"

# ════════════════════════════════════════════════════════════════════════════════
# PART 1 — Explore the route to find exact endpoint
# ════════════════════════════════════════════════════════════════════════════════
print("🔍 Exploring state-electricity-profiles sub-routes...")
r = requests.get(
    "https://api.eia.gov/v2/electricity/state-electricity-profiles/",
    params={"api_key": EIA_API_KEY}
)
routes = r.json().get("response", {}).get("routes", [])
for route in routes:
    print(f"   id={route.get('id'):<40} name={route.get('name')}")

# ════════════════════════════════════════════════════════════════════════════════
# PART 2 — Pull Supply and Disposition of Electricity
# Endpoint: /electricity/state-electricity-profiles/source-disposition/data/
# ════════════════════════════════════════════════════════════════════════════════
print("\n📡 Pulling Colorado supply and disposition of electricity...")

all_rows = []
offset   = 0

while True:
    r = requests.get(
        "https://api.eia.gov/v2/electricity/state-electricity-profiles/source-disposition/data/",
        params={
            "api_key":           EIA_API_KEY,
            "frequency":         "annual",
            "data[0]":           "customers",
            "data[1]":           "revenue",
            "data[2]":           "sales",
            "data[3]":           "price",
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
        print(f"   source-disposition returned empty. Trying alternative endpoint...")
        print(f"   Response preview: {str(resp)[:500]}")
        break

    all_rows.extend(batch)
    total = resp.get("total", len(all_rows))
    print(f"   Fetched {len(all_rows)} / {total} records...")
    if len(all_rows) >= int(total):
        break
    offset += 5000

# ── If source-disposition was empty, try the sales endpoint ───────────────────
if not all_rows:
    print("\n📡 Trying /electricity/retail-sales/data/ (state-level monthly)...")
    offset = 0
    while True:
        r = requests.get(
            "https://api.eia.gov/v2/electricity/retail-sales/data/",
            params={
                "api_key":           EIA_API_KEY,
                "frequency":         "monthly",
                "data[0]":           "customers",
                "data[1]":           "revenue",
                "data[2]":           "sales",
                "data[3]":           "price",
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
            print(f"   Response preview: {str(resp)[:500]}")
            break

        all_rows.extend(batch)
        total = resp.get("total", len(all_rows))
        print(f"   Fetched {len(all_rows)} / {total} records...")
        if len(all_rows) >= int(total):
            break
        offset += 5000

print(f"\n   ✅ Total records: {len(all_rows)}")

# ════════════════════════════════════════════════════════════════════════════════
# PART 3 — Process and Preview
# ════════════════════════════════════════════════════════════════════════════════
if all_rows:
    df = pd.DataFrame(all_rows)
    print(f"\n🔧 Columns  : {list(df.columns)}")
    print(f"   Sample  : {df.iloc[0].to_dict()}")
    print(f"   Periods : {sorted(df['period'].unique())[-10:]}")

    for cat_col in ["sectorName", "sector", "sectorid", "typeName", "type"]:
        if cat_col in df.columns:
            print(f"\n   {cat_col} values: {df[cat_col].unique()}")

    # ── Load to Snowflake ─────────────────────────────────────────────────────
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

    numeric_cols    = ["sales", "revenue", "customers", "price"]
    present_numeric = [c for c in numeric_cols if c in df.columns]

    cursor.execute("""
    CREATE OR REPLACE TABLE RAW_EIA_SUPPLY_DISPOSITION (
        PERIOD          VARCHAR(10),
        STATE_ID        VARCHAR(5),
        STATE_NAME      VARCHAR(50),
        SECTOR          VARCHAR(50),
        SECTOR_NAME     VARCHAR(100),
        DATA_TYPE       VARCHAR(50),
        VALUE           FLOAT,
        UNIT            VARCHAR(30),
        CREATED_AT      TIMESTAMP_NTZ
    )
    """)

    rows = []
    sector_col      = next((c for c in ["sectorid","sector"]            if c in df.columns), None)
    sector_name_col = next((c for c in ["sectorName","typeName"]        if c in df.columns), None)
    state_name_col  = next((c for c in ["stateName","stateDescription"] if c in df.columns), None)

    for _, row in df.iterrows():
        for metric in present_numeric:
            val = row.get(metric)
            try:
                val = float(val) if val is not None and str(val) != 'nan' else None
            except:
                val = None
            rows.append((
                str(row.get("period", "")),
                str(row.get("stateid", STATE)),
                str(row.get(state_name_col, "Colorado")) if state_name_col else "Colorado",
                str(row.get(sector_col, ""))      if sector_col      else "",
                str(row.get(sector_name_col, "")) if sector_name_col else "",
                metric,
                val,
                str(row.get(f"{metric}-units", "")) if f"{metric}-units" in row else "",
                datetime.now()
            ))

    for i in range(0, len(rows), 200):
        cursor.executemany(
            "INSERT INTO RAW_EIA_SUPPLY_DISPOSITION VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            rows[i:i+200]
        )

    conn.commit()
    cursor.close()
    conn.close()
    print(f"   ✅ Loaded {len(rows)} rows into RAW_EIA_SUPPLY_DISPOSITION")

print("\n🎉 Done! Colorado supply and disposition data is live in Snowflake.")