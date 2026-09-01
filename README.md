# Utilities Operations dbt Pipeline

End-to-end data pipeline for Colorado utilities operations — raw ingestion via Python, transformation via dbt, distributed analytics via PySpark, all landing in Snowflake.

---

## Project Structure

```
utilities_operations_dbt/
├── .env.example                        # Copy to .env and fill credentials (never commit .env)
├── .gitignore
│
├── pull_weather.py                     # Open-Meteo weather data → RAW_WEATHER
├── pull_eia_demand.py                  # EIA hourly grid demand (Xcel/PSCO) → RAW_EIA_DEMAND
├── pull_eia_capacity.py                # EIA generator inventory → RAW_EIA_GENERATORS
├── pull_eia_supply_disposition.py      # EIA retail sales by sector → RAW_EIA_SUPPLY_DISPOSITION
├── build_power_grid.py                 # NetworkX grid graph → RAW_GRID_NODES, RAW_GRID_EDGES
├── generate_outages.py                 # Simulated outage events → RAW_OUTAGES
├── generate_safety.py                  # Safety observations → RAW_SAFETY
├── generate_workforce.py               # Crew deployment records → RAW_WORKFORCE
├── generate_om_spend.py                # O&M spend by asset type → RAW_OM_SPEND
├── spark_operations_analysis.py        # PySpark city-level analysis → RAW_SPARK_CITY_SUMMARY
│
└── utilities_operations_dbt/           # dbt project
    ├── dbt_project.yml
    ├── models/
    │   ├── staging/                    # Cleaned source views
    │   └── marts/                      # Business-ready tables
    └── README.md
```

---

## Data Architecture

```
Open-Meteo API ──────────────────────────────────────────┐
EIA Open Data API ───────────────────────────────────────┤
Synthetic generators (outages, safety, workforce, O&M) ──┤
NetworkX power grid ─────────────────────────────────────┘
           │
           ▼
    RAW_* tables in Snowflake (UTILITIES_RAW.PUBLIC)
           │
           ▼
    dbt staging models (stg_*)
           │
           ▼
    dbt mart models (UTILITIES_RAW.PUBLIC_MARTS)
           │
           ▼
    PySpark analysis → RAW_SPARK_CITY_SUMMARY
```

### Raw Tables

| Table | Rows | Source |
|---|---|---|
| RAW_WEATHER | 330 | Open-Meteo (5 cities, daily) |
| RAW_EIA_DEMAND | 240 | EIA API — PSCO hourly demand |
| RAW_EIA_GENERATORS | 509 | EIA API — generator inventory |
| RAW_EIA_SUPPLY_DISPOSITION | 7,344 | EIA API — retail sales by sector |
| RAW_GRID_NODES | 15 | NetworkX graph nodes |
| RAW_GRID_EDGES | 17 | NetworkX graph edges |
| RAW_OUTAGES | 790 | Simulated (Poisson distribution) |
| RAW_SAFETY | 565 | Simulated safety observations |
| RAW_WORKFORCE | 1,567 | Simulated crew deployments |
| RAW_OM_SPEND | 360 | Simulated O&M spend |
| RAW_SPARK_CITY_SUMMARY | 5 | PySpark city-level stress ranking |

### dbt Marts

| Mart | Description |
|---|---|
| MART_OPERATIONS_SUMMARY | Daily ops summary per city — outages, stress score, weather, crews |
| MART_WEATHER_OPERATIONS | Weather joined with operational risk metrics |
| MART_GRID_OPERATIONS | Grid criticality — betweenness centrality, capacity, network importance |
| MART_OM_SPEND | O&M cost breakdown by asset type and cost category |
| MART_OUTAGES_OPERATIONS | Outage events with operational context |
| MART_SAFETY_OPERATIONS | Safety observations with operational context |
| MART_WORKFORCE_OPERATIONS | Crew deployment with weather and risk context |
| MART_EIA_WEATHER | EIA hourly demand joined with weather conditions |
| MART_EIA_SALES_BY_SECTOR | Retail electricity sales by sector |
| MART_GENERATOR_MIX | Generator capacity and fuel type mix |

---

## Setup

### Prerequisites

- Python 3.10+
- Snowflake account
- EIA Open Data API key (free — register at https://www.eia.gov/opendata/)
- dbt-snowflake: `pip install dbt-snowflake`
- PySpark: `pip install pyspark`

### Credentials

Copy `.env.example` to `.env` and fill in your values:

```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=UTILITIES_RAW
SNOWFLAKE_SCHEMA=PUBLIC
EIA_API_KEY=your_eia_api_key
```

> ⚠️ **Never commit `.env` to Git.** It is listed in `.gitignore`.

### VS Code — Auto-load `.env`

Add to `.vscode/settings.json`:

```json
{
  "python.terminal.useEnvFile": true,
  "python.envFile": "${workspaceFolder}/.env"
}
```

---

## Running the Pipeline

### Step 1 — Pull raw data

```bash
python pull_weather.py
python pull_eia_demand.py
python pull_eia_capacity.py
python pull_eia_supply_disposition.py
```

### Step 2 — Generate synthetic data

```bash
python generate_outages.py
python generate_safety.py
python generate_workforce.py
python build_power_grid.py
```

### Step 3 — Run dbt transformations

```bash
cd utilities_operations_dbt
dbt run
cd ..
```

### Step 4 — O&M spend (depends on dbt mart)

```bash
python generate_om_spend.py
```

### Step 5 — PySpark analysis (depends on dbt marts)

```bash
python spark_operations_analysis.py
```

---

## Cities Covered

Denver, Colorado Springs, Boulder, Fort Collins, Pueblo

---

## Tech Stack

| Layer | Technology |
|---|---|
| Data ingestion | Python, pandas, requests |
| External APIs | Open-Meteo (weather), EIA Open Data v2 |
| Graph analytics | NetworkX |
| Data warehouse | Snowflake |
| Transformation | dbt (dbt-snowflake) |
| Distributed analytics | Apache PySpark |
| Version control | Git / GitHub |
