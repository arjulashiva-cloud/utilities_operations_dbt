# ⚡ Utilities Operations Data Pipeline

A production-grade end-to-end data engineering pipeline for Colorado electric utilities — pulling **real data from live APIs**, running **advanced Python analytics**, transforming with **dbt**, and distributing compute with **PySpark** — all landing in **Snowflake**.

---

## 🎯 What We Built & Why It's Impressive

This isn't a toy dataset project. Every layer of this pipeline does something technically interesting:

| Layer | What We Did |
|---|---|
| **Real API Ingestion** | Live data from EIA Open Data v2 + Open-Meteo weather API |
| **Graph Analytics** | Built a real power grid network with NetworkX — calculated betweenness centrality, failure simulations |
| **Statistical Simulation** | Generated realistic outages using Poisson distributions correlated with weather risk |
| **dbt Transformation** | 9 staging models + 10 business marts with complex SQL joins and risk scoring |
| **PySpark** | City-level aggregations, window ranking functions, grid criticality joins at scale |
| **Snowflake** | Everything lands in a structured raw + marts schema — production-ready |

---

## 🌐 Real Data We Pulled from Live APIs

### 1. Open-Meteo Weather API (`pull_weather.py`)
- **Free, no auth required** — pulls real historical weather for 5 Colorado cities
- Daily temperature (avg/min/max), wind speed, precipitation, snowfall, weather codes
- Derived fields: **heating degree days**, **cooling degree days**, **is_high_wind**, **is_freeze_risk**, **is_heat_alert**
- Covers **Denver, Colorado Springs, Boulder, Fort Collins, Pueblo** — 330 rows of real daily weather

### 2. EIA Open Data v2 API — Grid Demand (`pull_eia_demand.py`)
- Pulls **real hourly electricity demand** from Xcel Energy / PSCO (Public Service Company of Colorado)
- Endpoint: `electricity/rto/region-data` — actual megawatt readings from the grid operator
- Calculates demand deviation from rolling averages to flag abnormal demand events
- **240 rows** of real grid load data

### 3. EIA Open Data v2 API — Generator Inventory (`pull_eia_capacity.py`)
- Pulls **real power plant inventory** for Colorado — every generator with GPS coordinates
- Captures fuel type (natural gas, wind, solar, coal, hydro), capacity in MW, operating status
- Enriches with **network importance score** based on capacity relative to fuel type peers
- **509 generators** — real facilities, real locations

### 4. EIA Open Data v2 API — Retail Sales (`pull_eia_supply_disposition.py`)
- Pulls **real retail electricity sales** broken down by sector: residential, commercial, industrial, transportation
- Monthly data with revenue, average retail price, and customer counts by sector
- **7,344 rows** of real energy sales data

---

## 🔬 Advanced Python Analytics We Built

### Power Grid Network Analysis (`build_power_grid.py`)
Built a **real graph-theoretic model** of the Colorado power grid using **NetworkX**:

- **15 nodes** — substations, solar farms, wind farms, transmission hubs across 5 cities
- **17 edges** — transmission lines with capacity (MW) and voltage (kV) as edge weights
- **Betweenness centrality** — calculated for every node to find which substations are most critical to grid flow
- **Degree centrality** — measures how connected each facility is
- **Failure simulation** — removed the highest-betweenness node and recalculated network resilience
- **Network importance score** — composite metric combining centrality + capacity
- Results: `RAW_GRID_NODES` (15 rows) + `RAW_GRID_EDGES` (17 rows)

### Outage Simulation (`generate_outages.py`)
Generated **statistically realistic outages** correlated with actual weather data:

- **Poisson distribution** — outage counts per day drawn from λ based on real weather risk score
- Weather-correlated severity — high wind days drive line outages, freeze days drive equipment failures, heat alerts drive transformer overloads
- Outage types: Equipment Failure, Weather-Related, Scheduled Maintenance, Animal Contact, Vegetation
- Cause-to-repair-time mapping — each cause has realistic duration distributions
- Customer impact calculation — affected customers × outage duration = customer minutes lost
- **790 outage records** across 5 cities over 66 days

### Safety Observation Simulation (`generate_safety.py`)
- Safety observations driven by operational stress + weather risk
- Observation types: Near Miss, Unsafe Condition, Safe Behavior, Toolbox Talk, PPE Compliance
- Severity scoring correlated with weather risk — more severe observations on high-risk days
- **565 safety records**

### Workforce Deployment Simulation (`generate_workforce.py`)
- Crew deployment counts driven by outage volume and weather severity
- Overtime hours calculated from stress score and outage count
- Labor cost = base rate × hours + overtime premium
- **1,567 crew deployment records**

### O&M Spend Simulation (`generate_om_spend.py`)
Reads **real dbt mart data** (`MART_WEATHER_OPERATIONS`) then generates spend correlated to it:

- **6 asset types**: Transformer, Power Line, Substation, Smart Meter, Distribution Panel, Underground Cable
- **4 cost categories**: Labor, Materials, Contractor, Equipment Rental — split with realistic percentages
- **Risk multiplier**: `1.0 + (avg_risk × 0.25)` — higher weather risk = higher O&M spend
- **Storm adder**: wind days × $800 + freeze days × $1,200 + heat days × $600
- **360 spend records** across 5 cities × 12 months

---

## 🔄 dbt Transformations

9 staging models clean and type-cast raw data. 10 marts deliver business logic:

### Key Mart Highlights

**`MART_OPERATIONS_SUMMARY`** — the crown jewel mart:
- Joins outages + weather + workforce + safety for each city-day
- Calculates **operational stress score**: composite of outage severity, customer impact, weather risk, crew utilization
- Assigns **ops risk labels**: Low / Moderate / Elevated / High / Critical
- Derives **season** from date for seasonal trend analysis

**`MART_GRID_OPERATIONS`** — graph meets mart:
- Joins NetworkX betweenness centrality with real EIA generator data
- Ranks facilities by network importance score
- Tags critical nodes whose failure would most disrupt grid flow

**`MART_WEATHER_OPERATIONS`** — weather risk intelligence:
- Combines real Open-Meteo weather with operational outcomes
- Calculates rolling weather risk scores
- Flags high-wind, freeze-risk, and heat-alert days with operational impact

**`MART_EIA_WEATHER`** — real demand meets real weather:
- Joins live EIA grid demand readings with actual weather conditions
- Captures demand spikes during extreme weather events

---

## ⚡ PySpark Distributed Analysis (`spark_operations_analysis.py`)

Ran distributed compute on top of the dbt marts:

- **City-level aggregations** — avg/sum/max across outages, customer minutes lost, labor cost, stress score, temperature per city
- **Window ranking function** — `rank().over(Window.orderBy(desc("avg_stress_score")))` — ranks all 5 cities by operational stress
- **Risk labeling** — High Risk / Elevated / Moderate / Low based on stress score thresholds
- **High-stress day filtering** — isolated all days with stress score ≥ 2 across all cities
- **Seasonal breakdown** — aggregated operational patterns by Winter / Spring / Summer / Fall
- **Grid criticality join** — joined city stress rankings with NetworkX betweenness centrality and capacity data
- Results written back to Snowflake: `RAW_SPARK_CITY_SUMMARY` — 5 rows, one per city

---

## 📊 Final Data Model

### Snowflake — Raw Layer (`UTILITIES_RAW.PUBLIC`)

| Table | Rows | Description |
|---|---|---|
| RAW_WEATHER | 330 | Real daily weather — 5 cities |
| RAW_EIA_DEMAND | 240 | Real hourly grid demand (Xcel/PSCO) |
| RAW_EIA_GENERATORS | 509 | Real Colorado power plants |
| RAW_EIA_SUPPLY_DISPOSITION | 7,344 | Real retail electricity sales |
| RAW_GRID_NODES | 15 | NetworkX graph nodes |
| RAW_GRID_EDGES | 17 | NetworkX graph edges |
| RAW_OUTAGES | 790 | Simulated outages (weather-correlated) |
| RAW_SAFETY | 565 | Simulated safety observations |
| RAW_WORKFORCE | 1,567 | Simulated crew deployments |
| RAW_OM_SPEND | 360 | Simulated O&M spend |
| RAW_SPARK_CITY_SUMMARY | 5 | PySpark city stress rankings |

### Snowflake — Mart Layer (`UTILITIES_RAW.PUBLIC_MARTS`)

| Mart | Description |
|---|---|
| MART_OPERATIONS_SUMMARY | Daily ops — outages, stress score, weather, crews, risk label |
| MART_WEATHER_OPERATIONS | Weather risk scores + operational outcomes |
| MART_GRID_OPERATIONS | Grid criticality — centrality, capacity, importance |
| MART_OM_SPEND | O&M cost by asset type and cost category |
| MART_OUTAGES_OPERATIONS | Outage detail with operational context |
| MART_SAFETY_OPERATIONS | Safety observations with risk context |
| MART_WORKFORCE_OPERATIONS | Crew deployment with weather + risk |
| MART_EIA_WEATHER | Real EIA demand + real weather joined |
| MART_EIA_SALES_BY_SECTOR | Real retail sales by residential/commercial/industrial |
| MART_GENERATOR_MIX | Real generator capacity and fuel type mix |

---

## 🗺️ Cities Covered

Denver · Colorado Springs · Boulder · Fort Collins · Pueblo

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Data ingestion | Python, pandas, requests |
| Real-time weather | Open-Meteo API (free, no auth) |
| Real grid data | EIA Open Data v2 API |
| Graph analytics | NetworkX — centrality, failure simulation |
| Statistical simulation | NumPy — Poisson distributions, correlated random |
| Data warehouse | Snowflake |
| Transformation | dbt-snowflake (staging + marts) |
| Distributed analytics | Apache PySpark — window functions, aggregations |
| Version control | Git / GitHub |
| BI & DAX | Power BI — DAX measures, calculated columns, KPI dashboards |

---

## 📈 Power BI & DAX Dashboards

Connected **Power BI directly to Snowflake** marts and built DAX-powered dashboards on top of the full pipeline:

- **Operational Stress Dashboard** — DAX measures for avg/peak stress score by city, risk label distribution, high-stress day counts
- **Outage Analytics** — customer minutes lost trending, outage type breakdown, seasonal patterns
- **Grid Criticality** — betweenness centrality rankings, capacity by fuel type, critical node identification
- **O&M Spend Analysis** — spend by asset type and cost category, risk multiplier impact, weather-driven cost spikes
- **Workforce & Safety** — crew utilization, overtime trending, safety observation severity by risk level
- **EIA Market Intelligence** — real grid demand vs weather, retail sales by sector, generator mix

DAX highlights: time intelligence functions, running totals, rank measures, weather-correlated KPIs, dynamic risk thresholds

---

## ⚙️ Setup & Running

### Prerequisites
```bash
pip install dbt-snowflake pyspark snowflake-connector-python pandas numpy networkx requests
```

### Credentials
Copy `.env.example` to `.env`:
```env
SNOWFLAKE_USER=your_username
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ACCOUNT=your_account_identifier
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=UTILITIES_RAW
SNOWFLAKE_SCHEMA=PUBLIC
EIA_API_KEY=your_eia_api_key
```
> ⚠️ Never commit `.env` — it is in `.gitignore`

### Run Order
```bash
# 1 — Pull real data
python pull_weather.py
python pull_eia_demand.py
python pull_eia_capacity.py
python pull_eia_supply_disposition.py

# 2 — Generate synthetic data
python generate_outages.py
python generate_safety.py
python generate_workforce.py
python build_power_grid.py

# 3 — dbt transformations
cd utilities_operations_dbt
dbt run
cd ..

# 4 — O&M spend (reads from dbt mart)
python generate_om_spend.py

# 5 — PySpark analysis (reads from dbt marts)
python spark_operations_analysis.py
```