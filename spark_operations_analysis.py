# spark_operations_analysis.py
# Purpose: PySpark analysis of utilities operations mart data.
#          Reads from Snowflake, runs city-level aggregations + window ranking,
#          high-stress day filtering, seasonal breakdown, grid criticality join,
#          then writes results back to Snowflake.
# Credentials: set SNOWFLAKE_* in .env before running.

import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, sum, count, max, min, round, when,
    rank, desc, lit, regexp_replace
)
from pyspark.sql.window import Window
import snowflake.connector
from datetime import datetime

# ── Credentials from environment ─────────────────────────────────────────────
SNOWFLAKE_USER    = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD= os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ACCOUNT = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_WH      = os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DB      = os.environ.get("SNOWFLAKE_DATABASE",  "UTILITIES_RAW")
SNOWFLAKE_SCHEMA  = os.environ.get("SNOWFLAKE_SCHEMA",    "PUBLIC")

# ── Step 1: Start Spark Session ───────────────────────────────────────────────
spark = SparkSession.builder \
    .appName("UtilitiesOperationsAnalysis") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
print("✅ Spark session started")
print(f"   Spark version : {spark.version}")

# ── Step 2: Load mart data from Snowflake → pandas → Spark ───────────────────
print("\n📥 Loading mart data from Snowflake...")

conn = snowflake.connector.connect(
    user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WH, database=SNOWFLAKE_DB, schema=SNOWFLAKE_SCHEMA
)
cursor = conn.cursor()

cursor.execute("SELECT * FROM UTILITIES_RAW.PUBLIC_MARTS.MART_OPERATIONS_SUMMARY")
columns  = [d[0].lower() for d in cursor.description]
rows     = cursor.fetchall()
import pandas as pd
summary_pd = pd.DataFrame(rows, columns=columns)

cursor.execute("SELECT * FROM UTILITIES_RAW.PUBLIC_MARTS.MART_GRID_OPERATIONS")
columns2 = [d[0].lower() for d in cursor.description]
rows2    = cursor.fetchall()
grid_pd  = pd.DataFrame(rows2, columns=columns2)

cursor.close()
conn.close()
print(f"   ✅ mart_operations_summary : {len(summary_pd)} rows")
print(f"   ✅ mart_grid_operations    : {len(grid_pd)} rows")

# ── Step 3: Convert pandas → Spark DataFrames ────────────────────────────────
summary_df = spark.createDataFrame(summary_pd)
grid_df    = spark.createDataFrame(grid_pd)
print(f"\n   summary_df partitions : {summary_df.rdd.getNumPartitions()}")
print(f"   grid_df partitions    : {grid_df.rdd.getNumPartitions()}")

# ── Step 4: City-Level Aggregations ──────────────────────────────────────────
print("\n📊 Running city-level aggregations...")

city_summary = summary_df.groupBy("city_name").agg(
    count("*").alias("total_days"),
    round(avg("total_outages"), 2).alias("avg_daily_outages"),
    round(sum("total_outages"), 0).alias("total_outages"),
    round(avg("total_customer_minutes_lost"), 0).alias("avg_customer_minutes_lost"),
    round(sum("total_customer_minutes_lost"), 0).alias("total_customer_minutes_lost"),
    round(avg("total_crews_deployed"), 2).alias("avg_crews_per_day"),
    round(sum("total_labor_cost"), 2).alias("total_labor_cost"),
    round(avg("operational_stress_score"), 3).alias("avg_stress_score"),
    round(max("operational_stress_score"), 3).alias("peak_stress_score"),
    round(avg("temp_avg_f"), 1).alias("avg_temperature_f"),
    round(avg("heating_degree_days"), 1).alias("avg_heating_degree_days"),
    round(avg("cooling_degree_days"), 1).alias("avg_cooling_degree_days"),
)

print("\n   City-Level Summary:")
city_summary.show(truncate=False)

# ── Step 5: Window Function — Rank cities by stress score ─────────────────────
print("🏆 Ranking cities by operational stress...")

window_spec = Window.orderBy(desc("avg_stress_score"))

city_ranked = city_summary.withColumn(
    "stress_rank", rank().over(window_spec)
).withColumn(
    "risk_label",
    when(col("avg_stress_score") >= 1.5, "High Risk")
    .when(col("avg_stress_score") >= 1.0, "Elevated Risk")
    .when(col("avg_stress_score") >= 0.5, "Moderate Risk")
    .otherwise("Low Risk")
)

city_ranked.select(
    "stress_rank", "city_name", "avg_stress_score", "risk_label",
    "avg_daily_outages", "total_labor_cost"
).show(truncate=False)

# ── Step 6: High-Stress Days ──────────────────────────────────────────────────
print("🔴 Analyzing high-stress days (stress score >= 2)...")

high_stress = summary_df.filter(col("operational_stress_score") >= 2) \
    .select(
        "summary_date", "city_name", "operational_stress_score",
        "total_outages", "total_customer_minutes_lost",
        "ops_risk_label", "season"
    ).orderBy(desc("operational_stress_score"))

print(f"   High-stress days found: {high_stress.count()}")
high_stress.show(10, truncate=False)

# ── Step 7: Seasonal Breakdown ────────────────────────────────────────────────
print("🌦️  Seasonal operational patterns...")

seasonal = summary_df.groupBy("season").agg(
    count("*").alias("total_days"),
    round(avg("total_outages"), 2).alias("avg_outages"),
    round(avg("total_customer_minutes_lost"), 0).alias("avg_cml"),
    round(avg("operational_stress_score"), 3).alias("avg_stress"),
    round(sum("total_labor_cost"), 2).alias("total_labor_cost"),
).orderBy(desc("avg_stress"))

seasonal.show(truncate=False)

# ── Step 8: Grid Criticality Join ─────────────────────────────────────────────
print("🔌 Grid criticality by city...")

grid_city = grid_df.groupBy("city").agg(
    count("*").alias("facility_count"),
    round(avg("betweenness_centrality"), 3).alias("avg_betweenness"),
    round(max("betweenness_centrality"), 3).alias("max_betweenness"),
    round(avg("network_importance_score"), 1).alias("avg_importance_score"),
    sum("capacity_mw").alias("total_capacity_mw"),
).withColumnRenamed("city", "city_name")

grid_city = grid_city.withColumn("city_name", regexp_replace(col("city_name"), "_", " "))

summary_city_norm = city_ranked.withColumn(
    "city_name_clean", regexp_replace(col("city_name"), "_", " ")
)

combined = summary_city_norm.join(
    grid_city,
    summary_city_norm["city_name_clean"] == grid_city["city_name"],
    how="left"
).select(
    summary_city_norm["city_name"], "stress_rank", "risk_label",
    "avg_stress_score", "avg_daily_outages",
    "facility_count", "total_capacity_mw",
    "avg_importance_score", "avg_betweenness",
)

print("   Combined operations + grid criticality:")
combined.show(truncate=False)

# ── Step 9: Export results to Snowflake ──────────────────────────────────────
print("\n📤 Exporting Spark results to Snowflake...")

conn2 = snowflake.connector.connect(
    user=SNOWFLAKE_USER, password=SNOWFLAKE_PASSWORD, account=SNOWFLAKE_ACCOUNT,
    warehouse=SNOWFLAKE_WH, database=SNOWFLAKE_DB, schema=SNOWFLAKE_SCHEMA
)
cursor2 = conn2.cursor()

cursor2.execute("""
CREATE OR REPLACE TABLE RAW_SPARK_CITY_SUMMARY (
    CITY_NAME               VARCHAR(50),
    STRESS_RANK             NUMBER,
    RISK_LABEL              VARCHAR(30),
    AVG_STRESS_SCORE        FLOAT,
    AVG_DAILY_OUTAGES       FLOAT,
    TOTAL_OUTAGES           FLOAT,
    TOTAL_CUSTOMER_MIN_LOST FLOAT,
    AVG_CREWS_PER_DAY       FLOAT,
    TOTAL_LABOR_COST        FLOAT,
    AVG_TEMPERATURE_F       FLOAT,
    AVG_HEATING_DEGREE_DAYS FLOAT,
    AVG_COOLING_DEGREE_DAYS FLOAT,
    CREATED_AT              TIMESTAMP_NTZ
)
""")

spark_rows   = city_ranked.collect()
insert_rows  = [
    (
        r['city_name'], int(r['stress_rank']), r['risk_label'],
        float(r['avg_stress_score']),    float(r['avg_daily_outages']),
        float(r['total_outages']),       float(r['total_customer_minutes_lost']),
        float(r['avg_crews_per_day']),   float(r['total_labor_cost']),
        float(r['avg_temperature_f']),   float(r['avg_heating_degree_days']),
        float(r['avg_cooling_degree_days']), datetime.now()
    )
    for r in spark_rows
]

cursor2.executemany(
    "INSERT INTO RAW_SPARK_CITY_SUMMARY VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
    insert_rows
)
conn2.commit()
cursor2.close()
conn2.close()
print(f"   ✅ Loaded {len(insert_rows)} rows into RAW_SPARK_CITY_SUMMARY")

# ── Step 10: Stop Spark ───────────────────────────────────────────────────────
spark.stop()
print("\n🎉 PySpark analysis complete!")
print("   RAW_SPARK_CITY_SUMMARY is live in Snowflake.")