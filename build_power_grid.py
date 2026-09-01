import os
import networkx as nx
import snowflake.connector
from datetime import datetime

# ── Snowflake connection ───────────────────────────────────────────────────────
conn = snowflake.connector.connect(
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    database=os.environ.get("SNOWFLAKE_DATABASE", "UTILITIES_RAW"),
    schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
)
cursor = conn.cursor()

# ── Build the Graph ────────────────────────────────────────────────────────────
G = nx.Graph()

# Add 15 nodes (substations + power plants)
nodes = [
    ("DEN_TRANS_HUB",  {"node_type": "Transmission Hub", "city": "Denver",           "voltage_kv": 345, "capacity_mw": 2500, "lat": 39.7392, "lon": -104.9903}),
    ("DEN_NORTH_SUB",  {"node_type": "Substation",       "city": "Denver",           "voltage_kv": 138, "capacity_mw": 800,  "lat": 39.7920, "lon": -104.9700}),
    ("DEN_SOUTH_SUB",  {"node_type": "Substation",       "city": "Denver",           "voltage_kv": 138, "capacity_mw": 750,  "lat": 39.6800, "lon": -104.9500}),
    ("DEN_EAST_SUB",   {"node_type": "Substation",       "city": "Denver",           "voltage_kv": 115, "capacity_mw": 600,  "lat": 39.7200, "lon": -104.8800}),
    ("CS_TRANS_HUB",   {"node_type": "Transmission Hub", "city": "Colorado Springs", "voltage_kv": 345, "capacity_mw": 1800, "lat": 38.8339, "lon": -104.8214}),
    ("CS_NORTH_SUB",   {"node_type": "Substation",       "city": "Colorado Springs", "voltage_kv": 138, "capacity_mw": 600,  "lat": 38.8800, "lon": -104.8200}),
    ("CS_SOUTH_SUB",   {"node_type": "Substation",       "city": "Colorado Springs", "voltage_kv": 115, "capacity_mw": 500,  "lat": 38.7800, "lon": -104.8300}),
    ("BLD_HUB",        {"node_type": "Transmission Hub", "city": "Boulder",          "voltage_kv": 230, "capacity_mw": 1200, "lat": 40.0150, "lon": -105.2705}),
    ("BLD_EAST_SUB",   {"node_type": "Substation",       "city": "Boulder",          "voltage_kv": 115, "capacity_mw": 400,  "lat": 40.0100, "lon": -105.2000}),
    ("FC_HUB",         {"node_type": "Transmission Hub", "city": "Fort Collins",     "voltage_kv": 230, "capacity_mw": 1100, "lat": 40.5853, "lon": -105.0844}),
    ("FC_SOUTH_SUB",   {"node_type": "Substation",       "city": "Fort Collins",     "voltage_kv": 115, "capacity_mw": 350,  "lat": 40.5400, "lon": -105.0700}),
    ("PUE_HUB",        {"node_type": "Transmission Hub", "city": "Pueblo",           "voltage_kv": 230, "capacity_mw": 900,  "lat": 38.2544, "lon": -104.6091}),
    ("PUE_NORTH_SUB",  {"node_type": "Substation",       "city": "Pueblo",           "voltage_kv": 115, "capacity_mw": 350,  "lat": 38.3000, "lon": -104.6100}),
    ("PAWNEE_PLANT",   {"node_type": "Power Plant",      "city": "Fort Collins",     "voltage_kv": 345, "capacity_mw": 505,  "lat": 40.6167, "lon": -104.6833}),
    ("COMANCHE_PLANT", {"node_type": "Power Plant",      "city": "Pueblo",           "voltage_kv": 345, "capacity_mw": 1500, "lat": 38.2167, "lon": -104.5167}),
]

for node_id, attrs in nodes:
    G.add_node(node_id, **attrs)

# Add 17 edges (power lines)
edges = [
    ("COMANCHE_PLANT", "CS_TRANS_HUB",  {"distance_miles": 8,  "capacity_mw": 1200, "line_type": "Transmission"}),
    ("COMANCHE_PLANT", "PUE_HUB",       {"distance_miles": 5,  "capacity_mw": 800,  "line_type": "Transmission"}),
    ("CS_TRANS_HUB",   "DEN_TRANS_HUB", {"distance_miles": 70, "capacity_mw": 1500, "line_type": "Transmission"}),
    ("CS_TRANS_HUB",   "CS_NORTH_SUB",  {"distance_miles": 8,  "capacity_mw": 600,  "line_type": "Distribution"}),
    ("CS_TRANS_HUB",   "CS_SOUTH_SUB",  {"distance_miles": 10, "capacity_mw": 500,  "line_type": "Distribution"}),
    ("CS_TRANS_HUB",   "PUE_HUB",       {"distance_miles": 45, "capacity_mw": 900,  "line_type": "Transmission"}),
    ("DEN_TRANS_HUB",  "DEN_NORTH_SUB", {"distance_miles": 6,  "capacity_mw": 800,  "line_type": "Distribution"}),
    ("DEN_TRANS_HUB",  "DEN_SOUTH_SUB", {"distance_miles": 8,  "capacity_mw": 750,  "line_type": "Distribution"}),
    ("DEN_TRANS_HUB",  "DEN_EAST_SUB",  {"distance_miles": 10, "capacity_mw": 600,  "line_type": "Distribution"}),
    ("DEN_TRANS_HUB",  "BLD_HUB",       {"distance_miles": 28, "capacity_mw": 1000, "line_type": "Transmission"}),
    ("DEN_TRANS_HUB",  "FC_HUB",        {"distance_miles": 65, "capacity_mw": 900,  "line_type": "Transmission"}),
    ("PAWNEE_PLANT",   "FC_HUB",        {"distance_miles": 12, "capacity_mw": 505,  "line_type": "Transmission"}),
    ("PAWNEE_PLANT",   "BLD_HUB",       {"distance_miles": 40, "capacity_mw": 400,  "line_type": "Transmission"}),
    ("BLD_HUB",        "FC_HUB",        {"distance_miles": 45, "capacity_mw": 600,  "line_type": "Transmission"}),
    ("BLD_HUB",        "BLD_EAST_SUB",  {"distance_miles": 5,  "capacity_mw": 400,  "line_type": "Distribution"}),
    ("FC_HUB",         "FC_SOUTH_SUB",  {"distance_miles": 6,  "capacity_mw": 350,  "line_type": "Distribution"}),
    ("PUE_HUB",        "PUE_NORTH_SUB", {"distance_miles": 7,  "capacity_mw": 350,  "line_type": "Distribution"}),
]

for source, target, attrs in edges:
    G.add_edge(source, target, **attrs)

# ── Graph Analysis ─────────────────────────────────────────────────────────────
print("=" * 55)
print("   COLORADO POWER GRID — NETWORK ANALYSIS")
print("=" * 55)

print(f"\n📊 Basic Stats:")
print(f"   Nodes (facilities) : {G.number_of_nodes()}")
print(f"   Edges (power lines): {G.number_of_edges()}")
print(f"   Fully connected    : {nx.is_connected(G)}")

# Shortest path
print(f"\n⚡ Shortest Path — COMANCHE_PLANT → FC_HUB:")
path = nx.shortest_path(G, source="COMANCHE_PLANT", target="FC_HUB", weight="distance_miles")
path_length = nx.shortest_path_length(G, source="COMANCHE_PLANT", target="FC_HUB", weight="distance_miles")
print(f"   Route : {' → '.join(path)}")
print(f"   Total : {path_length} miles")

# Degree centrality
degree_centrality = nx.degree_centrality(G)
sorted_dc = sorted(degree_centrality.items(), key=lambda x: x[1], reverse=True)
print(f"\n🔗 Degree Centrality — Top 5 (most connected):")
for node, score in sorted_dc[:5]:
    print(f"   {node:<22}: {score:.3f}")

# Betweenness centrality
betweenness = nx.betweenness_centrality(G, weight="distance_miles")
sorted_bc = sorted(betweenness.items(), key=lambda x: x[1], reverse=True)
print(f"\n🌐 Betweenness Centrality — Top 5 (biggest bridges):")
for node, score in sorted_bc[:5]:
    print(f"   {node:<22}: {score:.3f}")

# Failure simulation — remove Denver Transmission Hub
print(f"\n🔴 FAILURE SIMULATION — Remove DEN_TRANS_HUB:")
G_failure = G.copy()
G_failure.remove_node("DEN_TRANS_HUB")
print(f"   Grid still connected : {nx.is_connected(G_failure)}")
print(f"   Isolated islands     : {nx.number_connected_components(G_failure)}")
for i, component in enumerate(nx.connected_components(G_failure)):
    print(f"   Island {i+1}: {sorted(component)}")

print("=" * 55)

# ── Create Snowflake Tables ────────────────────────────────────────────────────
print("\n📤 Exporting to Snowflake...")

cursor.execute("""
CREATE OR REPLACE TABLE RAW_GRID_NODES (
    NODE_ID                 VARCHAR(50),
    NODE_TYPE               VARCHAR(50),
    CITY                    VARCHAR(50),
    VOLTAGE_KV              NUMBER,
    CAPACITY_MW             NUMBER,
    LATITUDE                FLOAT,
    LONGITUDE               FLOAT,
    DEGREE_CENTRALITY       FLOAT,
    BETWEENNESS_CENTRALITY  FLOAT,
    CREATED_AT              TIMESTAMP_NTZ
)
""")

cursor.execute("""
CREATE OR REPLACE TABLE RAW_GRID_EDGES (
    SOURCE_NODE     VARCHAR(50),
    TARGET_NODE     VARCHAR(50),
    DISTANCE_MILES  NUMBER,
    CAPACITY_MW     NUMBER,
    LINE_TYPE       VARCHAR(50),
    CREATED_AT      TIMESTAMP_NTZ
)
""")

# ── Insert Nodes ───────────────────────────────────────────────────────────────
node_rows = []
for node_id, attrs in G.nodes(data=True):
    node_rows.append((
        node_id,
        attrs['node_type'],
        attrs['city'],
        attrs['voltage_kv'],
        attrs['capacity_mw'],
        attrs['lat'],
        attrs['lon'],
        round(degree_centrality[node_id], 4),
        round(betweenness[node_id], 4),
        datetime.now()
    ))

cursor.executemany(
    "INSERT INTO RAW_GRID_NODES VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
    node_rows
)
print(f"   ✅ Loaded {len(node_rows)} rows into RAW_GRID_NODES")

# ── Insert Edges ───────────────────────────────────────────────────────────────
edge_rows = []
for source, target, attrs in G.edges(data=True):
    edge_rows.append((
        source,
        target,
        attrs['distance_miles'],
        attrs['capacity_mw'],
        attrs['line_type'],
        datetime.now()
    ))

cursor.executemany(
    "INSERT INTO RAW_GRID_EDGES VALUES (%s,%s,%s,%s,%s,%s)",
    edge_rows
)
print(f"   ✅ Loaded {len(edge_rows)} rows into RAW_GRID_EDGES")

# ── Cleanup ────────────────────────────────────────────────────────────────────
conn.commit()
cursor.close()
conn.close()
print("\n🎉 Done! RAW_GRID_NODES and RAW_GRID_EDGES are live in Snowflake.")