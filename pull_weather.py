# pull_weather.py
# Purpose: Pull real Colorado weather data from Open-Meteo API
# This is our data INGESTION layer - the first step in our pipeline

import requests   # for calling web APIs
import pandas as pd  # for organizing data into tables
from datetime import datetime, timedelta

# --- STEP 1: Define our Colorado locations ---
# These are real Colorado cities relevant to utilities operations
locations = [
    {"name": "Denver",         "lat": 39.7392, "lon": -104.9903},
    {"name": "Colorado_Springs","lat": 38.8339, "lon": -104.8214},
    {"name": "Boulder",        "lat": 40.0150, "lon": -105.2705},
    {"name": "Fort_Collins",   "lat": 40.5853, "lon": -105.0844},
    {"name": "Pueblo",         "lat": 38.2544, "lon": -104.6091},
]

# --- STEP 2: Set date range (last 90 days of real weather) ---
start_date = (datetime.today() - timedelta(days=90)).strftime('%Y-%m-%d')
end_date = (datetime.today() - timedelta(days=5)).strftime('%Y-%m-%d')

print(f"Pulling weather data from {start_date} to {end_date}")

# --- STEP 3: Pull data for each city ---
all_data = []  # empty list to collect all city data

for city in locations:
    print(f"  Fetching: {city['name']}...")
    
    # Build the API URL - Open-Meteo is free, no API key needed!
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min", 
            "precipitation_sum",
            "windspeed_10m_max",
            "weathercode"
        ],
        "temperature_unit": "fahrenheit",
        "windspeed_unit": "mph",
        "timezone": "America/Denver",
        "start_date": start_date,
        "end_date": end_date
    }
    
    # Make the API call
    response = requests.get(url, params=params)
    data = response.json()
    
    # Extract the daily data
    daily = data["daily"]
    
    # Create a mini-table for this city
    df = pd.DataFrame({
        "date": daily["time"],
        "city": city["name"],
        "temp_max_f": daily["temperature_2m_max"],
        "temp_min_f": daily["temperature_2m_min"],
        "precipitation_inches": daily["precipitation_sum"],
        "wind_speed_max_mph": daily["windspeed_10m_max"],
        "weather_code": daily["weathercode"]
    })
    
    all_data.append(df)

# --- STEP 4: Combine all cities into one table ---
final_df = pd.concat(all_data, ignore_index=True)
# Remove rows where weather data is missing
final_df = final_df.dropna(subset=['temp_max_f', 'temp_min_f'])

# --- STEP 5: Save to CSV ---
output_file = "raw_weather_colorado.csv"
final_df.to_csv(output_file, index=False)

print(f"\nDone! Saved {len(final_df)} rows to {output_file}")
print(final_df.head(10))