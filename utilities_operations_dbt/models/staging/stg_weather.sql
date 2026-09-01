-- stg_weather.sql
-- Purpose: Clean and standardize raw weather data

SELECT
    date                                    AS weather_date,
    city                                    AS city_name,
    temp_max_f,
    temp_min_f,
    ROUND((temp_max_f + temp_min_f) / 2, 1) AS temp_avg_f,
    precipitation_inches,
    wind_speed_max_mph,
    weather_code,
    CASE 
        WHEN weather_code = 0  THEN 'Clear Sky'
        WHEN weather_code <= 3 THEN 'Partly Cloudy'
        WHEN weather_code <= 48 THEN 'Foggy'
        WHEN weather_code <= 67 THEN 'Rainy'
        WHEN weather_code <= 77 THEN 'Snowy'
        WHEN weather_code <= 82 THEN 'Rain Showers'
        WHEN weather_code <= 99 THEN 'Thunderstorm'
        ELSE 'Unknown'
    END AS weather_description

FROM UTILITIES_RAW.PUBLIC.RAW_WEATHER