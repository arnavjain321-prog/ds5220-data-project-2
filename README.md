# DS5220 Data Project 2 - Weather Tracking Pipeline

## Data Source

This pipeline collects hourly weather data for Charlottesville, VA (lat 38.0293, lon -78.4767) using the [Open-Meteo API](https://open-meteo.com/en/docs). Open-Meteo provides free, no-key-required access to current and historical weather data worldwide. On each run the pipeline fetches three fields: temperature at 2 meters above ground (in Celsius), wind speed at 10 meters (in km/h), and precipitation (in mm).

## Scheduled Process

The pipeline runs as a Kubernetes CronJob on an EC2 instance, executing every 15 minutes. Each execution does the following:

1. Calls the Open-Meteo forecast API to get the current weather conditions for Charlottesville.
2. Queries the DynamoDB table (`weather-tracking`) for the most recent previous entry.
3. Computes the temperature delta between the current reading and the last recorded value.
4. Classifies the trend based on that delta and precipitation:
   - **PRECIP_EVENT** if precipitation exceeds 0.5 mm (highest priority)
   - **WARMING** if temperature increased by 0.5 C or more
   - **COOLING** if temperature decreased by 0.5 C or more
   - **STABLE** otherwise
5. Writes the new record (timestamp, temperature, wind speed, precipitation, delta, and trend) to DynamoDB.
6. Reads the full history from DynamoDB, generates an updated plot and CSV, and uploads both to S3.

A one-time backfill script (`backfill.py`) was used to seed the table with 76 hours of historical data from the Open-Meteo Archive API before the CronJob started running.

## Output

**data.csv** contains every recorded data point with columns: `location_id`, `timestamp`, `temperature_2m`, `wind_speed_10m`, `precipitation`, `temp_delta`, and `trend`. Each row represents one weather observation.

**plot.png** is a seaborn time-series chart showing temperature over time. Each data point is represented by a colored marker indicating its trend: red for WARMING, blue for COOLING, gray for STABLE, and yellow for PRECIP_EVENT. The title includes the location name and the timestamp of the most recent update. This plot is overwritten on every CronJob run so it always reflects the latest data.

Both files are served publicly from an S3 static website bucket.
