#!/usr/bin/env python3
"""
backfill.py — One-time script to load 76 hours of historical weather data
for Charlottesville, VA into DynamoDB table `weather-tracking`.

Uses Open-Meteo Archive API (no key required).
Relies on boto3 default credential chain (EC2 IAM role).
"""

import requests
import boto3
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError

# ── Configuration ───────────────────────────────────────────────────────────
LOCATION_ID = "charlottesville-va"
LAT, LON = 38.0293, -78.4767
TABLE_NAME = "weather-tracking"
HOURS_BACK = 76  # how many hours of history to fetch

# ── Helpers ─────────────────────────────────────────────────────────────────

def fetch_historical_weather():
    """Fetch the past HOURS_BACK hours from Open-Meteo Archive API."""
    now = datetime.now(timezone.utc)
    start = now - timedelta(hours=HOURS_BACK)

    # Archive API expects date strings (YYYY-MM-DD)
    start_date = start.strftime("%Y-%m-%d")
    end_date = now.strftime("%Y-%m-%d")

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": "temperature_2m,wind_speed_10m,precipitation",
        "timezone": "UTC",
    }

    print(f"Fetching historical data from {start_date} to {end_date} ...")
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    # The API returns arrays under data["hourly"]
    hourly = data["hourly"]
    times = hourly["time"]              # list of ISO strings
    temps = hourly["temperature_2m"]
    winds = hourly["wind_speed_10m"]
    precips = hourly["precipitation"]

    # Filter to only the most recent HOURS_BACK records
    records = []
    for i in range(len(times)):
        ts = datetime.fromisoformat(times[i]).replace(tzinfo=timezone.utc)
        if ts >= start:
            records.append({
                "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "temperature_2m": temps[i],
                "wind_speed_10m": winds[i],
                "precipitation": precips[i],
            })

    print(f"  Retrieved {len(records)} hourly records within the last {HOURS_BACK}h window.")
    return records


def compute_trend(temp_delta, precipitation):
    """
    Determine the trend label:
      PRECIP_EVENT  — precipitation > 0.5 mm  (highest priority)
      WARMING       — temp rose by ≥ +0.5 °C
      COOLING       — temp dropped by ≤ -0.5 °C
      STABLE        — otherwise
    """
    if precipitation is not None and precipitation > 0.5:
        return "PRECIP_EVENT"
    if temp_delta is not None and temp_delta >= 0.5:
        return "WARMING"
    if temp_delta is not None and temp_delta <= -0.5:
        return "COOLING"
    return "STABLE"


def put_record(table, record, prev_temp):
    """Write one record to DynamoDB. Skip if it already exists (idempotent)."""
    temp = record["temperature_2m"]
    precip = record["precipitation"]

    # Compute delta vs previous record
    if prev_temp is not None and temp is not None:
        temp_delta = round(temp - prev_temp, 2)
    else:
        temp_delta = 0.0

    trend = compute_trend(temp_delta, precip)

    item = {
        "location_id": LOCATION_ID,
        "timestamp": record["timestamp"],
        "temperature_2m": str(temp) if temp is not None else "0",
        "wind_speed_10m": str(record["wind_speed_10m"]) if record["wind_speed_10m"] is not None else "0",
        "precipitation": str(precip) if precip is not None else "0",
        "temp_delta": str(temp_delta),
        "trend": trend,
    }

    try:
        # ConditionExpression makes the put idempotent — skip if PK+SK already exist
        table.put_item(
            Item=item,
            ConditionExpression="attribute_not_exists(#ts)",
            ExpressionAttributeNames={"#ts": "timestamp"},
        )
        return "inserted", trend, temp_delta
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return "skipped", trend, temp_delta
        raise


# ── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("  Weather Backfill — Charlottesville, VA")
    print("=" * 60)

    # 1. Fetch historical weather
    records = fetch_historical_weather()
    if not records:
        print("No records returned. Exiting.")
        return

    # 2. Connect to DynamoDB
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table(TABLE_NAME)
    print(f"Connected to DynamoDB table: {TABLE_NAME}\n")

    # 3. Iterate chronologically and write each record
    inserted = 0
    skipped = 0
    prev_temp = None

    for i, rec in enumerate(records):
        status, trend, delta = put_record(table, rec, prev_temp)

        # Update prev_temp for the next iteration
        if rec["temperature_2m"] is not None:
            prev_temp = rec["temperature_2m"]

        label = "✓ INSERT" if status == "inserted" else "— SKIP  "
        delta_str = f"{delta:+.2f}" if delta else "+0.00"
        print(
            f"  [{i+1:3d}/{len(records)}] {label} | "
            f"{rec['timestamp']} | "
            f"temp={rec['temperature_2m']}°C | "
            f"delta={delta_str}°C | {trend}"
        )

        if status == "inserted":
            inserted += 1
        else:
            skipped += 1

    # 4. Summary
    print("\n" + "=" * 60)
    print(f"  Done! Inserted: {inserted} | Skipped (already existed): {skipped}")
    print(f"  Total records processed: {len(records)}")
    print("=" * 60)


if __name__ == "__main__":
    main()
