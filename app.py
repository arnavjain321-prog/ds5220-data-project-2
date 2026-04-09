#!/usr/bin/env python3
"""
app.py — CronJob script that runs every 15 minutes to:
  1. Fetch current weather for Charlottesville, VA from Open-Meteo
  2. Compute temp_delta and trend vs most recent DynamoDB entry
  3. Write new record to DynamoDB
  4. Generate plot.png (seaborn time-series) and data.csv
  5. Upload both to S3 (static website bucket)

Uses boto3 default credential chain (EC2 IAM role) — no hardcoded creds.
"""

import os
import csv
import io
import requests
import boto3
import pandas as pd
import seaborn as sns
import matplotlib
matplotlib.use("Agg")  # headless backend for containers
import matplotlib.pyplot as plt
from datetime import datetime, timezone
from boto3.dynamodb.conditions import Key

# ── Configuration ───────────────────────────────────────────────────────────
LOCATION_ID = "charlottesville-va"
LAT, LON = 38.0293, -78.4767
TABLE_NAME = "weather-tracking"
S3_BUCKET = os.environ.get("S3_BUCKET", "")

# ── Step 1: Fetch current weather ──────────────────────────────────────────

def fetch_current_weather():
    """Call Open-Meteo forecast API for current conditions."""
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": LAT,
        "longitude": LON,
        "current": "temperature_2m,wind_speed_10m,precipitation",
        "timezone": "UTC",
    }
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()["current"]
    return {
        "temperature_2m": data["temperature_2m"],
        "wind_speed_10m": data["wind_speed_10m"],
        "precipitation": data["precipitation"],
    }

# ── Step 2: Query most recent DynamoDB entry ───────────────────────────────

def get_latest_record(table):
    """Query the most recent record for our location (sort key descending)."""
    response = table.query(
        KeyConditionExpression=Key("location_id").eq(LOCATION_ID),
        ScanIndexForward=False,  # descending by sort key (timestamp)
        Limit=1,
    )
    items = response.get("Items", [])
    return items[0] if items else None

# ── Step 3: Compute trend ──────────────────────────────────────────────────

def compute_trend(temp_delta, precipitation):
    """
    PRECIP_EVENT  — precipitation > 0.5 mm  (highest priority)
    WARMING       — temp rose ≥ +0.5 °C
    COOLING       — temp dropped ≤ -0.5 °C
    STABLE        — otherwise
    """
    if precipitation is not None and precipitation > 0.5:
        return "PRECIP_EVENT"
    if temp_delta is not None and temp_delta >= 0.5:
        return "WARMING"
    if temp_delta is not None and temp_delta <= -0.5:
        return "COOLING"
    return "STABLE"

# ── Step 4: Write new record ───────────────────────────────────────────────

def write_record(table, weather, temp_delta, trend):
    """Put a new item into DynamoDB."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    item = {
        "location_id": LOCATION_ID,
        "timestamp": now,
        "temperature_2m": str(weather["temperature_2m"]),
        "wind_speed_10m": str(weather["wind_speed_10m"]),
        "precipitation": str(weather["precipitation"]),
        "temp_delta": str(temp_delta),
        "trend": trend,
    }
    table.put_item(Item=item)
    return now

# ── Step 5: Read full history ──────────────────────────────────────────────

def read_all_records(table):
    """Query all records for our location, sorted chronologically."""
    records = []
    response = table.query(
        KeyConditionExpression=Key("location_id").eq(LOCATION_ID),
        ScanIndexForward=True,
    )
    records.extend(response["Items"])

    # Handle pagination if there are many records
    while "LastEvaluatedKey" in response:
        response = table.query(
            KeyConditionExpression=Key("location_id").eq(LOCATION_ID),
            ScanIndexForward=True,
            ExclusiveStartKey=response["LastEvaluatedKey"],
        )
        records.extend(response["Items"])

    return records

# ── Step 6: Generate plot.png ──────────────────────────────────────────────

def generate_plot(records, last_updated):
    """Create a seaborn time-series plot colored by trend."""
    # Build a DataFrame
    df = pd.DataFrame(records)
    df["timestamp"] = pd.to_datetime(df["timestamp"])
    df["temperature_2m"] = df["temperature_2m"].astype(float)
    df["trend"] = df["trend"].astype(str)

    # Color mapping for trend markers
    color_map = {
        "WARMING": "red",
        "COOLING": "blue",
        "STABLE": "gray",
        "PRECIP_EVENT": "yellow",
    }

    sns.set_theme(style="whitegrid")
    fig, ax = plt.subplots(figsize=(14, 5))

    # Line plot
    ax.plot(df["timestamp"], df["temperature_2m"], color="black", linewidth=0.8, alpha=0.6)

    # Scatter markers colored by trend
    for trend_label, color in color_map.items():
        subset = df[df["trend"] == trend_label]
        ax.scatter(
            subset["timestamp"], subset["temperature_2m"],
            color=color, label=trend_label, s=40, zorder=5, edgecolors="black", linewidths=0.3,
        )

    ax.set_title(f"Weather — Charlottesville, VA  |  Last updated: {last_updated}", fontsize=12)
    ax.set_xlabel("Time (UTC)")
    ax.set_ylabel("Temperature (°C)")
    ax.legend(title="Trend", loc="upper left", fontsize=8)
    fig.autofmt_xdate()
    fig.tight_layout()

    path = "/tmp/plot.png"
    fig.savefig(path, dpi=120)
    plt.close(fig)
    print(f"  Plot saved to {path}")
    return path

# ── Step 7: Generate data.csv ──────────────────────────────────────────────

def generate_csv(records):
    """Write all records to a flat CSV file."""
    path = "/tmp/data.csv"
    if not records:
        print("  No records to write to CSV.")
        return path

    fieldnames = ["location_id", "timestamp", "temperature_2m", "wind_speed_10m",
                  "precipitation", "temp_delta", "trend"]

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for rec in records:
            writer.writerow(rec)

    print(f"  CSV saved to {path} ({len(records)} rows)")
    return path

# ── Step 8: Upload to S3 ──────────────────────────────────────────────────

def upload_to_s3(file_path, key, content_type):
    """Upload a file to the S3 static-website bucket."""
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.upload_file(
        file_path, S3_BUCKET, key,
        ExtraArgs={"ContentType": content_type},
    )
    print(f"  Uploaded {key} to s3://{S3_BUCKET}/{key}")

# ── Main ────────────────────────────────────────────────────────────────────

def main():
    if not S3_BUCKET:
        print("ERROR: S3_BUCKET environment variable is not set.")
        return

    print("=" * 60)
    print("  Weather CronJob — Charlottesville, VA")
    print("=" * 60)

    # Connect to DynamoDB
    dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
    table = dynamodb.Table(TABLE_NAME)

    # 1. Fetch current weather
    try:
        weather = fetch_current_weather()
        print(f"  Current: temp={weather['temperature_2m']}°C, "
              f"wind={weather['wind_speed_10m']} km/h, "
              f"precip={weather['precipitation']} mm")
    except Exception as e:
        print(f"ERROR fetching weather: {e}")
        return

    # 2. Get most recent entry for delta computation
    try:
        latest = get_latest_record(table)
    except Exception as e:
        print(f"ERROR querying DynamoDB: {e}")
        return

    # 3. Compute delta and trend (handle first-run gracefully)
    if latest:
        prev_temp = float(latest["temperature_2m"])
        temp_delta = round(weather["temperature_2m"] - prev_temp, 2)
    else:
        print("  First run — no prior entry found. Delta defaults to 0.")
        temp_delta = 0.0

    trend = compute_trend(temp_delta, weather["precipitation"])

    # 4. Write new record
    try:
        ts = write_record(table, weather, temp_delta, trend)
    except Exception as e:
        print(f"ERROR writing to DynamoDB: {e}")
        return

    # 5. Read full history
    try:
        records = read_all_records(table)
        print(f"  Total records in DB: {len(records)}")
    except Exception as e:
        print(f"ERROR reading history: {e}")
        return

    # 6. Generate plot and CSV
    try:
        plot_path = generate_plot(records, ts)
        csv_path = generate_csv(records)
    except Exception as e:
        print(f"ERROR generating outputs: {e}")
        return

    # 7. Upload to S3
    try:
        upload_to_s3(plot_path, "plot.png", "image/png")
        upload_to_s3(csv_path, "data.csv", "text/csv")
    except Exception as e:
        print(f"ERROR uploading to S3: {e}")
        return

    # 8. Summary log line
    delta_str = f"{temp_delta:+.1f}" if temp_delta else "+0.0"
    print(
        f"\nWEATHER | temp={weather['temperature_2m']}°C | "
        f"delta={delta_str}°C | {trend} | "
        f"wind={weather['wind_speed_10m']} km/h | "
        f"precip={weather['precipitation']}mm"
    )
    print("✅ CronJob complete.")


if __name__ == "__main__":
    main()
