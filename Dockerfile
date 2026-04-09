# Dockerfile — Weather CronJob container
# Runs app.py to fetch weather, update DynamoDB, and push artifacts to S3.

FROM python:3.11-slim

# Install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy application code
COPY app.py /app/app.py

WORKDIR /app

# Run the CronJob script
CMD ["python", "app.py"]
