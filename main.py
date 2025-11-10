import functions_framework
import pandas as pd
from google.cloud import storage, pubsub_v1
import json
import csv
from datetime import datetime, timedelta
import random

# Initialize clients
storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()

PROJECT_ID = '<project-id>'
TOPIC_PATH = publisher.topic_path(PROJECT_ID, 'social-media-posts')


@functions_framework.http
def preprocess_dataset(request):
    """
    Reads CSV from Cloud Storage, preprocesses, and publishes to Pub/Sub
    """
    # Parse request
    request_json = request.get_json(silent=True) or {}

    bucket_name = request_json.get('bucket', '<bucket-name>')
    file_name = request_json.get('file', 'raw/sample_20k.csv')
    batch_size = int(request_json.get('batch_size', 100))
    simulate_time = request_json.get('simulate_time', False)

    # Download file from GCS
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    try:
        # Try UTF-8 first
        content = blob.download_as_bytes().decode("utf-8")
    except UnicodeDecodeError:
        # Fallback to Latin-1 if UTF-8 fails
        print(f"UTF-8 decoding failed for {file_name}, falling back to Latin-1")
        content = blob.download_as_bytes().decode("latin-1", errors="replace")

    lines = content.splitlines()
    processed_count = 0
    error_count = 0

    # Column names for Sentiment140 dataset
    columns = ['target', 'ids', 'date', 'flag', 'user', 'text']
    sentiment_map = {0: -1.0, 2: 0.0, 4: 1.0}

    # Process in batches
    for i in range(0, len(lines), batch_size):
        batch = lines[i:i + batch_size]

        for line in batch:
            if not line.strip():
                continue

            try:
                row = next(csv.reader([line]))
                if len(row) != 6:
                    continue

                original_sentiment = int(row[0])

                post = {
                    'id': f"tweet_{row[1]}",
                    'text': row[5],
                    'author': f"user_{hash(row[4]) % 100000}",  # anonymized
                    'platform': 'twitter',
                    'timestamp': parse_twitter_date(row[2]),
                    'original_sentiment': sentiment_map.get(original_sentiment, 0.0),
                    'likes': random.randint(0, 1000),
                    'shares': random.randint(0, 500),
                    'comments': random.randint(0, 200)
                }

                if simulate_time:
                    post['timestamp'] = simulate_recent_timestamp()

                # Publish to Pub/Sub
                message_data = json.dumps(post).encode('utf-8')
                publisher.publish(TOPIC_PATH, message_data)

                processed_count += 1

            except Exception as e:
                error_count += 1
                print(f"Error processing row: {e}")

    return {
        'status': 'success',
        'processed': processed_count,
        'errors': error_count,
        'message': f'Processed {processed_count} tweets from {file_name}'
    }


def parse_twitter_date(date_str):
    """Parse Twitter date format: 'Mon May 11 03:17:40 UTC 2009'"""
    try:
        dt = datetime.strptime(date_str, "%a %b %d %H:%M:%S %Z %Y")
        return dt.isoformat()
    except Exception:
        return datetime.now().isoformat()


def simulate_recent_timestamp():
    """Generate timestamp within last 7 days for realistic simulation"""
    delta = timedelta(
        days=random.randint(0, 7),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59)
    )
    return (datetime.now() - delta).isoformat()