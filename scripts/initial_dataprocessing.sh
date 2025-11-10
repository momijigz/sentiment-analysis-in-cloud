# Get preprocessor function URL
PREPROCESS_URL=$(gcloud functions describe preprocess-dataset \
  --region=$REGION \
  --gen2 \
  --format='value(serviceConfig.uri)')

# Process sample dataset (20k tweets)
curl -X POST $PREPROCESS_URL \
  -H "Content-Type: application/json" \
  -d '{
    "bucket": "'${PROJECT_ID}'-sentiment-data",
    "file": "raw/sample_20ksv",
    "batch_size": 100,
    "simulate_time": true
  }'