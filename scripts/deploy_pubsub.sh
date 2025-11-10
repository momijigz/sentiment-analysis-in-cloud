# Deploy with Pub/Sub trigger
gcloud functions deploy analyze-sentiment \
  --gen2 \
  --runtime=python310 \
  --region=$REGION \
  --source=. \
  --entry-point=analyze_sentiment \
  --trigger-topic=social-media-posts \
  --memory=512MB \
  --timeout=60s \
  --set-env-vars PROJECT_ID=$PROJECT_ID