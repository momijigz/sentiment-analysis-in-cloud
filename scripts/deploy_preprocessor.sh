# Deploy preprocessor Cloud Function
gcloud functions deploy preprocess-dataset \
  --gen2 \
  --runtime=python310 \
  --region=$REGION \
  --source=. \
  --entry-point=preprocess_dataset \
  --trigger-http \
  --allow-unauthenticated \
  --memory=1GB \
  --timeout=540s \
  --set-env-vars PROJECT_ID=$PROJECT_ID