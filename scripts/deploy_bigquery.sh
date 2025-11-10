# Create dataset
bq mk --dataset --location=$REGION $PROJECT_ID:sentiment_data

# Create data content like tables etc.
bq query --use_legacy_sql=false < create_looker_views.sql