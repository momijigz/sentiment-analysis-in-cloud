# Extract first 10,000 rows (negative tweets)
head -n 10000 dataset/training.1600000.processed.noemoticon.csv > dataset/first_10k.csv

# Extract last 10,000 rows (positive tweets)
tail -n 10000 dataset/training.1600000.processed.noemoticon.csv > dataset/last_10k.csv

# Combine both into a balanced dataset
cat dataset/first_10k.csv dataset/last_10k.csv > dataset/sample_20k.csv

# Create GCS bucket
gsutil mb -p $PROJECT_ID -l $REGION gs ://${ PROJECT_ID }-sentiment -data

# Upload datasets
gsutil cp sample_10k .csv gs ://${ PROJECT_ID }-sentiment -data/raw/
