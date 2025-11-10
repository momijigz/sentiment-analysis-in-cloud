# Create topics
gcloud pubsub topics create social-media-posts
gcloud pubsub topics create sentiment-results

# Create subscriptions
gcloud pubsub subscriptions create posts-subscription \
  --topic=social-media-posts \
  --ack-deadline=60

gcloud pubsub subscriptions create results-subscription \
  --topic=sentiment-results \
  --ack-deadline=60