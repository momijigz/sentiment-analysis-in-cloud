-- Create dataset
CREATE SCHEMA IF NOT EXISTS sentiment_data;

-- Main table for sentiment analysis results
CREATE OR REPLACE TABLE sentiment_data.posts (
  -- Identifiers
  post_id STRING NOT NULL,
  author STRING,
  platform STRING NOT NULL,
  
  -- Content
  text STRING NOT NULL,
  word_count INT64,
  char_count INT64,
  
  -- Timestamps
  timestamp TIMESTAMP NOT NULL,
  collected_at TIMESTAMP NOT NULL,
  
  -- Sentiment Analysis (VADER)
  sentiment_score FLOAT64,
  sentiment_magnitude FLOAT64,
  sentiment_positive FLOAT64,
  sentiment_negative FLOAT64,
  sentiment_neutral FLOAT64,
  sentiment_label STRING,
  
  -- Original labels from dataset
  original_sentiment FLOAT64,
  prediction_match BOOL,
  
  -- Engagement
  likes INT64,
  shares INT64,
  comments INT64,
  engagement_total INT64,
  
  -- Text features
  has_hashtags BOOL,
  has_mentions BOOL,
  has_url BOOL
)
PARTITION BY DATE(timestamp)
CLUSTER BY platform, sentiment_label;