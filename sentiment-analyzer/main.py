"""
Cloud Function for Sentiment Analysis using NLTK VADER
Triggered by Pub/Sub messages
"""

import functions_framework
import json
import base64
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
from google.cloud import bigquery, firestore
from datetime import datetime
import logging

# Download NLTK data on cold start
try:
    nltk.data.find('vader_lexicon')
except LookupError:
    nltk.download('vader_lexicon', quiet=True)

# Initialize clients
sia = SentimentIntensityAnalyzer()
bq_client = bigquery.Client()
db = firestore.Client()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BQ_DATASET = 'sentiment_data'
BQ_TABLE = 'posts'

@functions_framework.cloud_event
def analyze_sentiment(cloud_event):
    """
    Analyzes sentiment of social media posts using NLTK VADER
    """
    try:
        # Decode Pub/Sub message
        pubsub_message = base64.b64decode(cloud_event.data["message"]["data"])
        post = json.loads(pubsub_message)
        
        # Validate
        if not post.get('text') or len(post['text']) < 5:
            logger.warning(f"Invalid post: {post.get('id')}")
            return {'status': 'skipped'}
        
        # Clean text
        text = clean_text(post['text'])
        
        # Analyze sentiment with NLTK VADER
        sentiment_scores = sia.polarity_scores(text)
        
        # Enrich post data
        result = {
            'post_id': post['id'],
            'text': post['text'][:500],  # Truncate for storage
            'author': post['author'],
            'platform': post['platform'],
            'timestamp': post['timestamp'],
            'collected_at': datetime.now().isoformat(),
            
            # VADER scores
            'sentiment_score': sentiment_scores['compound'],  # -1 to 1
            'sentiment_magnitude': abs(sentiment_scores['compound']),
            'sentiment_positive': sentiment_scores['pos'],
            'sentiment_negative': sentiment_scores['neg'],
            'sentiment_neutral': sentiment_scores['neu'],
            'sentiment_label': get_sentiment_label(sentiment_scores['compound']),
            
            # Original label from dataset (for comparison)
            'original_sentiment': post.get('original_sentiment'),
            
            # Engagement metrics
            'likes': post.get('likes', 0),
            'shares': post.get('shares', 0),
            'comments': post.get('comments', 0),
            'engagement_total': post.get('likes', 0) + post.get('shares', 0) + post.get('comments', 0),
            
            # Text features
            'word_count': len(text.split()),
            'char_count': len(text),
            'has_hashtags': '#' in text,
            'has_mentions': '@' in text,
            'has_url': 'http' in text.lower()
        }
        
        # Calculate accuracy if original sentiment exists
        if result['original_sentiment'] is not None:
            result['prediction_match'] = check_prediction_match(
                result['sentiment_score'],
                result['original_sentiment']
            )
        
        # Store results
        store_in_bigquery(result)
        update_firestore(result)
        
        logger.info(f"Processed: {result['post_id']}, Score: {result['sentiment_score']:.2f}")
        
        return {'status': 'success', 'post_id': result['post_id']}
        
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise

def clean_text(text):
    """
    Clean tweet text
    """
    # Remove URLs
    text = ' '.join([word for word in text.split() if not word.startswith('http')])
    
    # Basic cleaning
    text = text.strip()
    
    return text

def get_sentiment_label(compound_score):
    """
    Convert VADER compound score to label
    UPDATED: Binary classification (no neutral) to match Sentiment140 dataset
    """
    if compound_score >= 0:
        return 'positive'
    else:
        return 'negative'

def check_prediction_match(predicted_score, original_score):
    """
    Check if prediction matches original label
    FIXED: Uses binary classification since Sentiment140 has no neutral class
    """
    # For binary classification (positive vs negative only)
    predicted_label = 'positive' if predicted_score > 0 else 'negative'
    actual_label = 'positive' if original_score > 0 else 'negative'
    
    return predicted_label == actual_label

def store_in_bigquery(result):
    """
    Store result in BigQuery
    """
    table_id = f"{bq_client.project}.{BQ_DATASET}.{BQ_TABLE}"
    
    # Convert to BigQuery row
    row = {
        'post_id': result['post_id'],
        'text': result['text'],
        'author': result['author'],
        'platform': result['platform'],
        'timestamp': result['timestamp'],
        'collected_at': result['collected_at'],
        'sentiment_score': result['sentiment_score'],
        'sentiment_magnitude': result['sentiment_magnitude'],
        'sentiment_positive': result['sentiment_positive'],
        'sentiment_negative': result['sentiment_negative'],
        'sentiment_neutral': result['sentiment_neutral'],
        'sentiment_label': result['sentiment_label'],
        'original_sentiment': result.get('original_sentiment'),
        'prediction_match': result.get('prediction_match'),
        'likes': result['likes'],
        'shares': result['shares'],
        'comments': result['comments'],
        'engagement_total': result['engagement_total'],
        'word_count': result['word_count'],
        'char_count': result['char_count'],
        'has_hashtags': result['has_hashtags'],
        'has_mentions': result['has_mentions'],
        'has_url': result['has_url']
    }
    
    errors = bq_client.insert_rows_json(table_id, [row])
    
    if errors:
        logger.error(f"BigQuery errors: {errors}")
