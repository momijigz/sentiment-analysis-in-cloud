CREATE OR REPLACE VIEW sentiment_data.looker_executive_summary AS
SELECT
  DATE(timestamp) as date,
  COUNT(*) as total_posts,
  AVG(sentiment_score) as avg_sentiment,
  
  -- PREDICTED sentiment (what VADER says)
  COUNTIF(sentiment_score >= 0) as predicted_positive_count,
  COUNTIF(sentiment_score < 0) as predicted_negative_count,
  COUNTIF(sentiment_score >= 0) * 100.0 / COUNT(*) as predicted_positive_pct,
  COUNTIF(sentiment_score < 0) * 100.0 / COUNT(*) as predicted_negative_pct,
  
  -- ACTUAL sentiment (ground truth from dataset)
  COUNTIF(original_sentiment > 0) as actual_positive_count,
  COUNTIF(original_sentiment < 0) as actual_negative_count,
  COUNTIF(original_sentiment > 0) * 100.0 / NULLIF(COUNTIF(original_sentiment IS NOT NULL), 0) as actual_positive_pct,
  COUNTIF(original_sentiment < 0) * 100.0 / NULLIF(COUNTIF(original_sentiment IS NOT NULL), 0) as actual_negative_pct,
  
  -- Engagement
  SUM(engagement_total) as total_engagement,
  AVG(engagement_total) as avg_engagement,
  
  -- Authors
  COUNT(DISTINCT author) as unique_authors,
  
  -- Overall accuracy
  COUNTIF(
    (sentiment_score >= 0 AND original_sentiment > 0) OR
    (sentiment_score < 0 AND original_sentiment < 0)
  ) as correct_predictions,
  COUNTIF(original_sentiment IS NOT NULL) as total_with_labels,
  
  -- Overall accuracy percentage
  CASE 
    WHEN COUNTIF(original_sentiment IS NOT NULL) > 0 THEN
      COUNTIF(
        (sentiment_score >= 0 AND original_sentiment > 0) OR
        (sentiment_score < 0 AND original_sentiment < 0)
      ) * 100.0 / COUNTIF(original_sentiment IS NOT NULL)
    ELSE NULL
  END as daily_accuracy_pct,
  
  -- POSITIVE CLASS ACCURACY (Recall)
  COUNTIF(sentiment_score >= 0 AND original_sentiment > 0) as positive_correct,
  COUNTIF(original_sentiment > 0) as positive_total,
  CASE 
    WHEN COUNTIF(original_sentiment > 0) > 0 THEN
      COUNTIF(sentiment_score >= 0 AND original_sentiment > 0) * 100.0 / 
      COUNTIF(original_sentiment > 0)
    ELSE NULL
  END as positive_accuracy_pct,
  
  -- NEGATIVE CLASS ACCURACY (Recall)
  COUNTIF(sentiment_score < 0 AND original_sentiment < 0) as negative_correct,
  COUNTIF(original_sentiment < 0) as negative_total,
  CASE 
    WHEN COUNTIF(original_sentiment < 0) > 0 THEN
      COUNTIF(sentiment_score < 0 AND original_sentiment < 0) * 100.0 / 
      COUNTIF(original_sentiment < 0)
    ELSE NULL
  END as negative_accuracy_pct
  
FROM `sentiment_data.posts`
WHERE timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
GROUP BY date
ORDER BY date DESC;