CREATE OR REPLACE VIEW sentiment_data.looker_top_posts AS
SELECT
  post_id,
  text,
  author,
  -- Binary classification
  CASE 
    WHEN sentiment_score >= 0 THEN 'positive'
    ELSE 'negative'
  END as sentiment_label,
  sentiment_score,
  engagement_total,
  likes,
  shares,
  comments,
  timestamp,
  DATE(timestamp) as date
FROM `sentiment_data.posts`
WHERE 
  timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
  AND engagement_total > 0
ORDER BY engagement_total DESC
LIMIT 1000;