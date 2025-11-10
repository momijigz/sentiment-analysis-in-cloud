CREATE OR REPLACE VIEW sentiment_data.looker_confusion_matrix AS
SELECT
  -- Actual label
  CASE
    WHEN original_sentiment > 0 THEN 'Positive'
    ELSE 'Negative'
  END as actual_label,
  
  -- Predicted label
  CASE 
    WHEN sentiment_score >= 0 THEN 'Positive'
    ELSE 'Negative'
  END as predicted_label,
  
  COUNT(*) as count,
  
  -- Percentage of total
  COUNT(*) * 100.0 / SUM(COUNT(*)) OVER() as percentage
  
FROM `sentiment_data.posts`
WHERE original_sentiment IS NOT NULL
GROUP BY actual_label, predicted_label
ORDER BY actual_label, predicted_label;