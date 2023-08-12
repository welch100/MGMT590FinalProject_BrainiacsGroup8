SELECT 
    date,
    weekday,
    day_number,
    category_name,
    total_sales,
    total_bottles_sold,
    ROUND(total_sales / total_bottles_sold, 2) as avg_bottle_price,
    CAST(predicted_total_bottles_sold AS INT64) as predicted_total_bottles_sold,
    CASE 
        WHEN ROUND((ROUND(total_sales / total_bottles_sold, 2) * predicted_total_bottles_sold), 2) < 0 THEN 0
        ELSE ROUND((ROUND(total_sales / total_bottles_sold, 2) * predicted_total_bottles_sold), 2)
    END as predicted_sales,
    high_temperature,
    low_temperature,
    precip,
    snow
FROM ML.PREDICT(MODEL `final-project-395500.model.total_sold_pred`, 
(
SELECT
  date,
  weekday,
  day_number,
  category_name,
  total_sales,
  total_bottles_sold,
  high_temperature,
  low_temperature,
  precip,
  snow
FROM
  `model.aggregated_data`
))
