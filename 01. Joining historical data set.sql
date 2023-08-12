#standardsql
CREATE OR REPLACE MODEL`final-project-395500.model.total_sold_pred`
OPTIONS
  (model_type='linear_reg', input_label_cols=['total_bottles_sold']) AS
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
`final-project-395500.model.aggregated_data`;