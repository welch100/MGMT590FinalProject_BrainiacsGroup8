SELECT date, category_name, vendor_name, item_number, item_description, bottle_volume_ml, 
       state_bottle_cost, state_bottle_retail, bottles_sold, sale_dollars, 
       volume_sold_gallons, high, low, precip, snow
FROM
  `final-project-395500.iowa_liquor_sales.sales` AS A
JOIN
  `final-project-395500.historical_data.avg_wx` AS B
ON
  A.date = B.day
WHERE 
  date IS NOT NULL AND category_name IS NOT NULL AND vendor_name IS NOT NULL AND 
  item_number IS NOT NULL AND item_description IS NOT NULL AND bottle_volume_ml IS NOT NULL AND 
  state_bottle_cost IS NOT NULL AND state_bottle_retail IS NOT NULL AND bottles_sold IS NOT NULL AND 
  sale_dollars IS NOT NULL AND volume_sold_gallons IS NOT NULL AND high IS NOT NULL AND 
  low IS NOT NULL AND precip IS NOT NULL AND snow IS NOT NULL
ORDER BY date ASC;
