SELECT
	YEAR (order_date) order_year,
	MONTH(order_date) order_month,
	SUM (sales_amount) Total_Sales,
	COUNT(customer_key) Total_Customers,
	SUM (quantity) Total_Quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR (order_date), MONTH (order_date)
ORDER BY YEAR (order_date), MONTH (order_date)

OR

SELECT
	DATETRUNC(month, order_date) order_date,
	SUM (sales_amount) Total_Sales,
	COUNT(customer_key) Total_Customers,
	SUM (quantity) Total_Quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month, order_date)
ORDER BY DATETRUNC(month, order_date)
