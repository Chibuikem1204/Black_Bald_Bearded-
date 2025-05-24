--Calculate the total sales per month
--and the running total of sales over time

SELECT
	order_date,
	total_sales,
	SUM(total_sales) OVER (ORDER BY order_date) running_total,
	avg(avg_price) OVER (ORDER BY order_date) moving_average
FROM
(
SELECT
	DATETRUNC (YEAR, order_date) order_date,
	SUM(sales_amount) total_sales,
	AVG(price) avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC (YEAR, order_date) 
)t


