/*Segment Products into cost ranges and 
count how many products fall into each segment*/

WITH Product_Cost_Range AS

(SELECT 
	product_key,
	product_name,
	cost,
	CASE WHEN cost < 100 THEN 'Below 100'
		WHEN cost BETWEEN 100 AND 500 THEN '100-500'
		WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
		ELSE 'Above 1000'
	END AS 'cost_range'
FROM gold.dim_products
)
SELECT
	cost_range,
	COUNT (product_key) count_products
FROM Product_Cost_Range
GROUP BY cost_range
ORDER BY count_products desc


/*Group customers into three segments based on their spending behaviour:
	-VIP: Customers with atleast 12 months history and spending more than £5000
	-Regular: Customers with at least 12 months history but spending £5000 or less
	-New: Customers with a lifespan less than 12 months
And find the total number of customers by each group*/

WITH Customers_Lifespan AS 
(
SELECT
	c.customer_key,
	SUM(f.sales_amount) AS Total_Spending,
	MIN(order_date) AS first_order,
	MAX(order_date) AS last_order,
	DATEDIFF(Month, MIN(order_date), MAX(order_date)) AS lifespan
FROM gold.fact_sales F
LEFT JOIN gold.dim_customers C
ON C.customer_key = F.customer_key
GROUP BY c.customer_key
)

SELECT
		customer_seg,
		COUNT(Customer_key) TotalCustomers
FROM	(
SELECT
	Customer_Key,
	Total_Spending,
	lifespan,
		CASE WHEN lifespan >= 12 AND Total_Spending > 5000 THEN 'VIP'
		WHEN lifespan >= 12 AND Total_Spending <= 5000 THEN 'Regular'
		ELSE 'New'
	END AS Customer_Seg
FROM Customers_Lifespan
)t
GROUP BY customer_seg


