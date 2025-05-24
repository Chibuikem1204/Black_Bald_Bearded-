/* 
========================================================
Product Report
========================================================
Purpose:
	-This report consolidates key product metrics and behaviour

Highlights:
	1. Gathers essential fields such product name, category and subcategory and cost.
	2. Segments products by revenue to identify High-Performers, Mid-Range or Low-Performers.
	3. Aggregates product-level metrics:
		-total orders
		-total sales
		-total quantity purchased
		-total products
		-lifespan (in months)
	4. Calculates valuable KPIs:
		-recency (months since last order)
		-average order value
		-average month spend
=========================================================
*/

CREATE VIEW gold.report_products AS 
WITH base_product_query AS
/*-----------------------------------------------------------------------
1) Base Query: Retrieves core columns from tables
-------------------------------------------------------------------------*/
(SELECT
	P.product_number,
	F.order_number,
	F.order_date,
	F.quantity,
	F.sales_amount,
	F.customer_key,
	P.product_name,
	P.product_key,
	P.category,
	P.subcategory,
	P.cost
FROM gold.fact_sales F
LEFT JOIN gold.dim_products P
	   ON P.product_key = F.product_key
WHERE order_date IS NOT NULL
)
, Product_aggregations AS
/*-----------------------------------------------------------------------
2) Product Aggregation: Summarises key metrics at the product level
-------------------------------------------------------------------------*/
(
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	MAX (order_date) as latest_orderdate,
	DATEDIFF (MONTH, min (order_date), max (order_date)) as lifespan,
	COUNT (DISTINCT order_number) as total_orders,
	COUNT (DISTINCT customer_key) as total_customers,
	SUM (sales_amount) as total_sales,
	SUM (quantity) as total_quantity,
	ROUND(AVG(CAST(sales_amount AS Float) / NULLIF(quantity, 0)), 1) as avg_selling_price

FROM base_product_query
GROUP BY product_key,
		product_name,
		category,
		subcategory,
		cost
)

/*-----------------------------------------------------------------------
3) Final Query: Combines all product results into one output
-------------------------------------------------------------------------*/
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	latest_orderdate,
	DATEDIFF (MONTH, latest_orderdate, GETDATE()) recency_in_months,
	lifespan,
	total_orders,
	total_customers,
	total_sales,
	total_quantity,
	avg_selling_price,
	CASE WHEN total_sales > 50000 THEN 'high performer'
			WHEN total_sales >= 10000 THEN 'mid range'
			ELSE 'low performer'
	END AS product_segment,

	--compute Average Order revenue AOR = (total_sales / total_orders)
	CASE WHEN total_sales = 0 THEN 0
			ELSE total_sales / total_orders
	END AS avg_order_revenue,

	--compute the Average Monthly revenue = total_sales / number_of_months
	CASE 
		WHEN lifespan = 0 THEN total_sales
		ELSE total_sales/lifespan
	END AS avg_monthly_revenue

FROM Product_aggregations
