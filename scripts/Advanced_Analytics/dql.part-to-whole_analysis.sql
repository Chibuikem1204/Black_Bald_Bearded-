--Which category contributes the most to the overall sales?


WITH Category_Sales AS
(
SELECT	
	P.category,
	SUM (F.sales_amount) AS Total_Sales
FROM gold.fact_sales F
LEFT JOIN gold.dim_products P
ON P.product_key = F.product_key
GROUP BY p.category
)

SELECT
	category,
	Total_Sales,
	SUM(Total_Sales) OVER () AS Overall_Sales,
	CONCAT(ROUND((CAST(Total_Sales AS FLOAT)  / SUM(Total_Sales) OVER ()) * 100, 2), '%') AS Percentage_of_total_sales
FROM Category_Sales
ORDER BY Percentage_of_total_sales desc
