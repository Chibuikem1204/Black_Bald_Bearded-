/*
========================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver) 
========================================================
Script Purpose:
  This stored procedure performs the ETL (Extract, Transform and Load)
  process to populate the 'silver' schema tables from the 'bronze' schema.

Actions Performed
  -  Truncate Silver tables.
  -  Inserts transformed and cleansed data from the bronze into the silver tables.


Parameters:
  None.
  This Stored Procedure accepts no parameters and returns no values.

Usage Example:
EXEC silver.load_silver
=========================================================
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN

	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();
	BEGIN TRY
		PRINT '===========================================';
		PRINT 'Loading the Silver Layer';
		PRINT '===========================================';

		PRINT '-------------------------------------------';
		PRINT 'Loading Silver CRM Tables';
		PRINT '-------------------------------------------';

		--Loading silver.crm_cust_info
			SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';   --1
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Table: silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info (
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_marital_status,
			cst_gndr,
			cst_create_date
		)

		SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) cst_firstname,
			TRIM(cst_lastname) cst_lastname,
			CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
				WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
				ELSE 'N/A'
			END AS cst_marital_status,		
			CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
				WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
				ELSE 'N/A'
			END AS cst_gndr,		
			cst_create_date
		FROM (
			SELECT 
				*,
				ROW_NUMBER () OVER (PARTITION BY cst_id ORDER BY cst_create_date desc) flag
			FROM bronze.crm_cust_info
			WHERE cst_id IS NOT NULL
			)t
			WHERE flag = 1;
			SET @end_time = GETDATE()
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second,  @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> ---------------------------------------';

		--Loading silver.crm_prod_info
			SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prod_info';					--2
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Table: silver.crm_prod_info';
		INSERT INTO silver.crm_prod_info (
			prod_id,
			category_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_date
		)

		SELECT 
				prd_id,
				REPLACE(SUBSTRING (TRIM (prd_key), 1, 5), '-', '_') AS category_id, --Extract category ID
				SUBSTRING (TRIM (prd_key), 7, LEN (prd_key)) AS prd_key,		 --Extract product key
				prd_nm,
				COALESCE (prd_cost, 0) AS prd_cost,
				CASE UPPER( TRIM (prd_line))
					WHEN 'M' THEN 'Mountain'   
					WHEN 'R' THEN 'Road'
					WHEN 'S' THEN 'Other Sales'
					WHEN 'T' THEN 'Touring'
					ELSE 'n/a'
				END AS prd_line,		
				CAST(prd_start_dt AS date) AS prd_start_dt,
				CAST (
					LEAD (prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS date) 
					AS prd_end_date		
		FROM bronze.crm_prod_info;
			SET @end_time = GETDATE();
			PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------------------------------'

			--loading silver.crm_sales_details
			SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';					--3
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Table: silver.crm_sales_details';

		INSERT INTO silver.crm_sales_details (
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			sls_order_dt,
			sls_ship_dt,
			sls_due_dt,
			sls_sales,
			sls_quantity,
			sls_price
		)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
				ELSE CAST (CAST (sls_order_dt AS VARCHAR) AS DATE)
				END AS sls_order_dt,
			CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
				ELSE CAST (CAST (sls_ship_dt AS VARCHAR) AS DATE)
				END AS sls_ship_dt,
			CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
				ELSE CAST (CAST (sls_due_dt AS VARCHAR) AS DATE)
				END AS sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
				THEN sls_quantity * ABS(sls_price) 
				ELSE sls_sales
			END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price IS NULL OR sls_price <= 0
				THEN sls_sales / NULLIF(sls_quantity, 0)
				ELSE sls_price
			END AS sls_price
		FROM bronze.crm_sales_details;
			SET @end_time = GETDATE();
			PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -----------------------';

		
		PRINT '-------------------------------------------';
		PRINT 'Loading Silver ERP Tables';
		PRINT '-------------------------------------------';

		--loading silver.erp_cust_AZ12
		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_cust_AZ12';				--4
		TRUNCATE TABLE silver.erp_cust_AZ12;
		PRINT '>> Inserting Table: silver.erp_cust_AZ12';
		INSERT INTO silver.erp_cust_AZ12 (CID, BDATE, GEN)
		SELECT
			CASE WHEN CID LIKE 'NAS%' THEN SUBSTRING(CID, 4, LEN(CID))
				ELSE CID
			END AS CID,		
			CASE WHEN BDATE > GETDATE() THEN NULL
				ELSE BDATE
			END AS BDATE,		
			CASE WHEN UPPER(TRIM(GEN)) IN ('F', 'Female') THEN 'Female'
				WHEN UPPER(TRIM(GEN)) IN ('m', 'Male') THEN 'Male'
				ELSE 'n/a'
			END AS GEN		
		FROM bronze.erp_cust_AZ12;
			SET @end_time = GETDATE();
			PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -----------------------------------'

			--loading silver.erp_loc_A101
			SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_A101';			--5
		TRUNCATE TABLE silver.erp_loc_A101;
		PRINT '>> Inserting Table: silver.erp_loc_A101';
		INSERT INTO silver.erp_loc_A101 (CID, CNTRY)

		SELECT 
			REPLACE (CID, '-', '') CID,		--Handled invalid values. Removed the hyphen and replaced with an empty string
			CASE WHEN UPPER(TRIM (CNTRY)) IN ('USA', 'US', 'United States') THEN 'United States'
				WHEN UPPER(TRIM (CNTRY)) IN ('DE', 'Germany') THEN 'Germany'
				WHEN UPPER(TRIM (CNTRY)) = 'Australia' THEN 'Australia'
				WHEN UPPER(TRIM (CNTRY)) = 'United Kingdom' THEN 'United Kingdom'
				WHEN UPPER(TRIM (CNTRY)) = 'Canada' THEN 'Canada'
				WHEN UPPER(TRIM (CNTRY)) = 'France' THEN 'France'
				WHEN UPPER(TRIM (CNTRY)) = '' OR UPPER(TRIM (CNTRY)) IS NULL THEN 'n/a'
				ELSE UPPER(TRIM (CNTRY))
			END AS Country		
		FROM bronze.erp_loc_A101;
			SET @end_time = GETDATE();
			PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> ------------------------------------'

			--loading silver.erp_PX_CAT_G1V2
			SET @start_time = GETDATE()
		PRINT '>> Truncating Table: silver.erp_PX_CAT_G1V2';			--6
		TRUNCATE TABLE silver.erp_PX_CAT_G1V2;
		PRINT '>> Inserting Table: silver.erp_PX_CAT_G1V2';
		INSERT INTO silver.erp_PX_CAT_G1V2 

		(
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		)

		SELECT 	
			ID,
			CAT,
			SUBCAT,
			MAINTENANCE
		FROM bronze.erp_PX_CAT_G1V2;
		SET @end_time = GETDATE();
		SET @end_time = GETDATE();
			PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -----------------------'

		SET @batch_end_time = GETDATE();
		PRINT '============================================================='
			PRINT 'Loading Silver Layer is Completed';
			PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
			PRINT '=============================================================' 
		END TRY

		BEGIN CATCH
		PRINT '=============================================================' 
		PRINT 'ERROR OCCURED DURING LOADING THE BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '=============================================================' 
		END CATCH
END






