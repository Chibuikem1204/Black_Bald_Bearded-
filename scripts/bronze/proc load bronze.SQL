/* 
==============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
==============================================================================
Script Purpose:
  This stored procedure loads data into the 'Bronze' schema from an external csv files.
  It performs the following functions:
  -Truncates existing 'bronze' tables before loading data
  -Uses the 'BULK INSERT' command to load data from csv files into the bronze tables.

Parameters: None.
This stored procedure accepts no parameters and returns no values

Usage Example: EXEC bronze.load_bronze
================================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		SET @batch_start_time = GETDATE();
	BEGIN TRY
		PRINT '===================================================================';
		PRINT 'Loading the Bronze layer';
		PRINT '===================================================================';

		PRINT '-------------------------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '-------------------------------------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>>Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\chibu\OneDrive\Documents\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------'


		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_prod_info';
		TRUNCATE TABLE bronze.crm_prod_info;
			   		
		PRINT '>>Inserting Data Into: bronze.crm_prod_info';
		BULK INSERT bronze.crm_prod_info
		FROM 'C:\Users\chibu\OneDrive\Documents\datasets\source_crm\prd_info.csv'
		WITH(
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------'


		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\chibu\OneDrive\Documents\datasets\source_crm\sales_details.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);

		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------'


		PRINT '-------------------------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '-------------------------------------------------------------------';
 
		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_cust_AZ12';
		TRUNCATE TABLE bronze.erp_cust_AZ12

		PRINT '>>Inserting Data Into: bronze.erp_cust_AZ12';
		BULK INSERT bronze.erp_cust_AZ12
		FROM 'C:\Users\chibu\OneDrive\Documents\datasets\source_erp\cust_AZ12.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------'


		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_loc_A101';
		TRUNCATE TABLE bronze.erp_loc_A101

		PRINT '>>Inserting Data Into: bronze.erp_loc_A101';
		BULK INSERT bronze.erp_loc_A101
		FROM 'C:\Users\chibu\OneDrive\Documents\datasets\source_erp\loc_A101.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------'


		SET @start_time = GETDATE();
		PRINT '>>Truncating Table: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

		PRINT '>>Inserting Data Into: bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\chibu\OneDrive\Documents\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
				FIRSTROW = 2,
				FIELDTERMINATOR = ',',
				TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>> Load duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + ' seconds';
		PRINT '>> -----------------------'

		SET @batch_end_time = GETDATE();
		PRINT '============================================================='
		PRINT 'Loading Bronze Layer is Completed';
		PRINT ' - Total Load Duration: ' + CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=============================================================' 
	END TRY
	BEGIN CATCH
	PRINT '================================================================='
	PRINT 'Error Message' + ERROR_MESSAGE();
	PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR Message' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '================================================================='
	END CATCH
END
