/*
=================================================================
DDL Script: Create Bronze Tables
=================================================================
Script Purpose:
  This script creates tables in the 'Bronze' schema,
  dropping existing tables if they exist.
Run this script to redefine the DDL structure of the bronze tables
==================================================================
*/


IF OBJECT_ID ('bronze.crm_cust_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm_cust_info;
GO

  
CREATE TABLE bronze.crm_cust_info (
	cst_id INT,
	cst_key NVARCHAR (50),
	cst_firstname NVARCHAR (50),
	cst_lastname NVARCHAR (50),
	cst_marital_status NVARCHAR (50),
	cst_gndr NVARCHAR (50),
	cst_create_date DATE
);
GO

IF OBJECT_ID ('bronze.crm_prod_info', 'U') IS NOT NULL
	DROP TABLE bronze.crm__prod_info;

CREATE TABLE bronze.crm_prod_info (
	prd_id INT,
	prd_key NVARCHAR (50),
	prd_nm NVARCHAR (50),
	prd_cost INT,
	prd_line NVARCHAR (50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);
GO

  
IF OBJECT_ID ('bronze.crm_sales_details', 'U') IS NOT NULL
	DROP TABLE bronze.crm_sales_details;

CREATE TABLE bronze.crm_sales_details (
	sls_ord_num NVARCHAR (50),
	sls_prd_key NVARCHAR (50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
GO

  
--CREATE TABLES FOR ERP SOURCE SYSTEM 

IF OBJECT_ID ('bronze.erp_cust_AZ12', 'U') IS NOT NULL
	DROP TABLE bronze.erp_cust_AZ12;
GO

  
CREATE TABLE bronze.erp_cust_AZ12 (
	CID NVARCHAR (50),
	BDATE DATE,
	GEN NVARCHAR(50)
);
GO

  
IF OBJECT_ID ('bronze.erp_loc_A101', 'U') IS NOT NULL
	DROP TABLE bronze.erp_loc_A101;
GO

  
CREATE TABLE bronze.erp_loc_A101 (
	CID NVARCHAR (50),
	CNTRY NVARCHAR (50)
);
GO

IF OBJECT_ID ('bronze.erp_PX_CAT_G1V2', 'U') IS NOT NULL
	DROP TABLE bronze.erpPX_CAT_G1V2;
GO

  
CREATE TABLE bronze.erp_PX_CAT_G1V2 (
	ID NVARCHAR(50),
	CAT NVARCHAR (50),
	SUBCAT NVARCHAR (50),
	MAINTENANCE NVARCHAR (50)
);
GO
