/*
===============================================================================
Create Database and Schemas
===============================================================================
Script Purpose:
    This script creates a new database named 'Datawarehouse' after checking if it already exists.
    If the database exists, it will be dropped and created. Also, the script sets up three schemas within the database
    ('bronze', 'silver' and 'gold').

WARNING:
    Running this script will drop the whole 'Datawarehouse' database if it exists.
    All data in the database will be permenently deleted. So, proceed with caution
    and ensure you have proper backups before running this script.
*/

USE MASTER;
GO

--Drop and recreate the 'Datawarehouse' database
IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'Datawarehouse')
BEGIN
  ALTER DATABASE Datawarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

--Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE 'DataWarehouse'
GO

--Create Schema
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
