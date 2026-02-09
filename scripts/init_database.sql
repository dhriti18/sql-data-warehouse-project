/*Script Purpose : This script creates a new database 'DataWarehouse' after checking if it already exists , if it exists it is going to drop itand recreate.
  Aditionally, it sets up three schemas within the database 'bronze','silver' and 'gold'*/
  

USE master;
GO

  --Drop and recreate 'DataWarehouse' database
  IF EXISTS (SELECT 1 FROM sys.databases WHERE name = 'DataWarehouse')
  BEGIN
     DROP DATABASE DataWarehouse;
  END;
  GO
--create 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
USE DataWarehouse;

-- Create Schemas
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO
