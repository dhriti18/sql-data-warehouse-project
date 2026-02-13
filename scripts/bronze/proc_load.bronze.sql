/* Script Purpose :
This stored procedure loads data into the bronze schema from external csv files.
It performs the following actions :
-Truncates the bronze table before loading data 
-Uses the 'Bulk Insert' command to load data from csv files to bronze table.
*/
REATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN 

DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME ;
BEGIN TRY
SET @batch_start_time = GETDATE()
PRINT 'Loading data into bronze layer'
PRINT 'Loading CRM Tables'
set @start_time = GETDATE()
PRINT 'TRUNCATING TABLE : bronze.crm_cust_info'
TRUNCATE TABLE bronze.crm_cust_info;
PRINT 'INSERTING DATA INTO TABLE : bronze.crm_cust_info'
BULK INSERT bronze.crm_cust_info
FROM '/var/opt/mssql/datasets/source_crm/cust_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',' ,
    TABLOCK
);
set @end_time = GETDATE()
PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(10)) + ' seconds'

PRINT'TRUNCATING TABLE : bronze.crm_prd_info'
set @start_time = GETDATE()
TRUNCATE TABLE bronze.crm_prd_info;
PRINT'INSERTING DATA INTO TABLE : bronze.crm_prd_info'
BULK INSERT bronze.crm_prd_info
FROM '/var/opt/mssql/datasets/source_crm/prd_info.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',' ,
    TABLOCK
);
set @end_time = GETDATE()
PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(10)) + ' seconds'

PRINT 'TRUNCATING TABLE : bronze.crm_sales_details'
set @start_time = GETDATE()
TRUNCATE TABLE bronze.crm_sales_details;
PRINT 'INSERTING DATA INTO TABLE : bronze.crm_sales_details'
BULK INSERT bronze.crm_sales_details
FROM '/var/opt/mssql/datasets/source_crm/sales_details.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR =',',
    TABLOCK
);
set @end_time = GETDATE()
PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(10)) + ' seconds'


PRINT('Loading ERP Tables')
PRINT('TRUNCATING TABLE : bronze.erp_loc_a101')
set @start_time = GETDATE()
TRUNCATE TABLE bronze.erp_loc_a101;
PRINT('INSERTING DATA INTO TABLE : bronze.erp_loc_a101')
BULK INSERT bronze.erp_loc_a101
FROM '/var/opt/mssql/datasets/source_erp/loc_a101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
set @end_time = GETDATE()
PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(10)) + ' seconds'

PRINT('TRUNCATING TABLE : bronze.erp_cust_az12')
set @start_time = GETDATE()
TRUNCATE TABLE bronze.erp_cust_az12;
PRINT('INSERTING DATA INTO TABLE : bronze.erp_cust_az12')
BULK INSERT bronze.erp_cust_az12
FROM '/var/opt/mssql/datasets/source_erp/cust_az12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
set @end_time = GETDATE()
PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(10)) + ' seconds'

PRINT('TRUNCATING TABLE : bronze.erp_px_cat_g1v2')
set @start_time = GETDATE()
TRUNCATE TABLE bronze.erp_px_cat_g1v2;
PRINT('INSERTING DATA INTO TABLE : bronze.erp_px_cat_g1v2')
BULK INSERT bronze.erp_px_cat_g1v2
FROM '/var/opt/mssql/datasets/source_erp/px_cat_g1v2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
set @end_time = GETDATE()
PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(10)) + ' seconds'
SET @batch_end_time = GETDATE()
PRINT('Data loading into bronze layer completed successfully')
PRINT 'Total Load Duration TO LOAD IN BRONZE LAYER: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR(10)) + ' seconds'
END TRY
BEGIN CATCH
  PRINT 'ERROR OCCURED WHILE LOADING DATA INTO BRONZE LAYER'
END CATCH
END

