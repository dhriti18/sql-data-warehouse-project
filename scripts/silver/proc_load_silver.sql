/* Stored Procedure : Load Silver Layer
Script Purpose : This stored procedure performs the ETL (Extract Transform Load ) process to populate the silver schema tables from the bronze schema.
Actions performed : 1) Truncates silver table 2) Inserts Transformed and clean data from Bronze into silver.
*/


CREATE OR ALTER PROCEDURE silver.load_silver AS
  BEGIN 
  DECLARE @start_time DATETIME , @end_time DATETIME , @batch_start_time DATETIME , @batch_end_time DATETIME ;
  SET @batch_start_time = GETDATE()
    set @start_time = GETDATE()
     Print('Truncating silver_crm_cust_info to remove duplicates and keep only latest record for each customer')
    -- remove redundant records from bronze layer and keep only the latest record for each customer based on cst_create_date
    TRUNCATE TABLE DataWarehouse.silver.crm_cust_info; 
    Print'Inserting cleaned data from bronze layer into silver layer'
    INSERT INTO DataWarehouse.silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date)
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname) AS cst_lastname,
        --Data Normalization
      CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'N/A' -- Handle missing values
      END cst_marital_status,
      CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'N/A' --Handle missing values
      END cst_gndr,
      cst_create_date
    FROM(
      SELECT 
      *,
      ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
      FROM DataWarehouse.bronze.crm_cust_info
      WHERE cst_id IS NOT NULL -- Filter out records with null cst_id to avoid issues with partitioning
      ) t
    WHERE rn = 1; -- Keep only the latest record for each customer  
    set @end_time = GETDATE()
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(10)) + ' seconds'

    --prd_info
    set @start_time = GETDATE()
    Print('Truncating silver_crm_prd_info to remove duplicates and handle inconsistencies in date records')
    TRUNCATE TABLE DataWarehouse.silver.crm_prd_info; -- clear existing data to avoid duplicates before inserting cleaned data
    Print('Inserting cleaned data from bronze layer into silver layer with derived columns, data cleaning, normalization and enrichment')
    INSERT INTO DataWarehouse.silver.crm_prd_info (
        prd_id,
        cat_id ,
        prd_key ,
        prd_nm  ,
        prd_cost ,
        prd_line ,
        prd_start_dt ,
        prd_end_dt)
    SELECT 
      prd_id,
      REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,--Derived columns
      SUBSTRING(PRD_KEY , 7,len(PRD_KEY)) AS prd_key ,
      prd_nm,
      ISNULL(prd_cost, 0) AS prd_cost,--Data Cleaning
      CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'--Data Normalization
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'N/A' -- Handle missing or unknown values
      END AS prd_line,  
      CAST (prd_start_dt AS DATE) AS prd_start_dt,--removes redundant time information as it is not relevant for analysis and can lead to inconsistencies
      CAST(LEAD(prd_start_dt)  OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt -- end date inconsisyency is ammended by redefining end date as 1 day before next start date ( Data Enrichment)
    FROM DataWarehouse.bronze.crm_prd_info
    set @end_time = GETDATE()
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(10)) + ' seconds'


    --sales details
    set @start_time = GETDATE()
    Print('Truncating silver_crm_sales_details to remove duplicates and handle inconsistencies in date records, sales and price values')
    TRUNCATE TABLE DataWarehouse.silver.crm_sales_details; -- clear existing data to avoid duplicates before inserting cleaned data
    Print('Inserting cleaned data from bronze layer into silver layer with derived columns, data cleaning, normalization and enrichment')
    INSERT INTO DataWarehouse.silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price)
    SELECT 
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL 
        ELSE CAST(CAST(sls_order_dt AS VARCHAR) as DATE )
    END AS sls_order_dt, --invalid date orders are handled by converting them to NULL values which can be easily filtered out in analysis and prevents errors in date calculations
    CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL 
        ELSE CAST(CAST(sls_ship_dt AS VARCHAR) as DATE )-- datatype casting
    END AS sls_ship_dt,
    CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL 
        ELSE CAST(CAST(sls_due_dt AS VARCHAR) as DATE )
    END AS sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales != sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price) -- derive sales using quantity and price if sales is negative, zero or null
            ELSE sls_sales
    END AS sls_sales,

    CASE WHEN sls_price IS NULL OR sls_price <=0 
            THEN sls_sales / NULLIF(sls_quantity,0)
            ELSE sls_price
    END AS sls_price,
    sls_quantity
    FROM DataWarehouse.bronze.crm_sales_details
    set @end_time = GETDATE()
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(10)) + ' seconds'


    --erp_cust_az12
    set @start_time = GETDATE()
    Print('Truncating silver_erp_cust_az12 to remove duplicates and handle inconsistencies in date records')
    TRUNCATE TABLE DataWarehouse.silver.erp_cust_az12; -- clear existing data to avoid duplicates before inserting cleaned data
    Print('Inserting cleaned data from bronze layer into silver layer with derived columns, data cleaning and normalization')
    INSERT INTO DataWarehouse.silver.erp_cust_az12 (
        cid,
        bdate,
        gen)
    SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid,4,LEN(cid))
        ELSE cid
    END AS cid,--invalid values handled
    CASE WHEN bdate> GETDATE() THEN NULL
        ELSE bdate
    END AS bdate,
    CASE 
      WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('F', 'FEMALE') THEN 'Female'
      WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('M', 'MALE')   THEN 'Male'
      ELSE 'n/a' -- REPLACE(gen, CHAR(13), '') strips the \r carriage returns
    END AS gen
    FROM DataWarehouse.bronze.erp_cust_az12
    set @end_time = GETDATE()
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(10)) + ' seconds'

    --erp_loc_a101
    set @start_time = GETDATE()
     Print('Truncating silver_erp_loc_a101 to remove duplicates and handle inconsistencies in country codes')
    TRUNCATE TABLE DataWarehouse.silver.erp_loc_a101; -- clear existing data to avoid duplicates before inserting cleaned data
    Print('Inserting cleaned data from bronze layer into silver layer with data cleaning and normalization')
    INSERT INTO DataWarehouse.silver.erp_loc_a101 (
        cid,
        cntry)
    SELECT 
    REPLACE(cid,'-','') AS cid,
    CASE 
            WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = 'DE'            THEN 'Germany'
            WHEN TRIM(REPLACE(cntry, CHAR(13), '')) IN ('US', 'USA')  THEN 'United States'
            WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = '' 
                OR cntry IS NULL                                      THEN 'n/a'
            ELSE TRIM(REPLACE(cntry, CHAR(13), ''))
    END AS cntry

    FROM DataWarehouse.bronze.erp_loc_a101
    set @end_time = GETDATE()
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(10)) + ' seconds'

    --erp_px_cat_g1v2
    set @start_time = GETDATE()
    Print('Truncating silver_erp_px_cat_g1v2 to remove duplicates and handle inconsistencies in category data')
    TRUNCATE TABLE DataWarehouse.silver.erp_px_cat_g1v2; -- clear existing data to avoid duplicates before inserting cleaned data
    Print('Inserting cleaned data from bronze layer into silver layer with data cleaning and normalization')
    INSERT INTO DataWarehouse.silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance)
    SELECT 
    id,-- matched category id from crm_cust info 
    cat,
    subcat,
    TRIM(REPLACE(maintenance, CHAR(13), '')) AS maintenance --handling formatting issue due to window to mac data ingestion
    FROM DataWarehouse.bronze.erp_px_cat_g1v2
    set @end_time = GETDATE()
    PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR(10)) + ' seconds'
  SET @batch_end_time = GETDATE()
  PRINT('Data loading into silver layer completed successfully')
  PRINT 'Total Load Duration TO LOAD IN SILVER LAYER: ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR(10)) + ' seconds'
END

