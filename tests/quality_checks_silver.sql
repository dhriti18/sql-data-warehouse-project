/* Quality Checks 
This script performs various quality checks for data consistency, accuracy and standardization across the silver schema. It includes checks for 
-Null or Duplicate Primary keys,
-Unwanted spaces or string fiels
-Data standardization and consistency
-Invalid date ranges and orders
-Data consistency between related fields

Note :
- These scripts were ammended during silver loading stages , before importing modified output into proc
- Use these scripts to investigate any discrepancies in silver layer data tables
*/

SELECT 
prd_id,
COUNT(*)
FROM DataWarehouse.silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 

-- check for unwanted spaces
SELECT prd_cost
FROM DataWarehouse.bronze.crm_prd_info
WHERE prd_cost <0 OR prd_cost is NULL

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM DataWarehouse.bronze.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM DataWarehouse.bronze.crm_cust_info

--inspecting silvr accuracy
-- check for unwanted spaces
SELECT 
cst_id,
COUNT(*)
FROM DataWarehouse.silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 
SELECT cst_firstname, cst_lastname
FROM DataWarehouse.silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname) OR cst_lastname != TRIM(cst_lastname)

-- Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM DataWarehouse.silver.crm_cust_info

SELECT DISTINCT cst_marital_status
FROM DataWarehouse.silver.crm_cust_info

---empty records indicating all discrepancies ave been rectified
--prd_info
SELECT 
prd_id,
COUNT(*)
FROM DataWarehouse.silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 

-- check for invalid date orders
SELECT *
FROM DataWarehouse.bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt
-- data shows start date always ahead of end date which is logically inaccurate
-- redefine end date as one day before start date of next record for the same product 

SELECT 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity
FROM DataWarehouse.bronze.crm_sales_details


-- check for invalid dates
SELECT
NULLIF(sls_order_dt, 0) sls_order_dt -- making lL 0 VALUES NULL
FROM DataWarehouse.bronze.crm_sales_details
WHERE sls_order_dt <=0 OR LEN(sls_order_dt)!=8 

SELECT 
*
FROM DataWarehouse.bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt>sls_due_dt

--sum of sales = sum of quantity * cost price of product and only positive values
SELECT DISTINCT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price as old_sls_price,

CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price) -- derive sales using quantity and price if sales is negative, zero or null
        ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL OR sls_price <=0 
        THEN sls_sales / NULLIF(sls_quantity,NULL)
        ELSE sls_price
END AS sls_price
FROM DataWarehouse.bronze.crm_sales_details
WHERE sls_sales != sls_quantity *sls_price
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
or sls_sales is NULL OR sls_quantity is NULL OR sls_price is NULL

-- Business Rules :
-- if sales is negative, zero or null derive it using quantity and price
--if price is zero or null calculate using sales and quantity 
--  if price is negative convert it into a positive value

--erp_cust_az12
SELECT
cid,
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING (cid,4,LEN(cid))
    ELSE cid
END AS cid,
CASE WHEN bdate> GETDATE() THEN NULL
    ELSE bdate
END AS bdate,
CASE 
  WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('F', 'FEMALE') THEN 'Female'
  WHEN UPPER(TRIM(REPLACE(gen, CHAR(13), ''))) IN ('M', 'MALE')   THEN 'Male'
  ELSE 'n/a' -- REPLACE(gen, CHAR(13), '') strips the \r carriage returns
END AS gen
FROM DataWarehouse.bronze.erp_cust_az12

SELECT DISTINCT 
bdate
FROM DataWarehouse.bronze.erp_cust_az12
WHERE bdate < '1924-01-01' or bdate > GETDATE() -- checking for really old dates ot future dates
SELECT DISTINCT 
gen 
FROM DataWarehouse.bronze.erp_cust_az12
-- checking for inconsistent data 
SELECT * FROM DataWarehouse.silver.crm_cust_info -- matching the primary key of erp_cust_az12 with crm_cust_info to understand what derived columns to create 

--erp_loc_a101
SELECT 
REPLACE(cid,'-','') AS cid,-- handling invalied values
 CASE 
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = 'DE'            THEN 'Germany'
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) IN ('US', 'USA')  THEN 'United States'
        WHEN TRIM(REPLACE(cntry, CHAR(13), '')) = '' 
             OR cntry IS NULL                                      THEN 'n/a'
        ELSE TRIM(REPLACE(cntry, CHAR(13), ''))
END AS cntry

FROM DataWarehouse.bronze.erp_loc_a101
-- CHAR(13) is a carriage return character (\r), a hidden line-ending character
-- that comes from Windows-style line endings (Windows uses \r\n to end a line,
-- while Linux/Mac only use \n). When this data was exported or loaded, the
-- Windows line endings got embedded into the column values themselves.
--
-- Regular TRIM() only removes spaces (CHAR(32)) from the start and end of a string.
-- It does NOT remove \r or any other control characters, so even after TRIM()
-- the value still contains the hidden \r, meaning 'DE\r' != 'DE' and '' != '\r',
-- causing all comparisons to silently fail and fall through to ELSE.
--
-- REPLACE(cntry, CHAR(13), '') explicitly strips out the \r before TRIM() cleans
-- up any remaining whitespace, leaving a plain string that comparisons work on.

SELECT 
id,-- matched category id from crm_cust info 
cat,
subcat,
maintenance 
FROM DataWarehouse.bronze.erp_px_cat_g1v2
-- all data values are suitable for processing
