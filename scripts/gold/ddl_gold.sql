/* DDL Script : Create GOLD Views
Script Purpose : This script creates views for the gold layer in the data warehouse.
The gold layer represents the final dimension and fact tables (Star Schema)
Each view performs transformations and cobines data from the silver layer to produce a clean , enriched , business ready dataset.
Usage- These views can be queried directly for analytics and reporting.
*/

--Building customer object
--ensuring accurate and consistent naming conventions
-- Dimesion table as it holds descriptive information
CREATE VIEW gold.dim_customer AS 
SELECT 
    ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- this creates a surrogate key for the customer dimension which is more efficient for joins and indexing compared to natural keys
    ci.cst_id AS customer_id,
    ci.cst_key AS customer_number,
    ci.cst_firstname AS first_name,
    ci.cst_lastname AS last_name,
    la.cntry AS country,
    ci.cst_marital_status AS marital_status,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
        ELSE COALESCE(ca.gen, 'n/a')-- if the data is gen is also null then the gender information is n/a
    END AS gender,
    ca.bdate AS birthdate,
    ci.cst_create_date AS create_date
FROM DataWarehouse.silver.crm_cust_info ci -- giving an alias
LEFT JOIN DataWarehouse.silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN DataWarehouse.silver.erp_loc_a101 la  
ON ca.cid = la.cid

-- Since we have repetitive and inconsistent data we need to decide which source system is the master
-- As per the scope of the prject the crm system is the master data hence it has more accurate consistent data

SELECT DISTINCT -- Data Integration
    ci.cst_gndr,
    ca.gen,
    CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
            ELSE COALESCE(ca.gen, 'n/a')-- if the data is gen is also null then the gender information is n/a
    END AS new_gen
FROM DataWarehouse.silver.crm_cust_info ci -- giving an alias
LEFT JOIN DataWarehouse.silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN DataWarehouse.silver.erp_loc_a101 la  
ON ca.cid = la.cid
ORDER BY 1,2
-- Since we have repetitive and inconsistent data we need to decide which source system is the master
-- As per the scope of the prject the crm system is the master data hence it has more accurate consistent data

SELECT DISTINCT gender FROM gold.dim_customer -- quality checks


-- creating product object
-- This is another dimesion table as it holds descriptive information about the products
CREATE VIEW gold.dim_products AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY pn.prd_id) AS product_key, -- this creates a surrogate key for the product dimension which is more efficient for joins and indexing compared to natural keys
    pn.prd_id AS product_id,
    pn.prd_key AS product_number,
    pn.prd_nm AS product_name,
    pn.cat_id AS category_id,
    pc.cat AS category,
    pc.subcat AS subcategory,
    pc.maintenance,
    pn.prd_cost AS cost,
    pn.prd_line AS product_line,
    pn.prd_start_dt AS start_date
FROM DataWarehouse.silver.crm_prd_info pn
LEFT JOIN DataWarehouse.silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL --FILTER OUT ALL HISTORICAL DATE

SELECT * FROM gold.dim_products -- quality checks

-- crating sales object
-- This is a fact table as it holds transactional data about sales 
-- we will use the surrogate keys from the dimension tables to link the fact table to the dimensions (Data Lookup)
CREATE VIEW gold.fact_sales AS
SELECT
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_date,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM DataWarehouse.silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customer cu
ON sd.sls_cust_id = cu.customer_id

SELECT * FROM gold.fact_sales  -- quality check
-- foreign key integrity check, checking whether fact can be connected with product
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
