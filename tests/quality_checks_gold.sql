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
SELECT * FROM gold.dim_products -- quality checks
SELECT DISTINCT gender FROM gold.dim_customer -- quality checks

SELECT * FROM gold.fact_sales  -- quality check
-- foreign key integrity check, checking whether fact can be connected with product
SELECT * 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customer c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL
