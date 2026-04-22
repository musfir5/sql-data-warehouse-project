/*
============================================================
DDL Script: Create Silver Tables
============================================================
Script Purpose:
    This script creates tables in the 'silver' schema    
============================================================
*/
-- Connect as the 'silver' user or run as ADMIN
-- CRM Tables
CREATE TABLE silver.crm_cust_info (
    cst_id             NUMBER,
    cst_key            VARCHAR2(50),
    cst_firstname      VARCHAR2(50),
    cst_lastname       VARCHAR2(50),
    cst_marital_status VARCHAR2(50),
    cst_gndr           VARCHAR2(50),
    cst_create_date    DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SELECT * FROM silver.crm_cust_info;

--DROP TABLE silver.crm_prd_info

CREATE TABLE silver.crm_prd_info (
    prd_id       NUMBER,
    cat_id       VARCHAR2(50),
    prd_key      VARCHAR2(50),
    prd_nm       VARCHAR2(50),
    prd_cost     NUMBER,
    prd_line     VARCHAR2(50),
    prd_start_dt DATE,
    prd_end_dt   DATE,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

--drop table silver.crm_sales_details 

CREATE TABLE silver.crm_sales_details (
    sls_ord_num  VARCHAR2(50),
    sls_prd_key  VARCHAR2(50),
    sls_cust_id  NUMBER,
    sls_order_dt DATE, -- Stored as INT in source
    sls_ship_dt  DATE,
    sls_due_dt   DATE,
    sls_sales    NUMBER,
    sls_quantity NUMBER,
    sls_price    NUMBER,
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ERP Tables
CREATE TABLE silver.erp_loc_a101 (
    cid   VARCHAR2(50),
    cntry VARCHAR2(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.erp_cust_az12 (
    cid   VARCHAR2(50),
    bdate DATE,
    gen   VARCHAR2(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE silver.erp_px_cat_g1v2 (
    id          VARCHAR2(50),
    cat         VARCHAR2(50),
    subcat      VARCHAR2(50),
    maintenance VARCHAR2(50),
    dwh_create_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
