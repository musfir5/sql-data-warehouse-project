/*
===============================================================================
DDL Script: Create Bronze Tables
===============================================================================
Script Purpose:
    This script creates tables in the 'bronze' schema/user, dropping existing tables
    if they already exist.
    Run this script to re-define the DDL structure of 'bronze' Tables
===============================================================================
*/

-- Connect as the 'bronze' user
-- CRM Tables
CREATE TABLE bronze.crm_cust_info (
    cst_id             NUMBER,
    cst_key            VARCHAR2(50),
    cst_firstname      VARCHAR2(50),
    cst_lastname       VARCHAR2(50),
    cst_marital_status VARCHAR2(50),
    cst_gndr           VARCHAR2(50),
    cst_create_date    DATE
);

SELECT * FROM bronze.crm_cust_info

CREATE TABLE bronze.crm_prd_info (
    prd_id       NUMBER,
    prd_key      VARCHAR2(50),
    prd_nm       VARCHAR2(50),
    prd_cost     NUMBER,
    prd_line     VARCHAR2(50),
    prd_start_dt TIMESTAMP,
    prd_end_dt   TIMESTAMP
);

CREATE TABLE bronze.crm_sales_details (
    sls_ord_num  VARCHAR2(50),
    sls_prd_key  VARCHAR2(50),
    sls_cust_id  NUMBER,
    sls_order_dt NUMBER, -- Stored as INT in source
    sls_ship_dt  NUMBER,
    sls_due_dt   NUMBER,
    sls_sales    NUMBER,
    sls_quantity NUMBER,
    sls_price    NUMBER
);

-- ERP Tables
CREATE TABLE bronze.erp_loc_a101 (
    cid   VARCHAR2(50),
    cntry VARCHAR2(50)
);

CREATE TABLE bronze.erp_cust_az12 (
    cid   VARCHAR2(50),
    bdate DATE,
    gen   VARCHAR2(50)
);

CREATE TABLE bronze.erp_px_cat_g1v2 (
    id          VARCHAR2(50),
    cat         VARCHAR2(50),
    subcat      VARCHAR2(50),
    maintenance VARCHAR2(50)
);
