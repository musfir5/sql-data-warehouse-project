/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `External Table Load Method` and Directory to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
BEGIN
  bronze.load_bronze;
END;
===============================================================================
*/

--Create Directory of the files

CREATE OR REPLACE DIRECTORY dwh_src_crm AS 'F:\bronze\datasets\source_crm';--Folder Path Of The Dataset
CREATE OR REPLACE DIRECTORY dwh_src_erp AS 'F:\bronze\datasets\source_erp';

GRANT READ, WRITE ON DIRECTORY dwh_src_crm TO bronze;
GRANT READ, WRITE ON DIRECTORY dwh_src_erp TO bronze;

--Insert Data - External Table Load Method

-- Create the bridge to the CSV
--for cust_info
CREATE TABLE bronze.ext_crm_cust_info (
    cst_id             VARCHAR2(50),
    cst_key            VARCHAR2(50),
    cst_firstname      VARCHAR2(50),
    cst_lastname       VARCHAR2(50),
    cst_marital_status VARCHAR2(50),
    cst_gndr           VARCHAR2(50),
    cst_create_date    VARCHAR2(50)
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY dwh_src_crm
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        LOGFILE 'cust_info_load.log'
        BADFILE 'cust_info_load.bad'
        FIELDS TERMINATED BY ','
        MISSING FIELD VALUES ARE NULL
    )
    LOCATION ('cust_info.csv')
);

-- Product Info Bridge
CREATE TABLE bronze.ext_crm_prd_info (
    prd_id       VARCHAR2(50),
    prd_key      VARCHAR2(50),
    prd_nm       VARCHAR2(50),
    prd_cost     VARCHAR2(50),
    prd_line     VARCHAR2(50),
    prd_start_dt VARCHAR2(50),
    prd_end_dt   VARCHAR2(50)
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY dwh_src_crm
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ','
        MISSING FIELD VALUES ARE NULL
    )
    LOCATION ('prd_info.csv')
);


-- Sales Details Bridge
CREATE TABLE bronze.ext_crm_sales_details (
    sls_ord_num  VARCHAR2(50),
    sls_prd_key  VARCHAR2(50),
    sls_cust_id  VARCHAR2(50),
    sls_order_dt VARCHAR2(50),
    sls_ship_dt  VARCHAR2(50),
    sls_due_dt   VARCHAR2(50),
    sls_sales    VARCHAR2(50),
    sls_quantity VARCHAR2(50),
    sls_price    VARCHAR2(50)
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY dwh_src_crm
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ','
        MISSING FIELD VALUES ARE NULL
    )
    LOCATION ('sales_details.csv')
);


-- ERP Location Bridge
CREATE TABLE bronze.ext_erp_loc_a101 (
    cid   VARCHAR2(50),
    cntry VARCHAR2(50)
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY dwh_src_erp
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ','
        MISSING FIELD VALUES ARE NULL
    )
    LOCATION ('LOC_A101.csv')
);


-- ERP Customer Bridge
CREATE TABLE bronze.ext_erp_cust_az12 (
    cid   VARCHAR2(50),
    bdate VARCHAR2(50),
    gen   VARCHAR2(50)
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY dwh_src_erp
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ','
        MISSING FIELD VALUES ARE NULL
    )
    LOCATION ('cust_az12.csv')
);


-- ERP Category Bridge
CREATE TABLE bronze.ext_erp_px_cat_g1v2 (
    id          VARCHAR2(50),
    cat         VARCHAR2(50),
    subcat      VARCHAR2(50),
    maintenance VARCHAR2(50)
)
ORGANIZATION EXTERNAL (
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY dwh_src_erp
    ACCESS PARAMETERS (
        RECORDS DELIMITED BY NEWLINE
        SKIP 1
        FIELDS TERMINATED BY ','
        MISSING FIELD VALUES ARE NULL
    )
    LOCATION ('PX_CAT_G1V2.csv')
);

--PROCEDURE
BEGIN
bronze.load_bronze;
END;





----------------------


CREATE OR REPLACE PROCEDURE bronze.load_bronze AS
/*
===============================================================================
Procedure: load_bronze
Purpose:   This procedure implements the "Bronze Layer" ETL process.
           It performs a Truncate-and-Insert from External Tables (CSV bridge)
           to permanent Bronze tables.
           
Author:    Mohammed (DWH Project)
Date:      2026
===============================================================================
*/
    -- Variables to track execution time
    v_start_time TIMESTAMP;
    v_end_time   TIMESTAMP;
BEGIN
    v_start_time := SYSTIMESTAMP;
    
    DBMS_OUTPUT.PUT_LINE('================================================');
    DBMS_OUTPUT.PUT_LINE('Starting Bronze Layer Load Process...');
    DBMS_OUTPUT.PUT_LINE('================================================');

    ---------------------------------------------------------------------------
    -- 1. LOAD CRM DATA
    ---------------------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('>> Loading CRM Tables...');
    
    -- Customer Info
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.crm_cust_info';
    INSERT INTO bronze.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
    SELECT TO_NUMBER(cst_id), cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, TO_DATE(cst_create_date, 'YYYY-MM-DD')
    FROM bronze.ext_crm_cust_info;
    DBMS_OUTPUT.PUT_LINE(' [SUCCESS] crm_cust_info loaded: ' || SQL%ROWCOUNT || ' rows.');

    -- Product Info
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.crm_prd_info';
    INSERT INTO bronze.crm_prd_info (prd_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    SELECT TO_NUMBER(prd_id), prd_key, prd_nm, TO_NUMBER(prd_cost), prd_line, 
           TO_TIMESTAMP(prd_start_dt, 'YYYY-MM-DD HH24:MI:SS'), TO_TIMESTAMP(prd_end_dt, 'YYYY-MM-DD HH24:MI:SS')
    FROM bronze.ext_crm_prd_info;
    DBMS_OUTPUT.PUT_LINE(' [SUCCESS] crm_prd_info loaded: ' || SQL%ROWCOUNT || ' rows.');

    -- Sales Details
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.crm_sales_details';
    INSERT INTO bronze.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
    SELECT sls_ord_num, sls_prd_key, TO_NUMBER(sls_cust_id), TO_NUMBER(sls_order_dt), TO_NUMBER(sls_ship_dt), TO_NUMBER(sls_due_dt), TO_NUMBER(sls_sales), TO_NUMBER(sls_quantity), TO_NUMBER(sls_price)
    FROM bronze.ext_crm_sales_details;
    DBMS_OUTPUT.PUT_LINE(' [SUCCESS] crm_sales_details loaded: ' || SQL%ROWCOUNT || ' rows.');

    ---------------------------------------------------------------------------
    -- 2. LOAD ERP DATA
    ---------------------------------------------------------------------------
    DBMS_OUTPUT.PUT_LINE('>> Loading ERP Tables...');

    -- Location Data
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.erp_loc_a101';
    INSERT INTO bronze.erp_loc_a101 (cid, cntry)
    SELECT cid, cntry FROM bronze.ext_erp_loc_a101;
    DBMS_OUTPUT.PUT_LINE(' [SUCCESS] erp_loc_a101 loaded: ' || SQL%ROWCOUNT || ' rows.');

    -- Customer Data
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.erp_cust_az12';
    INSERT INTO bronze.erp_cust_az12 (cid, bdate, gen)
    SELECT cid, TO_DATE(bdate, 'YYYY-MM-DD'), gen FROM bronze.ext_erp_cust_az12;
    DBMS_OUTPUT.PUT_LINE(' [SUCCESS] erp_cust_az12 loaded: ' || SQL%ROWCOUNT || ' rows.');

    -- Category Data
    EXECUTE IMMEDIATE 'TRUNCATE TABLE bronze.erp_px_cat_g1v2';
    INSERT INTO bronze.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT id, cat, subcat, maintenance FROM bronze.ext_erp_px_cat_g1v2;
    DBMS_OUTPUT.PUT_LINE(' [SUCCESS] erp_px_cat_g1v2 loaded: ' || SQL%ROWCOUNT || ' rows.');

    -- Final Commit
    COMMIT;
    
    v_end_time := SYSTIMESTAMP;
    DBMS_OUTPUT.PUT_LINE('------------------------------------------------');
    DBMS_OUTPUT.PUT_LINE('Load Completed Successfully in ' || TO_CHAR(v_end_time - v_start_time));
    DBMS_OUTPUT.PUT_LINE('================================================');

EXCEPTION
    WHEN OTHERS THEN
        -- Rollback any pending inserts if an error occurs
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('================================================');
        DBMS_OUTPUT.PUT_LINE('ERROR DETECTED DURING LOAD!');
        DBMS_OUTPUT.PUT_LINE('Error Code: ' || SQLCODE);
        DBMS_OUTPUT.PUT_LINE('Error Message: ' || SQLERRM);
        DBMS_OUTPUT.PUT_LINE('================================================');
        -- Re-raise the error so the calling tool knows it failed
        RAISE;
END;
/
