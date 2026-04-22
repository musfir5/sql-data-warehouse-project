/*
========================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
========================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
    - Truncates Silver tables.
    - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
========================================================================
*/
CREATE OR REPLACE PROCEDURE SILVER.SP_LOAD_SILVER_LAYER AS
    v_start_time NUMBER;
    v_end_time   NUMBER;
    v_row_count  NUMBER;
    
    -- Helper procedure to handle logging to reduce code repetition
    PROCEDURE log_progress(p_table_name VARCHAR2, p_rows NUMBER, p_start NUMBER, p_end NUMBER) IS
    BEGIN
        DBMS_OUTPUT.PUT_LINE('Table: ' || RPAD(p_table_name, 25) || 
                             ' | Rows: ' || LPAD(p_rows, 8) || 
                             ' | Time: ' || LPAD((p_end - p_start)/100, 5) || ' sec');
    END log_progress;

BEGIN
    DBMS_OUTPUT.PUT_LINE('--- Starting Silver Layer Load: ' || TO_CHAR(SYSDATE, 'YYYY-MM-DD HH24:MI:SS') || ' ---');

    ---------------------------------------------------------------------------
    -- 1. Refresh silver.crm_cust_info
    ---------------------------------------------------------------------------
    v_start_time := DBMS_UTILITY.GET_TIME;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.crm_cust_info';
    
    INSERT INTO silver.crm_cust_info (
        CST_ID, CST_KEY, CST_FIRSTNAME, CST_LASTNAME, 
        CST_MARITAL_STATUS, CST_GNDR, CST_CREATE_DATE
    )
    SELECT CST_ID, CST_KEY, TRIM(CST_FIRSTNAME), TRIM(CST_LASTNAME),
        CASE WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'S' THEN 'Single'
             WHEN UPPER(TRIM(CST_MARITAL_STATUS)) = 'M' THEN 'Married'
             ELSE 'N/A' END,
        CASE WHEN UPPER(TRIM(CST_GNDR)) = 'F' THEN 'Female'
             WHEN UPPER(TRIM(CST_GNDR)) = 'M' THEN 'Male'
             ELSE 'N/A' END,
        CST_CREATE_DATE
    FROM (
        SELECT a.*, ROW_NUMBER() OVER(PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC) AS FLAG
        FROM bronze.crm_cust_info a WHERE cst_id IS NOT NULL
    ) WHERE FLAG = 1;
    
    v_row_count := SQL%ROWCOUNT;
    v_end_time := DBMS_UTILITY.GET_TIME;
    log_progress('crm_cust_info', v_row_count, v_start_time, v_end_time);

    ---------------------------------------------------------------------------
    -- 2. Refresh silver.crm_prd_info
    ---------------------------------------------------------------------------
    v_start_time := DBMS_UTILITY.GET_TIME;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.crm_prd_info';

    INSERT INTO silver.crm_prd_info (
        PRD_ID, CAT_ID, PRD_KEY, PRD_NM, 
        PRD_COST, PRD_LINE, PRD_START_DT, PRD_END_DT
    )
    SELECT PRD_ID, REPLACE(SUBSTR(PRD_KEY,1,5),'-','_'), SUBSTR(PRD_KEY,7,LENGTH(PRD_KEY)),
        PRD_NM, NVL(PRD_COST,0),
        CASE UPPER(TRIM(PRD_LINE))
             WHEN 'M' THEN 'Mountain' WHEN 'R' THEN 'Road'
             WHEN 'S' THEN 'Other sales' WHEN 'T' THEN 'Touring'
             ELSE 'N/A' END,
        TRUNC(PRD_START_DT),
        LEAD(PRD_START_DT) OVER(PARTITION BY PRD_KEY ORDER BY PRD_START_DT) - 1
    FROM bronze.crm_prd_info;

    v_row_count := SQL%ROWCOUNT;
    v_end_time := DBMS_UTILITY.GET_TIME;
    log_progress('crm_prd_info', v_row_count, v_start_time, v_end_time);

    ---------------------------------------------------------------------------
    -- 3. Refresh silver.crm_sales_details
    ---------------------------------------------------------------------------
    v_start_time := DBMS_UTILITY.GET_TIME;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details (
        SLS_ORD_NUM, SLS_PRD_KEY, SLS_CUST_ID, SLS_ORDER_DT, 
        SLS_SHIP_DT, SLS_DUE_DT, SLS_SALES, SLS_QUANTITY, SLS_PRICE
    )
    SELECT SLS_ORD_NUM, SLS_PRD_KEY, SLS_CUST_ID,
        CASE WHEN SLS_ORDER_DT = 0 OR LENGTH(SLS_ORDER_DT) <> 8 THEN NULL
             ELSE TO_DATE(TO_CHAR(SLS_ORDER_DT), 'YYYYMMDD') END,
        CASE WHEN SLS_SHIP_DT = 0 OR LENGTH(SLS_SHIP_DT) <> 8 THEN NULL
             ELSE TO_DATE(TO_CHAR(SLS_SHIP_DT), 'YYYYMMDD') END,
        CASE WHEN SLS_DUE_DT = 0 OR LENGTH(SLS_DUE_DT) <> 8 THEN NULL
             ELSE TO_DATE(TO_CHAR(SLS_DUE_DT), 'YYYYMMDD') END,
        CASE WHEN SLS_SALES IS NULL OR SLS_SALES <= 0 OR SLS_SALES <> SLS_QUANTITY * ABS(SLS_PRICE)
             THEN SLS_QUANTITY * ABS(SLS_PRICE) ELSE SLS_SALES END,
        SLS_QUANTITY,
        CASE WHEN SLS_PRICE IS NULL OR SLS_PRICE <= 0
             THEN SLS_SALES / NULLIF(SLS_QUANTITY, 0) ELSE SLS_PRICE END
    FROM bronze.crm_sales_details;

    v_row_count := SQL%ROWCOUNT;
    v_end_time := DBMS_UTILITY.GET_TIME;
    log_progress('crm_sales_details', v_row_count, v_start_time, v_end_time);

    ---------------------------------------------------------------------------
    -- 4. Refresh silver.erp_cust_az12
    ---------------------------------------------------------------------------
    v_start_time := DBMS_UTILITY.GET_TIME;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE SILVER.erp_cust_az12';

    INSERT INTO SILVER.erp_cust_az12 (CID, BDATE, GEN)
    SELECT CASE WHEN CID LIKE 'NAS%' THEN SUBSTR(CID,4,LENGTH(CID)) ELSE CID END,
        CASE WHEN BDATE > TRUNC(SYSDATE) THEN NULL ELSE BDATE END,
        CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
             WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
             ELSE 'N/A' END
    FROM bronze.erp_cust_az12;

    v_row_count := SQL%ROWCOUNT;
    v_end_time := DBMS_UTILITY.GET_TIME;
    log_progress('erp_cust_az12', v_row_count, v_start_time, v_end_time);

    ---------------------------------------------------------------------------
    -- 5. Refresh silver.erp_loc_a101
    ---------------------------------------------------------------------------
    v_start_time := DBMS_UTILITY.GET_TIME;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE silver.erp_loc_a101';

    INSERT INTO silver.erp_loc_a101 (CID, CNTRY)
    SELECT REPLACE(CID,'-',''),
        CASE WHEN TRIM(CNTRY) = 'DE' THEN 'Germany'
             WHEN TRIM(CNTRY) IN ('USA','US') THEN 'United States'
             WHEN TRIM(CNTRY) IS NULL THEN 'N/A'
             ELSE TRIM(CNTRY) END 
    FROM bronze.erp_loc_a101;

    v_row_count := SQL%ROWCOUNT;
    v_end_time := DBMS_UTILITY.GET_TIME;
    log_progress('erp_loc_a101', v_row_count, v_start_time, v_end_time);

    ---------------------------------------------------------------------------
    -- 6. Refresh silver.erp_px_cat_g1v2
    ---------------------------------------------------------------------------
    v_start_time := DBMS_UTILITY.GET_TIME;
    EXECUTE IMMEDIATE 'TRUNCATE TABLE SILVER.erp_px_cat_g1v2';

    INSERT INTO SILVER.erp_px_cat_g1v2 (ID, CAT, SUBCAT, MAINTENANCE)
    SELECT ID, CAT, SUBCAT, MAINTENANCE FROM bronze.erp_px_cat_g1v2;

    v_row_count := SQL%ROWCOUNT;
    v_end_time := DBMS_UTILITY.GET_TIME;
    log_progress('erp_px_cat_g1v2', v_row_count, v_start_time, v_end_time);

    COMMIT;
    DBMS_OUTPUT.PUT_LINE('--- Silver Layer Load Completed Successfully ---');
    
EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('ERROR: ' || SQLERRM);
        RAISE_APPLICATION_ERROR(-20001, 'Error loading Silver Layer: ' || SQLERRM);
END SP_LOAD_SILVER_LAYER;
/




--Give Needed Permissions

GRANT SELECT ON bronze.crm_cust_info TO SILVER;
GRANT SELECT ON bronze.crm_prd_info TO SILVER;
GRANT SELECT ON bronze.crm_sales_details TO SILVER;
GRANT SELECT ON bronze.erp_cust_az12 TO SILVER;
GRANT SELECT ON bronze.erp_loc_a101 TO SILVER;
GRANT SELECT ON bronze.erp_px_cat_g1v2 TO SILVER;

---Calling Method
BEGIN
SILVER.SP_LOAD_SILVER_LAYER;
END;

