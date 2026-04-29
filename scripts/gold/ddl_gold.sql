/*
========================================================================
DDL Script: Create Gold Views
========================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse.
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
========================================================================
*/

---------------------------------------------------------------GOLD INTEGRATION(GOLD.DIM_CUSTOMERS)---------------------------------------------------------------
--1--Create the possible joins
--2--check for the same column data from diff tables and fix the data integration
--3--Re Arrange the columns
--4--Give friendly names to the columns
--5--RE CHECK THE DATA QUALITY(Uniqueness)
---------------------------------------------------------------

CREATE OR REPLACE VIEW GOLD.DIM_CUSTOMERS AS
SELECT
    row_number() over(order by cst_id) customer_key,--Surrogate key
    CI.CST_ID customer_id,
    CI.CST_KEY customer_number,
    CI.CST_FIRSTNAME first_name,
    CI.CST_LASTNAME last_name,
    LA.CNTRY country,
    CI.CST_MARITAL_STATUS marital_status,
    CASE WHEN CI.CST_GNDR <>'N/A' THEN CI.CST_GNDR--CRM is the master for gender Info(Data Integration)
        ELSE NVL(CA.GEN,'N/A')
    END  gender,
    CA.BDATE birthdate,
    CI.CST_CREATE_DATE create_date
fROM SILVER.CRM_CUST_INFO CI
    LEFT JOIN SILVER.ERP_CUST_AZ12 CA
    ON CI.CST_KEY=CA.CID
    LEFT JOIN SILVER.ERP_LOC_A101 LA
    ON CI.CST_KEY=LA.CID;



SELECT * fROM GOLD.DIM_CUSTOMERS

---------------------------------------------------------------

--Give Permissions In Gold layer
GRANT CREATE ANY VIEW TO GOLD
    
GRANT SELECT ON SILVER.crm_cust_info TO GOLD;
GRANT SELECT ON SILVER.crm_prd_info TO GOLD;
GRANT SELECT ON SILVER.crm_sales_details TO GOLD;
GRANT SELECT ON SILVER.erp_cust_az12 TO GOLD;
GRANT SELECT ON SILVER.erp_loc_a101 TO GOLD;
GRANT SELECT ON SILVER.erp_px_cat_g1v2 TO GOLD;
    
---------------------------------------------------------------

--1--
--After join check for DUP-CST_ID

--2--
--check for the same column data from diff tables and fix the data integration
--Example

SELECT
   DISTINCT CI.CST_GNDR,
    CA.GEN,
    CASE WHEN CI.CST_GNDR <>'N/A' THEN CI.CST_GNDR--CRM is the master for gender Info(Data Integration)
    ELSE NVL(CA.GEN,'N/A')
END NEW_GEN
fROM SILVER.CRM_CUST_INFO CI
    LEFT JOIN SILVER.ERP_CUST_AZ12 CA
    ON CI.CST_KEY=CA.CID
    LEFT JOIN SILVER.ERP_LOC_A101 LA
    ON CI.CST_KEY=LA.CID
ORDER BY 1,2;


---------------------------------------------------------------
--The Joining Tables 
SELECT * fROM SILVER.CRM_CUST_INFO--Main Table

SELECT * FROM SILVER.ERP_CUST_AZ12

SELECT * FROM SILVER.ERP_LOC_A101

---------------------------------------------------------------GOLD INTEGRATION(GOLD.DIM_PRODUCTS)---------------------------------------------------------------
CREATE OR REPLACE VIEW GOLD.DIM_PRODUCTS AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,--Surrogate key
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
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL -- Filter out all historical data

SELECT * FROM GOLD.DIM_PRODUCTS


---------------------------------------------------------------

SELECT * FROM SILVER.CRM_PRD_INFO

select * from SILVER.ERP_PX_CAT_G1V2 


---------------------------------------------------------------GOLD INTEGRATION(GOLD.FACT_SALES)---------------------------------------------------------------

--1--Use the surrogate keys from the DIM VIEWs to create the FACT VIEWs(Foreign Key Integrity)


CREATE VIEW GOLD.FACT_SALES AS
SELECT
SD.SLS_ORD_NUM order_number,
PR.PRODUCT_KEY,--surrogate key from the gold.gim_products
--SD.SLS_PRD_KEY,--now using the surrogate key as the product key
CU.CUSTOMER_KEY,--surrogate key from the gold.gim_customers
--SD.SLS_CUST_ID,--now using the surrogate key as the customer key
SD.SLS_ORDER_DT order_date,
SD.SLS_SHIP_DT shipping_date,
SD.SLS_DUE_DT due_date,
SD.SLS_SALES sales_amount,
SD.SLS_QUANTITY quantity,
SD.SLS_PRICE price
FROM SILVER.CRM_SALES_DETAILS SD
LEFT JOIN GOLD.DIM_PRODUCTS PR--joining with gold.dim_products
ON SD.SLS_PRD_KEY=PR.PRODUCT_NUMBER
LEFT JOIN GOLD.DIM_CUSTOMERS CU--joining with gold.dim_products
ON SD.SLS_CUST_ID=CU.CUSTOMER_ID


SELECT * FROM GOLD.FACT_SALES

---------------------------------------------------------------
--Join all GOLD views-Recheck Data Integrity


SELECT * FROM GOLD.FACT_SALES F
LEFT JOIN GOLD.DIM_CUSTOMERS C
ON F.CUSTOMER_KEY=C.CUSTOMER_KEY
LEFT JOIN GOLD.DIM_PRODUCTS P
ON F.PRODUCT_KEY=P.PRODUCT_KEY
WHERE F.PRODUCT_KEY IS NULL OR C.CUSTOMER_KEY IS NULL--CHECK THE DATA QUALITY





---------------------------------------------------------------

SELECT * FROM SILVER.CRM_SALES_DETAILS

SELECT * FROM GOLD.DIM_PRODUCTS

SELECT * FROM GOLD.DIM_CUSTOMERS

