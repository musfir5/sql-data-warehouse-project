--Detailed queries of the ETL Process
--After The DDL

-------------------LOAD DATA(crm_cust_info)------------------------------------------------------------------------------------------------------------------

--1--
--Check For Nulls or Duplicate in Primary Key
--Expectation : No Result

SELECT cst_id,count(*)
FROM  bronze.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null;

--Action : Data Transfermation / Data Clensing
--Rank based on creation date



--2--
--Check Unwanted Spaces
--Expectation : No Result

select CST_FIRSTNAME 
from bronze.crm_cust_info a 
where CST_FIRSTNAME <>trim(CST_FIRSTNAME);

----Action : Use TRIM() To Remove The Unwanted Spaces

--3--
--Data Standardization & Consistency
--Check Gender

SELECT DISTINCT  CST_GNDR
FROM bronze.crm_cust_info 



--INSERT INTO silver.crm_cust_info(
CST_ID,
CST_KEY,
CST_FIRSTNAME,
CST_LASTNAME,
CST_MARITAL_STATUS,
CST_GNDR,
CST_CREATE_DATE
)
SELECT 
CST_ID,
CST_KEY,
TRIM(CST_FIRSTNAME)CST_FIRSTNAME,--TRIM()--Data Consistency
TRIM(CST_LASTNAME)CST_LASTNAME,--TRIM()
CASE WHEN UPPER(TRIM(CST_MARITAL_STATUS))='S' THEN 'Single'--Data Standardization 
     WHEN UPPER(TRIM(CST_MARITAL_STATUS))='M' THEN 'Married'
     ELSE 'N/A'   --Data Consistency
END CST_MARITAL_STATUS,
CASE WHEN UPPER(TRIM(CST_GNDR))='F' THEN 'Female'--Data Standardization 
     WHEN UPPER(TRIM(CST_GNDR))='M' THEN 'Male'
     ELSE 'N/A'   --Data Consistency
END CST_GNDR,
CST_CREATE_DATE
FROM (
select a.*,
ROW_NUMBER() OVER(PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC)FLAG--Removing the duplicates
FROM  bronze.crm_cust_info a 
where cst_id IS NOT NULL
)WHERE FLAG =1


--After the insertion recheck the above scenarios with the silver layer tables

SELECT cst_id,count(*)
FROM  silver.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null

--
select CST_FIRSTNAME ,CST_LASTNAME
from silver.crm_cust_info a 
where CST_FIRSTNAME <>trim(CST_FIRSTNAME) or CST_LASTNAME<>trim(CST_LASTNAME)

--
SELECT DISTINCT  CST_GNDR--,CST_MARITAL_STATUS
FROM silver.crm_cust_info a

SELECT * fROM silver.crm_cust_info

-------------------LOAD DATA(crm_prd_info)------------------------------------------------------------------------------------------------------------------
--1--Dup check

SELECT * FROM  bronze.crm_prd_info;

SELECT DISTINCT PRD_ID,COUNT(*)
FROM  bronze.crm_prd_info
GROUP BY PRD_ID
HAVING COUNT(*)>1 OR PRD_ID IS NULL


--2--Null check

select PRD_NM,PRD_COST
from bronze.crm_prd_info a 
where PRD_NM <>trim(PRD_NM) or PRD_NM is null or PRD_COST<0 or PRD_COST is null

--3--
--Data Standardization & Consistency
--Check Gender

select DISTINCT PRD_LINE
from bronze.crm_prd_info a

--4--Checking for invalid Date orders

select *
from bronze.crm_prd_info a
WHERE PRD_END_DT<PRD_START_DT

--FIX


select PRD_ID,
PRD_KEY,
PRD_NM, 
PRD_START_DT,
PRD_END_DT,
LEAD(PRD_START_DT) OVER(PARTITION BY PRD_KEY ORDER BY PRD_START_DT)-1 PRD_END_DT_TEST
from bronze.crm_prd_info a
--WHERE PRD_END_DT<PRD_START_DT
WHERE PRD_KEY IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')

-------

--INSERT INTO silver.crm_prd_info
(
PRD_ID,
CAT_ID,
PRD_KEY,
PRD_NM,
PRD_COST,
PRD_LINE,
PRD_START_DT,
PRD_END_DT
)
SELECT PRD_ID,
REPLACE(SUBSTR(PRD_KEY,1,5),'-','_') AS CAT_ID,--making the CAT_ID from the PRD_KEY
SUBSTR(PRD_KEY,7,LENGTH(PRD_KEY)) AS PRD_KEY,--Transformed the column data for analytics
PRD_NM,
NVL(PRD_COST,0) PRD_COST,--Convert the null into '0'
CASE UPPER(TRIM(PRD_LINE))
     WHEN 'M' THEN 'Mountain'
     WHEN 'R' THEN 'Road'
     WHEN 'S' THEN 'Other sales'
     WHEN 'T' THEN 'Touring'
     ELSE 'N/A' 
END PRD_LINE,--Converting to descriptive values(Data Normalization)
trunc(PRD_START_DT)PRD_START_DT,--Making it a Date field
LEAD(PRD_START_DT) OVER(PARTITION BY PRD_KEY ORDER BY PRD_START_DT)-1 PRD_END_DT--Solved the Start date end date mismatch
FROM  bronze.crm_prd_info;
--WHERE REPLACE(SUBSTR(PRD_KEY,1,5),'-','_') NOT IN (SELECT ID FROM bronze.erp_px_cat_g1v2)--Re checking the categories are matching
--WHERE SUBSTR(PRD_KEY,7,LENGTH(PRD_KEY))  NOT IN (SELECT SLS_PRD_KEY FROM bronze.crm_sales_details)--Re checking the prpduct key are matching with the sales details TABLE


SELECT * FROM silver.crm_prd_info;
--Re Check All Steps


SELECT DISTINCT PRD_ID,COUNT(*)
FROM  silver.crm_prd_info
GROUP BY PRD_ID
HAVING COUNT(*)>1 OR PRD_ID IS NULL;


select PRD_NM,PRD_COST
from silver.crm_prd_info a 
where PRD_NM <>trim(PRD_NM) or PRD_NM is null or PRD_COST<0 or PRD_COST is null;


select DISTINCT PRD_LINE
from silver.crm_prd_info a

select *
from silver.crm_prd_info a
WHERE PRD_END_DT<PRD_START_DT



-------------------LOAD DATA(crm_sales_details)------------------------------------------------------------------------------------------------------------------

--check the invalid dates

SELECT
NVL(SLS_ORDER_DT,0)SLS_ORDER_DT 
fROM 
bronze.crm_sales_details
WHERE SLS_ORDER_DT<=0 OR 
LENGTH(SLS_ORDER_DT)<>8 OR 
SLS_ORDER_DT>20500101 

select * from 
bronze.crm_sales_details
WHERE SLS_ORDER_DT>SLS_SHIP_DT OR SLS_ORDER_DT>SLS_DUE_DT

--Check the business rules
--sales  = quantity * price
--non negative,zero,null(sales,quantity,price)

SELECT
SLS_SALES OLD_SALES,
SLS_QUANTITY,
SLS_PRICE OLD_SLS_PRICE,
CASE WHEN SLS_SALES IS NULL OR SLS_SALES <=0 OR SLS_SALES <> SLS_QUANTITY*ABS(SLS_PRICE)
     THEN SLS_QUANTITY*ABS(SLS_PRICE) --Data Correction
     ELSE SLS_SALES
END SLS_SALES,
CASE WHEN SLS_PRICE IS NULL OR SLS_PRICE <=0
     THEN SLS_SALES/NVL(SLS_QUANTITY,0) --Data Correction
     ELSE SLS_PRICE
END SLS_PRICE
FROM bronze.crm_sales_details
WHERE SLS_SALES<>SLS_QUANTITY*SLS_PRICE
OR SLS_SALES IS NULL OR SLS_SALES<=0
OR SLS_QUANTITY IS NULL OR SLS_QUANTITY<=0
OR SLS_PRICE IS NULL OR SLS_PRICE<=0
ORDER BY SLS_SALES DESC


SELECT abs(10) abs FROM DUAL





--INSERT INTO  silver.crm_sales_details 
(
SLS_ORD_NUM,
SLS_PRD_KEY,
SLS_CUST_ID,
SLS_ORDER_DT,
SLS_SHIP_DT,
SLS_DUE_DT,
SLS_SALES,
SLS_QUANTITY,
SLS_PRICE
)
SELECT SLS_ORD_NUM,
SLS_PRD_KEY,
SLS_CUST_ID,
CASE WHEN SLS_ORDER_DT =0 OR LENGTH(SLS_ORDER_DT)<>8 THEN NULL--Convert Number to date format
     ELSE TO_DATE(TO_CHAR(SLS_ORDER_DT), 'YYYYMMDD') 
END SLS_ORDER_DT,
CASE WHEN SLS_SHIP_DT =0 OR LENGTH(SLS_SHIP_DT)<>8 THEN NULL--Convert Number to date format
     ELSE TO_DATE(TO_CHAR(SLS_SHIP_DT), 'YYYYMMDD') 
END SLS_SHIP_DT,
CASE WHEN SLS_DUE_DT =0 OR LENGTH(SLS_DUE_DT)<>8 THEN NULL--Convert Number to date format
     ELSE TO_DATE(TO_CHAR(SLS_DUE_DT), 'YYYYMMDD') 
END SLS_DUE_DT,
CASE WHEN SLS_SALES IS NULL OR SLS_SALES <=0 OR SLS_SALES <> SLS_QUANTITY*ABS(SLS_PRICE)
     THEN SLS_QUANTITY*ABS(SLS_PRICE) --Data Correction
     ELSE SLS_SALES
END SLS_SALES,
SLS_QUANTITY,
CASE WHEN SLS_PRICE IS NULL OR SLS_PRICE <=0
     THEN SLS_SALES/NVL(SLS_QUANTITY,0) --Data Correction
     ELSE SLS_PRICE
END SLS_PRICE
FROM bronze.crm_sales_details
--WHERE SLS_ORD_NUM<>TRIM(SLS_ORD_NUM)--check for unwanted spaces
--WHERE SLS_PRD_KEY NOT IN (SELECT PRD_KEY FROM silver.crm_prd_info)--check data matching
--WHERE SLS_CUST_ID NOT IN (SELECT cst_id FROM silver.crm_cust_info)--check data matching



SELECT * FROM silver.crm_sales_details 

--Re check all issues

SELECT
SLS_ORDER_DT
fROM 
silver.crm_sales_details
WHERE SLS_ORDER_DT is null OR 
SLS_ORDER_DT>'31-dec-2050' 


-------------------LOAD DATA(erp_cust_az12)------------------------------------------------------------------------------------------------------------------

--1--
--Check DOB

SELECT * fROM bronze.erp_cust_az12
WHERE BDATE<'01-JAN-1924' OR BDATE>TRUNC(SYSDATE)

--

SELECT DISTINCT GEN,CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
     ELSE 'N/A'
END GEN 
FROM bronze.erp_cust_az12




--INSERT INTO SILVER.erp_cust_az12
(
CID,
BDATE,
GEN
)
SELECT
CASE WHEN CID LIKE 'NAS%' THEN SUBSTR(CID,4,LENGTH(CID))--Data transfermation for the matching with (SILVER.CRM_CUST_INFO)
     ELSE CID
END CID,
CASE WHEN BDATE>TRUNC(SYSDATE) THEN NULL
     ELSE BDATE
END BDATE,
CASE WHEN UPPER(TRIM(GEN)) IN ('F','FEMALE') THEN 'Female'
     WHEN UPPER(TRIM(GEN)) IN ('M','MALE') THEN 'Male'
     ELSE 'N/A'
END GEN
FROM bronze.erp_cust_az12;


--RECHECK

SELECT DISTINCT GEN fROM SILVER.erp_cust_az12;


SELECT * fROM SILVER.erp_cust_az12
WHERE BDATE<'01-JAN-1924' OR BDATE>TRUNC(SYSDATE)



-------------------LOAD DATA(erp_loc_a101)------------------------------------------------------------------------------------------------------------------
--Standardize the countries
select distinct cntry,CASE WHEN TRIM(CNTRY)='DE' THEN 'Germany'
     WHEN TRIM(CNTRY) in ('USA','US') THEN 'United States'
     WHEN TRIM(CNTRY) ='' or TRIM(CNTRY) is null THEN 'N/A'
     ELSE TRIM(CNTRY)
END CNTRY  from  bronze.erp_loc_a101;



--INSERT INTO silver.erp_loc_a101
(
CID,CNTRY
)
SELECT
REPLACE(CID,'-','')CID,--Data Consistency & Standardization
CASE WHEN TRIM(CNTRY)='DE' THEN 'Germany'
     WHEN TRIM(CNTRY) in ('USA','US') THEN 'United States'
     WHEN TRIM(CNTRY) ='' or TRIM(CNTRY) is null THEN 'N/A'--Normalization , missing and Nulls
     ELSE TRIM(CNTRY)
END CNTRY 
FROM bronze.erp_loc_a101;


--RECHECK

SELECT
DISTINCT CNTRY
FROM silver.erp_loc_a101;


SELECT * fROM SILVER.erp_loc_a101;



-------------------LOAD DATA(erp_px_cat_g1v2)------------------------------------------------------------------------------------------------------------------

--1--
--Check for unwanted spaces

select * from bronze.erp_px_cat_g1v2
where cat<>trim(cat) 
or SUBCAT<>trim(SUBCAT) 
or MAINTENANCE<>trim(MAINTENANCE) 

--2--
--Data Consistency & Standardization

SELECT DISTINCT  MAINTENANCE
FROM bronze.erp_px_cat_g1v2




--INSERT INTO SILVER.erp_px_cat_g1v2
(
ID,
CAT,
SUBCAT,
MAINTENANCE 
)
SELECT
ID,--Equal CAT_ID(silver.crm_prd_info)
CAT,
SUBCAT,
MAINTENANCE 
FROM bronze.erp_px_cat_g1v2;


SELECT * FROM SILVER.erp_px_cat_g1v2



-------After that create the procedure for the ETL
