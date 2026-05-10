-----------------------------------------Change Over Time Analysis-------------------------------------------------------------------------------------------------------------------------------------------------------------

--1--By Year

SELECT 
TO_CHAR(S.ORDER_DATE,'YYYY')ORDER_YEAR,
COUNT(DISTINCT S.CUSTOMER_KEY)TOT_CUSTOMER,
SUM(S.SALES_AMOUNT)Total_sales,
SUM(S.QUANTITY)TOTAL_QUANTITY
FROM GOLD.FACT_SALES S
WHERE  S.ORDER_DATE IS NOT NULL
GROUP BY TO_CHAR(S.ORDER_DATE,'YYYY')
ORDER BY TO_CHAR(S.ORDER_DATE,'YYYY')


--2--By Month


SELECT 
TO_CHAR(S.ORDER_DATE,'YYYY-MON')ORDER_YEAR,
COUNT(DISTINCT S.CUSTOMER_KEY)TOT_CUSTOMER,
SUM(S.SALES_AMOUNT)Total_sales,
SUM(S.QUANTITY)TOTAL_QUANTITY
FROM GOLD.FACT_SALES S
WHERE  S.ORDER_DATE IS NOT NULL
GROUP BY TO_CHAR(S.ORDER_DATE,'YYYY-MON')
ORDER BY Total_sales DESC


-----------------------------------------Cumulative Analysis-------------------------------------------------------------------------------------------------------------------------------------------------------------

--Calculate the total sales per month
--and the running total of sales over time

SELECT
ORDER_YEAR,
TOT_SALES,ROUND(AVG_PRICE)AVG_PRICE,
SUM(TOT_SALES) OVER() sales_TOT,
SUM(TOT_SALES) OVER(ORDER BY ORDER_YEAR ASC) RUNNING_TOT,
ROUND(AVG(AVG_PRICE) OVER()) avg_AVERAGE,
ROUND(AVG(AVG_PRICE) OVER(ORDER BY ORDER_YEAR ASC)) MOVING_AVERAGE
FROM (
SELECT
TO_CHAR(S.ORDER_DATE,'YYYY')ORDER_YEAR,
SUM(S.SALES_AMOUNT)TOT_SALES,
avg(S.PRICE) AVG_PRICE
FROM 
GOLD.FACT_SALES S
WHERE S.ORDER_DATE IS NOT NULL
GROUP BY TO_CHAR(S.ORDER_DATE,'YYYY'))
ORDER BY 1




-----------------------------------------Performance Analysis-------------------------------------------------------------------------------------------------------------------------------------------------------------


/* Analyze the yearly performance of products by comparing their sales 
to both the average sales performance of the product and the previous year's sales */



--It can be used for MoM as well as YoY by changing the 'ORDER_DATE'
WITH YEARLY_PRODUCT_SALES AS (
SELECT
TO_CHAR(F.ORDER_DATE,'YYYY')ORDER_YEAR,
P.PRODUCT_NAME,
SUM(F.SALES_AMOUNT)CURRENT_SALES
FROM GOLD.FACT_SALES  F
LEFT JOIN GOLD.DIM_PRODUCTS P
ON F.PRODUCT_KEY =P.PRODUCT_KEY
WHERE F.ORDER_DATE IS NOT NULL
GROUP BY P.PRODUCT_NAME,TO_CHAR(F.ORDER_DATE,'YYYY'))
SELECT 
ORDER_YEAR,
PRODUCT_NAME,
CURRENT_SALES,
AVG(CURRENT_SALES) OVER(PARTITION BY PRODUCT_NAME) AVG_SALES,
CURRENT_SALES-AVG(CURRENT_SALES) OVER(PARTITION BY PRODUCT_NAME) DIFF_IN_AVG,
CASE WHEN CURRENT_SALES-AVG(CURRENT_SALES) OVER(PARTITION BY PRODUCT_NAME) >0 THEN 'Above Avg'
     WHEN CURRENT_SALES-AVG(CURRENT_SALES) OVER(PARTITION BY PRODUCT_NAME) <0 THEN 'Below Avg'
     ELSE 'Avg'
END AVG_CHANGE,
LAG(CURRENT_SALES) OVER(PARTITION BY PRODUCT_NAME ORDER BY ORDER_YEAR) PRE_SALES,--Taking the previous year sales using window function
--Year-over-year Analysis
CURRENT_SALES-LAG(CURRENT_SALES) OVER(PARTITION BY PRODUCT_NAME ORDER BY ORDER_YEAR) DIFF_PRE_SALES,
CASE WHEN CURRENT_SALES-LAG(CURRENT_SALES) OVER(PARTITION BY PRODUCT_NAME ORDER BY ORDER_YEAR) >0 THEN 'Increase'
     WHEN CURRENT_SALES-LAG(CURRENT_SALES) OVER(PARTITION BY PRODUCT_NAME ORDER BY ORDER_YEAR) <0 THEN 'Decrease'
     ELSE 'No Change'
END PRE_DIFF
FROM YEARLY_PRODUCT_SALES
ORDER BY PRODUCT_NAME,ORDER_YEAR



-----------------------------------------Part-To-Whole Analysis-------------------------------------------------------------------------------------------------------------------------------------------------------------

--Which categories contribute the most to overall sales?

--Method 1
SELECT
    CATEGORY,
    sum(F.SALES_AMOUNT)CAT_SALES,
    sum(SUM(F.SALES_AMOUNT)) OVER() TOT_SALES,
    ROUND((sum(F.SALES_AMOUNT)/sum(SUM(F.SALES_AMOUNT)) OVER())*100,2)||'%' perct_total
FROM GOLD.FACT_SALES F
LEFT JOIN GOLD.DIM_PRODUCTS P
ON F.PRODUCT_KEY=P.PRODUCT_KEY
GROUP BY CATEGORY
ORDER BY 2 DESC



--Method 2
WITH CAT_SALES AS (
SELECT
    CATEGORY,
    sum(F.SALES_AMOUNT)CAT_SALES
FROM GOLD.FACT_SALES F
LEFT JOIN GOLD.DIM_PRODUCTS P
ON F.PRODUCT_KEY=P.PRODUCT_KEY
GROUP BY CATEGORY)
SELECT 
CATEGORY,
CAT_SALES,
SUM(CAT_SALES) OVER() TOT_SALES,
ROUND((CAT_SALES/SUM(CAT_SALES) OVER())*100,2) ||'%' PERCT_TOTAL
FROM CAT_SALES
ORDER BY 2 DESC


-----------------------------------------Data segmentation-------------------------------------------------------------------------------------------------------------------------------------------------------------
/*Segment products into cost ranges and count how many products fall into each segment*/

WITH PROD_SEGMENTS AS (
SELECT
    PRODUCT_KEY,
    PRODUCT_NAME,
    COST,
    CASE WHEN COST<100 THEN 'Below 100'
         WHEN COST BETWEEN 100 AND 500 THEN '100-500'
         WHEN COST BETWEEN 500 AND 1000 THEN '500-1000'
         ELSE 'Above 500'
    END COST_RANGE       
FROM GOLD.DIM_PRODUCTS)
SELECT
COST_RANGE,
COUNT(PRODUCT_KEY)PROD_COUNT
FROM PROD_SEGMENTS
GROUP BY COST_RANGE
ORDER BY 2 DESC


/*Group customers into three segments based on their spending behavior:  

VIP: at least 12 months of history and spending more than €5,000.  
Regular: at least 12 months of history but spending €5,000 or less.  
New: lifespan less than 12 months.*/


WITH CUST_HISTORY AS (
SELECT
    C.CUSTOMER_KEY,
    C.CUSTOMER_NUMBER,
    C.FIRST_NAME,
    C.LAST_NAME,
    MIN(ORDER_DATE) FIRST_ORDER,
    MAX(ORDER_DATE) LAST_ORDER,
    TRUNC(MONTHS_BETWEEN(MAX(ORDER_DATE), min(ORDER_DATE))) MONTHS,
    SUM(F.SALES_AMOUNT)TOT_SALES
FROM GOLD.DIM_CUSTOMERS c
LEFT JOIN GOLD.FACT_SALES F
ON F.CUSTOMER_KEY=C.CUSTOMER_KEY
GROUP BY
C.CUSTOMER_KEY,
C.CUSTOMER_NUMBER,
C.FIRST_NAME,
C.LAST_NAME)
select Customer_type,count(*)segment_count FROM (
SELECT CUSTOMER_KEY,CUSTOMER_NUMBER,FIRST_NAME,LAST_NAME,FIRST_ORDER,MONTHS,TOT_SALES,
CASE WHEN (MONTHS>=12 AND TOT_SALES>5000) THEN 'VIP'
     WHEN (MONTHS>=12 AND TOT_SALES<=5000) THEN 'Regular'
     ELSE 'New'
END Customer_type     
FROM CUST_HISTORY)
GROUP BY Customer_type
ORDER BY 1 DESC 


--------------------------------------------------------------------------Report 1(Customer Report)---------------------------------------------------------------------------------------------------------------------------

/*
========================================================================
Customer Report
========================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Group customers into three segments based on their spending behavior:

- VIP: at least 12 months of history and spending more than €5,000.
- Regular: at least 12 months of history but spending €5,000 or less.
- New: lifespan less than 12 months.

Highlights:
1. Gathers essential fields such as names, ages, and transaction details.
2. Segments customers into categories (VIP, Regular, New) and age groups.
3. Aggregates customer-level metrics:
    - total orders
    - total sales
    - total quantity purchased
    - total products
    - lifespan (in months)
4. Calculates valuable KPIs:
    - recency (months since last order)
    - average order value
    - average monthly spend
========================================================================
*/



--CREATE OR REPLACE VIEW GOLD.REPORT_CUSTOMERS AS
WITH BASE_CUST_QUERY AS(--Base CTE
SELECT
    F.ORDER_NUMBER,
    F.PRODUCT_KEY,
    F.ORDER_DATE,
    F.SALES_AMOUNT,
    F.QUANTITY,
    C.CUSTOMER_KEY,
    C.CUSTOMER_NUMBER,
    C.FIRST_NAME || ' ' ||C.LAST_NAME CUSTOMER_NAME,
    FLOOR(MONTHS_BETWEEN(SYSDATE, C.BIRTHDATE) / 12) AS CUSTOMER_AGE
FROM GOLD.DIM_CUSTOMERS c
LEFT JOIN GOLD.FACT_SALES F
ON F.CUSTOMER_KEY=C.CUSTOMER_KEY
WHERE F.ORDER_DATE IS NOT NULL)
,CUSTOMER_AGGREGATION  AS (--Aggregation CTE
SELECT
    CUSTOMER_KEY,
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    CUSTOMER_AGE,
    COUNT(DISTINCT ORDER_NUMBER)TOTAL_ORDERS,
    SUM(SALES_AMOUNT)TOTAL_SALES,
    SUM(QUANTITY)TOTAL_QTY,
    COUNT(DISTINCT PRODUCT_KEY)TOTAL_PRODUCT,    
    MAX(ORDER_DATE)LAST_ORDER_DATE,
    TRUNC(MONTHS_BETWEEN(MAX(ORDER_DATE), min(ORDER_DATE))) MONTHS    
FROM BASE_CUST_QUERY
    GROUP BY
    CUSTOMER_KEY,
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    CUSTOMER_AGE
)
SELECT 
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    CUSTOMER_AGE,
    TOTAL_ORDERS,
    TOTAL_SALES,
    TOTAL_QTY,
    TOTAL_PRODUCT,
    MONTHS ORDER_MONTH,
    LAST_ORDER_DATE,
    TRUNC(MONTHS_BETWEEN(TRUNC(SYSDATE),LAST_ORDER_DATE)) RECENCY,  
    CASE WHEN CUSTOMER_AGE<20 THEN 'Under 20'
         WHEN CUSTOMER_AGE BETWEEN 20 AND 29 THEN '20-29'
         WHEN CUSTOMER_AGE BETWEEN 30 AND 39 THEN '30-39'
         WHEN CUSTOMER_AGE BETWEEN 40 AND 49 THEN '40-49'
         ELSE '50 and above'
    END AGE_GROUP,
    CASE WHEN (MONTHS>=12 AND TOTAL_SALES>5000) THEN 'VIP'
         WHEN (MONTHS>=12 AND TOTAL_SALES<=5000) THEN 'Regular'
         ELSE 'New'
    END Customer_type,  
    --Cumpute average order value(AVO)
    CASE WHEN TOTAL_ORDERS=0 THEN 0
         ELSE ROUND(TOTAL_SALES/TOTAL_ORDERS,3)
    END AVG_ORDER_VALUE,
    --Cumpute monthly spend
    CASE WHEN MONTHS=0 THEN TOTAL_SALES
         ELSE ROUND(TOTAL_SALES/MONTHS,3)
    END AVG_MONTHLY_SPEND    
fROM CUSTOMER_AGGREGATION;




SELECT * fROM GOLD.REPORT_CUSTOMERS


--------------------------------------------------------------------------Report 1(Customer Report)---------------------------------------------------------------------------------------------------------------------------
/*
================================================================================
Product Report
================================================================================
Purpose:
    - This report consolidates key product metrics and behaviors.

Highlights:
    1. Gathers essential fields such as product name, category, subcategory, and cost.
    2. Segments products by revenue to identify High-Performers, Mid-Range, or Low-Performers.
    3. Aggregates product-level metrics:
        - total orders
        - total sales
        - total quantity sold
        - total customers (unique)
        - lifespan (in months)
    4. Calculates valuable KPIs:
        - recency (months since last sale)
        - average order revenue (AOR)
        - average monthly revenue
================================================================================
*/;







--CREATE OR REPLACE VIEW GOLD.REPORT_PRODUCTS AS
WITH BASE_PROD_DETL AS(--Base CTE
SELECT
    F.ORDER_NUMBER,
    F.ORDER_DATE,
    F.SALES_AMOUNT,
    F.QUANTITY,
    F.CUSTOMER_KEY,
    P.PRODUCT_NUMBER,
    P.PRODUCT_NAME,  
    P.CATEGORY_ID,
    CATEGORY,
    SUBCATEGORY,
    COST
FROM GOLD.DIM_PRODUCTS P
LEFT JOIN GOLD.FACT_SALES F 
on P.PRODUCT_KEY=F.PRODUCT_KEY
WHERE ORDER_DATE IS NOT NULL)
, PRODUCT_AGGREGATION AS (--Aggregation CTE
SELECT 
    PRODUCT_NUMBER,
    PRODUCT_NAME,
    CATEGORY_ID,
    CATEGORY,
    SUBCATEGORY,
    SUM(SALES_AMOUNT)TOTAL_SALES,
    SUM(QUANTITY)TOTAL_QTY,
    COUNT(DISTINCT ORDER_NUMBER)TOTAL_ORDERS,
    COUNT(DISTINCT CUSTOMER_KEY)TOTAL_CUSTOMER,
    min(ORDER_DATE)LAST_ORDER_DATE,
    TRUNC(MONTHS_BETWEEN(MAX(ORDER_DATE), min(ORDER_DATE))) MONTHS
FROM BASE_PROD_DETL
GROUP BY PRODUCT_NUMBER,PRODUCT_NAME,
CATEGORY_ID,CATEGORY,SUBCATEGORY)
SELECT
    PRODUCT_NUMBER,
    PRODUCT_NAME,
    CATEGORY_ID,
    CATEGORY,
    SUBCATEGORY,
    TOTAL_SALES,
    TOTAL_QTY,
    TOTAL_ORDERS,
    TOTAL_CUSTOMER,
    TRUNC(MONTHS_BETWEEN(TRUNC(SYSDATE),LAST_ORDER_DATE)) RECENCY, 
    --Cumpute avg order revenue-AOR
    CASE WHEN TOTAL_ORDERS = 0 THEN 0
         ELSE ROUND(TOTAL_SALES/TOTAL_ORDERS,3)
    END AVG_ORDER_VALUE,  
    MONTHS,
    --Cumpute monthly sales
    CASE WHEN MONTHS = 0 THEN TOTAL_SALES
         ELSE ROUND(TOTAL_SALES/MONTHS,3)
    END AVG_SALES_MONTH        
FROM PRODUCT_AGGREGATION;




SELECT * FROM GOLD.REPORT_PRODUCTS


SELECT * FROM GOLD.DIM_PRODUCTS WHERE PRODUCT_NUMBER='PD-M282'

SELECT * FROM GOLD.DIM_CUSTOMERS WHERE CUSTOMER_KEY='1'


SELECT * FROM GOLD.FACT_SALES -- WHERE CUSTOMER_KEY='1'
