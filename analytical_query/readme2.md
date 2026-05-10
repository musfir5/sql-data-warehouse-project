# 📊 Gold Layer Analytics — Oracle SQL | Customer & Product Reporting

A production-ready analytics layer built on the **Gold schema** using Oracle SQL. This project contains two core reporting objects that transform raw transactional data into clean, BI-ready datasets for customer and product analysis.

---

## 🏗️ Architecture

This project follows a **medallion architecture** pattern:

```
Bronze Layer (Raw)        Silver Layer (Cleaned)        Gold Layer (Aggregated)
──────────────────        ──────────────────────        ───────────────────────
Raw order tables    →     Transformed & joined    →     GOLD.REPORT_CUSTOMERS ✅
Raw customer data         Standardized types            GOLD.REPORT_PRODUCTS  ✅
Raw product data          Validated records             BI-ready, pre-aggregated
```

---



## 👤 Part 1 — Customer Reporting View

### `GOLD.REPORT_CUSTOMERS`

A reporting view that surfaces key behavioral, demographic, and financial metrics per customer.

```sql
SELECT * FROM GOLD.REPORT_CUSTOMERS;
```

### Columns Reference

| Column | Type | Description |
|---|---|---|
| `CUSTOMER_NUMBER` | VARCHAR | Unique customer identifier |
| `CUSTOMER_NAME` | VARCHAR | Full name of the customer |
| `CUSTOMER_AGE` | NUMBER | Age of the customer |
| `TOTAL_ORDERS` | NUMBER | Total number of orders placed |
| `TOTAL_SALES` | NUMBER | Cumulative sales amount |
| `TOTAL_QTY` | NUMBER | Total quantity of items ordered |
| `TOTAL_PRODUCT` | NUMBER | Number of distinct products ordered |
| `ORDER_MONTH` | NUMBER | Month number of the most recent order |
| `LAST_ORDER_DATE` | DATE | Date of the most recent order |
| `RECENCY` | NUMBER | Months since last order (lower = more recent) |
| `AGE_GROUP` | VARCHAR | Age bracket (e.g. `50 and above`, `40-49`) |
| `CUSTOMER_TYPE` | VARCHAR | Segment label (e.g. `VIP`) |
| `AVG_ORDER_VALUE` | NUMBER | Average value per order |
| `AVG_MONTHLY_SPEND` | NUMBER | Average spend per month |

### Sample Output

| CUSTOMER_NUMBER | CUSTOMER_NAME | AGE | TOTAL_ORDERS | TOTAL_SALES | RECENCY | CUSTOMER_TYPE | AVG_ORDER_VALUE | AVG_MONTHLY_SPEND |
|---|---|---|---|---|---|---|---|---|
| AW00011000 | Jon Yang | 54 | 3 | 8,249 | 156 | VIP | 2,749.67 | 305.52 |
| AW00011001 | Eugene Huang | 49 | 3 | 6,384 | 148 | VIP | 2,128.00 | 187.77 |
| AW00011002 | Ruben Torres | 55 | 3 | 8,114 | 158 | VIP | 2,704.67 | 324.56 |
| AW00011003 | Christy Zhu | 52 | 3 | 8,139 | 155 | VIP | 2,713.00 | 290.68 |

---

## 📦 Part 2 — Product Reporting Query

### `GOLD.REPORT_PRODUCTS` (CTE-based)

A multi-CTE analytical query that joins `GOLD.DIM_PRODUCTS` with `GOLD.FACT_SALES` to produce a product-level performance report — including revenue, recency, and average order metrics.

```sql
WITH BASE_PROD_DETL AS (
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
        ON P.PRODUCT_KEY = F.PRODUCT_KEY
    WHERE ORDER_DATE IS NOT NULL
),
PRODUCT_AGGREGATION AS (
    SELECT
        PRODUCT_NUMBER,
        PRODUCT_NAME,
        CATEGORY_ID,
        CATEGORY,
        SUBCATEGORY,
        SUM(SALES_AMOUNT)                                        TOTAL_SALES,
        SUM(QUANTITY)                                            TOTAL_QTY,
        COUNT(DISTINCT ORDER_NUMBER)                             TOTAL_ORDERS,
        COUNT(DISTINCT CUSTOMER_KEY)                             TOTAL_CUSTOMER,
        MIN(ORDER_DATE)                                          LAST_ORDER_DATE,
        TRUNC(MONTHS_BETWEEN(MAX(ORDER_DATE), MIN(ORDER_DATE))) MONTHS
    FROM BASE_PROD_DETL
    GROUP BY PRODUCT_NUMBER, PRODUCT_NAME, CATEGORY_ID, CATEGORY, SUBCATEGORY
)
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
    TRUNC(MONTHS_BETWEEN(TRUNC(SYSDATE), LAST_ORDER_DATE))      RECENCY,
    CASE WHEN TOTAL_ORDERS = 0 THEN 0
         ELSE ROUND(TOTAL_SALES / TOTAL_ORDERS, 3)
    END                                                          AVG_ORDER_VALUE,
    MONTHS,
    CASE WHEN MONTHS = 0 THEN TOTAL_SALES
         ELSE ROUND(TOTAL_SALES / MONTHS, 3)
    END                                                          AVG_SALES_MONTH
FROM PRODUCT_AGGREGATION;
```

### CTE Breakdown

| CTE | Purpose |
|---|---|
| `BASE_PROD_DETL` | Joins `DIM_PRODUCTS` ↔ `FACT_SALES` on `PRODUCT_KEY`. Filters out records with no order date. |
| `PRODUCT_AGGREGATION` | Groups by product and computes totals: sales, quantity, orders, unique customers, and active months. |
| Final `SELECT` | Calculates derived KPIs: recency (months since last order), average order value (AOR), and average monthly sales. |

### Columns Reference

| Column | Type | Description |
|---|---|---|
| `PRODUCT_NUMBER` | VARCHAR | Unique product identifier |
| `PRODUCT_NAME` | VARCHAR | Name of the product |
| `CATEGORY_ID` | NUMBER | Category foreign key |
| `CATEGORY` | VARCHAR | Product category (e.g. Bikes, Accessories) |
| `SUBCATEGORY` | VARCHAR | Product subcategory |
| `TOTAL_SALES` | NUMBER | Cumulative revenue from this product |
| `TOTAL_QTY` | NUMBER | Total units sold |
| `TOTAL_ORDERS` | NUMBER | Total distinct orders containing this product |
| `TOTAL_CUSTOMER` | NUMBER | Total distinct customers who ordered this product |
| `RECENCY` | NUMBER | Months since the last sale (via `SYSDATE`) |
| `AVG_ORDER_VALUE` | NUMBER | Total sales ÷ total orders (AOR) |
| `MONTHS` | NUMBER | Active selling window in months |
| `AVG_SALES_MONTH` | NUMBER | Total sales ÷ active months |

### Key Business Logic

```
AVG_ORDER_VALUE  = TOTAL_SALES / TOTAL_ORDERS
                   → 0 if no orders exist (division-by-zero guard)

AVG_SALES_MONTH  = TOTAL_SALES / MONTHS
                   → TOTAL_SALES if only 1 month of data (MONTHS = 0 guard)

RECENCY          = MONTHS_BETWEEN(SYSDATE, LAST_ORDER_DATE)
                   → Lower value = more recently sold product
```

---

## 🔗 Source Tables

| Table | Schema | Type | Description |
|---|---|---|---|
| `FACT_SALES` | GOLD | Fact | Order-level transactional records |
| `DIM_PRODUCTS` | GOLD | Dimension | Product master with category hierarchy |
| `REPORT_CUSTOMERS` | GOLD | View | Pre-built customer analytics view |

---

## ⚙️ Example Analytical Queries

### Top 10 products by revenue
```sql
SELECT PRODUCT_NAME, CATEGORY, TOTAL_SALES, AVG_ORDER_VALUE
FROM GOLD.REPORT_PRODUCTS
ORDER BY TOTAL_SALES DESC
FETCH FIRST 10 ROWS ONLY;
```

### Products with declining recency (not sold recently)
```sql
SELECT PRODUCT_NAME, RECENCY, TOTAL_SALES
FROM GOLD.REPORT_PRODUCTS
WHERE RECENCY > 12
ORDER BY RECENCY DESC;
```

### Category-level performance summary
```sql
SELECT
    CATEGORY,
    COUNT(*)                           PRODUCT_COUNT,
    SUM(TOTAL_SALES)                   CATEGORY_REVENUE,
    ROUND(AVG(AVG_SALES_MONTH), 2)     AVG_MONTHLY_REVENUE
FROM GOLD.REPORT_PRODUCTS
GROUP BY CATEGORY
ORDER BY CATEGORY_REVENUE DESC;
```

### Cross-analysis — VIP customers + their top products
```sql
SELECT C.CUSTOMER_NAME, C.CUSTOMER_TYPE, P.PRODUCT_NAME, P.CATEGORY
FROM GOLD.REPORT_CUSTOMERS C
JOIN GOLD.FACT_SALES F    ON C.CUSTOMER_KEY  = F.CUSTOMER_KEY
JOIN GOLD.DIM_PRODUCTS P  ON F.PRODUCT_KEY   = P.PRODUCT_KEY
WHERE C.CUSTOMER_TYPE = 'VIP'
ORDER BY C.CUSTOMER_NAME;
```

---

## 🛠️ Tech Stack

- **Database**: Oracle SQL
- **Schema layer**: Gold (medallion architecture)
- **Techniques**: CTEs, `MONTHS_BETWEEN`, `SYSDATE`, aggregation, LEFT JOIN, CASE guards
- **IDE**: Toad For Oracle
- **Use case**: Customer analytics, product performance, RFM segmentation, BI reporting

---

## 📬 Contact

musfirmohammed555@gmail.com
---

*Built with Oracle SQL · Gold schema layer · Customer & Product Analytics*
