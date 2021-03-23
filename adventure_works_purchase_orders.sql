

DROP TABLE IF EXISTS "sloba_test"."adventure_works_purchase_orders";
CREATE TABLE "sloba_test"."adventure_works_purchase_orders"
(
    "SalesOrderNumber"     VARCHAR(20) ENCODE ZSTD NOT NULL,
    "OrderDate"            DATE        ENCODE ZSTD NOT NULL,
    "DueDate"              VARCHAR(10) ENCODE ZSTD NOT NULL,  --I had a problem with formating this field as time so it is varchar
    "ShipDate"             VARCHAR(10) ENCODE ZSTD NOT NULL,  --I had a problem with formating this field as time so it is varchar
    "Sales_Person"         VARCHAR(30) ENCODE ZSTD NOT NULL,
    "Sales_Region"         VARCHAR(20) ENCODE ZSTD NOT NULL,
    "Sales_Province"       VARCHAR(20) ENCODE ZSTD NOT NULL,
    "Sales_City"           VARCHAR(20) ENCODE ZSTD NOT NULL,
    "Sales_Postal_Code"    VARCHAR(10) ENCODE ZSTD NOT NULL,
    "Customer_Code"        VARCHAR(20) ENCODE ZSTD NOT NULL,
    "Customer_Name"        VARCHAR(50) ENCODE ZSTD NOT NULL,
    "Customer_Region"      VARCHAR(20) ENCODE ZSTD NOT NULL,
    "Customer_Province"    VARCHAR(20) ENCODE ZSTD NOT NULL,
    "Customer_City"        VARCHAR(20) ENCODE ZSTD NOT NULL,
    "Customer_Postal_Code" VARCHAR(10) ENCODE ZSTD NOT NULL,
    "LineItem_Id"          VARCHAR(20) ENCODE ZSTD NOT NULL,
    "Product_Category"     VARCHAR(20) ENCODE ZSTD NOT NULL,
    "Product_Sub_Category" VARCHAR(20) ENCODE ZSTD NOT NULL,
    "Product_Name"         VARCHAR(40) ENCODE ZSTD NOT NULL,
    "Product_Code"         VARCHAR(20) ENCODE ZSTD NOT NULL,
    "Unit_Cost"            FLOAT       ENCODE ZSTD NOT NULL,
    "UnitPrice"            FLOAT       ENCODE ZSTD NOT NULL,
    "UnitPriceDiscount"    FLOAT       ENCODE ZSTD NOT NULL,
    "OrderQty"             INT         ENCODE ZSTD NOT NULL,
    "Unit_Freight_Cost"    FLOAT       ENCODE ZSTD NOT NULL
)
    DISTSTYLE ALL
    SORTKEY ("Sales_Region", "Product_Category", "OrderDate")
;

--I used copy command to get the data from S3 to redshift table
COPY "sloba_test"."adventure_works_purchase_orders"
    FROM 's3://mybucket/adventure_works_purchase_orders.csv'
    CREDENTIALS 'xxxxx-xxxxx'
    IGNOREHEADER 1
    CSV
    DATEFORMAT AS 'MM/DD/YYYY';


--Price changes trend over months
CREATE TEMP TABLE "tmp_analyze_price_change"
AS
SELECT *, "unitprice" - "price_before" AS "price_increase",
       "price_increase" / "price_before"::FLOAT AS "price_perc_increase"
FROM (
         SELECT "sales_region",
                "product_category",
                "product_name",
                "orderdate",
                "unitprice",
                LAG("unitprice") OVER (PARTITION BY "sales_region", "product_category", "product_name" ORDER BY "orderdate") AS "price_before"
         FROM "sloba_test"."adventure_works_purchase_orders"
         GROUP BY 1, 2, 3, 4, 5
         ORDER BY 1, 2, 3, 4, 5 DESC)
;

--used this query to check if there was increase of price for specific product, returned no data

-- SELECT * 
-- FROM "tmp_analyze_price_change" 
-- WHERE "unitprice" != "price_before";


--Cost changes trend over month per product
CREATE TEMP TABLE "tmp_analyze_cost_change"
AS
SELECT "sales_region",
       "product_category",
       "product_name",
       "unit_cost",
       "unit_cost" - "cost_before"            AS "cost_increase",
       "cost_increase" / "cost_before"::FLOAT AS "cost_perc_increase"
FROM (
         SELECT "sales_region",
                "product_category",
                "product_name",
                "orderdate",
                "unit_cost",
                LAG("unit_cost")
                OVER (PARTITION BY "sales_region", "product_category", "product_name" ORDER BY "orderdate") AS "cost_before"
         FROM "sloba_test"."adventure_works_purchase_orders"
         GROUP BY 1, 2, 3, 4, 5
         ORDER BY 1, 2, 3, 4, 5 DESC)
WHERE "unit_cost" != "cost_before"
;

--difference in percentages between price and cost per product and months
CREATE TEMP TABLE "tmp_cost_and_price_difference"
AS
SELECT "sales_region",
       "product_category",
       "product_name",
       "orderdate",
       "unit_cost",
       "unitprice",
       ("unitprice" - "unit_cost") / "unit_cost" AS "difference"
FROM "sloba_test"."adventure_works_purchase_orders"
ORDER BY 1, 2, 3, 4
;

--use discount to create discount_price, lost money because not increased, and expected profit if the price was up
DROP TABLE IF EXISTS "sloba_test"."discount_counted";
CREATE TABLE "sloba_test"."discount_counted"
AS
SELECT *, "unitprice" - "unitprice" * "unitpricediscount"       AS "discount_price",
       COALESCE("discount_price" * b."cost_perc_increase",0)    AS "price_cut",
       "discount_price" + "price_cut"                           AS "fixed_price",
       "orderqty" * "price_cut"                                 AS "lost_money",
       "orderqty" * ("discount_price" - a."unit_cost")          AS "current_profit",
       "orderqty" * ("fixed_price" - a."unit_cost")             AS "expected_profit"
FROM "sloba_test"."adventure_works_purchase_orders" a
         LEFT JOIN "tmp_analyze_cost_change" b USING ( "sales_region",
                                                       "product_category",
                                                       "product_name",
                                                       "unit_cost" )
ORDER BY 1,2,3
;


CREATE TEMP TABLE "tmp_agregated"
AS
SELECT "sales_region",
       "orderdate",
       SUM("orderqty") AS "total_order",
       SUM("lost_money") AS "lost_money",
       SUM("current_profit") AS "current_profit",
       SUM("expected_profit") AS "expected_profit"
FROM "sloba_test"."discount_counted"
GROUP BY 1, 2;

--find total shipping cost per sales region and month
CREATE TEMP TABLE "tmp_shipping_costs"
AS
SELECT "sales_region",
       "orderdate",
       SUM("unit_freight_cost") AS "total_shipping_cost"
FROM (
         SELECT DISTINCT "sales_region",
                         "salesordernumber",
                         "orderdate",
                         "unit_freight_cost"
         FROM "sloba_test"."adventure_works_purchase_orders")
GROUP BY 1, 2;

--create final profit based on shipping costs
DROP TABLE IF EXISTS "sloba_test"."tableau_analyze";
CREATE TABLE "sloba_test"."tableau_analyze"
AS
SELECT "sales_region",
       "orderdate",
       "total_order",
       "lost_money",
       "current_profit",
       "expected_profit",
       "total_shipping_cost",
       "expected_profit" - "total_shipping_cost" AS "expected_total_profit",
       "current_profit" - "total_shipping_cost"  AS "current_total_profit"
FROM "tmp_agregated" a
         LEFT JOIN "tmp_shipping_costs" b USING ("sales_region", "orderdate");
