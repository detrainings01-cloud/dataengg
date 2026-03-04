SHOW DATABASES ;

CREATE OR REPLACE TABLE sales_data (
    order_id INT,
    customer_id INT,
    order_date DATE,
    amount NUMBER(10,2),
    region STRING
);

SHOW TABLES;

INSERT INTO sales_data VALUES
(1, 101, '2024-01-01', 100, 'East'),
(2, 101, '2024-01-02', 200, 'East'),
(3, 101, '2024-01-03', 350, 'East'),
(4, 102, '2024-01-01', 200, 'West'),
(5, 102, '2024-01-02', 200, 'West'),
(6, 102, '2024-01-03', 150, 'West'),
(7, 102, '2024-01-06', 300, 'West'),
(8, 103, '2024-01-03', 250, 'East'),
(9, 103, '2024-01-07', 350, 'East'),
(10, 104, '2024-01-04', 400, 'West'),
(11, 104, '2024-01-08', 450, 'West'),
(12, 105, '2024-01-09', 500, 'East'),
(13, 105, '2024-01-10', 550, 'East');


SELECT * FROM sales_data;

SELECT *,
SUM(AMOUNT) OVER(PARTITION BY CUSTOMER_ID ) AS TOTAL_CUSTOMER_SALES
FROM SALES_DATA ;


SELECT *,
SUM(AMOUNT) OVER(PARTITION BY CUSTOMER_ID ORDER BY ORDER_DATE ) AS RUNNING_TOTAL
FROM SALES_DATA ;

--- RANK(), DENSE_RANK() AND ROW_NUMBER()
SELECT *,
ROW_NUMBER() OVER(ORDER BY AMOUNT DESC ) AS RNM,
RANK() OVER(ORDER BY AMOUNT DESC ) AS RNK,
DENSE_RANK() OVER(ORDER BY AMOUNT DESC ) AS D_RNK
FROM SALES_DATA;

SELECT *,
ROW_NUMBER() OVER(PARTITION BY REGION ORDER BY AMOUNT DESC ) AS RNM,
RANK() OVER(PARTITION BY REGION ORDER BY AMOUNT DESC ) AS RNK,
DENSE_RANK() OVER(PARTITION BY REGION ORDER BY AMOUNT DESC ) AS D_RNK
FROM SALES_DATA;

-- QUALIFY 
SELECT * FROM (
SELECT *,
ROW_NUMBER() OVER(PARTITION BY REGION ORDER BY AMOUNT DESC ) AS RNM
FROM SALES_DATA)
WHERE RNM = 1
;

SELECT * 
FROM SALES_DATA
QUALIFY ROW_NUMBER() OVER(PARTITION BY REGION ORDER BY AMOUNT DESC ) = 1 ;

WITH CTE AS (
SELECT 
REGION, CUSTOMER_ID, SUM(AMOUNT) AS TOTAL_AMOUNT
FROM SALES_DATA 
GROUP BY 1,2 ) 
SELECT * FROM CTE 
QUALIFY ROW_NUMBER() OVER(PARTITION BY REGION  ORDER BY TOTAL_AMOUNT DESC) <=  2 
;

SELECT * FROM SALES_DATA ;


SELECT * 
, LAG(AMOUNT)  OVER(PARTITION BY CUSTOMER_ID ORDER BY ORDER_DATE) AS LAG_AMOUNT   
, LEAD(AMOUNT)  OVER(PARTITION BY CUSTOMER_ID ORDER BY ORDER_DATE) AS LEAD_AMOUNT 
FROM SALES_DATA ORDER BY CUSTOMER_ID;


-- •	Semi structured data (JSON)  
CREATE OR REPLACE TABLE RAW_ORDERS(
    ORDER_DATA VARIANT
);

INSERT INTO RAW_ORDERS 
SELECT PARSE_JSON('{
    "order_id":1001,
    "customer": {"id": 101, "name": "John Doe", "email": "john.doe@example.com"},
    "order_date": "2024-01-01",
    "items": [
        {"product_name": "Laptop", "quantity": 1, "price": 999.99},
        {"product_name": "Mouse", "quantity": 1, "price": 200.00}
    ],
    "total_amount": 1199.99,
    "payment_method": "Credit Card",
}') ;

SELECT * FROM RAW_ORDERS ;

SELECT ORDER_DATA, ORDER_DATA:order_id::INTEGER AS order_id
, ORDER_DATA:order_date::date AS order_date , ORDER_DATA:total_amount::NUMERIC(10,2) AS total_amount , ORDER_DATA:payment_method::VARCHAR AS payment_method 
FROM RAW_ORDERS ;



SELECT ORDER_DATA, ORDER_DATA:order_id::INTEGER AS order_id
, ORDER_DATA:order_date::date AS order_date , ORDER_DATA:total_amount::NUMERIC(10,2) AS total_amount , ORDER_DATA:payment_method::VARCHAR AS payment_method 
FROM RAW_ORDERS ;


SELECT ORDER_DATA, 
ORDER_DATA:order_id::INTEGER AS order_id
-- , ORDER_DATA:customer:id::INT AS CUSTOMER_ID 
, ORDER_DATA:customer:name::VARCHAR AS CUSTOMER_NAME
, ORDER_DATA:customer:email::VARCHAR AS CUSTOMER_EMAIL
, ORDER_DATA:items[0]
, ORDER_DATA:items[1]
, ORDER_DATA:order_date::date AS order_date , ORDER_DATA:total_amount::NUMERIC(10,2) AS total_amount , ORDER_DATA:payment_method::VARCHAR AS payment_method 
FROM RAW_ORDERS ;


SELECT ORDER_DATA:order_id::INTEGER AS order_id
-- , ORDER_DATA:customer:id::INT AS CUSTOMER_ID 
, ORDER_DATA:customer:name::VARCHAR AS CUSTOMER_NAME
, ORDER_DATA:customer:email::VARCHAR AS CUSTOMER_EMAIL
, ORDER_DATA:items[0]:price::numeric(10,2) AS ITEM_PRICE 
, ORDER_DATA:items[0]:product_name::VARCHAR AS product_name 
, ORDER_DATA:items[0]:quantity::INTEGER AS quantity 
, ORDER_DATA:order_date::date AS order_date , ORDER_DATA:total_amount::NUMERIC(10,2) AS total_amount , ORDER_DATA:payment_method::VARCHAR AS payment_method 
FROM RAW_ORDERS 
UNION ALL 
SELECT ORDER_DATA:order_id::INTEGER AS order_id
-- , ORDER_DATA:customer:id::INT AS CUSTOMER_ID 
, ORDER_DATA:customer:name::VARCHAR AS CUSTOMER_NAME
, ORDER_DATA:customer:email::VARCHAR AS CUSTOMER_EMAIL
, ORDER_DATA:items[1]:price::numeric(10,2) AS ITEM_PRICE 
, ORDER_DATA:items[1]:product_name::VARCHAR AS product_name 
, ORDER_DATA:items[1]:quantity::INTEGER AS quantity 
, ORDER_DATA:order_date::date AS order_date , ORDER_DATA:total_amount::NUMERIC(10,2) AS total_amount , ORDER_DATA:payment_method::VARCHAR AS payment_method 
FROM RAW_ORDERS ;


SELECT ORDER_DATA:order_id::INTEGER AS order_id
-- , ORDER_DATA:customer:id::INT AS CUSTOMER_ID 
, ORDER_DATA:customer:name::VARCHAR AS CUSTOMER_NAME
, ORDER_DATA:customer:email::VARCHAR AS CUSTOMER_EMAIL
, f.value:product_name::VARCHAR AS product_name   
,f.value:price::numeric(10,2) as price
,f.value:quantity::integer as quantity
, ORDER_DATA:order_date::date AS order_date , ORDER_DATA:total_amount::NUMERIC(10,2) AS total_amount , ORDER_DATA:payment_method::VARCHAR AS payment_method 
FROM RAW_ORDERS ,
LATERAL FLATTEN (input => ORDER_DATA:items) f ;


INSERT INTO RAW_ORDERS  
SELECT PARSE_JSON('{
    "order_id":1002,
    "customer": {"id": 102, "name": "Jane Smith", "email": "jane.smith@example.com"},
    "order_date": "2024-01-02",
    "items": [
        {"product_name": "Desktop", "quantity": 1, "price": 500.99}
    ],
    "total_amount": 500.99,
    "payment_method": "Credit Card",
}') ;

SELECT * FROM RAW_ORDERS ;

SELECT ORDER_DATA:order_id::INTEGER AS order_id
-- , ORDER_DATA:customer:id::INT AS CUSTOMER_ID 
, ORDER_DATA:customer:name::VARCHAR AS CUSTOMER_NAME
, ORDER_DATA:customer:email::VARCHAR AS CUSTOMER_EMAIL
, f.value:product_name::VARCHAR AS product_name   
,f.value:price::numeric(10,2) as price
,f.value:quantity::integer as quantity
, ORDER_DATA:order_date::date AS order_date , ORDER_DATA:total_amount::NUMERIC(10,2) AS total_amount , ORDER_DATA:payment_method::VARCHAR AS payment_method 
FROM RAW_ORDERS ,
LATERAL FLATTEN (input => ORDER_DATA:items) f ;

CREATE OR REPLACE TABLE ORDERS AS  
SELECT ORDER_DATA:order_id::INTEGER AS order_id
-- , ORDER_DATA:customer:id::INT AS CUSTOMER_ID 
, ORDER_DATA:customer:name::VARCHAR AS CUSTOMER_NAME
, ORDER_DATA:customer:email::VARCHAR AS CUSTOMER_EMAIL
, f.value:product_name::VARCHAR AS product_name   
,f.value:price::numeric(10,2) as price
,f.value:quantity::integer as quantity
, ORDER_DATA:order_date::date AS order_date , ORDER_DATA:total_amount::NUMERIC(10,2) AS total_amount , ORDER_DATA:payment_method::VARCHAR AS payment_method 
FROM RAW_ORDERS ,
LATERAL FLATTEN (input => ORDER_DATA:items) f ;

SELECT * FROM ORDERS ;