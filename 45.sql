-- Stages -- Temporary storage locations 
--  Internal stage  (With in Snowflake)  
--              Named stage (Database level) | User Stage | Table Stage   
--  External Stage (We create on top of AWS/Azure/GCP Storage Services   S3/Azure Blob/Cloud Storage)

CREATE OR REPLACE STAGE  MY_INT_STAGE;

LIST @MY_INT_STAGE/;

create or replace table student (
StudentID int ,Name varchar ,Age integer ,Course varchar, Email varchar 
);

DESC TABLE student;

SELECT GET_DDL('TABLE','STUDENT');

create or replace TABLE STUDENT (
	STUDENTID NUMBER(38,0),
	NAME VARCHAR(16777216),
	AGE NUMBER(38,0),
	COURSE VARCHAR(16777216),
	EMAIL VARCHAR(16777216)
);

CREATE FILE FORMAT  MY_CSV_FORMAT
   TYPE =  CSV  SKIP_HEADER  = 1 
;
COPY INTO STUDENT
     FROM @MY_INT_STAGE/student/
FILE_FORMAT = (  FORMAT_NAME = 'MY_CSV_FORMAT' ) 
VALIDATION_MODE =  RETURN_ALL_ERRORS ;

COPY INTO STUDENT
     FROM @MY_INT_STAGE/student/
FILE_FORMAT = (  FORMAT_NAME = 'MY_CSV_FORMAT' ) ;

select * from STUDENT;

COPY INTO STUDENT
     FROM @MY_INT_STAGE/student/ 
FILE_FORMAT = (  FORMAT_NAME = 'MY_CSV_FORMAT' ) ;


select * from STUDENT;

COPY INTO STUDENT
     FROM @MY_INT_STAGE/student/ 
FILE_FORMAT = (  FORMAT_NAME = 'MY_CSV_FORMAT' )
force = true  
;

select * from STUDENT;

truncate table student ;
COPY INTO STUDENT
     FROM @MY_INT_STAGE/student/ 
FILE_FORMAT = (  FORMAT_NAME = 'MY_CSV_FORMAT' )
PURGE = TRUE  
;

LIST @MY_INT_STAGE/;

CREATE TABLE SALES (
order_id INT ,customer_id INT ,product_id INT ,order_date DATE ,quantity INT,product_name VARCHAR,category VARCHAR,price NUMERIC(10,2),total_amount NUMERIC(10,2)
);

DESC TABLE SALES;
SHOW STAGES;

COPY INTO SALES 
FROM @MY_S3_STAGE/
FILE_FORMAT = (  FORMAT_NAME = 'MY_CSV_FORMAT' ) ;

SELECT * FROM SALES;


list @MY_INT_STAGE/;

create or replace table raw_orders_json (  raw_json variant) ;
select * from raw_orders_json;

copy into raw_orders_json 
from @MY_INT_STAGE/orders/
file_format = ( type = json   ) ;

select * from raw_orders_json;

create or replace table raw_customer_json (raw_json variant) ;
select * from raw_customer_json; 

truncate table raw_customer_json;
copy into raw_customer_json 
from @MY_INT_STAGE/customers/
file_format = (type = json STRIP_OUTER_ARRAY = TRUE ) ;

select * from raw_customer_json;

create or replace table raw_customer  as 
select raw_json:customer_id::int as customer_id
, raw_json:name::string as name 
, raw_json:city::string as city 
, raw_json:signup_date::date as signup_date 
from raw_customer_json; 

select * from raw_customer;


----  Capstone Project 

CREATE OR REPLACE DATABASE RETAIL_DW;
USE DATABASE RETAIL_DW;

CREATE OR REPLACE SCHEMA RAW;
CREATE OR REPLACE SCHEMA CURATED;

SHOW INTEGRATIONS;
CREATE OR REPLACE STAGE retail_stage
STORAGE_INTEGRATION = MY_S3_INT
URL = 's3://de-aws-snowflake-demo/retail/'
;

LIST @retail_stage/;


CREATE OR REPLACE TABLE RAW.customers_raw(
    customer_id INT,
    customer_name STRING,
    region STRING
);


CREATE OR REPLACE TABLE RAW.products_raw(
    product_id INT,
    product_name STRING,
    category STRING,
    price NUMBER
);

CREATE OR REPLACE TABLE RAW.sales_raw(
    order_id INT,
    order_date DATE,
    customer_id INT,
    product_id INT,
    quantity INT
);


COPY INTO RAW.customers_raw
FROM @retail_stage/customers.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

SELECT * FROM  RAW.customers_raw;

COPY INTO RAW.products_raw
FROM @retail_stage/products.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

COPY INTO RAW.sales_raw
FROM @retail_stage/sales.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

SELECT * FROM RAW.SALES_RAW;

CREATE OR REPLACE TABLE CURATED.dim_customer AS
SELECT DISTINCT
    customer_id,
    customer_name,
    region
FROM RAW.customers_raw;

CREATE OR REPLACE TABLE CURATED.dim_product AS
SELECT DISTINCT
    product_id,
    product_name,
    category,
    price
FROM RAW.products_raw;


CREATE OR REPLACE TABLE CURATED.dim_date AS
SELECT DISTINCT
    order_date,
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    DAY(order_date) AS day
FROM RAW.sales_raw;

SELECT * FROM DIM_DATE ;

CREATE OR REPLACE TABLE CURATED.fact_sales AS
SELECT
    s.order_id,
    s.order_date,
    s.customer_id,
    s.product_id,
    s.quantity,
    p.price,
    (s.quantity * p.price) AS total_amount
FROM RAW.sales_raw s
JOIN RAW.products_raw p
ON s.product_id = p.product_id;

SELECT * FROM fact_sales;


-- RAW --> CURATED  --> 

SELECT SUM(TOTAL_AMOUNT ) AS REVENUE 
FROM FACT_SALES;

SELECT P.PRODUCT_NAME,  SUM(TOTAL_AMOUNT ) AS REVENUE 
FROM FACT_SALES S 
JOIN DIM_PRODUCT P  ON P.PRODUCT_ID = S.PRODUCT_ID
GROUP BY 1
;

SELECT C.REGION ,  SUM(TOTAL_AMOUNT) AS REVENUE  
FROM FACT_SALES S 
JOIN DIM_CUSTOMER C 
ON S.CUSTOMER_ID = C.CUSTOMER_ID 
GROUP BY 1
;


SELECT P.PRODUCT_NAME,  SUM(TOTAL_AMOUNT ) AS REVENUE 
FROM FACT_SALES S 
JOIN DIM_PRODUCT P  ON P.PRODUCT_ID = S.PRODUCT_ID
GROUP BY 1
ORDER BY REVENUE DESC 
LIMIT 5
;

CREATE OR REPLACE SCHEMA  REPORTING ;

CREATE OR REPLACE VIEW VW_TOP5_PRODUCTS AS 
SELECT P.PRODUCT_NAME,  SUM(TOTAL_AMOUNT ) AS REVENUE 
FROM CURATED.FACT_SALES S 
JOIN CURATED.DIM_PRODUCT P  ON P.PRODUCT_ID = S.PRODUCT_ID
GROUP BY 1
ORDER BY REVENUE DESC 
LIMIT 5
;

SELECT * FROM VW_TOP5_PRODUCTS;


CREATE OR REPLACE VIEW VW_TOP5_PRODUCTS AS 
SELECT P.PRODUCT_NAME,  SUM(TOTAL_AMOUNT ) AS REVENUE 
FROM CURATED.FACT_SALES S 
JOIN CURATED.DIM_PRODUCT P  ON P.PRODUCT_ID = S.PRODUCT_ID
GROUP BY 1
ORDER BY REVENUE DESC 
LIMIT 5 ;
SELECT * FROM VW_TOP5_PRODUCTS;





CREATE OR REPLACE MATERIALIZED VIEW MV_PRODUCT_REVENUE AS 
SELECT PRODUCT_ID, SUM(TOTAL_AMOUNT ) AS REVENUE 
FROM CURATED.FACT_SALES  GROUP BY 1;
SELECT * FROM MV_PRODUCT_REVENUE;   --  VERY FAST 


CREATE OR REPLACE MATERIALIZED VIEW M_VW_TOP5_PRODUCTS AS 
SELECT P.PRODUCT_NAME,  SUM(TOTAL_AMOUNT ) AS REVENUE 
FROM CURATED.FACT_SALES S 
JOIN CURATED.DIM_PRODUCT P  ON P.PRODUCT_ID = S.PRODUCT_ID
GROUP BY 1
ORDER BY REVENUE DESC 
LIMIT 5;



CREATE OR REPLACE secure VIEW SVW_TOP5_PRODUCTS AS 
SELECT P.PRODUCT_NAME,  SUM(TOTAL_AMOUNT ) AS REVENUE 
FROM CURATED.FACT_SALES S 
JOIN CURATED.DIM_PRODUCT P  ON P.PRODUCT_ID = S.PRODUCT_ID
GROUP BY 1
ORDER BY REVENUE DESC 
LIMIT 5
;

CREATE ROLE ROLE_1;
GRANT USAGE ON DATABASE RETAIL_DW TO ROLE ROLE_1;
GRANT USAGE ON SCHEMA RETAIL_DW.REPORTING TO ROLE ROLE_1;
GRANT SELECT ON SVW_TOP5_PRODUCTS TO ROLE ROLE_1;
GRANT ROLE ROLE_1 TO USER DETRAININGS01;

SELECT GET_DDL('VIEW','SVW_TOP5_PRODUCTS');

DROP TABLE DT_VW_TOP5_PRODUCTS;

CREATE OR REPLACE DYNAMIC TABLE DT_VW_TOP5_PRODUCTS 
TARGET_LAG = DOWNSTREAM
WAREHOUSE = MY_WH
REFRESH_MODE = auto 
AS 
SELECT P.PRODUCT_NAME,  SUM(TOTAL_AMOUNT ) AS REVENUE 
FROM CURATED.FACT_SALES S 
JOIN CURATED.DIM_PRODUCT P  ON P.PRODUCT_ID = S.PRODUCT_ID
GROUP BY 1
ORDER BY REVENUE DESC 
LIMIT 5
;

SELECT * FROM DT_VW_TOP5_PRODUCTS ;

drop dynamic table retail_dw.reporting.DT_VW_TOP5_PRODUCTS;