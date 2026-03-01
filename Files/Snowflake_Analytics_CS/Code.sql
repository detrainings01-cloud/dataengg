CREATE OR REPLACE DATABASE RETAIL_DW;
USE DATABASE RETAIL_DW;

CREATE OR REPLACE SCHEMA RAW;
CREATE OR REPLACE SCHEMA CURATED;

CREATE OR REPLACE STAGE retail_stage;


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

COPY INTO RAW.products_raw
FROM @retail_stage/products.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

COPY INTO RAW.sales_raw
FROM @retail_stage/sales.csv
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);



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

