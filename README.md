# Big Data Tools - PySpark

Hello developers, welcome to my repository for Pyspark studies recap 😄

In here you will find the following files:

* Dockerfile: Custom Python Image that is being created to run Pyspark;
* docker-compose.yaml: List of containers that it is necessary ro run Pyspark, such as:

  * Spark-master node;
  * Spark-worker node;
  * Pyspark image (being custom build with the help of Dockerfile).
* data: In this directory it is being put the data that is going to be loaded into Pyspark so manipulations can be done, suche as:

  * Data Frames Manipulations;
  * Data Frames Optimizations;
  * Data Sets;
  * RDDs;


## Data Frames Manipulations

- df_manipulation.py File: [df_manipulation.py](https://github.com/dgzem/big_data_pyspark/blob/main/df_manipulation.py)
- df_query_translator.py File: [df_query_translator.py](https://github.com/dgzem/big_data_pyspark/blob/main/df_query_translator.py)

### What is being done?

#### df_manipulation.py
Features & SQL Equivalents

* Selection & Filtering (SELECT, WHERE, DISTINCT) ✔️
* Aggregation & Grouping (GROUP BY, HAVING, AVG, MAX, MIN) ✔️
* Conditional Transformations (CASE WHEN) ✔️
* Sorting (ORDER BY) ✔️
* Joins (INNER JOIN, LEFT JOIN, CROSS JOIN) ✔️
* Window Functions (ROW_NUMBER, RANK, DENSE_RANK) ✔️
* String & Numeric Operations (COALESCE, LOWER, COUNT, SPLIT) ✔️
 
#### df_query_translator.py 
Using the following .CSVs file, we will have 3 tables:
 - [orders.csv](https://github.com/dgzem/big_data_pyspark/blob/main/data/orders.csv)
 - [products.csv](https://github.com/dgzem/big_data_pyspark/blob/main/data/products.csv)
 - [users.csv](https://github.com/dgzem/big_data_pyspark/blob/main/data/users.csv)

 With those we are able to  translate and run the following query:

 ```sql
 WITH product_sales AS (
    -- Calculate total revenue and number of orders per product
    SELECT 
        p.product_id,
        LOWER(p.name) AS product_name,
        COUNT(o.order_id) AS total_orders,
        SUM(p.price) AS total_revenue
    FROM products p
    JOIN orders o ON p.product_id = o.product_id
    GROUP BY p.product_id, p.name
    HAVING COUNT(o.order_id) > 2  -- Only products with more than 2 orders
),
user_latest_purchase AS (
    -- Get each user's most recent order using ROW_NUMBER()
    SELECT 
        o.user_id,
        u.name AS user_name,
        p.name AS last_product_purchased,
        o.order_date,
        ROW_NUMBER() OVER (PARTITION BY o.user_id ORDER BY o.order_date DESC) AS rn
    FROM orders o
    JOIN users u ON o.user_id = u.user_id
    JOIN products p ON o.product_id = p.product_id
)
, user_spending AS (
    -- Rank users based on their total spending
    SELECT 
        o.user_id,
        u.name AS user_name,
        COALESCE(SUM(p.price), 0) AS total_spent,
        RANK() OVER (ORDER BY SUM(p.price) DESC) AS spending_rank
    FROM orders o
    JOIN users u ON o.user_id = u.user_id
    JOIN products p ON o.product_id = p.product_id
    GROUP BY o.user_id, u.name
)

-- Final SELECT combining the analysis
SELECT 
    u.user_id,
    u.user_name,
    us.total_spent,
    us.spending_rank,
    plp.last_product_purchased,
    ps.total_orders,
    ps.total_revenue
FROM user_spending us
LEFT JOIN user_latest_purchase plp ON us.user_id = plp.user_id AND plp.rn = 1
LEFT JOIN product_sales ps ON ps.name = plp.last_product_purchased
ORDER BY us.spending_rank;

 ```