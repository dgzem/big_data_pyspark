from pyspark.sql import SparkSession, functions as F, Window
from pyspark.sql.functions import col

spark = SparkSession.builder.master("spark://spark-master:7077").appName('queryTranslator').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Read the CSV files
df_users = spark.read.csv("file:/workspace/data/users.csv", header=True, inferSchema=True, sep = ';')
df_products = spark.read.csv("file:/workspace/data/products.csv", header=True, inferSchema=True, sep = ';')
df_orders = spark.read.csv("file:/workspace/data/orders.csv", header=True, inferSchema=True, sep = ';')
#
"""
product_sales AS (
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
    
+----------+----------------+------------+-------------+
|product_id|            name|total_orders|total_revenue|
+----------+----------------+------------+-------------+
|         3|      headphones|           6|          900|
|         7|    gaming mouse|           5|          350|
|         1|          laptop|           5|         6000|
|         6|wireless charger|           5|          200|
|         2|      smartphone|           5|         4000|
|         4|          tablet|           5|         2500|
+----------+----------------+------------+-------------+
    
),
"""
df_product_sales = df_products.join(df_orders,df_products.product_id == df_orders.product_id, 'inner').select(df_products.product_id,
                                                                                                              F.lower(df_products.name).alias('name'),
                                                                                                              df_orders.order_id,
                                                                                                              df_products.price).groupBy('product_id','name').agg(
                                                                                                                  F.count('order_id').cast('int').alias('total_orders'),
                                                                                                                  F.sum('price').alias('total_revenue')
                                                                                                              ).filter(col('total_orders') > 2)

#df_product_sales.show()

"""
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
    JOIN products p ON o.product_id = p.product_id -- HERE I SPECIFICALL FILTERED ONLY HAVING rn == 1
),

+-------+-------------+----------------------+----------+---+
|user_id|    user_name|last_product_purchased|order_date| rn|
+-------+-------------+----------------------+----------+---+
|      1|Alice Johnson|            Smartphone|03/03/2025|  1|
|      2|    Bob Smith|                Laptop|03/03/2025|  1|
|      3|Charlie Brown|            Headphones|03/03/2025|  1|
|      4| David Wilson|            Smartwatch|03/03/2025|  1|
|      5|   Emma Davis|                Tablet|03/03/2025|  1|
|      6| Frank Martin|      Wireless Charger|03/03/2025|  1|
|      7|   Grace Hall|          Gaming Mouse|03/03/2025|  1|
+-------+-------------+----------------------+----------+---+

"""

df_user_latest_purchase = df_orders.join(
    df_users, df_orders.user_id == df_users.user_id,'left').join(
    df_products, df_orders.product_id == df_products.product_id, 'left').filter(
        (df_users.user_id.isNotNull()) & (df_products.product_id.isNotNull())).select(
            df_orders.user_id.alias('user_id'),
            df_users.name.alias('user_name'),
            df_products.name.alias('last_product_purchased'),
            df_orders.order_date.alias('order_date'),
            F.row_number().over(Window.orderBy(F.desc(df_orders.order_date)).partitionBy(df_orders.user_id)).alias('rn')
        ).filter(col('rn') == 1)

#df_user_latest_purchase.show()

"""
user_spending AS (
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
+-------+-------------+-----------+-------------+
|user_id|    user_name|total_spent|spending_rank|
+-------+-------------+-----------+-------------+
|      5|   Emma Davis|       2720|            1|
|      6| Frank Martin|       2440|            2|
|      4| David Wilson|       2360|            3|
|      3|Charlie Brown|       2170|            4|
|      2|    Bob Smith|       2140|            5|
|      1|Alice Johnson|       1560|            6|
|      7|   Grace Hall|       1560|            6|
+-------+-------------+-----------+-------------+
"""

df_user_spending = df_orders.join(
    df_users, df_orders.user_id == df_users.user_id,'left').join(
        df_products, df_orders.product_id == df_products.product_id,'left').filter(
            (df_users.user_id.isNotNull()) & (df_products.product_id.isNotNull())).groupBy(df_orders.user_id, df_users.name.alias('user_name')).agg(
                F.sum(df_products.price).alias('total_spent'),
                F.rank().over(Window.orderBy(F.desc(F.sum(df_products.price)))).alias('spending_rank')
            )
#df_user_spending.show()

"""
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
)
ORDER BY us.spending_rank;

+-------+-------------+-----------+-------------+----------------------+------------+-------------+
|user_id|    user_name|total_spent|spending_rank|last_product_purchased|total_orders|total_revenue|
+-------+-------------+-----------+-------------+----------------------+------------+-------------+
|      1|Alice Johnson|       1560|            6|            Smartphone|           5|         4000|
|      7|   Grace Hall|       1560|            6|          Gaming Mouse|           5|          350|
|      2|    Bob Smith|       2140|            5|                Laptop|           5|         6000|
|      3|Charlie Brown|       2170|            4|            Headphones|           6|          900|
|      4| David Wilson|       2360|            3|            Smartwatch|           4|         1000|
|      6| Frank Martin|       2440|            2|      Wireless Charger|           5|          200|
|      5|   Emma Davis|       2720|            1|                Tablet|           5|         2500|
+-------+-------------+-----------+-------------+----------------------+------------+-------------+
"""


df = df_user_spending.join(df_user_latest_purchase, df_user_spending.user_id == df_user_latest_purchase.user_id, 'left').join(
    df_product_sales, F.lower(df_product_sales.name) == F.lower(df_user_latest_purchase.last_product_purchased),'left'
).orderBy(F.desc(df_user_spending.spending_rank)).select(
    df_user_spending.user_id,
    df_user_spending.user_name,
    df_user_spending.total_spent,
    df_user_spending.spending_rank,
    df_user_latest_purchase.last_product_purchased,
    df_product_sales.total_orders,
    df_product_sales.total_revenue
)

df.show()
