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
    ps.product_name AS most_popular_product,
    ps.total_orders,
    ps.total_revenue
FROM user_spending us
LEFT JOIN user_latest_purchase plp ON us.user_id = plp.user_id AND plp.rn = 1
LEFT JOIN product_sales ps ON ps.product_id = (
    SELECT product_id FROM orders
    GROUP BY product_id
    ORDER BY COUNT(order_id) DESC
    LIMIT 1  -- Most ordered product
)
ORDER BY us.spending_rank;
