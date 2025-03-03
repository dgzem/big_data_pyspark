-- Create users table
CREATE TABLE users (
    user_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL
);

-- Create products table
CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    price DECIMAL(10,2) NOT NULL
);

-- Create orders table
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    user_id INT REFERENCES users(user_id),
    product_id INT REFERENCES products(product_id),
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert more data into users
INSERT INTO users (name, email) VALUES
('Alice Johnson', 'alice@example.com'),
('Bob Smith', 'bob@example.com'),
('Charlie Brown', 'charlie@example.com'),
('David Wilson', 'david@example.com'),
('Emma Davis', 'emma@example.com'),
('Frank Martin', 'frank@example.com'),
('Grace Hall', 'grace@example.com');

-- Insert more data into products
INSERT INTO products (name, price) VALUES
('Laptop', 1200.00),
('Smartphone', 800.00),
('Headphones', 150.00),
('Tablet', 500.00),
('Smartwatch', 250.00),
('Wireless Charger', 40.00),
('Gaming Mouse', 70.00);

-- Insert more data into orders
INSERT INTO orders (user_id, product_id) VALUES
(1, 2),  -- Alice buys a Smartphone
(2, 1),  -- Bob buys a Laptop
(3, 3),  -- Charlie buys Headphones
(4, 5),  -- David buys a Smartwatch
(5, 4),  -- Emma buys a Tablet
(6, 6),  -- Frank buys a Wireless Charger
(7, 7),  -- Grace buys a Gaming Mouse
(1, 4),  -- Alice buys a Tablet
(2, 3),  -- Bob buys Headphones
(3, 1),  -- Charlie buys a Laptop
(4, 2),  -- David buys a Smartphone
(5, 7),  -- Emma buys a Gaming Mouse
(6, 5),  -- Frank buys a Smartwatch
(7, 6),  -- Grace buys a Wireless Charger
(1, 6),  -- Alice buys a Wireless Charger
(2, 5),  -- Bob buys a Smartwatch
(3, 7),  -- Charlie buys a Gaming Mouse
(4, 1),  -- David buys a Laptop
(5, 2),  -- Emma buys a Smartphone
(6, 3),  -- Frank buys Headphones
(7, 4),  -- Grace buys a Tablet
(1, 7),  -- Alice buys a Gaming Mouse
(2, 4),  -- Bob buys a Tablet
(3, 5),  -- Charlie buys a Smartwatch
(4, 6),  -- David buys a Wireless Charger
(5, 1),  -- Emma buys a Laptop
(6, 2),  -- Frank buys a Smartphone
(7, 3),  -- Grace buys Headphones
(1, 3),  -- Alice buys Headphones
(2, 6),  -- Bob buys a Wireless Charger
(3, 4),  -- Charlie buys a Tablet
(4, 7),  -- David buys a Gaming Mouse
(5, 3),  -- Emma buys Headphones
(6, 1),  -- Frank buys a Laptop
(7, 2);  -- Grace buys a Smartphone

-- Verify inserted data
SELECT * FROM users;
SELECT * FROM products;
SELECT * FROM orders;
