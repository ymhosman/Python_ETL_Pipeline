-- 1. Create database
CREATE DATABASE IF NOT EXISTS sales_etl_db;
USE sales_etl_db;

-- 2. Source Table A (Transactional - Sales Data)
CREATE TABLE source_sales (
    sale_id INT PRIMARY KEY AUTO_INCREMENT,
    restaurant_code VARCHAR(20) NOT NULL,
    quantity INT NOT NULL,
    order_price DECIMAL(12,2) NOT NULL,
    sale_date DATE NOT NULL,
    customer_rating DECIMAL(2,1),
    discount_applied DECIMAL(5,2) DEFAULT 0.00
);