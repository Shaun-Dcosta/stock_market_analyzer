CREATE DATABASE stock_data;

USE stock_data;

CREATE TABLE stock_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    symbol VARCHAR(10),
    price DECIMAL(10, 2),
    volume BIGINT,
    timestamp DATETIME
);
