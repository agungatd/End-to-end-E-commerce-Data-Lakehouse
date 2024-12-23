-- ref: https://medium.com/@asuarezaceves/initializing-a-postgresql-database-with-a-dataset-using-docker-compose-a-step-by-step-guide-3feebd5b1545

CREATE TABLE customer_acquisition_channels (
    channel_id INTEGER UNIQUE GENERATED ALWAYS AS IDENTITY,
    category VARCHAR(30) DEFAULT 'other',
    channel_name VARCHAR(100) NOT NULL,
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

COPY customer_acquisition_channels(category, channel_name, description, created_at)
FROM '/docker-entrypoint-initdb.d/customer_acquisition_channels.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE customers (
    customer_id BIGINT UNIQUE GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(100) NOT NULL,
    gender CHAR(1) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    phone VARCHAR(20) UNIQUE NOT NULL,
    country CHAR(3),
    registration_date TIMESTAMP DEFAULT NOW(),
    acquisition_channel_id INTEGER REFERENCES customer_acquisition_channels(channel_id)
);

COPY customers(name, gender, email, phone, country, registration_date, acquisition_channel_id)
FROM '/docker-entrypoint-initdb.d/customers.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE product_categories (
    category_id BIGINT UNIQUE GENERATED ALWAYS AS IDENTITY,
    category_name VARCHAR(100) NOT NULL,
    parent_category_id INTEGER REFERENCES product_categories(category_id),
    created_at TIMESTAMP DEFAULT NOW()
);

COPY product_categories(category_name, parent_category_id, created_at)
FROM '/docker-entrypoint-initdb.d/product_categories.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE products (
    product_id INTEGER UNIQUE GENERATED ALWAYS AS IDENTITY,
    name VARCHAR(100) NOT NULL,
    description TEXT,
    price DECIMAL(10, 2) NOT NULL,
    category_id INTEGER REFERENCES product_categories(category_id),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

COPY products(name, description, price, category_id, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/products.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE inventory (
    inventory_id BIGINT UNIQUE GENERATED ALWAYS AS IDENTITY,
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    warehouse_location VARCHAR(100),
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

COPY inventory(product_id, quantity, warehouse_location, created_at, updated_at)
FROM '/docker-entrypoint-initdb.d/inventory.csv'
DELIMITER ','
CSV HEADER;

-- CREATE TABLE product_availability_logs (
--     log_id BIGINT UNIQUE GENERATED ALWAYS AS IDENTITY,
--     product_id INTEGER REFERENCES products(product_id),
--     status VARCHAR(20) NOT NULL,
--     timestamp TIMESTAMP DEFAULT NOW()
-- );

-- COPY product_availability_logs
-- FROM '/docker-entrypoint-initdb.d/product_availability_logs.csv'
-- DELIMITER ','
-- CSV HEADER;

CREATE TABLE orders (
    order_id BIGINT UNIQUE GENERATED ALWAYS AS IDENTITY,
    customer_id INTEGER REFERENCES customers(customer_id),
    order_date TIMESTAMP DEFAULT NOW(),
    order_status VARCHAR(20) NOT NULL,
    total_amount DECIMAL(10, 2),
    payment_method VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

COPY orders(customer_id, order_date, order_status, total_amount, payment_method, created_at)
FROM '/docker-entrypoint-initdb.d/orders.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE order_items (
    order_item_id BIGINT UNIQUE GENERATED ALWAYS AS IDENTITY,
    order_id INTEGER REFERENCES orders(order_id),
    product_id INTEGER REFERENCES products(product_id),
    quantity INTEGER NOT NULL,
    price DECIMAL(16, 2),
    created_at TIMESTAMP DEFAULT NOW()
);

COPY order_items(order_id, product_id, quantity, price, created_at)
FROM '/docker-entrypoint-initdb.d/order_items.csv'
DELIMITER ','
CSV HEADER;
