CREATE DATABASE IF NOT EXISTS ecommerce;

CREATE TABLE IF NOT EXISTS ecommerce.customer_acquisition_channels (
    channel_id          int,
    category            string,
    channel_name        string,
    description         string,
    created_at          timestamp
)
USING iceberg;
-- PARTITIONED BY (days(created_at))

CREATE TABLE IF NOT EXISTS ecommerce.customers (
    customer_id                 int,
    name                        string,
    gender                      string,
    email                       string,
    phone                       string,
    country                     string,
    registration_date           timestamp,
    acquisition_channel_id      int
)
USING iceberg
PARTITIONED BY (days(registration_date));

CREATE TABLE IF NOT EXISTS ecommerce.inventory (
    inventory_id            int,
    product_id              int,
    quantity                int,
    warehouse_location      string,
    created_at              timestamp,
    updated_at              timestamp
)
USING iceberg
PARTITIONED BY (days(created_at));

CREATE TABLE IF NOT EXISTS ecommerce.order_items (
    order_item_id           int,
    order_id                int,
    product_id              int,
    quantity                int,
    price                   decimal(16,2),
    created_at              timestamp
)
USING iceberg
PARTITIONED BY (days(created_at));

CREATE TABLE IF NOT EXISTS ecommerce.orders (
    order_id                int,
    customer_id             int,
    order_date              timestamp,
    order_status            string,
    total_amount            decimal(10,2),
    payment_method          string,
    created_at              timestamp
)
USING iceberg
PARTITIONED BY (days(created_at));

CREATE TABLE IF NOT EXISTS ecommerce.product_categories (
    category_id             int,
    category_name           string,
    parent_category_id      int,
    created_at              timestamp
)
USING iceberg
PARTITIONED BY (days(created_at));

CREATE TABLE IF NOT EXISTS ecommerce.products (
    product_id              int,
    name                    string,
    description             string,
    price                   decimal(10,2),
    category_id             int,
    created_at              timestamp,
    updated_at              timestamp
)
USING iceberg
PARTITIONED BY (days(created_at));

-- CREATE TABLE IF NOT EXISTS ecommerce.xxx (

--     created_at                  timestamp
-- )
-- USING iceberg
-- PARTITIONED BY (days(created_at));