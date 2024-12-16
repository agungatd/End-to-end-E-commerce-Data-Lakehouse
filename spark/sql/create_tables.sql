CREATE DATABASE IF NOT EXISTS dev_raw_ecommerce;

CREATE TABLE IF NOT EXISTS dev_raw_ecommerce.customer_acquisition_channels (
    channel_id          int NOT NULL COMMENT 'unique id',
    category            string,
    channel_name        string,
    description         string,
    created_at          timestamp NOT NULL
)
USING iceberg;
-- PARTITIONED BY (days(created_at))

CREATE TABLE IF NOT EXISTS dev_raw_ecommerce.customers (
    customer_id                 bigint NOT NULL COMMENT 'unique id',
    name                        string,
    gender                      string,
    email                       string,
    phone                       string,
    country                     string,
    registration_date           timestamp NOT NULL,
    acquisition_channel_id      int
)
USING iceberg
PARTITIONED BY (days(registration_date));

CREATE TABLE IF NOT EXISTS dev_raw_ecommerce.inventory (
    inventory_id            bigint NOT NULL COMMENT 'unique id',
    product_id              bigint NOT NULL,
    quantity                int,
    warehouse_location      string,
    created_at              timestamp NOT NULL,
    updated_at              timestamp
)
USING iceberg
PARTITIONED BY (days(created_at));

CREATE TABLE IF NOT EXISTS dev_raw_ecommerce.order_items (
    order_item_id           bigint NOT NULL COMMENT 'unique id',
    order_id                bigint NOT NULL,
    product_id              bigint NOT NULL,
    quantity                int,
    price                   decimal(16,2),
    created_at              timestamp NOT NULL
)
USING iceberg
PARTITIONED BY (days(created_at));

CREATE TABLE IF NOT EXISTS dev_raw_ecommerce.orders (
    order_id                bigint NOT NULL COMMENT 'unique id',
    customer_id             bigint NOT NULL,
    order_date              timestamp,
    order_status            string,
    total_amount            decimal(10,2),
    payment_method          string,
    created_at              timestamp NOT NULL
)
USING iceberg
PARTITIONED BY (days(created_at));

CREATE TABLE IF NOT EXISTS dev_raw_ecommerce.product_categories (
    category_id             int NOT NULL COMMENT 'unique id',
    category_name           string,
    parent_category_id      int,
    created_at              timestamp NOT NULL
)
USING iceberg
PARTITIONED BY (days(created_at));

CREATE TABLE IF NOT EXISTS dev_raw_ecommerce.products (
    product_id              bigint NOT NULL COMMENT 'unique id',
    name                    string,
    description             string,
    price                   decimal(10,2),
    category_id             int NOT NULL,
    created_at              timestamp NOT NULL,
    updated_at              timestamp
)
USING iceberg
PARTITIONED BY (days(created_at));

-- CREATE TABLE IF NOT EXISTS dev_raw_ecommerce.xxx (

--     created_at                  timestamp NOT NULL
-- )
-- USING iceberg
-- PARTITIONED BY (days(created_at));

CREATE DATABASE IF NOT EXISTS dev_curated_ecommerce;
CREATE TABLE IF NOT EXISTS dev_curated_ecommerce.customers (
    customer_id                 bigint NOT NULL COMMENT 'unique id',
    name                        string,
    first_name                  string,
    last_name                   string,
    gender                      string,
    email                       string,
    phone                       string,
    country                     string,
    registration_date           timestamp NOT NULL,
    acquisition_channel_id      int
)
USING iceberg
PARTITIONED BY (days(registration_date));

CREATE DATABASE IF NOT EXISTS dev_star_ecommerce;
CREATE TABLE IF NOT EXISTS dev_star_ecommerce.dim_customers (
    customer_id                 bigint NOT NULL COMMENT 'unique id',
    name                        string,
    first_name                  string,
    last_name                   string,
    gender                      string,
    email                       string,
    phone                       string,
    country                     string,
    registration_date           timestamp NOT NULL,
    -- acquisition_channel_id      int
    channel_id                  int,
    category                    string,
    channel_name                string,
    description                 string,
    acq_channel_created_at      timestamp,
    created_at                  timestamp NOT NULL

)
USING iceberg
PARTITIONED BY (days(registration_date));
CREATE DATABASE IF NOT EXISTS dev_analytic_ecommerce;