
-- Casts the columns in the orders_table table to appropriate data types

ALTER TABLE orders_table
    ALTER COLUMN date_uuid TYPE uuid USING date_uuid::uuid;

ALTER TABLE orders_table
    ALTER COLUMN user_uuid TYPE uuid USING user_uuid::uuid;

ALTER TABLE orders_table
    ALTER COLUMN card_number TYPE VARCHAR(19);

ALTER TABLE orders_table
    ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE orders_table
    ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE orders_table
    ALTER COLUMN product_quantity TYPE SMALLINT;

-- Casts the columns in the dim_users table to the appropriate data types

ALTER TABLE dim_users
    ALTER COLUMN first_name TYPE VARCHAR(255);

ALTER TABLE dim_users
    ALTER COLUMN last_name TYPE VARCHAR(255);

ALTER TABLE dim_users
    ALTER COLUMN date_of_birth TYPE DATE;

ALTER TABLE dim_users
    ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_users
    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;

ALTER TABLE dim_users
    ALTER COLUMN join_date TYPE DATE;

-- Casts columns in the dim_store_details table to the correct datatypes and changes the location values for the row pertaining to the web store to NULL

UPDATE dim_store_details
SET address = NULL, longitude = NULL, locality = NULL
WHERE store_type = 'Web Portal';

ALTER TABLE dim_store_details
    ALTER COLUMN longitude TYPE FLOAT USING longitude::FLOAT;

ALTER TABLE dim_store_details
    ALTER COLUMN locality TYPE VARCHAR(255);

ALTER TABLE dim_store_details
    ALTER COLUMN store_code TYPE VARCHAR(12);

ALTER TABLE dim_store_details
    ALTER COLUMN staff_numbers TYPE SMALLINT;

ALTER TABLE dim_store_details
    ALTER COLUMN opening_date TYPE DATE;

ALTER TABLE dim_store_details
    ALTER COLUMN store_type TYPE VARCHAR(255);

ALTER TABLE dim_store_details
    ALTER COLUMN store_type DROP NOT NULL;

ALTER TABLE dim_store_details
    ALTER COLUMN latitude TYPE FLOAT USING latitude::FLOAT;

ALTER TABLE dim_store_details
    ALTER COLUMN country_code TYPE VARCHAR(2);

ALTER TABLE dim_store_details
    ALTER COLUMN continent TYPE VARCHAR(255);

-- Cast columns in the dim_products table to appropriate data types and make additional changes

-- Remove the '£' symbol from prices 

-- Create a new column 'weight-class' which gives each product a weight category. The weight categories are: Light, Mid-Sized, Heavy, Truck-Required. 

UPDATE dim_products 
SET product_price = REPLACE(product_price, '£', '');

ALTER TABLE dim_products
    ADD COLUMN IF NOT EXISTS weight_class VARCHAR(14);

UPDATE dim_products
SET weight_class = (
CASE WHEN 
    weight < 2 THEN 'Light'
WHEN 
    weight >= 2 AND weight < 40 THEN 'Mid_Sized'
WHEN 
    weight >= 40 AND weight < 140 THEN 'Heavy'
ELSE 'Truck_Required'
END);   

ALTER TABLE dim_products
    ALTER COLUMN product_price TYPE FLOAT USING product_price::FLOAT;

ALTER TABLE dim_products
    ALTER COLUMN weight TYPE FLOAT USING weight::FLOAT;

ALTER TABLE dim_products
    ALTER COLUMN "EAN" TYPE VARCHAR(17);

ALTER TABLE dim_products
    ALTER COLUMN product_code TYPE VARCHAR(11);

ALTER TABLE dim_products
    ALTER COLUMN date_added TYPE DATE;

ALTER TABLE dim_products
    ALTER COLUMN uuid TYPE UUID USING uuid::uuid;

ALTER TABLE dim_products
    RENAME COLUMN removed TO still_available;

UPDATE dim_products
    SET still_available = (
        CASE WHEN still_available = 'Still_avaliable' THEN True
        ELSE FALSE
        END
    );

ALTER TABLE dim_products   
    ALTER COLUMN still_available TYPE BOOL USING still_available::BOOL;

-- Casts columns in the dim_date_time table to the appropriate data types

ALTER TABLE dim_date_times
    ALTER COLUMN month TYPE VARCHAR(2);

ALTER TABLE dim_date_times
    ALTER COLUMN year TYPE VARCHAR(4);

ALTER TABLE dim_date_times
    ALTER COLUMN day TYPE VARCHAR(2);

ALTER TABLE dim_date_times
    ALTER COLUMN time_period TYPE VARCHAR(10);

ALTER TABLE dim_date_times
    ALTER COLUMN date_uuid TYPE UUID USING date_uuid::uuid;

-- Casts columns in the dim_card_details table to the appropriate data types

ALTER TABLE dim_card_details
    ALTER COLUMN card_number TYPE VARCHAR(22);

ALTER TABLE dim_card_details
    ALTER COLUMN expiry_date TYPE VARCHAR(7);

ALTER TABLE dim_card_details
    ALTER COLUMN date_payment_confirmed TYPE DATE;

-- Sets a primary key in the dim tables that corresponds to the orders table

ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid)

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number)

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code)

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code)

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid)

