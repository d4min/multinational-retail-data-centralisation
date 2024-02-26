
-- How many stores does the business have in each country?

SELECT country_code, 
    COUNT(store_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    total_no_stores DESC;

-- Which locations currently have the most stores?

SELECT locality, 
    COUNT(store_code) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY 
    total_no_stores DESC
LIMIT 7;

-- Which months produced the largest amount of sales?

SELECT 
    SUM(product_quantity * product_price) AS total_sales, 
    month
FROM   
    orders_table
INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    month
ORDER BY 
    total_sales DESC;

-- How many sales are coming from online?

SELECT
    COUNT(date_uuid) AS number_of_sales,
    SUM(product_quantity) AS product_quantity_count,
CASE WHEN 
    store_type = 'Web Portal' THEN 'Web'
ELSE
    'Offline'
END AS 
    location
FROM
    orders_table
INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY 
    location
ORDER BY 
    number_of_sales ASC;

-- What percentage of sales come through each type of store?

SELECT
    store_type,
    ROUND(SUM(product_price * product_quantity)::numeric, 2) AS total_sales,

    ROUND((SUM(product_quantity * product_price) * 100)::numeric / (
        SELECT
            SUM(product_quantity * product_price)
        FROM 
            orders_table
        INNER JOIN
            dim_products ON orders_table.product_code = dim_products.product_code
    )::numeric, 2) AS Percentage

FROM
    orders_table
INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY 
    store_type
ORDER BY 
    total_sales DESC;

-- Which month in each year produced the highest cost of sales?

SELECT
    ROUND(SUM(product_price * product_quantity)::NUMERIC, 2) AS total_sales,
    year,
    month
FROM
    orders_table
INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY
    year,
    month
ORDER BY
    total_sales DESC
LIMIT 10;

-- What is the staff headcount?

SELECT 
    SUM(staff_numbers) AS total_staff_numbers,
    country_code
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY
    total_staff_numbers DESC;

-- Which German store type is making the most sales?

SELECT
    ROUND(SUM(product_price * product_quantity)::numeric, 2) AS total_sales,
    store_type,
    country_code
FROM 
    orders_table
INNER JOIN
    dim_products ON orders_table.product_code = dim_products.product_code
INNER JOIN
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE country_code = 'DE'
GROUP BY
    store_type,
    country_code
ORDER BY
    total_sales ASC;

-- How quickly is the company making sales?

WITH next_sale AS(
SELECT date_uuid, 
	make_date(year::int, month::int, day::int) + timestamp AS sale,
	LEAD(make_date(year::int, month::int, day::int) + timestamp)
	OVER( ORDER BY make_date(year::int, month::int, day::int) + timestamp) AS next_sale
FROM dim_date_times)

SELECT
	year, 
	AVG(next_sale - sale) AS average,
	date_part('hour', avg(next_sale - sale)) AS hour,
	date_part('minute', avg(next_sale - sale)) AS minute,
	date_part('second', avg(next_sale - sale)) AS second,
	date_part('millisecond', avg(next_sale - sale)) AS millisecond
FROM 
	next_sale
INNER JOIN 
	dim_date_times ON next_sale.date_uuid = dim_date_times.date_uuid
GROUP BY 
	year
ORDER BY 
	average DESC
LIMIT 5









