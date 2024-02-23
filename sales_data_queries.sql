
-- How many stores does the business have in each country  

SELECT country_code, 
    COUNT(store_code) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    total_no_stores DESC;

-- Which locations currently have the most stores 

SELECT locality, 
    COUNT(store_code) AS total_no_stores
FROM
    dim_store_details
GROUP BY
    locality
ORDER BY 
    total_no_stores DESC
LIMIT 7;

-- Which months produced the largest amount of sales
