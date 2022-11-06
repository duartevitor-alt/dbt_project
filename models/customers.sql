/*
WITH CTE_ONE AS (
    SELECT 
        customer_id
    ,   company_name
    ,   contact_name
    ,   contact_title
    ,   address
    ,   city
    ,   region
    ,   postal_code
    ,   country
    ,   phone
    ,   ROW_NUMBER() OVER (PARTITION BY company_name, contact_name ORDER BY company_name) AS N_COMPNAME
    FROM {{source('sources', 'customers')}}
)
SELECT 
        customer_id
    ,   company_name
    ,   contact_name
    ,   contact_title
    ,   address
    ,   city
    ,   region
    ,   postal_code
    ,   country
    ,   phone
from CTE_ONE 
WHERE N_COMPNAME = 1
MODELO PROF*/
WITH Markup AS (
    SELECT *
    , FIRST_VALUE(customer_id) OVER (PARTITION BY company_name, contact_name 
                                        ORDER BY company_name 
                                        ROWS BETWEEN    UNBOUNDED PRECEDING AND
                                                        UNBOUNDED FOLLOWING) AS RESULT
    -- indica que iniciara na primeira linha da parition e acabara na ultima linha da partition
    FROM {{source('sources', 'customers')}}
), Removed AS (
    SELECT DISTINCT RESULT FROM Markup
)
SELECT *
FROM {{source('sources', 'customers')}} 
--INNER JOIN Removed ON a.customer_id = Removed.RESULT
WHERE customer_id IN (SELECT RESULT FROM Removed)


