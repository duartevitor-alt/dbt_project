--partition 2020 by order_date
SELECT *
FROM {{ref('joins')}}
WHERE DATE_PART(YEAR, order_date) = 2020