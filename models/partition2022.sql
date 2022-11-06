--partition 2022 by order_date
SELECT *
FROM {{ref('joins')}}
WHERE DATE_PART(YEAR, order_date) = 2022