SELECT *
FROM {{ref('joins')}}
WHERE DATE_PART(YEAR, order_date) = 2022