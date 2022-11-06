WITH order_details AS (
    SELECT  
        order_id
    ,   product_id
    ,   unit_price
    ,   quantity
    FROM {{source('sources', 'order_details')}}
), products AS (
    SELECT 
        unit_price
    ,   product_id 
    ,   product_name
    ,   supplier_id
    ,   category_id  
    FROM {{source('sources', 'products')}}
)
SELECT  
    od.order_id
,   od.product_id
,   od.unit_price
,   od.quantity
,   p.product_name
,   p.supplier_id
,   p.category_id  
,   ROUND(od.quantity * od.unit_price , 2)         AS Total
,   ROUND(od.quantity * p.unit_price , 2) - Total  AS DiscountExerc
FROM order_details od
LEFT JOIN products p ON p.product_id = od.product_id