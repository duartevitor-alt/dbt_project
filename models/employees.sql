WITH Absolute_values AS (
    SELECT 
        *
    ,   last_name || ' ' || first_name        AS Name
    FROM {{source('sources', 'employees')}}
)
SELECT 
    *
,   CASE 
    WHEN    EXTRACT ( MONTH FROM birth_date ) < EXTRACT ( MONTH FROM CURRENT_DATE )
        THEN DATEDIFF(YEAR, birth_date, GETDATE())
    WHEN    EXTRACT ( MONTH FROM birth_date ) = EXTRACT ( MONTH FROM CURRENT_DATE ) AND
            EXTRACT ( DAY FROM birth_date ) <=  EXTRACT ( DAY FROM CURRENT_DATE )
        THEN DATEDIFF(YEAR, birth_date, GETDATE())
    ELSE DATEDIFF(YEAR, birth_date, GETDATE()) - 1 END AS Age
,   CASE 
    WHEN    EXTRACT ( MONTH FROM hire_date ) < EXTRACT ( MONTH FROM CURRENT_DATE ) 
        THEN DATEDIFF(YEAR, hire_date , GETDATE())
    WHEN    EXTRACT ( MONTH FROM hire_date ) = EXTRACT ( MONTH FROM CURRENT_DATE ) AND
            EXTRACT ( DAY FROM hire_date ) <=  EXTRACT ( DAY FROM CURRENT_DATE )
        THEN DATEDIFF(YEAR, hire_date , GETDATE())
    ELSE DATEDIFF(YEAR, hire_date , GETDATE()) - 1 END AS LengService
FROM Absolute_values


