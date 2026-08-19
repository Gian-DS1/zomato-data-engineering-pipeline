SELECT
     customer_id,
     customer_name,
     email,
     age,
     CASE
          WHEN age IS NULL THEN 'Unknown'
          WHEN age < 25 THEN 'Gen Z'
          WHEN age < 40 THEN 'Millennial'
          WHEN age < 55 THEN 'Gen X'
          ELSE 'Boomers'
     END AS age_segment,
     gender,
     marital_status,
     occupation,
     income_band,
     education,
     family_size
FROM
     {{ ref('stg_users') }}
