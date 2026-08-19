SELECT
    city,
    HOUR(order_timestamp) AS order_hour,
    count_if(is_delivered) AS delivered_orders,
    ROUND(
        MEDIAN(
            CASE
                WHEN is_delivered THEN delivery_time_min
            END
        ),
        1
    ) AS p50_delivery_min,
    ROUND(PERCENTILE_CONT(0.9) within GROUP (
ORDER BY
    delivery_time_min), 1) AS p90_delivery_min,
    ROUND(
        div0(
            count_if(
                is_delivered
                AND delivery_time_min > 45
            ),
            count_if(is_delivered)
        ),
        4
    ) AS late_rate
FROM
    {{ ref('fct_orders') }}
WHERE
    is_delivered
GROUP BY
    1,
    2
