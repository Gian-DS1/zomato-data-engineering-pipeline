{{ config(
    tags = ['ai']
) }}

SELECT
    rr.city,
    e.topic,
    e.sentiment_label,
    COUNT(*) AS reviews,
    ROUND(AVG(e.sentiment_score), 3) AS avg_sentiment_score,
    ROUND(AVG(rr.rating), 2) AS avg_star_rating,
    count_if(
        e.key_issue IS NOT NULL
    ) AS flagged_issues
FROM
    {{ source(
        'ai',
        'review_enriched'
    ) }}
    e
    INNER JOIN {{ ref('stg_reviews') }}
    rr USING (review_id)
GROUP BY
    1,
    2,
    3
