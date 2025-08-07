/*Запрос создает витрину для модели атрибуции Last Paid Click*/
WITH sl AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        l.lead_id,
        l.created_at,
        l.amount,
        l.closing_reason,
        l.status_id
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON s.visitor_id = l.visitor_id
    WHERE s.medium != 'organic'
),
rnkd AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY visitor_id
            ORDER BY visit_date DESC
        ) AS rn
    FROM sl
)
SELECT
    r.visitor_id,
    r.visit_date,
    r.utm_source,
    r.utm_medium,
    r.utm_campaign,
    r.lead_id,
    r.created_at,
    r.amount,
    r.closing_reason,
    r.status_id
FROM rnkd AS r
WHERE r.rn = 1
ORDER BY
    r.amount DESC NULLS LAST,
    r.visit_date ASC NULLS LAST,
    r.utm_source ASC NULLS LAST,
    r.utm_medium ASC NULLS LAST,
    r.utm_campaign ASC NULLS LAST
LIMIT 10;