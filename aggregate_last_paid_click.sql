WITH leads_ranked AS (
    SELECT
        l.lead_id,
        l.closing_reason,
        l.status_id,
        l.amount,
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        ROW_NUMBER() OVER (
            PARTITION BY l.lead_id
            ORDER BY s.visit_date ASC
        ) AS rn
    FROM leads AS l
    JOIN sessions AS s
        ON l.visitor_id = s.visitor_id
    WHERE s.visit_date < l.created_at
),

leads_best AS (
    SELECT *
    FROM leads_ranked
    WHERE rn = 1
),

leads_joined AS (
    SELECT
        s.visitor_id,
        s.visit_date,
        s.source AS utm_source,
        s.medium AS utm_medium,
        s.campaign AS utm_campaign,
        lb.lead_id,
        lb.closing_reason,
        lb.status_id,
        lb.amount
    FROM sessions AS s
    LEFT JOIN leads_best AS lb
        ON
            s.visit_date = lb.visit_date
            AND s.source = lb.utm_source
            AND s.medium = lb.utm_medium
            AND s.campaign = lb.utm_campaign
),

vk_ya_costs AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        CAST(DATE_TRUNC('day', campaign_date) AS date) AS visit_date,
        SUM(daily_spent) AS cost
    FROM (
        SELECT
            utm_source,
            utm_medium,
            utm_campaign,
            campaign_date,
            daily_spent
        FROM vk_ads
        UNION ALL
        SELECT
            utm_source,
            utm_medium,
            utm_campaign,
            campaign_date,
            daily_spent
        FROM ya_ads
    ) AS tab
    GROUP BY 1, 2, 3, 4
)

SELECT
    CAST(DATE_TRUNC('day', lj.visit_date) AS date) AS visit_date,
    lj.utm_source,
    lj.utm_medium,
    lj.utm_campaign,
    COUNT(lj.visitor_id) AS visitors_count,
    SUM(vyc.cost) AS total_cost,
    COUNT(lj.lead_id) AS leads_count,
    COUNT(lj.lead_id) FILTER (
        WHERE
        lj.closing_reason = 'Успешная продажа'
        OR lj.status_id = 142
    ) AS purchases_count,
    COALESCE(SUM(lj.amount) FILTER (WHERE
        lj.closing_reason = 'Успешная продажа'
        OR lj.status_id = 142), 0
    ) AS revenue
FROM leads_joined AS lj
LEFT JOIN vk_ya_costs AS vyc
    ON
        CAST(DATE_TRUNC('day', lj.visit_date) AS date) = vyc.visit_date
        AND lj.utm_source = vyc.utm_source
        AND lj.utm_medium = vyc.utm_medium
        AND lj.utm_campaign = vyc.utm_campaign
GROUP BY
    CAST(DATE_TRUNC('day', lj.visit_date) AS date),
    lj.utm_source,
    lj.utm_medium,
    lj.utm_campaign
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    lj.utm_source ASC,
    lj.utm_medium ASC,
    lj.utm_campaign ASC
LIMIT 15;
