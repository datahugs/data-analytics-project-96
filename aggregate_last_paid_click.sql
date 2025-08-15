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
            PARTITION BY s.visitor_id
            ORDER BY s.visit_date DESC
        ) AS rn
    FROM sessions AS s
    left JOIN leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at 
    WHERE s.medium != 'organic'
),
leads_joined AS (
    SELECT *
    FROM leads_ranked
    WHERE rn = 1
),
vk_ya_costs AS (
    SELECT
        utm_source,
        utm_medium,
        utm_campaign,
        CAST((campaign_date) AS date) AS visit_date,
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
    CAST((lj.visit_date) AS date) AS visit_date,
    lj.utm_source,
    lj.utm_medium,
    lj.utm_campaign,
    COUNT(lj.visitor_id) AS visitors_count,
    vyc.cost AS total_cost,
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
        cast((lj.visit_date) AS date) = vyc.visit_date
        AND lj.utm_source = vyc.utm_source
        AND lj.utm_medium = vyc.utm_medium
        AND lj.utm_campaign = vyc.utm_campaign
GROUP BY
    CAST((lj.visit_date) AS date),
    lj.utm_source,
    lj.utm_medium,
    lj.utm_campaign,
    vyc.cost
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    lj.utm_source ASC,
    lj.utm_medium ASC,
    lj.utm_campaign ASC
LIMIT 15;
