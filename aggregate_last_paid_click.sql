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
            ORDER BY ABS(EXTRACT(EPOCH FROM (l.created_at - s.visit_date))) ASC
        ) AS rn
    FROM leads AS l
    JOIN sessions AS s
        ON l.visitor_id = s.visitor_id
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
vk_costs AS (
    SELECT
        va.utm_source,
        va.utm_medium,
        va.utm_campaign,
        CAST(DATE_TRUNC('day', va.campaign_date) AS date) AS visit_date,
        SUM(va.daily_spent) AS vk_cost
    FROM vk_ads AS va
    GROUP BY 1, 2, 3, 4
),
ya_costs AS (
    SELECT
        ya.utm_source,
        ya.utm_medium,
        ya.utm_campaign,
        CAST(DATE_TRUNC('day', ya.campaign_date) AS date) AS visit_date,
        SUM(ya.daily_spent) AS ya_cost
    FROM ya_ads AS ya
    GROUP BY 1, 2, 3, 4
)
SELECT
    CAST(DATE_TRUNC('day', lj.visit_date) AS date) AS visit_date,
    lj.utm_source,
    lj.utm_medium,
    lj.utm_campaign,
    COUNT(lj.visitor_id) AS visitors_count,
    COALESCE(vc.vk_cost, 0::bigint)
    + COALESCE(yc.ya_cost, 0::bigint) AS total_cost,
    COUNT(lj.lead_id) AS leads_count,
    COUNT(lj.lead_id) FILTER (WHERE lj.closing_reason = 'Успешная продажа'
        OR lj.status_id = 142) AS purchases_count,
    COALESCE(SUM(lj.amount) FILTER (WHERE lj.closing_reason = 'Успешная продажа'
        OR status_id = 142),0) AS revenue
FROM leads_joined AS lj
LEFT JOIN vk_costs AS vc
    ON 
        CAST(DATE_TRUNC('day', lj.visit_date) AS DATE) = vc.visit_date
        AND lj.utm_source = vc.utm_source
        AND lj.utm_medium = vc.utm_medium
        AND lj.utm_campaign = vc.utm_campaign
LEFT JOIN ya_costs AS yc
    ON
        CAST(DATE_TRUNC('day', lj.visit_date) AS date) = yc.visit_date
        AND lj.utm_source = yc.utm_source
        AND lj.utm_medium = yc.utm_medium
        AND lj.utm_campaign = yc.utm_campaign
GROUP BY
    CAST(DATE_TRUNC('day', lj.visit_date) AS date),
    lj.utm_source,
    lj.utm_medium,
    lj.utm_campaign,
    vc.vk_cost,
    yc.ya_cost
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    lj.utm_source ASC,
    lj.utm_medium ASC,
    lj.utm_campaign ASC
LIMIT 15;
