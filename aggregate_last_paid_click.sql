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
    FROM leads l
    JOIN sessions s
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
        FROM sessions s
    LEFT JOIN leads_best lb
        ON lb.visit_date = s.visit_date 
        AND lb.utm_source = s.source
        AND lb.utm_medium = s.medium
        AND lb.utm_campaign = s.campaign
),
vk_costs as (
select
cast(date_trunc('day', va.campaign_date) as date) as visit_date,
utm_source,
utm_medium,
utm_campaign,
sum(daily_spent) as vk_cost
from vk_ads as va
group by 1,2,3,4
),
ya_costs as (
select 
cast(date_trunc('day', ya.campaign_date) as date) as visit_date,
utm_source,
utm_medium,
utm_campaign,
sum(daily_spent) as ya_cost
from ya_ads as ya
group by 1,2,3,4
)
SELECT 
    cast(date_trunc('day', lj.visit_date) as date) as visit_date,
    lj.utm_source,
    lj.utm_medium,
    lj.utm_campaign,
    COUNT(lj.visitor_id) AS visitors_count, 
    coalesce(vc.vk_cost,0::bigint) + coalesce(yc.ya_cost,0::bigint) as total_cost,
    COUNT(lj.lead_id) AS leads_count,
    COUNT(lj.lead_id) filter (where lj.closing_reason = 'Успешная продажа' or status_id = 142) as purchases_count,
    coalesce(SUM(lj.amount) filter (where lj.closing_reason = 'Успешная продажа' or status_id = 142),0) as revenue
FROM leads_joined AS lj
LEFT JOIN vk_costs as vc
    on cast(date_trunc('day', lj.visit_date) as date) = vc.visit_date
    AND lj.utm_source = vc.utm_source
    AND lj.utm_medium = vc.utm_medium
    AND lj.utm_campaign = vc.utm_campaign
left JOIN ya_costs as yc
    on cast(date_trunc('day', lj.visit_date) as date) = yc.visit_date
    AND lj.utm_source = yc.utm_source
    AND lj.utm_medium = yc.utm_medium
    AND lj.utm_campaign = yc.utm_campaign
GROUP BY
    cast(date_trunc('day', lj.visit_date) as date),
    lj.utm_source,
    lj.utm_medium,
    lj.utm_campaign,
    vc.vk_cost,
    ya_cost
order by
revenue desc nulls last,
visit_date asc,
visitors_count desc,
lj.utm_source asc,
lj.utm_medium asc,
lj.utm_campaign asc
limit 15;
