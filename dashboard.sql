--Запрос, который вычисляет конверсию из клика в лид
WITH vi AS (
    SELECT
        CAST(visit_date AS date) AS visit_date,
        COUNT(*) AS visits_count
    FROM sessions
    GROUP BY CAST(visit_date AS date)
    ORDER BY visit_date ASC
),

le AS (
    SELECT
        CAST(created_at AS date) AS leads_created_at,
        COUNT(*) AS leads_count
    FROM leads
    GROUP BY CAST(created_at AS date)
    ORDER BY leads_created_at ASC
),

resulted AS (
    SELECT
        vi.visit_date,
        vi.visits_count,
        le.leads_count
    FROM vi
    LEFT JOIN le
        ON vi.visit_date = le.leads_created_at
)

SELECT
    *,
    ROUND((1.0 * leads_count / visits_count) * 100, 2) AS cr
FROM resulted;
--Запрос, который вычисляет конверсию из лида в оплату
SELECT
    CAST(created_at AS date) AS visit_date,
    COUNT(lead_id) AS leads_count,
    COUNT(lead_id) FILTER (
        WHERE closing_reason = 'Успешная продажа'
    ) AS purchases_count,
    ROUND(
        (
            1.0 * (COUNT(lead_id) FILTER (
                WHERE closing_reason = 'Успешная продажа'
            )) / COUNT(lead_id)
        ) * 100,
        2
    ) AS leads_to_payment_cr
FROM leads
GROUP BY CAST(created_at AS date)
ORDER BY visit_date ASC;
--Запрос, который агрегирует данные по модели last paid click
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
    LEFT JOIN leads AS l
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
        CAST(campaign_date AS date) AS visit_date,
        SUM(daily_spent) AS costs
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
    lj.utm_source,
    lj.utm_medium,
    lj.utm_campaign,
    vyc.costs AS total_cost,
    COUNT(lj.visitor_id) AS visitors_count,
    CAST(lj.visit_date AS date) AS visit_date,
    COUNT(lj.lead_id) AS leads_count,
    COUNT(lj.lead_id) FILTER (
        WHERE
        lj.closing_reason = 'Успешная продажа'
        OR lj.status_id = 142
    ) AS purchases_count,
    COALESCE(
        SUM(lj.amount) FILTER (WHERE
        lj.closing_reason = 'Успешная продажа'
        OR lj.status_id = 142), 0
    ) AS revenue
FROM leads_joined AS lj
LEFT JOIN vk_ya_costs AS vyc
    ON
        CAST(lj.visit_date AS date) = vyc.visit_date
        AND lj.utm_source = vyc.utm_source
        AND lj.utm_medium = vyc.utm_medium
        AND lj.utm_campaign = vyc.utm_campaign
GROUP BY
    CAST(lj.visit_date AS date),
    lj.utm_source,
    lj.utm_medium,
    lj.utm_campaign,
    vyc.costs
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    lj.utm_source ASC,
    lj.utm_medium ASC,
    lj.utm_campaign ASC;
--Запрос, который расчитывает расходы по каналам привлечения в течение месяца
    SELECT
        utm_source,
        CAST(campaign_date AS date) AS visit_date,
        SUM(daily_spent) AS cost
    FROM (
        SELECT
            utm_source,
            campaign_date,
            daily_spent
        FROM vk_ads
        UNION ALL
        SELECT
            utm_source,
            campaign_date,
            daily_spent
        FROM ya_ads
    ) AS tab
    GROUP BY 1, 2
    ORDER BY 2 ASC;
--Запрос, который рассчитывает базовые маркетинговые метрики для сводной таблицы
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
    LEFT JOIN leads AS l
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
        CAST(campaign_date AS date) AS visit_date,
        SUM(daily_spent) AS costs
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
),

resulted AS (
SELECT
    ,
    lj.utm_source,
    lj.utm_medium,
    lj.utm_campaign,
    vyc.costs AS total_cost,
    COUNT(lj.visitor_id) AS visitors_count,
    CAST(lj.visit_date AS date) AS visit_date,
    COUNT(lj.lead_id) AS leads_count,
    COUNT(lj.lead_id) FILTER (
        WHERE
        lj.closing_reason = 'Успешная продажа'
        OR lj.status_id = 142
    ) AS purchases_count,
    COALESCE(
        SUM(lj.amount) FILTER (WHERE
        lj.closing_reason = 'Успешная продажа'
        OR lj.status_id = 142), 0
    ) AS revenue
FROM leads_joined AS lj
LEFT JOIN vk_ya_costs AS vyc
    ON
        CAST((lj.visit_date) AS date) = vyc.visit_date
        AND lj.utm_source = vyc.utm_source
        AND lj.utm_medium = vyc.utm_medium
        AND lj.utm_campaign = vyc.utm_campaign
GROUP BY
    CAST(lj.visit_date AS date),
    lj.utm_source,
    lj.utm_medium,
    lj.utm_campaign,
    vyc.costs
HAVING COUNT(lj.visitor_id) != 0 AND COUNT(lj.lead_id) != 0
    AND COUNT(lj.lead_id) FILTER (
        WHERE
        lj.closing_reason = 'Успешная продажа'
        OR lj.status_id = 142
    ) != 0
ORDER BY
    revenue DESC NULLS LAST,
    visit_date ASC,
    visitors_count DESC,
    lj.utm_source ASC,
    lj.utm_medium ASC,
    lj.utm_campaign ASC
)

SELECT
    r.utm_source,
    r.utm_medium,
    r.utm_campaign,
    CASE 
        WHEN r.total_cost = 0 THEN NULL 
        ELSE ROUND((r.revenue - r.total_cost)/r.total_cost * 100, 2) 
    END AS roi,
    CASE 
        WHEN r.visitors_count = 0 THEN NULL 
        ELSE ROUND(r.total_cost/r.visitors_count, 2) 
    END AS cpu,
    CASE 
        WHEN r.leads_count = 0 THEN NULL 
        ELSE ROUND(r.total_cost/r.leads_count, 2) 
    END AS cpl,
    CASE 
        WHEN r.purchases_count = 0 THEN NULL 
        ELSE ROUND(r.total_cost/r.purchases_count, 2) 
    END AS cppu,
    CASE 
        WHEN r.leads_count = 0 THEN NULL 
        ELSE ROUND(r.revenue/r.leads_count, 2) 
    END AS rpl,
    CASE 
        WHEN r.total_cost = 0 THEN NULL 
        ELSE ROUND(r.purchases_count/r.total_cost, 2) 
    END AS cpo,
    CASE 
        WHEN r.purchases_count = 0 THEN NULL 
        ELSE ROUND(r.revenue/r.purchases_count, 2) 
    END AS aov
FROM resulted r;
--Запрос, который рассчитывает корреляцию между расходами на рекламу 
--в день и количеством органических пользователей через 7 дней
WITH organic_daily AS (
    SELECT
        CAST(visit_date AS DATE) AS visit_day,
        COUNT(DISTINCT visitor_id) AS organic_users
    FROM sessions
    WHERE source NOT IN ('yandex', 'vk')
    GROUP BY CAST(visit_date AS date)
),

ads_daily AS (
    SELECT
        campaign_date AS spend_day,
        SUM(daily_spent) AS total_spent
    FROM (
        SELECT campaign_date, daily_spent FROM vk_ads
        UNION ALL
        SELECT campaign_date, daily_spent FROM ya_ads
    ) t
    GROUP BY campaign_date
),

combined AS (
    SELECT
        o.visit_day,
        o.organic_users,
        COALESCE(a.total_spent, 0) AS ad_spent
    FROM organic_daily o
    LEFT JOIN ads_daily a
        ON o.visit_day = a.spend_day
),

ld_tab AS (
    SELECT 
        c.visit_day,
        c.organic_users,
        c.ad_spent,
        LEAD(c.organic_users, 1) OVER (ORDER BY c.visit_day) AS ld_1,
        LEAD(c.organic_users, 2) OVER (ORDER BY c.visit_day) AS ld_2,
        LEAD(c.organic_users, 3) OVER (ORDER BY c.visit_day) AS ld_3,
        LEAD(c.organic_users, 4) OVER (ORDER BY c.visit_day) AS ld_4,
        LEAD(c.organic_users, 5) OVER (ORDER BY c.visit_day) AS ld_5,
        LEAD(c.organic_users, 6) OVER (ORDER BY c.visit_day) AS ld_6,
        LEAD(c.organic_users, 7) OVER (ORDER BY c.visit_day) AS ld_7
    FROM combined AS c
),

corr_1 AS (
    SELECT 1 AS lag_day,
        CORR(ad_spent, ld_1) AS correlation
    FROM ld_tab
    WHERE ld_1 IS NOT NULL
),

corr_2 AS (
    SELECT 2 AS lag_day,
        CORR(ad_spent, ld_2) AS correlation
    FROM ld_tab
    WHERE ld_2 IS NOT NULL
),
corr_3 AS (
    SELECT 3 AS lag_day,
        CORR(ad_spent, ld_3) AS correlation
    FROM ld_tab
    WHERE ld_3 IS NOT NULL
),
corr_4 AS (
    SELECT 4 AS lag_day,
        CORR(ad_spent, ld_4) AS correlation
    FROM ld_tab
    WHERE ld_4 IS NOT NULL
),
corr_5 AS (
    SELECT 5 AS lag_day,
       CORR(ad_spent, ld_5) AS correlation
    FROM ld_tab
    WHERE ld_5 IS NOT NULL
),
corr_6 AS (
    SELECT 6 AS lag_day,
       CORR(ad_spent, ld_6) AS correlation
    FROM ld_tab
    WHERE ld_6 IS NOT NULL
),
corr_7 AS (
    SELECT 7 AS lag_day,
       CORR(ad_spent, ld_7) AS correlation
    FROM ld_tab
    WHERE ld_7 IS NOT NULL
)
SELECT 1 AS lag_day, CORR(ad_spent, ld_1) AS correlation
FROM ld_tab WHERE ld_1 IS NOT NULL
UNION ALL
SELECT 2 AS lag_day, CORR(ad_spent, ld_2)
FROM ld_tab WHERE ld_2 IS NOT NULL
UNION ALL
SELECT 3 AS lag_day, CORR(ad_spent, ld_3)
FROM ld_tab WHERE ld_3 IS NOT NULL
UNION ALL
SELECT 4 AS lag_day, CORR(ad_spent, ld_4)
FROM ld_tab WHERE ld_4 IS NOT NULL
UNION ALL
SELECT 5 AS lag_day, CORR(ad_spent, ld_5)
FROM ld_tab WHERE ld_5 IS NOT NULL
UNION ALL
SELECT 6 AS lag_day, CORR(ad_spent, ld_6)
FROM ld_tab WHERE ld_6 IS NOT NULL
UNION ALL
SELECT 7 AS lag_day, CORR(ad_spent, ld_7)
FROM ld_tab WHERE ld_7 IS NOT NULL
ORDER BY lag_day;
--Запрос, который рассчитывает количество привлеченных 
--каналами лидов по форматам обучения
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
        l.learning_format
    FROM sessions AS s
    LEFT JOIN leads AS l
        ON s.visitor_id = l.visitor_id AND s.visit_date <= l.created_at
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
),

resulted AS (
    SELECT
        r.visitor_id,
        r.visit_date,
        r.utm_source,
        r.utm_medium,
        r.utm_campaign,
        r.lead_id,
        r.created_at,
        r.amount,
        r.learning_format
    FROM rnkd AS r
    WHERE r.rn = 1
    ORDER BY
        r.amount DESC NULLS LAST,
        r.visit_date ASC NULLS LAST,
        r.utm_source ASC NULLS LAST,
        r.utm_medium ASC NULLS LAST,
        r.utm_campaign ASC NULLS LAST
)

SELECT
    r.utm_source,
    COUNT(r.lead_id) FILTER (WHERE r.learning_format = 'group') AS group_format,
    COUNT(r.lead_id) FILTER (WHERE r.learning_format = 'base') AS base_format,
    COUNT(r.lead_id) FILTER (WHERE r.learning_format = 'premium') AS premium_format,
    COUNT(r.lead_id) FILTER (WHERE r.learning_format = 'bootcamp') AS bootcamp_format
FROM resulted AS r
GROUP BY 1;