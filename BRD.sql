use bharat_herald;

----Creating Tables

CREATE TABLE ad_revenue_new (
    id INT AUTO_INCREMENT PRIMARY KEY,
    edition_id VARCHAR(20) NOT NULL,
    ad_category VARCHAR(10) NOT NULL,
    ad_revenue DECIMAL(18,2),
    currency VARCHAR(10),
    comments TEXT,
    final_revenue_inr DECIMAL(18,2),
    quarter VARCHAR(5),
    year INT
);


CREATE INDEX idx_ar_edition   ON ad_revenue_new(edition_id);
CREATE INDEX idx_ar_category  ON ad_revenue_new(ad_category);
CREATE INDEX idx_ar_time      ON ad_revenue_new(year, quarter);


CREATE TABLE city_readiness_new (
   id INT AUTO_INCREMENT PRIMARY KEY,
    city_id VARCHAR(20) NOT NULL,
    year INT,
    quater VARCHAR(5), -- kept your spelling
    literacy_rate DECIMAL(5,2),
    smartphone_penetration DECIMAL(5,2),
    internet_penetration DECIMAL(5,2)
);

CREATE INDEX idx_cr_city_time ON city_readiness_new(city_id, year, quater);

CREATE TABLE digital_pilot_new (
	id INT AUTO_INCREMENT PRIMARY KEY,
    platform VARCHAR(50),
    launch_month VARCHAR(20),
    ad_category VARCHAR(10),
    dev_cost DECIMAL(18,2),
    marketing_cost DECIMAL(18,2),
    users_reached BIGINT,
    downloads_or_accesses BIGINT,
    avg_bounce_rate DECIMAL(5,2),
    Feedback TEXT,
    city_id VARCHAR(20),
    Year INT,
    quarter VARCHAR(10),
    main VARCHAR(50)
);

CREATE INDEX idx_dp_category   ON digital_pilot_new(ad_category);
CREATE INDEX idx_dp_city_time  ON digital_pilot_new(city_id, Year, quarter);


CREATE TABLE city_new (
    city_id VARCHAR(20) PRIMARY KEY,
    city VARCHAR(100),
    state VARCHAR(100),
    tier VARCHAR(20)
);

CREATE TABLE print_circulation_new (
    id INT AUTO_INCREMENT PRIMARY KEY,
    edition_ID VARCHAR(20),
    City_ID VARCHAR(20),
    State VARCHAR(100),
    copies_wastage INT,
    copies_sold INT,
    Copies_print INT,
    Year INT,
    quater VARCHAR(10)
);

CREATE INDEX idx_pc_edition   ON print_circulation_new(edition_ID);
CREATE INDEX idx_pc_city_time ON print_circulation_new(City_ID, Year, quater);

CREATE TABLE ad_category_new (
    ad_category_id VARCHAR(10) PRIMARY KEY,
    standard_ad_category VARCHAR(100),
    category_group VARCHAR(100),
    example_brands VARCHAR(255)
);



--- Buisness Request 1 : Quaterly Circulation Drop Check 

WITH cleaned AS (
SELECT c.city AS city_name,
pc.Year AS year, UPPER(pc.quater) AS quater, pc.copies_sold,
CASE UPPER(pc.quater)
WHEN 'Q1' THEN 1
WHEN 'Q2' THEN 2
WHEN 'Q3' THEN 3
WHEN 'Q4' THEN 4
END AS q_num
FROM print_circulation_new pc
JOIN city_new c ON pc.City_ID = c.city_id
WHERE pc.Year BETWEEN 2019 AND 2024
),
base AS (
SELECT city_name, year, quater, q_num,
SUM(copies_sold) AS net_circulation
FROM cleaned
GROUP BY city_name, year, quater, q_num
),
with_lag AS (
SELECT city_name, year, quater, net_circulation,
LAG(net_circulation) OVER (
PARTITION BY city_name
ORDER BY year, q_num
) AS prev_net
FROM base
)
SELECT city_name, year, quater, net_circulation,
(prev_net - net_circulation) AS drop_amount_from_previous_quater
FROM with_lag
WHERE prev_net IS NOT NULL
AND net_circulation < prev_net
ORDER BY drop_amount_from_previous_quater DESC
LIMIT 3;


--- Buisness Request 2 :  Yearly Revenue Concentration by Category

WITH category_yearly AS (
SELECT ar.year, ac.standard_ad_category AS category_name,
SUM(ar.final_revenue_inr) AS category_revenue
FROM ad_revenue_new ar
JOIN ad_category_new ac ON ar.ad_category = ac.ad_category_id
GROUP BY ar.year, ac.standard_ad_category
),
year_totals AS (
SELECT year, SUM(category_revenue) AS total_revenue_year
FROM category_yearly
GROUP BY year
)
SELECT cy.year, cy.category_name, cy.category_revenue, yt.total_revenue_year,
ROUND((cy.category_revenue / yt.total_revenue_year) * 100, 2) AS pct_of_year_total
FROM category_yearly cy
JOIN year_totals yt ON cy.year = yt.year
ORDER BY pct_of_year_total DESC;


--- Buisness Request 3 :  2024 Print Efficiency Leaderboard

WITH city_efficiency AS (
SELECT c.city AS city_name,
SUM(pc.Copies_print) AS copies_printed_2024,
SUM(pc.Copies_print - pc.Copies_wastage) AS net_circulation_2024,
SUM(pc.Copies_print - pc.Copies_wastage) / SUM(pc.Copies_print) AS efficiency_ratio
FROM print_circulation_new pc
JOIN city_new c ON pc.City_ID = c.city_id
WHERE pc.Year = 2024
GROUP BY c.city
)
SELECT city_name, copies_printed_2024, net_circulation_2024,
ROUND(efficiency_ratio, 4) AS efficiency_ratio,
RANK() OVER (ORDER BY efficiency_ratio DESC) AS efficiency_rank_2024
FROM city_efficiency
ORDER BY efficiency_rank_2024
LIMIT 5;


--- Buisness Request 4 :   Internet Readiness Growth (2021)

SELECT c.city AS city_name, cr.year,
ROUND(AVG(CASE WHEN cr.quater = 'q1' THEN cr.internet_penetration END), 2) AS internet_penetration_rate_q1,
ROUND(AVG(CASE WHEN cr.quater = 'q4' THEN cr.internet_penetration END), 2) AS internet_penetration_rate_q4,
ROUND((AVG(CASE WHEN cr.quater = 'q4' THEN cr.internet_penetration END) - 
AVG(CASE WHEN cr.quater = 'q1' THEN cr.internet_penetration END)), 2
) AS delta_internet_penetration_rate
FROM city_readiness_new cr
JOIN city_new c ON cr.city_id = c.city_id
WHERE cr.year = 2019 AND cr.quater IN ('q1','q4')
GROUP BY c.city, cr.year
ORDER BY delta_internet_penetration_rate DESC;


--- Buisness Request 5 :   Consistent Multi-Year Decline (2019→2024) 

WITH
net AS (
SELECT pc.City_ID AS city_id, pc.Year  AS year,
SUM(pc.copies_sold) AS yearly_net_circulation
FROM print_circulation_new pc
WHERE pc.Year IN (2019, 2024)
GROUP BY pc.City_ID, pc.Year
),
edition_city AS (
SELECT DISTINCT edition_ID, City_ID AS city_id, Year AS year
FROM print_circulation_new
WHERE Year IN (2019, 2024)
),
ad_by_city AS (
SELECT ec.city_id, ar.year AS year,
SUM(ar.final_revenue_inr) AS yearly_ad_revenue
FROM ad_revenue_new ar
JOIN edition_city ec ON ar.edition_id = ec.edition_id
AND ar.year = ec.year
WHERE ar.year IN (2019, 2024)
GROUP BY ec.city_id, ar.year
),
combined AS (
SELECT cy.city_id, cy.year,
COALESCE(n.yearly_net_circulation, 0) AS yearly_net_circulation,
COALESCE(a.yearly_ad_revenue, 0) AS yearly_ad_revenue
FROM (SELECT city_id, year FROM net
UNION
SELECT city_id, year FROM ad_by_city) cy
LEFT JOIN net n ON cy.city_id = n.city_id AND cy.year = n.year
LEFT JOIN ad_by_city a ON cy.city_id = a.city_id AND cy.year = a.year
),
pivoted AS (
SELECT c.city AS city_name,
MAX(CASE WHEN year = 2019 THEN yearly_net_circulation END) AS net_2019,
MAX(CASE WHEN year = 2024 THEN yearly_net_circulation END) AS net_2024,
MAX(CASE WHEN year = 2019 THEN yearly_ad_revenue END)      AS ad_2019,
MAX(CASE WHEN year = 2024 THEN yearly_ad_revenue END)      AS ad_2024
FROM combined co
JOIN city_new c ON co.city_id = c.city_id
GROUP BY c.city
)
SELECT city_name,
net_2019 AS circulation_2019,
net_2024 AS circulation_2024,
(net_2024 - net_2019) AS circulation_changed,
ad_2019  AS ad_revenue_2019,
ad_2024  AS ad_revenue_2024,
(ad_2024 - ad_2019) AS revenue_changed,
CASE 
WHEN (net_2024 - net_2019) < 0 THEN 'YES'
ELSE 'NO'
END AS declining_print,
CASE 
WHEN (ad_2024 - ad_2019) < 0 THEN 'YES'
ELSE 'NO'
END AS declining_adrevenue,
CASE 
WHEN (net_2024 - net_2019) < 0 AND (ad_2024 - ad_2019) < 0 THEN 'YES'
ELSE 'NO'
END AS declining_both

FROM pivoted
ORDER BY revenue_changed DESC;


--- Buisness Request 6 :   2021 Readiness vs Pilot Engagement Outlier 

WITH
readiness AS (
SELECT cr.city_id,
ROUND(AVG((cr.smartphone_penetration + cr.internet_penetration + cr.literacy_rate) / 3.0), 2) AS readiness_score_2021
FROM city_readiness_new cr
WHERE cr.year = 2021
GROUP BY cr.city_id
),
engagement AS (
SELECT dp.city_id,
ROUND(SUM(dp.downloads_or_accesses) * 100.0 / NULLIF(SUM(dp.users_reached), 0), 2) AS engagement_rate
FROM digital_pilot_new dp
GROUP BY dp.city_id
),
combined AS (
SELECT c.city, r.readiness_score_2021, e.engagement_rate
FROM readiness r
JOIN engagement e ON r.city_id = e.city_id
JOIN city_new c ON r.city_id = c.city_id
),
ranked AS (
SELECT city, readiness_score_2021, engagement_rate,
RANK() OVER (ORDER BY readiness_score_2021 DESC) AS readiness_rank_desc,
RANK() OVER (ORDER BY engagement_rate ASC) AS engagement_rank_asc
FROM combined
)
SELECT city AS city_name,
readiness_score_2021,
engagement_rate,
readiness_rank_desc,
engagement_rank_asc,
CASE
WHEN readiness_rank_desc = 1 AND engagement_rank_asc <= 3 THEN 'YES'
ELSE 'NO'
END AS is_outlier
FROM ranked
ORDER BY readiness_rank_desc, engagement_rank_asc;



