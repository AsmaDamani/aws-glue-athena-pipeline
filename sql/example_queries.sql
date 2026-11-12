-- Run in the project's Athena workgroup. Replace events_pipeline_dev_db with your database.

-- Daily revenue by country for the last 7 days (scans only 7 partitions)
SELECT event_date, country, purchases, revenue, refunds, revenue - refunds AS net_revenue
FROM events_pipeline_dev_db.daily_summary
WHERE event_date >= current_date - INTERVAL '7' DAY
ORDER BY event_date DESC, net_revenue DESC;

-- Funnel: users who viewed, added to cart and purchased on a given day
SELECT
  count(DISTINCT CASE WHEN event_type = 'view' THEN user_id END)        AS viewed,
  count(DISTINCT CASE WHEN event_type = 'add_to_cart' THEN user_id END) AS added_to_cart,
  count(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END)    AS purchased
FROM events_pipeline_dev_db.events
WHERE event_date = DATE '2026-11-01';

-- What failed data-quality checks, and why
SELECT reason, count(*) AS rows
FROM events_pipeline_dev_db.rejected_events
CROSS JOIN UNNEST(dq_reasons) AS t(reason)
GROUP BY reason
ORDER BY rows DESC;
