WITH sales AS (
  SELECT
      user_id
      , event_time
      , order_id
      , ARRAY_AGG(STRUCT(item_id, quantity, price)) AS items
  FROM {EXTERNAL_TABLE_NAME}
  GROUP BY 1,2,3
),
treatments AS (
  SELECT * FROM `bud-gcp-meetup.treatments.customer_123`
),
attribution AS (
  SELECT
    sales.user_id
    , sales.event_time
    , sales.order_id
    , ARRAY_AGG(STRUCT(sales.items, treatments.event_time AS treatment_event_time) ORDER BY treatments.event_time DESC LIMIT 1)[ORDINAL(1)] AS ungrouped
  FROM sales
  LEFT JOIN treatments
  ON sales.user_id = treatments.user_id
    AND treatments.event_time BETWEEN TIMESTAMP_SUB(sales.event_time, INTERVAL 7 DAY) AND sales.event_time
  GROUP BY sales.user_id, sales.event_time, sales.order_id
)

SELECT
  user_id
  , event_time
  , order_id
  , ungrouped.items
  , STRUCT(ungrouped.treatment_event_time AS event_time) AS attributed_treatment
FROM attribution
