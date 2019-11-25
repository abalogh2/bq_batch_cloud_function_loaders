DECLARE min_event_time, max_event_time TIMESTAMP;
DECLARE attribution_window_size INT64 DEFAULT 7;

-- define partition boundaries for querying treatment data
SET (min_event_time, max_event_time) = (
  SELECT AS STRUCT min(event_time), max(event_time)
  FROM {EXTERNAL_TABLE_NAME}
);

-- extending with attribution window
SET min_event_time = TIMESTAMP_TRUNC(TIMESTAMP_SUB(min_event_time, INTERVAL attribution_window_size DAY), DAY);

-- creating destination table
CREATE TABLE IF NOT EXISTS `bud-gcp-meetup.sales_data_attributed.customer_123`
(
  user_id INT64,
  event_time TIMESTAMP,
  order_id INT64,
  items ARRAY<STRUCT<item_id INT64, quantity FLOAT64, price FLOAT64>>,
  attributed_treatment STRUCT<event_time TIMESTAMP> 
)
PARTITION BY DATE(_PARTITIONTIME)
CLUSTER BY user_id
OPTIONS(require_partition_filter=true);

-- performing attribution and appending to table
INSERT INTO `bud-gcp-meetup.sales_data_attributed.customer_123`
(user_id, event_time, order_id, items, attributed_treatment)
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
      WHERE event_time BETWEEN min_event_time AND max_event_time
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
        AND treatments.event_time BETWEEN TIMESTAMP_SUB(sales.event_time, INTERVAL attribution_window_size DAY) AND sales.event_time
      GROUP BY sales.user_id, sales.event_time, sales.order_id
    )

    SELECT
      user_id
      , event_time
      , order_id
      , ungrouped.items
      , STRUCT(ungrouped.treatment_event_time AS event_time) AS attributed_treatment
    FROM attribution
