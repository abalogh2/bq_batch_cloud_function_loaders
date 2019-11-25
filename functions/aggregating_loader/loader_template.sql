SELECT
    user_id
    , event_time
    , order_id
    , ARRAY_AGG(
        STRUCT(
            item_id
            , quantity
            , price
        )
    ) AS items
FROM {EXTERNAL_TABLE_NAME}
GROUP BY 1,2,3
