READ_RAW_DATA = """
SELECT 
    id, 
    trade_codes, 
    price, 
    ticker,
    unix_timestamp,
    volume
FROM {}
WHERE id > 
(SELECT 
    COALESCE(MAX(id), 0)
FROM {}
);
"""

INSERT_CLEANED_DATA="""
INSERT INTO {}
(id, trade_conditions, price, ticker, timestamp, volume)
VALUES {};
"""

AGGREGATE_DATA="""
INSERT INTO {} 
(start_id, aggregation_period, ticker, start_timestamp, trade_conditions, 
number_of_trades, total_volume, average_volume, average_price, maximum_price, 
minimum_price, opening_price, closing_price)
SELECT 
    MIN(id) AS start_id,
    {} AS aggregation_period, 
    ticker,
    MIN(timestamp) AS start_timestamp,
    JSON_ARRAYAGG(trade_conditions) AS trade_conditions,
    COUNT(id) AS number_of_trades,
    SUM(volume) AS total_volume,
    AVG(volume) AS average_volume,
    AVG(price) AS average_price,
    MAX(price) AS maximum_price,
    MIN(price) AS minimum_price,
    (SELECT price FROM {} ORDER BY timestamp ASC LIMIT 1) AS opening_price,
    (SELECT price FROM {} ORDER BY timestamp DESC LIMIT 1) AS closing_price
FROM {}
WHERE timestamp >= NOW() - INTERVAL {} MINUTE
GROUP BY ticker;
"""

UNION_DATA = """
SELECT * FROM {} UNION 
"""