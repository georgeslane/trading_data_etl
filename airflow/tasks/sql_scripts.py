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