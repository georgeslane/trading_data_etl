CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS aggregated_trades (
    aggregation_period INT NOT NULL,
    ticker VARCHAR(31) NOT NULL,
    opening_timestamp DATETIME NOT NULL,
    closing_timestamp DATETIME,
    num_trades INT,
    total_volume FLOAT,
    average_volume FLOAT
    average_price FLOAT,
    max_price FLOAT,
    min_price FLOAT,
    opening_price FLOAT,
    closing_price FLOAT,
    news JSON,
    PRIMARY KEY (ticker, aggregation_period, opening_timestamp)
)
"""

INSERT_INTO_TABLE = """
INSERT IGNORE INTO aggregated_trades (aggregation_period, ticker, opening_timestamp, 
    closing_timestamp, num_trades, total_volume, average_volume, average_price, 
    max_price, min_price, opening_price, closing_price, news)
VALUES {};
"""