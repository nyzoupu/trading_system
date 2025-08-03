CREATE TABLE kline_data (
    id INT  PRIMARY KEY,
    symbol VARCHAR(20) NOT NULL COMMENT '交易对，如BTCUSDT',
    `interval` VARCHAR(10) NOT NULL COMMENT 'K线周期，如1h, 4h, 1d',
    open_time DATETIME NOT NULL COMMENT 'K线开始时间',
    open DECIMAL(10, 4) NOT NULL COMMENT '开盘价',
    high DECIMAL(10, 4) NOT NULL COMMENT '最高价',
    low DECIMAL(10, 4) NOT NULL COMMENT '最低价',
    close DECIMAL(10, 4) NOT NULL COMMENT '收盘价',
    volume DECIMAL(20, 1) NOT NULL COMMENT '成交量',
    create_datetime DATETIME NOT NULL COMMENT '记录创建日期时间',
    UNIQUE KEY uq_symbol_time_interval (symbol, open_time, `interval`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='K线历史数据表';