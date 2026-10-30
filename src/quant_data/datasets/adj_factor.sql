CREATE SCHEMA IF NOT EXISTS canonical;

-- 原始因子无量纲，保留源精度，不按抓取日期或复权基准归一化。
CREATE TABLE IF NOT EXISTS canonical.adj_factor (
    ts_code VARCHAR NOT NULL CHECK (length(trim(ts_code))>0),
    trade_date DATE NOT NULL,
    -- 因子必填、有限且为正；不设置初值、上限或跨日期单调性约束。
    adj_factor DOUBLE NOT NULL CHECK (isfinite(adj_factor) AND adj_factor>0),
    PRIMARY KEY (ts_code,trade_date)
);
