CREATE SCHEMA IF NOT EXISTS canonical;

-- 未复权；股票价格为元，涨跌幅为%，vol 手，amount 千元；除权昨收不是上一行原始收盘。
CREATE TABLE IF NOT EXISTS canonical.stock_daily (
    ts_code VARCHAR NOT NULL CHECK (length(trim(ts_code))>0),
    trade_date DATE NOT NULL,
    open DOUBLE NOT NULL CHECK (isfinite(open) AND open>0),
    high DOUBLE NOT NULL CHECK (isfinite(high) AND high>0),
    low DOUBLE NOT NULL CHECK (isfinite(low) AND low>0),
    close DOUBLE NOT NULL CHECK (isfinite(close) AND close>0),
    pre_close DOUBLE NOT NULL CHECK (isfinite(pre_close) AND pre_close>=0),
    change DOUBLE NOT NULL CHECK (isfinite(change)),
    pct_chg DOUBLE NOT NULL CHECK (isfinite(pct_chg)),
    vol DOUBLE NOT NULL CHECK (isfinite(vol) AND vol>=0),
    amount DOUBLE NOT NULL CHECK (isfinite(amount) AND amount>=0),
    PRIMARY KEY (ts_code,trade_date),
    -- 业务最终防线：四价必填且保持区间关系；不检查跨行昨收或固定涨跌停幅度。
    CHECK (high>=low AND open BETWEEN low AND high AND close BETWEEN low AND high)
);
