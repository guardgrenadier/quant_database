CREATE SCHEMA IF NOT EXISTS canonical;

-- 点位为源值，涨跌幅为%；vol 统一手、amount 统一千元。SW 不提供 pre_close，保留 NULL。
CREATE TABLE IF NOT EXISTS canonical.index_daily (
    ts_code VARCHAR NOT NULL CHECK (length(trim(ts_code))>0),
    trade_date DATE NOT NULL,
    open DOUBLE CHECK (isfinite(open) AND open>0),
    high DOUBLE CHECK (isfinite(high) AND high>0),
    low DOUBLE CHECK (isfinite(low) AND low>0),
    close DOUBLE CHECK (isfinite(close) AND close>0),
    pre_close DOUBLE CHECK (isfinite(pre_close) AND pre_close>=0),
    change DOUBLE CHECK (isfinite(change)),
    pct_chg DOUBLE CHECK (isfinite(pct_chg)),
    vol DOUBLE CHECK (isfinite(vol) AND vol>=0),
    amount DOUBLE CHECK (isfinite(amount) AND amount>=0),
    PRIMARY KEY (ts_code,trade_date),
    -- 业务最终防线：部分缺值合法，不能是九个数值全空的伪行情；四价齐全才比较。
    CHECK (coalesce(open,high,low,close,pre_close,change,pct_chg,vol,amount) IS NOT NULL),
    CHECK (open IS NULL OR high IS NULL OR low IS NULL OR close IS NULL OR
           (high>=low AND open BETWEEN low AND high AND close BETWEEN low AND high))
);
