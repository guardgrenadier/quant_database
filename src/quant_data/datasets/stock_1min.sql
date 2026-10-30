CREATE SCHEMA IF NOT EXISTS canonical;

-- 单一物化研究网格：真实成交、项目构造的零成交与停牌分钟通过 status 区分。
-- 价格为元，volume 为股，amount 为元；bar_time 为开始分钟标签。
-- 09:25:00 恒为开盘集合竞价 bar；09:30 OHLC 保留银河混合口径，量额扣除开盘竞价。
CREATE TABLE IF NOT EXISTS canonical.stock_1min (
    ts_code         VARCHAR NOT NULL CHECK (length(trim(ts_code)) > 0),
    trade_date      DATE NOT NULL,
    bar_time        TIME_NS NOT NULL,
    status          VARCHAR NOT NULL CHECK (status IN ('TRADED', 'NO_TRADE', 'SUSPENDED')),
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          BIGINT NOT NULL CHECK (volume >= 0),
    amount          DOUBLE NOT NULL CHECK (isfinite(amount) AND amount >= 0),
    -- 连续估值价格：TRADED 等于 close，其余状态取最近有效参考价；无可靠值时允许 NULL。
    reference_price DOUBLE,
    PRIMARY KEY (ts_code, trade_date, bar_time),
    -- OHLC 必须同时有值或同时为空；有值时为有限正数并满足区间关系。
    CHECK ((open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL)
        OR (open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL)),
    CHECK (open IS NULL OR (isfinite(open) AND isfinite(high) AND isfinite(low) AND isfinite(close)
                            AND open > 0 AND high > 0 AND low > 0 AND close > 0
                            AND high >= low AND open BETWEEN low AND high AND close BETWEEN low AND high)),
    CHECK (reference_price IS NULL OR (isfinite(reference_price) AND reference_price > 0)),
    CHECK (
        (status = 'TRADED' AND open IS NOT NULL AND volume > 0 AND amount > 0
             AND reference_price = close)
        OR
        (status = 'NO_TRADE' AND volume = 0 AND amount = 0
             AND ((reference_price IS NULL AND open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL)
               OR (reference_price IS NOT NULL AND open = reference_price AND high = reference_price
                   AND low = reference_price AND close = reference_price)))
        OR
        (status = 'SUSPENDED' AND open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL
             AND volume = 0 AND amount = 0)
    )
);
