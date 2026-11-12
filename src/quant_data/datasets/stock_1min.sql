CREATE SCHEMA IF NOT EXISTS canonical;

-- 单一物化研究网格：真实成交、项目构造的零成交与停牌分钟通过 status 区分。
-- 价格为元，volume 为股，amount 为元；bar_time 为开始分钟标签，TIME 已覆盖分钟精度。
-- 09:25:00 恒为开盘集合竞价 bar；09:30 OHLC 保留银河混合口径，量额扣除开盘竞价。
-- 逻辑键 (ts_code,trade_date,bar_time) 由 Dataset 验收，不为大表物化 ART 主键。
CREATE TABLE IF NOT EXISTS canonical.stock_1min (
    ts_code         VARCHAR NOT NULL CHECK (length(trim(ts_code)) > 0),
    trade_date      DATE NOT NULL,
    bar_time        TIME NOT NULL,
    status          VARCHAR NOT NULL CHECK (status IN ('TRADED', 'NO_TRADE', 'SUSPENDED')),
    open            DOUBLE,
    high            DOUBLE,
    low             DOUBLE,
    close           DOUBLE,
    volume          BIGINT NOT NULL CHECK (volume >= 0),
    amount          DOUBLE NOT NULL CHECK (isfinite(amount) AND amount >= 0),
    -- OHLC 必须同时有值或同时为空；有值时为有限正数并满足区间关系。
    CHECK ((open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL)
        OR (open IS NOT NULL AND high IS NOT NULL AND low IS NOT NULL AND close IS NOT NULL)),
    CHECK (open IS NULL OR (isfinite(open) AND isfinite(high) AND isfinite(low) AND isfinite(close)
                            AND open > 0 AND high > 0 AND low > 0 AND close > 0
                            AND high >= low AND open BETWEEN low AND high AND close BETWEEN low AND high)),
    -- 连续参考价只进入 Serving；NO_TRADE 的 OHLC 仍须等于该派生价格或全空。
    CHECK (
        (status = 'TRADED' AND open IS NOT NULL AND volume > 0 AND amount > 0)
        OR
        (status = 'NO_TRADE' AND volume = 0 AND amount = 0
             AND ((open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL)
               OR (open = high AND high = low AND low = close)))
        OR
        (status = 'SUSPENDED' AND open IS NULL AND high IS NULL AND low IS NULL AND close IS NULL
             AND volume = 0 AND amount = 0)
    )
);
