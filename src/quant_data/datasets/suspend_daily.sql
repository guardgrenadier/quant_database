-- 正式稀疏日集合：只保存供应商报告的停复牌事实，不补正常交易股票行。
CREATE SCHEMA IF NOT EXISTS canonical;

CREATE TABLE IF NOT EXISTS canonical.suspend_daily (
    -- 业务约束：保留历史特殊代码和前导零，不要求存在于当前 stock_basic。
    ts_code        VARCHAR NOT NULL CHECK (length(trim(ts_code)) > 0),
    -- 业务类型：市场本地交易日，发布前须归属已确认的开市日 Scope。
    trade_date     DATE NOT NULL,
    -- 业务约束：S 为停牌、R 为复牌，不将未知类型或 NULL 写入权威事实。
    suspend_type   VARCHAR NOT NULL CHECK (suspend_type IN ('S', 'R')),
    -- 业务语义：保留完整日内时段（包括多段和跨午休）；R 表示复牌，不能因时段为空推断全天停牌。
    suspend_timing VARCHAR,
    -- 业务粒度：同一证券、交易日、类型一条记录；timing 不能改变键或触发静默覆盖。
    PRIMARY KEY (ts_code, trade_date, suspend_type),
    -- 业务约束：未提供时段用 NULL 表示；有值时不得是空白文本。
    CHECK (suspend_timing IS NULL OR length(trim(suspend_timing)) > 0)
);
