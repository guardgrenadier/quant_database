CREATE SCHEMA IF NOT EXISTS canonical;

-- 供应商每日 ST 名单；不补正常股票，不推导新增/撤销风险警示事件。
CREATE TABLE IF NOT EXISTS canonical.stock_st (
    -- 业务标识：保留历史证券及前导零，不要求存在于当前 stock_basic。
    ts_code    VARCHAR NOT NULL CHECK (length(trim(ts_code)) > 0),
    -- 业务描述可空，缺失在验收中告警，不用当前股票名称回填。
    name       VARCHAR,
    trade_date DATE NOT NULL,
    -- 首版枚举：未知类型需另行确认，不能从名称中的 ST/*ST 重新分类。
    type       VARCHAR NOT NULL CHECK (type = 'ST'),
    type_name  VARCHAR,
    -- 同证券同日一条名单事实，描述和类型不扩充业务键。
    PRIMARY KEY (ts_code, trade_date)
);
