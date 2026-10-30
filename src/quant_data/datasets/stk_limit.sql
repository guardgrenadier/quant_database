CREATE SCHEMA IF NOT EXISTS canonical;

-- 全市场证券源报价，含 B 股；不统一解释为人民币，不换汇或推导无限制标志。
CREATE TABLE IF NOT EXISTS canonical.stk_limit (
    ts_code VARCHAR NOT NULL CHECK (length(trim(ts_code))>0),
    trade_date DATE NOT NULL,
    -- 源昨收可空或为零，分别保留；不借其他 Dataset 回填。
    pre_close DOUBLE CHECK (isfinite(pre_close) AND pre_close>=0),
    up_limit DOUBLE NOT NULL CHECK (isfinite(up_limit) AND up_limit>0),
    -- 已确认源下限可为零，原样保存且由 Dataset 告警，不推导无限制。
    down_limit DOUBLE NOT NULL CHECK (isfinite(down_limit) AND down_limit>=0),
    -- 开放源分类，可空，不以当前样本固化全集枚举。
    asset_type VARCHAR,
    exchange VARCHAR,
    PRIMARY KEY (ts_code,trade_date),
    -- 最终业务约束只检查上下限顺序，不限制幅度或与昨收的关系。
    CHECK (up_limit>=down_limit)
);
