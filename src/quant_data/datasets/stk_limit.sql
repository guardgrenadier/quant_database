CREATE SCHEMA IF NOT EXISTS canonical;

-- 全市场证券源报价，含 B 股；不统一解释为人民币，不换汇或推导无限制标志。
CREATE TABLE IF NOT EXISTS canonical.stk_limit (
    ts_code VARCHAR NOT NULL CHECK (length(trim(ts_code))>0),
    trade_date DATE NOT NULL,
    -- 源昨收可空或为零，分别保留；不借其他 Dataset 回填。
    pre_close DOUBLE CHECK (isfinite(pre_close) AND pre_close>=0),
    -- 仅批准的三零价格组可由 prepare 整组置 NULL；其他非 NULL 值域不变。
    up_limit DOUBLE CHECK (isfinite(up_limit) AND up_limit>0),
    -- 源下限可为零并告警；三零组处置后也可为 NULL，不推导无限制标志。
    down_limit DOUBLE CHECK (isfinite(down_limit) AND down_limit>=0),
    -- 开放源分类，可空，不以当前样本固化全集枚举。
    asset_type VARCHAR,
    exchange VARCHAR,
    -- 逻辑键由 Dataset 验收，不为持续增长的大表物化 ART 主键。
    -- 最终业务约束只检查上下限顺序，不限制幅度或与昨收的关系。
    CHECK (up_limit>=down_limit)
);
