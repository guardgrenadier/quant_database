CREATE SCHEMA IF NOT EXISTS canonical;

-- 银河源值：股本为股，自由流通比例和权重为百分数，不归一化或补造成分。
CREATE TABLE IF NOT EXISTS canonical.index_weight (
    index_code VARCHAR NOT NULL CHECK (length(trim(index_code))>0),
    con_code VARCHAR NOT NULL CHECK (length(trim(con_code))>0),
    trade_date DATE NOT NULL,
    total_share DOUBLE NOT NULL CHECK (isfinite(total_share) AND total_share>0),
    -- 已实测该字段可整体为 NULL；合法缺失与零比例分别保留。
    free_share_ratio DOUBLE CHECK (isfinite(free_share_ratio) AND free_share_ratio BETWEEN 0 AND 100),
    calc_share DOUBLE NOT NULL CHECK (isfinite(calc_share) AND calc_share>=0),
    weight_factor DOUBLE NOT NULL CHECK (isfinite(weight_factor) AND weight_factor>=0),
    weight DOUBLE NOT NULL CHECK (isfinite(weight) AND weight>=0),
    close DOUBLE NOT NULL CHECK (isfinite(close) AND close>0),
    PRIMARY KEY (index_code,con_code,trade_date)
);
