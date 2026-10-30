CREATE SCHEMA IF NOT EXISTS canonical;

-- 供应商每日指标，股本万股、市值万元、比率百分数；不重算、不复权，不保证财务 PIT。
CREATE TABLE IF NOT EXISTS canonical.daily_basic (
    ts_code VARCHAR NOT NULL CHECK (length(trim(ts_code))>0),
    trade_date DATE NOT NULL,
    close DOUBLE NOT NULL CHECK (isfinite(close) AND close>0),
    -- 以下十五个指标允许合法 NULL；零不代表缺失，换手率不设 100 的上限。
    turnover_rate DOUBLE CHECK (isfinite(turnover_rate) AND turnover_rate>=0),
    turnover_rate_f DOUBLE CHECK (isfinite(turnover_rate_f) AND turnover_rate_f>=0),
    volume_ratio DOUBLE CHECK (isfinite(volume_ratio) AND volume_ratio>=0),
    -- 估值有符号值原样保存，NULL 不以零或其他来源回填。
    pe DOUBLE CHECK (isfinite(pe)),
    pe_ttm DOUBLE CHECK (isfinite(pe_ttm)),
    pb DOUBLE CHECK (isfinite(pb)),
    ps DOUBLE CHECK (isfinite(ps)),
    ps_ttm DOUBLE CHECK (isfinite(ps_ttm)),
    dv_ratio DOUBLE CHECK (isfinite(dv_ratio) AND dv_ratio>=0),
    dv_ttm DOUBLE CHECK (isfinite(dv_ttm) AND dv_ttm>=0),
    total_share DOUBLE CHECK (isfinite(total_share) AND total_share>=0),
    float_share DOUBLE CHECK (isfinite(float_share) AND float_share>=0),
    free_share DOUBLE CHECK (isfinite(free_share) AND free_share>=0),
    total_mv DOUBLE CHECK (isfinite(total_mv) AND total_mv>=0),
    circ_mv DOUBLE CHECK (isfinite(circ_mv) AND circ_mv>=0),
    -- 股本和市值的跨字段关系由 Dataset 告警，不设为数据库阻断约束。
    PRIMARY KEY (ts_code,trade_date)
);
