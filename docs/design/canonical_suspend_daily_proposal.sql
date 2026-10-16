-- suspend_daily Schema 建议，尚未作为正式 Dataset 发布契约冻结；本文件不发布数据。
-- stock_basic 正式 DDL 位于 src/quant_data/datasets/stock_basic.sql。
CREATE SCHEMA IF NOT EXISTS canonical;

CREATE TABLE canonical.suspend_daily (
    ts_code        VARCHAR NOT NULL CHECK (length(trim(ts_code)) > 0),
    trade_date     DATE NOT NULL,
    suspend_type   VARCHAR NOT NULL CHECK (suspend_type IN ('S', 'R')),
    suspend_timing VARCHAR,
    PRIMARY KEY (ts_code, trade_date, suspend_type),
    CHECK (suspend_timing IS NULL OR length(trim(suspend_timing)) > 0)
);
