CREATE SCHEMA IF NOT EXISTS canonical;

-- 当前供应商报告的完整归属历史；out_date 是包含当天的结束日，不保存历次抓取镜像。
CREATE TABLE IF NOT EXISTS canonical.index_member_all (
    l1_code VARCHAR NOT NULL CHECK (length(trim(l1_code))>0),
    l1_name VARCHAR,
    l2_code VARCHAR NOT NULL CHECK (length(trim(l2_code))>0),
    l2_name VARCHAR,
    l3_code VARCHAR NOT NULL CHECK (length(trim(l3_code))>0),
    l3_name VARCHAR,
    ts_code VARCHAR NOT NULL CHECK (length(trim(ts_code))>0),
    name VARCHAR,
    in_date DATE NOT NULL,
    out_date DATE,
    is_new VARCHAR NOT NULL CHECK (is_new IN ('Y','N')),
    PRIMARY KEY (l3_code,ts_code,in_date),
    -- 闭区间允许单日归属；Y/N 分别对应未结束/已结束，不表示上市状态。
    CHECK (out_date IS NULL OR out_date>=in_date),
    CHECK ((is_new='Y' AND out_date IS NULL) OR (is_new='N' AND out_date IS NOT NULL))
);
