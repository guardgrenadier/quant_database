CREATE SCHEMA IF NOT EXISTS canonical;

-- 固定目标快照：宽基白名单及按类别/发布方获取的申万行业，不保存抓取时间为业务字段。
CREATE TABLE IF NOT EXISTS canonical.index_basic (
    -- 业务约束：完整字符串代码为唯一键；五个必填文本由下方约束排除空白。
    ts_code VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    fullname VARCHAR,
    market VARCHAR NOT NULL,
    publisher VARCHAR NOT NULL,
    index_type VARCHAR,
    category VARCHAR NOT NULL,
    base_date DATE,
    -- 业务约束：NULL、零及负基点可存储；非有限值阻断，负值另由 Dataset 告警。
    base_point DOUBLE CHECK (base_point IS NULL OR isfinite(base_point)),
    list_date DATE,
    weight_rule VARCHAR,
    "desc" VARCHAR,
    exp_date DATE,
    CHECK (length(trim(ts_code)) > 0),
    CHECK (length(trim(name)) > 0),
    CHECK (length(trim(market)) > 0),
    CHECK (length(trim(publisher)) > 0),
    CHECK (length(trim(category)) > 0)
    -- 分类采用开放字符串，日期先后仅告警，不增加未经确认的枚举或日期硬约束。
);
