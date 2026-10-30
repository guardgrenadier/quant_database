CREATE SCHEMA IF NOT EXISTS canonical;

-- 官方北交所新旧代码映射完整快照；不保存抓取时间为业务字段。
CREATE TABLE IF NOT EXISTS canonical.bse_mapping (
    -- 业务约束：四字段必填；o_code 主键，n_code 在全快照内同样唯一。
    name VARCHAR NOT NULL,
    o_code VARCHAR PRIMARY KEY,
    n_code VARCHAR NOT NULL UNIQUE,
    list_date DATE NOT NULL,
    CHECK (length(trim(name)) > 0),
    CHECK (length(trim(o_code)) > 0),
    CHECK (length(trim(n_code)) > 0),
    -- 业务约束：只保存北交所代码变化映射，不派生或改写代码。
    CHECK (o_code LIKE '%.BJ'),
    CHECK (n_code LIKE '%.BJ'),
    CHECK (o_code <> n_code)
);
