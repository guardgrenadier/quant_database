CREATE SCHEMA IF NOT EXISTS canonical;

-- 当前完整快照：一条 ts_code 一行，历史特殊标识与前导零按字符串保留。
CREATE TABLE IF NOT EXISTS canonical.stock_basic (
    -- 业务约束：一条 ts_code 一行；标识、代码、名称为必填字符串，空白值另由下方约束拒绝。
    ts_code      VARCHAR PRIMARY KEY,
    symbol       VARCHAR NOT NULL,
    name         VARCHAR NOT NULL,
    -- 业务约束：交易所及上市状态只能使用已确认枚举，不能写入未知取值。
    exchange     VARCHAR NOT NULL CHECK (exchange IN ('SSE', 'SZSE', 'BSE')),
    list_status  VARCHAR NOT NULL CHECK (list_status IN ('L', 'D', 'P', 'G', 'UN')),
    list_date    DATE,
    delist_date  DATE,
    market       VARCHAR,
    area         VARCHAR,
    industry     VARCHAR,
    fullname     VARCHAR,
    enname       VARCHAR,
    cnspell      VARCHAR,
    curr_type    VARCHAR,
    -- 业务约束：沪深港通标识可空；有值时只能是 N/H/S。其余可选属性允许缺失。
    is_hs        VARCHAR CHECK (is_hs IN ('N', 'H', 'S')),
    act_name     VARCHAR,
    act_ent_type VARCHAR,
    -- 业务约束：必填文本不能只由空白组成；数据库约束是业务验收后的最终防线。
    CHECK (length(trim(ts_code)) > 0),
    CHECK (length(trim(symbol)) > 0),
    CHECK (length(trim(name)) > 0),
    -- 已上市类状态要求上市日期，G/UN 可空；L 也允许提供未来退市日期。
    CHECK (list_status NOT IN ('L', 'D', 'P') OR list_date IS NOT NULL),
    -- 业务约束：D 状态必须提供退市日期，其他状态不要求该日期为空。
    CHECK (list_status <> 'D' OR delist_date IS NOT NULL),
    -- 业务约束：仅当两个日期均有值时比较先后，不因条件可空日期缺失而拒绝记录。
    CHECK (delist_date IS NULL OR list_date IS NULL OR delist_date >= list_date)
);
