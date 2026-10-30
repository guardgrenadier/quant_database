CREATE SCHEMA IF NOT EXISTS canonical;

-- 首版只保存 SSE 供应商事实；休市日必须有显式记录，不复制生成其他交易所日历。
CREATE TABLE IF NOT EXISTS canonical.trade_calendar (
    -- 业务约束：首版只保存 SSE 实际交付，不写入其他交易所或复制的替代行。
    exchange      VARCHAR NOT NULL CHECK (exchange = 'SSE'),
    -- 业务类型：市场本地自然日，开市、休市都使用该日期定位事实。
    cal_date      DATE NOT NULL,
    -- 业务约束：必须明确开市或休市，未知/缺失不能作为 NULL 写入权威日历。
    is_open       BOOLEAN NOT NULL,
    -- 业务空值：历史首日可能没有前交易日，保留供应商 NULL，不强行补日期。
    pretrade_date DATE,
    -- 业务约束：每个交易所 × 自然日至多一行；“至少一行”另由发布前验收保证。
    PRIMARY KEY (exchange, cal_date),
    -- 业务约束：有值时前交易日严格早于当日，允许引用本次范围之外的日期。
    CHECK (pretrade_date IS NULL OR pretrade_date < cal_date)
);
