CREATE SCHEMA IF NOT EXISTS meta;
CREATE SEQUENCE IF NOT EXISTS meta.publication_seq;

-- Run 保存计数与公共引用；原始响应和整批业务数据不进入 summary。
CREATE TABLE IF NOT EXISTS meta.run (
    run_id VARCHAR PRIMARY KEY, dataset VARCHAR NOT NULL, params JSON NOT NULL,
    -- 生命周期：登记 Run 时自动开始计时；汇总结局时才写 finished_at。
    started_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp, finished_at TIMESTAMPTZ,
    -- 运行结局与产物状态独立：清理/导出失败不把已提交的 SUCCESS 改成 FAILED。
    status VARCHAR NOT NULL CHECK (status IN ('RUNNING','SUCCESS','PARTIAL','FAILED')),
    summary JSON NOT NULL DEFAULT '{}', error VARCHAR, failed_stage VARCHAR,
    artifact_dir VARCHAR NOT NULL, artifact_status VARCHAR NOT NULL DEFAULT 'ACTIVE',
    artifacts_complete BOOLEAN, export_errors JSON NOT NULL DEFAULT '[]',
    -- 中断收尾时间不是进程死亡时间；审计明细生命周期与业务结局、Landing 状态独立。
    reconciled_at TIMESTAMPTZ,
    audit_state VARCHAR NOT NULL DEFAULT 'ONLINE' CHECK (audit_state IN ('ONLINE','ARCHIVED','EXPIRED')),
    audit_archive_path VARCHAR,
    CHECK (audit_state='ONLINE' OR audit_archive_path IS NOT NULL)
);
-- 一行一个本次 Unit，Scope 决策独立于后续交付与执行状态。
CREATE TABLE IF NOT EXISTS meta.run_unit (
    run_id VARCHAR NOT NULL, dataset VARCHAR NOT NULL, unit_key VARCHAR NOT NULL,
    -- 范围决策描述是否应处理；执行状态描述本轮结果，COMPLETE 交付不等于成功发布。
    scope_decision VARCHAR NOT NULL CHECK (scope_decision IN ('IN_SCOPE','OUT_OF_SCOPE','UNKNOWN')),
    reason VARCHAR, status VARCHAR NOT NULL CHECK (status IN ('PENDING','UNKNOWN','OUT_OF_SCOPE','FAILED','PUBLISHED')),
    stage VARCHAR NOT NULL, delivery_state VARCHAR,
    request_ids JSON NOT NULL DEFAULT '[]', check_ids JSON NOT NULL DEFAULT '[]',
    publication_id BIGINT, row_count BIGINT, error VARCHAR,
    PRIMARY KEY (run_id,dataset,unit_key)
);
CREATE TABLE IF NOT EXISTS meta.run_dependency (
    -- 每个上游范围记录实际使用的一个成功版本，供发布事务复核与事后追溯。
    run_id VARCHAR NOT NULL, upstream_dataset VARCHAR NOT NULL, upstream_unit VARCHAR NOT NULL,
    publication_id BIGINT NOT NULL, PRIMARY KEY (run_id,upstream_dataset,upstream_unit)
);
-- 每次尝试只保存控制信息，证据路径的可用性结合 Run 清理状态解释。
CREATE TABLE IF NOT EXISTS meta.request_attempt (
    run_id VARCHAR NOT NULL, request_id VARCHAR NOT NULL, attempt INTEGER NOT NULL,
    api_name VARCHAR NOT NULL, params JSON NOT NULL, fields JSON NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL, elapsed_seconds DOUBLE NOT NULL,
    http_status INTEGER, provider_code INTEGER, returned_fields JSON NOT NULL,
    row_count BIGINT, has_more BOOLEAN, error VARCHAR, evidence_path VARCHAR,
    PRIMARY KEY (run_id,request_id,attempt)
);
-- 检查只存一次，units=NULL 表示全局影响；Unit 只保存其 check_id 引用。
CREATE TABLE IF NOT EXISTS meta.validation_check (
    run_id VARCHAR NOT NULL, check_id INTEGER NOT NULL, level VARCHAR NOT NULL CHECK (level IN ('ERROR','WARNING')),
    rule VARCHAR NOT NULL, stage VARCHAR NOT NULL, units JSON, detail JSON NOT NULL,
    PRIMARY KEY (run_id,check_id)
);
CREATE TABLE IF NOT EXISTS meta.publication (
    -- 仅成功发布生成版本；序列号回滚可留空隙，不能据连续性推断业务完整性。
    publication_id BIGINT PRIMARY KEY DEFAULT nextval('meta.publication_seq'),
    dataset VARCHAR NOT NULL, unit_key VARCHAR NOT NULL,
    -- 零行也是合法的发布计数；Dataset 自行决定其业务范围是否允许空交付。
    row_count BIGINT NOT NULL CHECK (row_count>=0), run_id VARCHAR NOT NULL,
    published_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp
);
CREATE TABLE IF NOT EXISTS meta.coverage (
    -- 当前权威覆盖以 dataset + unit_key 定位；运行失败不能覆盖已有 PUBLISHED 指针。
    dataset VARCHAR NOT NULL, unit_key VARCHAR NOT NULL,
    status VARCHAR NOT NULL CHECK (status IN ('PENDING','FAILED','UNKNOWN','PUBLISHED')),
    publication_id BIGINT, updated_at TIMESTAMPTZ NOT NULL DEFAULT current_timestamp, note VARCHAR,
    PRIMARY KEY (dataset,unit_key),
    -- 业务不变量：成功覆盖必须指向版本，非成功覆盖不得携带成功版本指针。
    CHECK ((status='PUBLISHED' AND publication_id IS NOT NULL) OR (status<>'PUBLISHED' AND publication_id IS NULL))
);
