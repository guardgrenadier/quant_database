"""统一编排：控制信息具名建模，业务批次按 DataFrame 在内存传递。"""

from dataclasses import replace
from quant_data.core import audit, deps, publisher, validation
from quant_data.core.artifacts import Artifacts
from quant_data.core.models import Dataset, SourceAdapter, RunOptions
from quant_data.db import transaction


def run(con, dataset: Dataset, adapter: SourceAdapter, params: dict, options: RunOptions):
    """在同一连接组织获取、验收、批量发布和审计，返回 RunResult。

    原始证据旁路落盘，处理路径不读取证据文件。诊断导出独立于业务结局，
    Candidate 生命周期覆盖验收与提交/回滚，完成后释放连接级临时关系。
    """
    # 先登记 Run 和唯一产物目录；started_at 由数据库填写，finished_at 等到执行结局确定。
    run_id, landing = audit.begin(con, dataset.name, params, options.landing_root)
    artifacts, scope, candidate = None, None, None
    stage = "dependencies"
    try:
        artifacts = Artifacts(landing, options.keep_artifacts)
        # 在同一个读取事务中固定上游成功版本并解析 Scope，保证范围决策基于一致快照。
        with transaction(con):
            pins = deps.check(con, dataset.dependencies(params))
            stage = "scope"
            scope = dataset.resolve_scope(params, pins, con)
            audit.scope(con, run_id, dataset.name, scope, pins)
        artifacts.capture("scope", scope)
        artifacts.capture("pins", pins)
        artifacts.snapshot(con, "scope")
        # 业务分支：没有 IN_SCOPE Unit 时不请求供应商；排除/未知决策仍保留在审计中参与结局计算。
        if scope.units:
            stage = "fetch"
            delivery = adapter.fetch(scope, artifacts)
            names = tuple(u.unit_key for u in delivery.units)
            # 业务检查：Adapter 不能重复声明交付 Unit，也不能扩大已解析范围。
            # 少报的 Unit 由验收阶段判为未完整交付。
            if len(set(names)) != len(names) or not set(names) <= set(scope.units):
                raise ValueError("Adapter delivery units duplicate or outside scope")
            with transaction(con):
                # 请求和交付按阶段批量写审计；证据 JSON 已在每次尝试发生时即时落盘。
                audit.record_delivery(con, run_id, delivery)
            artifacts.capture("delivery", delivery)
            if delivery.frame is not None:
                artifacts.capture("landing", delivery.frame)
            stage = "source_validation"
            source_checks = adapter.validate_source(delivery, scope)
            artifacts.capture("source_checks", source_checks)
            complete = {u.unit_key for u in delivery.units if u.state == "COMPLETE"}
            # 业务检查：至少存在一个交付完整且未被源检查阻断的 Unit，才进行标准化。
            # 这里不删掉其他 Unit 的业务行；后续验收和 Publisher 根据显式许可控制实际发布范围。
            if complete - validation.blocked_units(scope.units, delivery.checks + source_checks):
                stage = "normalize"
                candidate, conversion_checks = adapter.normalize(delivery, scope)
                source_checks += conversion_checks
                if candidate is not None:
                    artifacts.capture("candidate", candidate)
            stage = "validation"
            verdict = validation.validate(con, dataset, scope, delivery, candidate, source_checks)
            # 先保存检查和失败 Unit，成功 Unit 的审计留给发布事务一起提交。
            with transaction(con):
                audit.record_checks(con, run_id, verdict.checks)
                audit.failure(con, run_id, verdict.blocked_units, stage)
            artifacts.capture("validation", verdict)
            artifacts.snapshot(con, "validation")
            stage = "publish"
            # 只传入验收许可集合；Publisher 原子提交业务替换、版本、Coverage 和成功 Unit 审计。
            publications = publisher.publish(con, dataset, "candidate" if candidate is not None else None,
                                             verdict.passed_units, pins, run_id)
            artifacts.capture("publication", publications)
            artifacts.snapshot(con, "published", dataset)
        stage = "finish"
        # Run 结局来自已持久化的 Unit 状态，支持部分成功而不丢失已经提交的成功版本。
        result = audit.finish(con, run_id)
    except Exception as exc:
        error = f"{type(exc).__name__}: {exc}"
        with transaction(con):
            # 一次读取控制记录，以集合更新尚未提交的 Unit，保护成功版本。
            remaining = tuple(r[0] for r in con.execute(
                "SELECT unit_key FROM meta.run_unit WHERE run_id=? AND scope_decision='IN_SCOPE' AND status<>'PUBLISHED'", [run_id]).fetchall())
            audit.failure(con, run_id, remaining, stage, error)
            result = audit.finish(con, run_id, error, stage)
    if artifacts is not None:
        # 发布和 Run 结局已确定后再清理/保留；导出或清理失败单独记录，不撤销成功发布。
        artifacts.finalize(result)
        audit.record_artifacts(con, run_id, artifacts)
        result = replace(result, artifact_dir=str(landing), artifact_status=artifacts.status,
                         artifacts_complete=artifacts.complete, export_errors=tuple(artifacts.errors))
        if options.keep_artifacts:
            # 快照包含最终保留状态；后续导出故障仍独立更新诊断结果。
            artifacts.snapshot(con, "finished")
            artifacts.seal(result)
            audit.record_artifacts(con, run_id, artifacts)
            result = replace(result, artifacts_complete=artifacts.complete, export_errors=tuple(artifacts.errors))
    else:
        result = replace(result, artifact_dir=str(landing), artifact_status="RETAINED")
    # 不保留上一次 Run 的临时数据供下一次误用；明确在发布/回滚之后释放。
    con.execute("DROP TABLE IF EXISTS candidate")
    con.execute("DROP TABLE IF EXISTS _scope_units")
    return result
