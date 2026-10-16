"""基于列式 Candidate 与 Scope 关系作集合检查，产出显式发布许可。"""

from dataclasses import replace
from quant_data.core.models import Check, ValidationResult
from quant_data.db import registered, relation


def blocked_units(units, checks):
    """汇总 ERROR 的影响集合；未知影响按全局处理，WARNING 不阻断。"""
    known = set(units)
    blocked = set()
    for check in checks:
        if check.level == "ERROR":
            # 业务检查：可定位错误只阻断指定 Unit；全局或无法定位的错误阻断整个纳入范围。
            blocked.update(check.units if check.units is not None and set(check.units) <= known else known)
    return blocked


def validate(con, dataset, scope, delivery, candidate, source_checks=()):
    """校验 Schema、业务规则和记录归属，返回与业务行独立的合格 Unit 集合。"""
    checks = list(delivery.checks + tuple(source_checks))
    states = {u.unit_key: u.state for u in delivery.units}
    # 业务检查：每个纳入范围的 Unit 都必须明确 COMPLETE；缺失、失败和不完整均不可发布。
    # 这里依据独立交付状态判定，不用行数推断是否完成，因此完整零行交付仍可进入验收。
    for unit in scope.units:
        if states.get(unit) != "COMPLETE":
            checks.append(Check("ERROR", "delivery.complete", "validation", (unit,), {"state": states.get(unit)}))
    if candidate is not None:
        # 业务检查：字段名、顺序和类型必须与 Canonical 契约一致，不在验收层隐式修正类型。
        if candidate.schema != dataset.schema:
            checks.append(Check("ERROR", "candidate.schema", "validation", None,
                                {"actual": {k: str(v) for k, v in candidate.schema.items()}}))
        else:
            # 临时关系从 DataFrame 取得，不依赖裸文件路径；相同连接供 Publisher 使用。
            with registered(con, "_candidate_arrow", candidate) as source:
                con.execute(f"CREATE OR REPLACE TEMP TABLE candidate AS SELECT * FROM {source}")
            # Dataset 负责股票等具体业务规则；通用层不解释证券代码或日期语义。
            checks.extend(dataset.validate_candidate(con, "candidate", scope))
            key = dataset.unit_key_sql("c")
            # 业务检查：每条 Candidate 必须能 JOIN 到 IN_SCOPE Unit；范围外/归属为空均全局阻断。
            count = con.execute(f"SELECT count(*) FROM {relation('candidate')} c LEFT JOIN _scope_units s "
                                f"ON ({key})=s.unit_key WHERE s.unit_key IS NULL").fetchone()[0]
            if count:
                checks.append(Check("ERROR", "candidate.scope", "validation", None, {"rows": count}))
    elif any(states.get(u) == "COMPLETE" for u in scope.units) and not blocked_units(scope.units, checks) >= set(scope.units):
        # 业务检查：尚有可发布范围却没有 Candidate 属于异常；显式零行表与 None 不等价。
        checks.append(Check("ERROR", "candidate.missing", "validation"))
    # 不能定位的影响明确存成全局，审计引用也能解释为何所有 Unit 被阻断。
    normalized = tuple(replace(c, units=None, detail={**(c.detail or {}), "unresolved_units": c.units})
                       if c.units is not None and not set(c.units) <= set(scope.units) else c for c in checks)
    blocked = blocked_units(scope.units, normalized)
    # 发布许可按 Unit 显式给出；WARNING 不移出许可集合，阻断 Unit 的记录留在 Candidate 供诊断。
    return ValidationResult(tuple(u for u in scope.units if u not in blocked),
                            tuple(u for u in scope.units if u in blocked), normalized)
