"""基于列式 Candidate 与 Scope 关系作集合检查，产出显式发布许可。"""

from dataclasses import replace
from quant_data.core.models import Check, ValidationResult
from quant_data.db import registered, relation


def _normalize_check(check, known):
    """无法定位的检查归属统一表达为全局，并保留原标识供审计。"""
    # 业务归属：仅接受 IN_SCOPE Unit；包含未知或排除范围时不能猜测其真实影响。
    if check.units is not None and not set(check.units) <= known:
        return replace(check, units=None, detail={**(check.detail or {}), "unresolved_units": check.units})
    return check


def blocked_units(units, checks):
    """汇总 ERROR 的影响集合；未知影响按全局处理，WARNING 不阻断。"""
    known = set(units)
    blocked = set()
    for check in checks:
        if check.level == "ERROR":
            # 业务检查：归属政策复用同一处定义；WARNING 不参与阻断。
            affected = _normalize_check(check, known).units
            blocked.update(known if affected is None else affected)
    return blocked


def validate(con, dataset, scope, delivery, candidate, preparation_checks=()):
    """按五步形成发布许可；调用前须准备 _scope_units，Schema 合格时创建 candidate 临时表。"""
    # 1. 收集并规范已有诊断：归属先统一，后续阻断计算和审计使用同一含义。
    known = set(scope.units)
    checks = [_normalize_check(c, known) for c in delivery.checks + tuple(preparation_checks)]
    blocked = blocked_units(known, checks)

    # 2. 补齐获取完成性问题：状态独立于行数，完整零行交付不能因无记录被误判。
    states = {u.unit_key: u.state for u in delivery.units}
    for unit in scope.units:
        # 业务检查：缺失、失败和不完整均不可发布；已有阻断原因时不再补泛化错误。
        if states.get(unit) != "COMPLETE" and unit not in blocked:
            checks.append(Check("ERROR", "delivery.complete", "validation", (unit,), {"state": states.get(unit)}))
            blocked.add(unit)

    # 3. 检查 Candidate 是否可用：None 与有明确 Schema 的零行表不同。
    candidate_usable = False
    if candidate is None:
        # 业务检查：尚有未阻断范围却无 Candidate 必须报错；全部阻断时不补泛化错误。
        if blocked != known:
            checks.append(Check("ERROR", "candidate.missing", "validation"))
    elif candidate.schema != dataset.schema:
        # 业务检查：字段名、顺序和类型须符合契约；结构不合格时不能执行后续 SQL。
        checks.append(Check("ERROR", "candidate.schema", "validation", None,
                            {"actual": {k: str(v) for k, v in candidate.schema.items()}}))
    else:
        candidate_usable = True

    # 4. 执行通用范围检查与 Dataset 规则：共享同一批 Candidate 和原始 Scope。
    if candidate_usable:
        # 数据载体：临时表同时供本次验收及 Publisher 使用，生命周期由 Runner 管理。
        with registered(con, "_candidate_arrow", candidate) as source:
            con.execute(f"CREATE OR REPLACE TEMP TABLE candidate AS SELECT * FROM {source}")
        key = dataset.unit_key_sql("c")
        # 业务检查：每条记录必须属于 IN_SCOPE；越界或归属为空均产生全局 ERROR。
        count = con.execute(f"SELECT count(*) FROM {relation('candidate')} c LEFT JOIN _scope_units s "
                            f"ON ({key})=s.unit_key WHERE s.unit_key IS NULL").fetchone()[0]
        if count:
            checks.append(Check("ERROR", "candidate.scope", "validation", None, {"rows": count}))
        # 业务短路：仅在获取/准备阶段已全部阻断时跳过业务规则；范围错误不省略可执行诊断。
        if blocked != known:
            for check in dataset.validate_candidate(con, "candidate", scope):
                check = _normalize_check(check, known)
                # 诊断保留政策：沿用仅影响此前已阻断范围的 ERROR 不再保留的行为。
                # 这不是按规则去重；WARNING、新受影响范围及全局问题仍保留。
                if check.level == "ERROR" and check.units is not None and set(check.units) <= blocked:
                    continue
                checks.append(check)

    # 5. 汇总发布许可：中间 blocked 只决定短路和诊断保留，最终许可包含全部验收 ERROR。
    blocked = blocked_units(known, checks)
    # 发布许可按 Unit 显式给出；WARNING 不移出许可集合，阻断 Unit 的记录留在 Candidate 供诊断。
    return ValidationResult(tuple(u for u in scope.units if u not in blocked),
                            tuple(u for u in scope.units if u in blocked), tuple(checks))
