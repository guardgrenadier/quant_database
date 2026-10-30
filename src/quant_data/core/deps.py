"""上游版本使用批量关系查询，并在 Scope 事务与发布事务中分别检查。"""

from quant_data.core.models import DependencyPin, DependencyRef
from quant_data.db import registered, strings_frame


def check(con, required):
    """批量确认所有所需范围已发布；缺失时阻断，空真实依赖直接返回空集合。"""
    # 无依赖就是空 pin 集合，不制造虚构的上游版本；stock_basic 走这个分支。
    if not required:
        return ()
    # 业务检查：同一上游范围只能声明一次，保证 pin 与需求一一对应。
    if len(set(required)) != len(required):
        raise ValueError("Duplicate dependency requirements")
    frame = strings_frame([(d.dataset, d.unit_key) for d in required], ("dataset", "unit_key"))
    with registered(con, "_dependency_refs", frame) as refs:
        # 业务检查：仅 PUBLISHED Coverage 可作为依赖；PENDING/FAILED/UNKNOWN 均不能提供版本。
        rows = con.execute(f"SELECT r.dataset,r.unit_key,c.publication_id FROM {refs} r "
                           "LEFT JOIN meta.coverage c ON r.dataset=c.dataset AND r.unit_key=c.unit_key AND c.status='PUBLISHED'").fetchall()
    missing = [(d, u) for d, u, p in rows if p is None]
    if missing:
        raise ValueError(f"Dependency not published: {missing}")
    by_ref = {DependencyRef(d, u): p for d, u, p in rows}
    # 业务转换：保存当前实际使用的 publication_id，后续发布据此复核，审计可追溯。
    return tuple(DependencyPin(ref, by_ref[ref]) for ref in required)


def recheck(con, pins):
    """在提交事务内复核所有固定版本，变化或消失均阻断发布。"""
    # 业务检查：范围解析后上游若被修订，旧 Scope 不得继续发布，必须重新解析范围。
    if pins and check(con, tuple(pin.ref for pin in pins)) != pins:
        raise ValueError("Dependency changed; resolve scope again")
