"""跨模块控制模型；业务记录保持为独立的列式 DataFrame。"""

from dataclasses import dataclass, fields, is_dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any, Literal, Protocol

import polars as pl


@dataclass(frozen=True, slots=True)
class ScopeReason:
    """一项排除或未知范围及其原因。"""
    unit_key: str
    reason: str


@dataclass(frozen=True, slots=True)
class Scope:
    """统一业务范围；各决策集合互斥，Unit 标识唯一且非空。"""
    units: tuple[str, ...]
    out_of_scope: tuple[ScopeReason, ...] = ()
    unknown: tuple[ScopeReason, ...] = ()

    def __post_init__(self):
        """在构造处检查范围结构，避免请求和验收消费冲突决策。"""
        # 业务检查：同一 Unit 只能有一个范围决策，不能同时属于纳入、排除或未知集合。
        # 空白标识无法对应业务范围；这里只验证，不自动 trim 后改变 Unit 身份。
        names = self.units + tuple(x.unit_key for x in self.out_of_scope + self.unknown)
        if any(not isinstance(x, str) or not x.strip() for x in names) or len(names) != len(set(names)):
            raise ValueError("Scope units must be nonempty strings with disjoint decisions")


@dataclass(frozen=True, slots=True)
class DependencyRef:
    """所需上游业务范围。"""
    dataset: str
    unit_key: str


@dataclass(frozen=True, slots=True)
class DependencyPin:
    """Scope 实际使用的成功版本。"""
    ref: DependencyRef
    publication_id: int


@dataclass(frozen=True, slots=True)
class Check:
    """一项检查；units=None 明确表示影响整个业务范围。"""
    level: Literal["ERROR", "WARNING"]
    rule: str
    stage: str
    units: tuple[str, ...] | None = None
    detail: dict[str, Any] | None = None

    def __post_init__(self):
        """验证控制信息，空影响集合应由调用者省略该检查。"""
        # 业务检查：ERROR 阻断、WARNING 仅告警；None 表示全局，空 tuple 不表示“无影响”。
        if self.level not in ("ERROR", "WARNING") or self.units == ():
            raise ValueError("Invalid check level or empty affected units; use None for global checks")


@dataclass(frozen=True, slots=True)
class RequestAttempt:
    """一次 HTTP 尝试的控制信息，不包含响应业务行。"""
    request_id: str
    attempt: int
    api_name: str
    params: dict[str, Any]
    fields: tuple[str, ...]
    requested_at: datetime
    elapsed_seconds: float
    http_status: int | None = None
    provider_code: int | None = None
    returned_fields: tuple[str, ...] = ()
    row_count: int | None = None
    has_more: bool | None = None
    error: str | None = None
    evidence_path: str | None = None


@dataclass(frozen=True, slots=True)
class UnitDelivery:
    """获取状态独立于行数、转换与验收；COMPLETE 仅表示请求及终止条件完成。"""
    unit_key: str
    state: Literal["COMPLETE", "FAILED", "INCOMPLETE"]
    request_ids: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Delivery:
    """Raw 与获取证据；构表失败也可无 frame，但必须通过 checks 保留明确问题。"""
    frame: pl.DataFrame | None
    units: tuple[UnitDelivery, ...]
    attempts: tuple[RequestAttempt, ...] = ()
    checks: tuple[Check, ...] = ()


@dataclass(frozen=True, slots=True)
class ValidationResult:
    """显式发布许可，合法零行 Unit 也可以出现在 passed_units 中。"""
    passed_units: tuple[str, ...]
    blocked_units: tuple[str, ...]
    checks: tuple[Check, ...]


@dataclass(frozen=True, slots=True)
class Publication:
    """已提交的成功发布版本。"""
    unit_key: str
    publication_id: int
    row_count: int


@dataclass(frozen=True, slots=True)
class UnitResult:
    """本次 Unit 的执行结局。"""
    unit_key: str
    status: str
    stage: str


@dataclass(frozen=True, slots=True)
class RunResult:
    """运行结局和独立的产物状态，不将导出失败混为发布回滚。"""
    run_id: str
    status: str
    units: tuple[UnitResult, ...]
    publications: tuple[Publication, ...]
    error: str | None = None
    artifact_dir: str | None = None
    artifact_status: str = "ACTIVE"
    artifacts_complete: bool | None = None
    export_errors: tuple[dict[str, str], ...] = ()


@dataclass(frozen=True, slots=True)
class RunOptions:
    """本轮实现支持单一内存批次与显式产物保留。"""
    landing_root: Path = Path("data/landing")
    keep_artifacts: bool = False


class Dataset(Protocol):
    """业务契约接口；Unit 的 SQL 归属表达式由 Dataset 定义。"""
    name: str
    table: str
    schema: pl.Schema
    ddl: str
    record_key: tuple[str, ...]

    def dependencies(self, params: dict) -> tuple[DependencyRef, ...]:
        """声明本次目标需要的上游成功范围。"""
        ...

    def resolve_scope(self, params: dict, pins: tuple[DependencyPin, ...], con: Any) -> Scope:
        """依据固定的上游版本与实际读取确定统一业务范围。"""
        ...

    def unit_key_sql(self, alias: str) -> str:
        """定义一条业务记录归属的 Unit，供通用批量关系操作使用。"""
        ...

    def validate_candidate(self, con: Any, candidate_relation: str, scope: Scope) -> tuple[Check, ...]:
        """对标准化业务关系执行 Dataset 专属集合验收。"""
        ...


class SourceAdapter(Protocol):
    """供应商适配接口；数据始终以 DataFrame 传递。"""
    def fetch(self, scope: Scope, artifacts: Any) -> Delivery:
        """组织供应商请求，交付原始列式批次与独立控制结果。"""
        ...

    def prepare(self, delivery: Delivery, scope: Scope) -> tuple[pl.DataFrame | None, tuple[Check, ...]]:
        """集中核对源记录并标准化；返回 Candidate 与诊断，不产生发布许可。"""
        ...


def control_data(value: Any) -> Any:
    """仅序列化控制信息，明确拒绝展开 DataFrame，保留 JSON 可读性。"""
    # 序列化边界：业务行独立写列式产物，避免被审计递归展开成巨大的 JSON。
    if isinstance(value, pl.DataFrame):
        raise TypeError("Serialize DataFrames as columnar artifacts, not control JSON")
    if isinstance(value, Delivery):
        # 交付控制只保留状态、尝试、检查和行数；None 表示无可处理表，0 表示显式空表。
        return {"units": control_data(value.units), "attempts": control_data(value.attempts),
                "checks": control_data(value.checks), "rows": value.frame.height if value.frame is not None else None}
    if is_dataclass(value) and not isinstance(value, type):
        # 控制模型递归转成 JSON 可读结构；tuple 保持业务集合含义，落盘表示为数组。
        return {f.name: control_data(getattr(value, f.name)) for f in fields(value)}
    if isinstance(value, dict):
        return {k: control_data(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [control_data(x) for x in value]
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return value
