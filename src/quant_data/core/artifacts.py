"""显式阶段产物导出，诊断副本与发布结局分别记录。"""

from dataclasses import replace
import hashlib
import json
from pathlib import Path
import shutil

import polars as pl

from quant_data.core.models import control_data
from quant_data.db import relation

META_TABLES = ("run", "run_unit", "run_dependency", "request_attempt", "validation_check", "publication", "coverage")


class Artifacts:
    """原始尝试即时留档；可选完整阶段导出，错误不污染已经提交的业务结果。"""

    def __init__(self, root, keep):
        """每个 Run 使用新目录，拒绝覆盖旧产物。"""
        self.root = Path(root)
        self.keep = keep
        self.errors = []
        self.status = "ACTIVE"
        self.complete = None
        self.seen = set()
        self.root.mkdir(parents=True, exist_ok=False)

    def _json(self, path, value):
        """控制 JSON 显式序列化；原始信封已经由 Client 脱敏。"""
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(control_data(value), ensure_ascii=False, indent=2) + "\n")

    def record_attempt(self, attempt, payload):
        """原始证据写失败时抛出异常，在发布前明确阻断获取。"""
        # 同一逻辑请求的每次重试各写一个文件；即时留证不受 keep 开关影响，成功后再决定清理。
        path = self.root / "requests" / f"{attempt.request_id}-attempt-{attempt.attempt:02d}.json"
        updated = replace(attempt, evidence_path=str(path))
        self._json(path, {"attempt": updated, "response": payload})
        return updated

    def capture(self, stage, value):
        """保留模式下导出控制或 DataFrame，诊断故障独立收集。"""
        if not self.keep:
            return
        try:
            if isinstance(value, pl.DataFrame):
                # 业务批次按列式 Parquet 留存，CSV 方便人工阅读，Schema 文件明确记录类型。
                value.write_parquet(self.root / f"{stage}.parquet")
                value.write_csv(self.root / f"{stage}.csv")
                self._json(self.root / f"{stage}.schema.json", {k: str(v) for k, v in value.schema.items()})
            else:
                # 控制对象单独写 JSON；Delivery 的序列化只含控制字段和行数，不含业务行。
                self._json(self.root / f"{stage}.json", value)
            self.seen.add(stage)
        except Exception as exc:
            # 可选诊断导出失败只影响产物完整性，不能改变获取、验收或发布的业务结局。
            self.errors.append({"stage": stage, "error": str(exc)})

    def snapshot(self, con, stage, dataset=None):
        """显式阶段导出七张 Meta；发布后可同时导出真实 Canonical。"""
        if not self.keep:
            return
        try:
            folder = self.root / "stages" / stage
            folder.mkdir(parents=True, exist_ok=True)
            names = [(f"meta.{t}", t) for t in META_TABLES]
            if dataset is not None:
                # 发布阶段额外读取已提交的 Canonical，便于与 Candidate 和成功版本做对照。
                names.append((dataset.table, "canonical"))
            for name, label in names:
                con.execute(f"COPY (SELECT * FROM {relation(name)}) TO ? (FORMAT PARQUET)", [str(folder / f"{label}.parquet")])
            self.seen.add(f"meta:{stage}")
        except Exception as exc:
            self.errors.append({"stage": stage, "error": str(exc)})

    def finalize(self, result):
        """确定保留结果；生产成功清理，诊断模式准备导航和完成状态。"""
        # 保留规则：仅 SUCCESS 且未显式保留时删除当前 Run 目录；FAILED/PARTIAL 留下已有证据。
        # 此处不扫描历史目录，也不按文件年龄清理；运行时崩溃遗留目录不会自动回收。
        if result.status == "SUCCESS" and not self.keep:
            try:
                shutil.rmtree(self.root)
                self.status = "CLEANED"
            except OSError as exc:
                # 清理发生在发布提交之后；失败单独标记，不能撤销成功的业务数据和版本。
                self.status = "CLEANUP_FAILED"
                self.errors.append({"stage": "cleanup", "error": str(exc)})
            return
        self.status = "RETAINED"
        if not self.keep:
            # 失败运行的默认模式仅保留已产生的证据，不宣称导出了完整 walkthrough。
            return
        # 完整性检查：保留模式必须成功且关键阶段产物齐全；目录存在本身不代表诊断完整。
        required = {"scope", "delivery", "landing", "candidate", "validation", "publication", "meta:published"}
        self.complete = result.status == "SUCCESS" and not self.errors and required <= self.seen
        try:
            (self.root / "README.md").write_text(
                "# stock_basic V3.2 walkthrough\n\n"
                f"Run：{result.run_id}；执行状态：{result.status}。\n\n"
                "按 scope.json → requests/ → delivery.json → landing.parquet → candidate.parquet → "
                "validation.json → publication.json → result.json 阅读。\n\n"
                "DataFrame 的 CSV 和 Schema JSON 同时保留。stages/scope、validation、published、finished "
                "保存各阶段七张 Meta 表；published/canonical.parquet 是本次发布后的实际业务数据。\n\n"
                "原始信封仅在请求时写入；处理流程直接使用内存 DataFrame。manifest.json 标明诊断是否完整、"
                "错误和文件 SHA-256。此目录属于当前 Run，旧样本保持原状。\n")
        except Exception as exc:
            self.complete = False
            self.errors.append({"stage": "readme", "error": str(exc)})

    def seal(self, result):
        """最终元数据快照后写结果和清单，清单中的完整性与错误是诊断最终结论。"""
        if not self.keep:
            return
        self.capture("result", result)
        # 最终状态、Meta 快照和之前阶段均成功，才声明完整；结果与 manifest 共同解释导出故障。
        self.complete = bool(self.complete and not self.errors and "result" in self.seen and "meta:finished" in self.seen)
        try:
            # 为当前已导出的文件记录大小和摘要，方便人工核对产物一致性；清单不对自身做摘要。
            files = [{"path": str(p.relative_to(self.root)), "bytes": p.stat().st_size,
                      "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in sorted(self.root.rglob("*")) if p.is_file()]
            self._json(self.root / "manifest.json", {"run_id": result.run_id, "complete": self.complete, "errors": self.errors, "files": files})
        except Exception as exc:
            self.complete = False
            self.errors.append({"stage": "manifest", "error": str(exc)})
