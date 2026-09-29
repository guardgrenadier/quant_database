"""将曾隔离后成功的切片作为长期质量告警保留。"""

from __future__ import annotations

import hashlib
import json
from datetime import date

from .ledger import Ledger
from .raw import RawStore


def record_quarantine_self_healed(ledger: Ledger, raw: RawStore, dataset: str, day: date) -> int:
    """按持久证据生成稳定告警键，发布重跑不会重复写入同一事件。"""

    if not ledger.complete(dataset, day):
        return 0  # 网盘历史从 temp 入场，没有 API 切片账。
    completed = {code for code, *_ in ledger.completed_slices(dataset, day)}
    directory = raw.root.parent / "quarantine" / dataset / f"trade_date={day:%Y%m%d}"
    if not directory.is_dir():
        return 0
    findings: list[dict] = []
    for metadata in sorted(directory.glob("*.meta.json")):
        response = metadata.with_name(metadata.name.removesuffix(".meta.json") + ".response")
        if not response.is_file():
            continue
        value = json.loads(metadata.read_text(encoding="utf-8"))
        code = value.get("slice_key")
        if code in completed:
            findings.append({
                "ts_code": code if dataset == "stk_mins" else None,
                "slice_key": code, "trade_date": day.isoformat(),
                "reason": value.get("reason"), "evidence_path": str(response),
            })
    # 兼容此前只有 .response、没有元数据的证据；摘要明确标为待人工核查。
    known = {item["evidence_path"] for item in findings}
    for response in sorted(directory.glob("*.response")):
        if str(response) in known:
            continue
        code = next((key for key in completed if
                     dataset != "stk_mins" or response.name.endswith(f"-{key.replace('.', '_')}.response")), None)
        if code:
            findings.append({
                "ts_code": code if dataset == "stk_mins" else None,
                "slice_key": code, "trade_date": day.isoformat(),
                "reason": "旧版未登记摘要，请检查原始隔离响应", "evidence_path": str(response),
            })
    if not findings:
        return 0
    evidence = "\n".join(sorted(item["evidence_path"] for item in findings))
    stable_run = f"quarantine-healed-{day:%Y%m%d}-{hashlib.sha256(evidence.encode()).hexdigest()[:16]}"
    ledger.record_finding(stable_run, dataset, day, "quarantine_self_healed", "warn",
                          len({item["slice_key"] for item in findings}), findings)
    return len(findings)
