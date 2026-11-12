"""Tushare 质量处置的少量共同审计函数；不决定处置政策或发布许可。"""

import hashlib

from quant_data.core.models import Check


def disposition_check(delivery, *, rule, unit, policy, action, source_rule, records):
    """为一次 Unit 级政策处置附加最终成功 Attempt 的证据身份。"""
    request_ids = {r["request_id"] for r in records}
    attempts = {}
    for attempt in delivery.attempts:
        if attempt.request_id in request_ids and attempt.error is None:
            # 处置溯源必须指向实际返回该 Raw 的唯一成功 Attempt，不能引用已失败重试。
            if attempt.request_id in attempts or attempt.evidence_path is None:
                raise ValueError("Disposition requires one successful recorded attempt per source request")
            with open(attempt.evidence_path, "rb") as stream:
                digest = hashlib.file_digest(stream, "sha256").hexdigest()
            attempts[attempt.request_id] = (attempt, digest)
    missing = request_ids - set(attempts)
    if missing:
        raise ValueError(f"Disposition source attempts are missing: {sorted(missing)}")
    traced = []
    for record in records:
        attempt, digest = attempts[record["request_id"]]
        traced.append({**record, "attempt": attempt.attempt, "evidence_sha256": digest})
    disposition = {"policy": policy, "policy_version": "v1", "action": action,
                   "source_rule": source_rule, "records": traced}
    return Check("WARNING", rule, "prepare", (unit,), {"disposition": disposition})
