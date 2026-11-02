"""只读探查银河 EXTRA_INDEX_A 候选池的指数成分支持名单；不写正式数据库。"""

import argparse
import csv
from datetime import date, datetime
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import threading
import time
from zoneinfo import ZoneInfo

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parents[1]
FUNCTION_ID = "A010200002"
PROXY_NAMES = (
    "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY",
    "http_proxy", "https_proxy", "all_proxy", "no_proxy",
)
SECRETS = set()


def timestamp():
    return datetime.now(ZoneInfo("Asia/Shanghai")).isoformat()


def json_safe(value):
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if hasattr(value, "item"):
        try:
            return json_safe(value.item())
        except (TypeError, ValueError):
            pass
    return str(value)


def redact(value):
    text = str(value)
    for secret in SECRETS:
        if secret:
            text = text.replace(secret, "<redacted>")
    return text


def write_json(path, payload, *, compact=False):
    text = json.dumps(
        json_safe(payload), ensure_ascii=False, allow_nan=False,
        indent=None if compact else 2,
        separators=(",", ":") if compact else None,
    )
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(redact(text) + "\n", encoding="utf-8")
    temporary.replace(path)


def frame_records(frame):
    records = []
    for index, row in frame.iterrows():
        record = {str(key): json_safe(value) for key, value in row.to_dict().items()}
        record["__index"] = json_safe(index)
        records.append(record)
    return records


def response_path(output, code):
    return output / "responses" / f"{code.replace('.', '_')}.json"


def query_constituent(tgw, code, timeout):
    finished = threading.Event()
    state = {}

    def callback(result, status):
        state["result"], state["status"] = result, status
        finished.set()

    task_id = tgw.GetTaskID()
    started = time.monotonic()
    # 第三方资讯请求必须显式指定成分功能号和单一指数代码。
    param_status = [
        tgw.SetThirdInfoParam(task_id, "function_id", FUNCTION_ID),
        tgw.SetThirdInfoParam(task_id, "index_code", code),
    ]
    accepted, immediate_error = tgw.QueryThirdInfo(
        task_id, query_spi=callback, return_df_format=False,
    )
    if accepted is not True:
        result, status = None, None
    elif not finished.wait(timeout):
        raise TimeoutError(f"constituent callback exceeded {timeout}s for {code}")
    else:
        result, status = state.get("result"), state.get("status")
    records = result if isinstance(result, list) else []
    fields = sorted({str(key) for row in records if isinstance(row, dict) for key in row})
    response = {
        "accepted": accepted,
        "immediate_error": immediate_error,
        "callback_status": status,
        "error_message": tgw.GetErrorMsg(status) if isinstance(status, int) else None,
        "elapsed_seconds": round(time.monotonic() - started, 3),
        "return_type": type(result).__name__,
        "rows": len(records),
        "fields": fields,
        "records": records,
    }
    return task_id, param_status, response


def summarize_response(code, response):
    records = response["records"] if isinstance(response["records"], list) else []
    rows = [row for row in records if isinstance(row, dict)]
    keys = [(row.get("INDEX_CODE"), row.get("CON_CODE"), row.get("CON_INDATE")) for row in rows]
    returned_codes = sorted({str(row.get("INDEX_CODE")) for row in rows})
    names = sorted({str(row.get("INDEXNAME")) for row in rows if row.get("INDEXNAME") is not None})
    status = response["callback_status"]
    if response["accepted"] is not True:
        outcome = "REQUEST_REJECTED"
    elif isinstance(status, int) and status == -76:
        outcome = "NO_DATA"
    elif isinstance(status, int) and status != 0:
        outcome = "PROVIDER_ERROR"
    elif rows:
        outcome = "RETURNED"
    else:
        outcome = "EMPTY"
    # 返回其他指数的记录属于身份异常，不能纳入该代码的支持名单。
    mismatches = sum(row.get("INDEX_CODE") != code for row in rows)
    if rows and mismatches:
        outcome = "IDENTITY_MISMATCH"
    return {
        "code": code,
        "outcome": outcome,
        "index_names": names,
        "rows": len(rows),
        "current_rows": sum(row.get("CURRENT_SIGN") == 1 for row in rows),
        "historical_rows": sum(row.get("CURRENT_SIGN") == 0 for row in rows),
        "other_current_sign_rows": sum(row.get("CURRENT_SIGN") not in (0, 1) for row in rows),
        "duplicate_business_keys": len(keys) - len(set(keys)),
        "returned_index_codes": returned_codes,
        "identity_mismatch_rows": mismatches,
        "callback_status": status,
        "error_message": response["error_message"],
        "accepted": response["accepted"],
        "immediate_error": response["immediate_error"],
        "elapsed_seconds": response["elapsed_seconds"],
    }


def write_csv(path, rows, fieldnames):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({
                key: "|".join(value) if isinstance(value, list) else value
                for key, value in row.items()
            })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="新证据目录；配合 --resume 可续跑")
    parser.add_argument("--resume", action="store_true", help="跳过已有且身份匹配的逐指数响应")
    parser.add_argument("--query-timeout", type=float, default=30.0)
    parser.add_argument("--request-interval", type=float, default=0.2)
    args = parser.parse_args()
    if args.query_timeout <= 0 or args.request_interval < 0:
        parser.error("query timeout must be positive and request interval nonnegative")

    output = (args.output or ROOT / "data/probes" / datetime.now().strftime(
        "galaxy-index-constituent-coverage-%Y%m%d-%H%M%S",
    )).resolve()
    if args.resume:
        if not output.is_dir():
            parser.error("--resume requires an existing output directory")
    else:
        output.mkdir(parents=True, exist_ok=False)
    (output / "responses").mkdir(exist_ok=True)
    work = output / ".work"
    work.mkdir(exist_ok=True)

    values = dotenv_values(ROOT / ".env")
    username = (values.get("AD_USERNAME") or "").strip()
    password = (values.get("AD_PASSWORD") or "").strip()
    host = (values.get("AD_HOST") or "101.230.159.235").strip()
    port = int(values.get("AD_PORT") or "8600")
    if not username or not password:
        raise RuntimeError("AD_USERNAME and AD_PASSWORD are required")
    SECRETS.update((username, password))
    removed_proxy = [name for name in PROXY_NAMES if os.environ.pop(name, None) is not None]

    manifest_path = output / "run_manifest.json"
    if args.resume and manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        if manifest.get("purpose") != "Galaxy EXTRA_INDEX_A constituent coverage":
            raise ValueError("Refusing to resume an unrelated probe")
        manifest["resumed_at"] = timestamp()
    else:
        manifest = {
            "started_at": timestamp(),
            "purpose": "Galaxy EXTRA_INDEX_A constituent coverage",
            "function_id": FUNCTION_ID,
            "security_type": "EXTRA_INDEX_A",
            "host": host,
            "port": port,
            "force_logout": False,
            "query_timeout_seconds": args.query_timeout,
            "request_interval_seconds": args.request_interval,
            "proxy_environment_removed": removed_proxy,
            "database_written": False,
        }
    write_json(manifest_path, manifest)

    original_cwd = Path.cwd()
    os.chdir(work)
    tgw = None
    session_open = False
    try:
        import AmazingData as ad
        import tgw as tgw_module
        from AmazingData.login.tgw_login import set_cfg

        tgw = tgw_module
        manifest["sdk"] = {
            "AmazingData": ad.__version__,
            "tgw_runtime": tgw.GetVersion(),
            "python": os.sys.version.split()[0],
        }
        cfg, api_mode, log_spi = set_cfg(username, password, host, port, force_logout=False)
        started = time.monotonic()
        session_open = bool(tgw.Login(cfg, api_mode))
        write_json(output / "login.json", {
            "success": session_open,
            "elapsed_seconds": round(time.monotonic() - started, 3),
            "max_limitation_observed": bool(log_spi.max_limitation),
            "force_logout": bool(cfg.force_logout),
        })
        if not session_open:
            manifest["status"] = "LOGIN_FAILED"
            return 1

        candidates_path = output / "candidates.json"
        if args.resume and candidates_path.exists():
            candidates_payload = json.loads(candidates_path.read_text(encoding="utf-8"))
        else:
            base = ad.BaseData()
            # 官方手册指定的候选池；代码列表与基础信息分别保留，不用成分响应反推候选全集。
            raw_codes = base.get_code_list(security_type="EXTRA_INDEX_A")
            codes = sorted(set(raw_codes))
            if not codes or any(not isinstance(code, str) or not code for code in codes):
                raise ValueError("Invalid EXTRA_INDEX_A candidate codes")
            info = base.get_code_info(security_type="EXTRA_INDEX_A")
            info_records = frame_records(info)
            write_json(output / "index_info.json", {
                "columns": [str(column) for column in info.columns],
                "rows": len(info_records),
                "records": info_records,
            }, compact=True)
            candidates_payload = {
                "security_type": "EXTRA_INDEX_A",
                "rows": len(raw_codes),
                "unique_rows": len(codes),
                "codes": codes,
            }
            write_json(candidates_path, candidates_payload)
        codes = candidates_payload["codes"]
        manifest["candidate_rows"] = len(codes)

        for position, code in enumerate(codes, 1):
            path = response_path(output, code)
            if args.resume and path.exists():
                try:
                    old = json.loads(path.read_text(encoding="utf-8"))
                    if old["request"]["index_code"] == code and old["request"]["function_id"] == FUNCTION_ID:
                        print(f"[{position}/{len(codes)}] {code} resumed", flush=True)
                        continue
                except (OSError, ValueError, KeyError, TypeError):
                    pass
            evidence = {
                "request": {"function_id": FUNCTION_ID, "index_code": code},
                "requested_at": timestamp(),
            }
            try:
                task_id, param_status, response = query_constituent(tgw, code, args.query_timeout)
                evidence.update({
                    "task_id": task_id,
                    "set_param_status": param_status,
                    "response": response,
                    "summary": summarize_response(code, response),
                })
            except Exception as exc:
                evidence["summary"] = {
                    "code": code,
                    "outcome": "CLIENT_ERROR",
                    "error": f"{type(exc).__name__}: {redact(exc)}",
                }
            write_json(path, evidence, compact=True)
            summary = evidence["summary"]
            print(
                f"[{position}/{len(codes)}] {code} {summary['outcome']} "
                f"rows={summary.get('rows', '')} name={'|'.join(summary.get('index_names', []))}",
                flush=True,
            )
            time.sleep(args.request_interval)

        summaries = []
        for code in codes:
            path = response_path(output, code)
            if not path.exists():
                summaries.append({"code": code, "outcome": "MISSING_RESPONSE"})
                continue
            evidence = json.loads(path.read_text(encoding="utf-8"))
            if evidence["request"] != {"function_id": FUNCTION_ID, "index_code": code}:
                summaries.append({"code": code, "outcome": "INVALID_EVIDENCE"})
                continue
            summaries.append(evidence["summary"])
        supported = [row for row in summaries if row["outcome"] == "RETURNED" and row.get("rows", 0) > 0]
        no_data = [row for row in summaries if row["outcome"] in ("NO_DATA", "EMPTY")]
        errors = [row for row in summaries if row not in supported and row not in no_data]
        fields = [
            "code", "index_names", "outcome", "rows", "current_rows", "historical_rows",
            "other_current_sign_rows", "duplicate_business_keys", "returned_index_codes",
            "identity_mismatch_rows", "callback_status", "error_message", "accepted",
            "immediate_error", "elapsed_seconds", "error",
        ]
        write_csv(output / "supported.csv", supported, fields)
        write_csv(output / "no_data.csv", no_data, fields)
        write_csv(output / "errors.csv", errors, fields)
        target_codes = ("000300.SH", "000905.SH", "000852.SH", "932000.CSI")
        summary = {
            "candidate_scope": "EXTRA_INDEX_A",
            "candidates": len(codes),
            "responses": len(codes) - sum(row["outcome"] == "MISSING_RESPONSE" for row in summaries),
            "supported": len(supported),
            "no_data": len(no_data),
            "errors": len(errors),
            "supported_with_duplicate_business_keys": sum(row["duplicate_business_keys"] > 0 for row in supported),
            "supported_with_unexpected_current_sign": sum(row["other_current_sign_rows"] > 0 for row in supported),
            "target_indices": {code: next((row for row in summaries if row["code"] == code), None) for code in target_codes},
            "supported_sha256": hashlib.sha256((output / "supported.csv").read_bytes()).hexdigest(),
        }
        write_json(output / "summary.json", summary)
        manifest["status"] = "COMPLETED" if not errors else "COMPLETED_WITH_ERRORS"
        manifest["summary"] = summary
        return int(bool(errors))
    except Exception as exc:
        manifest["status"] = "FATAL_ERROR"
        manifest["fatal_error"] = f"{type(exc).__name__}: {redact(exc)}"
        return 1
    finally:
        if tgw is not None and session_open:
            try:
                tgw.Close()
                manifest["session_closed"] = True
            except Exception as exc:
                manifest["session_close_error"] = f"{type(exc).__name__}: {redact(exc)}"
        manifest["finished_at"] = timestamp()
        write_json(manifest_path, manifest)
        os.chdir(original_cwd)
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
