"""两类日线共享的严格源类型解码，不组织请求或决定业务范围。"""

import polars as pl
import pyarrow as pa


def raw_frame(data, fields):
    """按字段名构造明确 Arrow 类型；证券/日期不数值化，数值拒绝 bool 和字符串。"""
    arrays = []
    for field in fields:
        index = data["fields"].index(field)
        values = [r[index] for r in data["items"]]
        if field in ("ts_code", "trade_date"):
            arrays.append(pa.array(values, type=pa.string()))
        else:
            # 外部数值边界：禁止隐式类型修复；非有限数保留给 Dataset 明确阻断。
            if any(v is not None and type(v) not in (int, float) for v in values):
                raise ValueError(f"{field} must be a JSON number or NULL, excluding bool")
            arrays.append(pa.array(values, type=pa.float64()))
    return pl.from_arrow(pa.Table.from_arrays(arrays, names=fields))
