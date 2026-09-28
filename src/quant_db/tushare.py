"""旧导入路径兼容层；实际实现位于 :mod:`quant_db.sources.tushare`。"""

from quant_db.sources.tushare.client import (
    AuthenticationError,
    QueryResult,
    RequestRecord,
    TransportError,
    TushareClient,
    TushareError,
)

__all__ = [
    "AuthenticationError",
    "QueryResult",
    "RequestRecord",
    "TransportError",
    "TushareClient",
    "TushareError",
]
