"""兼容 Tushare 协议的 ProMax 数据源。"""

from .client import AuthenticationError, QueryResult, TransportError, TushareClient, TushareError

__all__ = ["AuthenticationError", "QueryResult", "TransportError", "TushareClient", "TushareError"]
