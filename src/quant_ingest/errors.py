"""来源适配器与 pipeline 共用的契约异常。"""


class SourceContractError(ValueError):
    """携带违约响应原文，供隔离区保留证据。"""

    def __init__(self, message: str, body: bytes, requests: list[dict] | None = None) -> None:
        """记录错误描述和单页原文，不包含请求密钥。"""

        super().__init__(message)
        self.body = body
        self.requests = requests or []


class SourceRequestError(RuntimeError):
    """携带失败前 HTTP 尝试记录的传输或网关错误。"""

    def __init__(self, message: str, requests: list[dict]) -> None:
        """记录失败原因和脱敏请求摘要。"""

        super().__init__(message)
        self.requests = requests
