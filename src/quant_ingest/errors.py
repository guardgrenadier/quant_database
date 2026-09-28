"""来源适配器与 pipeline 共用的契约异常。"""


class SourceContractError(ValueError):
    """携带违约响应原文，供隔离区保留证据。"""

    def __init__(self, message: str, body: bytes) -> None:
        """记录错误描述和单页原文，不包含请求密钥。"""

        super().__init__(message)
        self.body = body
