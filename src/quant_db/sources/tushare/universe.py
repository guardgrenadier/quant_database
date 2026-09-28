"""分钟 ingestion 使用的窄日历、证券集合与停牌信息依赖。"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

from .client import RequestRecord, TushareClient, TushareError

STOCK_FIELDS = "ts_code,symbol,name,exchange,market,list_date"


class TushareUniverse:
    """通过 Tushare 提供请求计划所需的交易日和证券快照。"""

    def __init__(self, client: TushareClient) -> None:
        """保存共享 client，并累计计划阶段的实际请求记录。"""

        self.client = client
        self.requests: list[RequestRecord] = []

    def is_trade_day(self, day: date) -> bool:
        """查询上交所日历，确认指定日期是否为开市日。"""

        rows = self._rows(
            "trade_cal",
            exchange="SSE",
            start_date=day.strftime("%Y%m%d"),
            end_date=day.strftime("%Y%m%d"),
            fields="exchange,cal_date,is_open,pretrade_date",
            limit=10,
        )
        return any(str(row.get("cal_date")) == day.strftime("%Y%m%d") and int(row.get("is_open", 0)) == 1 for row in rows)

    def latest_closed_trade_day(self, now: datetime | None = None) -> date:
        """返回上海时间最近已收盘的开市日，15:30 前不选择当天。"""

        now = now or datetime.now(ZoneInfo("Asia/Shanghai"))
        # 留出半小时缓冲；这里只判断“已收盘”，不承诺供应商已经完成更新。
        latest_candidate = now.date() if now.time() >= time(15, 30) else now.date() - timedelta(days=1)
        rows = self._rows(
            "trade_cal",
            exchange="SSE",
            start_date=(latest_candidate - timedelta(days=45)).strftime("%Y%m%d"),
            end_date=latest_candidate.strftime("%Y%m%d"),
            fields="exchange,cal_date,is_open,pretrade_date",
            limit=100,
        )
        days = [
            datetime.strptime(str(row["cal_date"]), "%Y%m%d").date()
            for row in rows
            if int(row.get("is_open", 0)) == 1
        ]
        if not days:
            raise ValueError("trade_cal returned no closed trading day")
        return max(days)

    def listed_stocks(self) -> list[dict]:
        """分页取得当前 L 股票快照，并拒绝缺失或重复的证券代码。"""

        rows: list[dict] = []
        seen: set[str] = set()
        # 5000 是已验证的单页大小；20 页只是防止异常分页无限进行。
        for page in range(20):
            page_rows = self._rows(
                "stock_basic",
                list_status="L",
                fields=STOCK_FIELDS,
                limit=5000,
                offset=page * 5000,
            )
            codes = {str(row.get("ts_code")) for row in page_rows}
            if any(not row.get("ts_code") for row in page_rows) or len(codes) != len(page_rows):
                raise ValueError("stock_basic page has missing or duplicate codes")
            if codes & seen:
                raise ValueError("stock_basic pagination returned duplicate codes")
            rows.extend(page_rows)
            seen.update(codes)
            if len(page_rows) < 5000:
                return rows
        raise ValueError("stock_basic pagination exceeded 20 pages")

    def suspensions(self, day: date) -> list[dict]:
        """取得指定日停牌记录，仅供 manifest 交叉记录，不改变切片判定。"""

        return self._rows(
            "suspend_d",
            trade_date=day.strftime("%Y%m%d"),
            suspend_type="S",
            fields="ts_code,trade_date,suspend_type",
            limit=5000,
            offset=0,
        )

    def _rows(self, api_name: str, **params) -> list[dict]:
        """执行计划端点并累计请求证据；这些端点不接受 pending。"""

        try:
            result = self.client.query(api_name, **params)
        except TushareError as exc:
            self.requests.extend(exc.requests)
            raise
        self.requests.extend(result.requests)
        if result.status == "pending":
            raise TushareError(f"{api_name}: unexpected pending result", requests=result.requests)
        return result.rows
