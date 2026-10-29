"""股票未复权全市场日集合，不由当前股票快照推导历史应交易名单。"""

from pathlib import Path

from quant_data.core.models import Check
from quant_data.datasets.daily import SCHEMA, calendar_dependencies, calendar_scope, validate_quotes
from quant_data.db import relation


class StockDailyDataset:
    name, table = "stock_daily", "canonical.stock_daily"
    record_key = ("ts_code", "trade_date")
    schema = SCHEMA
    ddl = Path(__file__).with_suffix(".sql").read_text()

    def dependencies(self, params, con):
        # 业务边界：首版全市场逐日获取，不接受股票、交易所或状态子集。
        if set(params) != {"start_date", "end_date"}:
            raise ValueError("stock_daily requires only start_date and end_date")
        return calendar_dependencies(params)

    def resolve_scope(self, params, pins, con):
        return calendar_scope(pins, con)

    def unit_key_sql(self, alias):
        return f"strftime({alias}.trade_date,'%Y-%m-%d')"

    def validate_candidate(self, con, candidate_relation, scope):
        checks = list(validate_quotes(con, candidate_relation, self.unit_key_sql("c"), nullable=False))
        # 业务非空政策：个股停牌可无行，但开市日全市场零行不清除旧日集合。
        empty = con.execute(f"SELECT s.unit_key FROM _scope_units s LEFT JOIN {relation(candidate_relation)} c "
                            f"ON s.unit_key={self.unit_key_sql('c')} GROUP BY s.unit_key HAVING count(c.trade_date)=0").fetchall()
        if empty:
            checks.append(Check("ERROR", "stock_daily.nonempty", "validation", tuple(r[0] for r in empty)))
        return tuple(checks)
