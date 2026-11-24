"""模块服务基类：共享查询、登记、动作和统计骨架，基于 SQLite 数据库。"""
from __future__ import annotations

from typing import Any

from app.database import db


class BaseModuleService:
    MODULE = ""
    ENTITY = ""
    OPERATION_TERM = ""
    LIST_FIELDS: list[str] = []
    REQUIRED_FIELDS: list[str] = []
    STATUS_ORDER: list[str] = []
    ACTION_RULES: dict[str, str] = {}
    NEGATIVE_ACTIONS: list[str] = []

    def _module_table(self) -> str:
        """返回模块主表名，子类可覆盖。"""
        table_map: dict[str, str] = {
            "realtime_agg": "aggregation_windows",
            "event_tracker": "events",
            "dashboard_builder": "dashboards",
            "query_cache": "aggregation_windows",
            "segment_engine": "segments",
            "metric_store": "metrics",
            "drill_path": "drill_rules",
            "alert_policy": "alerts",
            "data_report": "reports",
            "anomaly_analysis": "anomaly_analyses",
            "forecast_model": "forecast_tasks",
            "cohort_compare": "cohort_comparisons",
            "data_export": "exports",
            "data_subscription": "subscriptions",
            "query_history": "query_log",
            "report_snapshot": "snapshots",
            "data_api": "api_endpoints",
            "retention_analysis": "retention_tasks",
            "funnel_analyzer": "funnels",
            "cube_builder": "cubes",
        }
        return table_map.get(self.MODULE, self.MODULE)

    def list_entries(
        self,
        *,
        filters: dict[str, str] | None = None,
        keyword: str | None = None,
        status: str | None = None,
        page: int = 1,
        size: int = 20,
    ) -> tuple[list[dict[str, Any]], int]:
        table = self._module_table()
        where = ["1=1"]
        params: list[Any] = []

        for field, expected in (filters or {}).items():
            if field in self.LIST_FIELDS:
                col = _to_column_name(field)
                where.append(f"{col} LIKE ?")
                params.append(f"%{expected}%")

        if keyword and self.LIST_FIELDS:
            col = _to_column_name(self.LIST_FIELDS[0])
            where.append(f"{col} LIKE ?")
            params.append(f"%{keyword}%")

        if status:
            where.append("status = ?")
            params.append(status)

        where_clause = " AND ".join(where)
        total = db.count(table, where_clause, params)
        rows = db.fetchall(
            f"SELECT * FROM {table} WHERE {where_clause} ORDER BY id DESC LIMIT ? OFFSET ?",
            params + [size, (page - 1) * size],
        )
        return rows, total

    def get_entry(self, entry_id: int) -> dict[str, Any] | None:
        return db.fetchone(f"SELECT * FROM {self._module_table()} WHERE id=?", [entry_id])

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        missing = [
            field for field in self.REQUIRED_FIELDS
            if not str(values.get(field) or "").strip()
        ]
        if missing:
            return None, missing

        table = self._module_table()
        data: dict[str, Any] = {}
        for field in self.LIST_FIELDS:
            col = _to_column_name(field)
            data[col] = values.get(field, "")
        if "name" not in data and self.LIST_FIELDS and _has_column(table, "name"):
            data["name"] = values.get(self.LIST_FIELDS[0], "")
        data["status"] = self.STATUS_ORDER[0]

        entry_id = db.insert(table, data)
        entry = db.fetchone(f"SELECT * FROM {table} WHERE id=?", [entry_id])
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry = self.get_entry(entry_id)
        if entry is None:
            return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
        target = self.ACTION_RULES.get(action)
        if target is None:
            return None, f"动作「{action}」不属于{self.MODULE}可执行范围"
        db.update(self._module_table(), {"status": target}, "id=?", [entry_id])
        return db.fetchone(f"SELECT * FROM {self._module_table()} WHERE id=?", [entry_id]), f"{self.ENTITY}已{action}"

    def domain_snapshot(self) -> dict[str, Any]:
        table = self._module_table()
        total = db.count(table)
        by_status = {
            status: db.count(table, "status=?", [status])
            for status in self.STATUS_ORDER
        }
        return {
            "module": self.MODULE,
            "entity": self.ENTITY,
            "artifact": f"{self.ENTITY}数据集",
            "count": total,
            "by_status": by_status,
            "abnormal": 0,
        }


def _has_column(table: str, column: str) -> bool:
    """检查表是否真有该列，避免向不存在的列写入。"""
    try:
        rows = db.fetchall(f"PRAGMA table_info({table})")
    except Exception:
        return False
    return any(r.get("name") == column for r in rows)


def _to_column_name(chinese_field: str) -> str:
    """中文字段名转DB列名映射。"""
    mapping: dict[str, str] = {
        "窗口名称": "window_name",
        "数据源": "data_source_module",
        "聚合维度": "dimensions",
        "聚合函数": "agg_function",
        "窗口大小": "window_size_seconds",
        "事件名称": "event_name",
        "事件属性": "event_attrs",
        "触发条件": "trigger_condition",
        "关联指标": "related_metric",
        "最近触达": "recent_trigger",
        "仪表盘名称": "dashboard_name",
        "包含图表": "chart_configs",
        "刷新间隔": "refresh_interval_seconds",
        "数据范围": "data_scope",
        "共享状态": "share_status",
        "配置名称": "config_name",
        "关联数据集": "related_dataset",
        "缓存时长秒数": "cache_ttl_seconds",
        "失效策略": "invalidation_policy",
        "命中率": "hit_rate",
        "分群名称": "segment_name",
        "分群条件": "conditions",
        "所属人口特征": "population_attr",
        "预设大小": "estimated_size",
        "最近更新时间": "last_computed_at",
        "指标名称": "metric_name",
        "快照值": "snapshot_value",
        "快照时间": "snapshot_time",
        "口径版本": "calibration_version",
        "对比基准": "baseline_value",
        "规则名称": "rule_name",
        "起始指标": "start_metric",
        "下钻维度": "drill_dimension",
        "跳转目标": "target_module",
        "默认粒度": "default_granularity",
        "监控指标": "monitor_metric",
        "告警条件": "alert_condition",
        "通知方式": "notify_channel",
        "连续触发次数": "consecutive_threshold",
        "报告名称": "report_name",
        "包含指标集": "metric_ids",
        "报告周期": "report_period",
        "输出格式": "output_format",
        "自动发送": "auto_send",
        "分析对象": "target_name",
        "异常时间点": "anomaly_time",
        "归因维度": "attribution_dimensions",
        "贡献度排序": "contribution_ranking",
        "分析结论": "conclusion",
        "任务名称": "task_name",
        "预测指标": "target_metric",
        "预测周期": "forecast_period_days",
        "预测方式": "forecast_method",
        "历史基线天数": "baseline_days",
        "对比名称": "comparison_name",
        "分群A": "segment_a_id",
        "分群B": "segment_b_id",
        "对比维度": "compare_dimensions",
        "差异显著性": "significance_level",
        "导出名称": "export_name",
        "查询条件": "query_condition",
        "导出格式": "export_format",
        "排队状态": "status",
        "文件大小": "file_size_bytes",
        "订阅名称": "sub_name",
        "推送条件": "push_condition",
        "推送渠道": "push_channel",
        "生效状态": "status",
        "查询时间": "executed_at",
        "执行人": "executor",
        "涉及数据集": "dataset",
        "查询语句摘要": "query_summary",
        "返回行数": "row_count",
        "快照名称": "snapshot_name",
        "所属报告": "report_id",
        "数据摘要": "snapshot_content",
        "差异对比": "comparison_diff",
        "接口名称": "endpoint_name",
        "请求方式": "http_method",
        "返回格式": "response_format",
        "发布状态": "status",
        "行为事件": "behavior_event",
        "留存天数": "retention_days",
        "分群维度": "segment_dimension",
        "最近结果": "status",
        "漏斗名称": "funnel_name",
        "步骤序列": "steps",
        "转化窗口小时": "conversion_window_hours",
        "最近转化率": "status",
        "Cube名称": "cube_name",
        "事实表": "fact_table",
        "维度组合": "dimensions",
        "指标计算": "measure_expr",
        "预计算状态": "precomputation_status",
    }
    return mapping.get(chinese_field, chinese_field)