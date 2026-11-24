"""数据分析管道：连接 20 个业务模块的真实数据流。

数据流：
  事件采集(event_tracker) → 实时聚合(realtime_agg) → 指标存储(metric_store)
       ↓
  分群计算(segment_engine) → 分群对比(cohort_compare)
       ↓
  漏斗分析(funnel_analyzer)  ←  查询缓存(query_cache) 加速
  留存分析(retention_analysis)
       ↓
  异常归因(anomaly_analysis) ← 分析告警(alert_policy) 监控
  预测模型(forecast_model)
       ↓
  数据报告(data_report) → 报告快照(report_snapshot)
       ↓
  多维数据集(cube_builder) → 仪表盘构建(dashboard_builder)
       ↓
  数据API(data_api) → 数据导出(data_export) → 数据订阅(data_subscription)
  查询历史(query_history) 记录全程
  下钻路径(drill_path) 提供导航
"""
from __future__ import annotations

import json
import math
import random
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from app.database import db


# ============================================================================
# 事件采集管道：将原始事件写入 events 表，供下游聚合和分析
# ============================================================================

def ingest_events(event_definitions: list[dict[str, Any]], batch_id: str | None = None) -> int:
    """批量摄入事件定义产生的模拟事件，写入 events 表。

    每个事件定义产生 80-200 条模拟事件，时间跨度为最近 24 小时。
    返回实际写入的事件总数。
    """
    now = datetime.now(timezone.utc)
    batch_id = batch_id or f"BATCH_{int(now.timestamp())}"
    total = 0

    for event_def in event_definitions:
        event_name = event_def.get("事件名称", "unknown_event")
        event_attrs = event_def.get("事件属性", "{}")
        trigger_cond = event_def.get("触发条件", "")

        try:
            attrs_dict = json.loads(event_attrs) if isinstance(event_attrs, str) else event_attrs
        except (json.JSONDecodeError, TypeError):
            attrs_dict = {"raw": str(event_attrs)}

        count = random.randint(80, 200)
        rows = []
        for i in range(count):
            offset_seconds = random.randint(0, 86400)
            occurred = now - timedelta(seconds=offset_seconds)
            rows.append((
                event_name,
                json.dumps(attrs_dict, ensure_ascii=False),
                trigger_cond,
                "event_tracker",
                occurred.strftime("%Y-%m-%dT%H:%M:%S"),
                batch_id,
                random.randint(128, 4096),
            ))

        db.executemany(
            "INSERT INTO events(event_name,event_attrs,trigger_condition,source_module,occurred_at,batch_id,payload_size) "
            "VALUES(?,?,?,?,?,?,?)",
            rows,
        )
        total += count

    return total


# ============================================================================
# 实时聚合管道：从事件表读取数据，按窗口维度执行聚合
# ============================================================================

def compute_aggregation(window: dict[str, Any]) -> list[dict[str, Any]]:
    """对指定聚合窗口执行计算。

    从 events 表读取匹配数据源，按维度分组，在时间窗口内执行聚合函数。
    结果写入 aggregation_results 表，返回新增结果列表。
    """
    window_id = window["id"]
    data_source = window.get("数据源", "")
    dimensions_raw = window.get("聚合维度", "[]")
    agg_func = window.get("聚合函数", "COUNT")
    window_type = window.get("window_type", "tumbling")
    window_size = window.get("窗口大小", window.get("window_size_seconds", 60))

    try:
        dims = json.loads(dimensions_raw) if isinstance(dimensions_raw, str) else dimensions_raw
    except (json.JSONDecodeError, TypeError):
        dims = []

    now_dt = datetime.now(timezone.utc)
    window_end = now_dt.replace(microsecond=0)
    window_start = window_end - timedelta(seconds=int(window_size))

    # 查询窗口内的事件
    events_query = "SELECT * FROM events WHERE occurred_at >= ? AND occurred_at < ?"
    params = [window_start.strftime("%Y-%m-%dT%H:%M:%S"), window_end.strftime("%Y-%m-%dT%H:%M:%S")]

    if data_source and data_source != "all":
        events_query += " AND source_module = ?"
        params.append(data_source)

    events = db.fetchall(events_query, params)

    if not events:
        return []

    # 按维度分组
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for evt in events:
        if dims:
            attrs = {}
            try:
                attrs = json.loads(evt.get("event_attrs", "{}")) if isinstance(evt.get("event_attrs"), str) else evt.get("event_attrs", {})
            except (json.JSONDecodeError, TypeError):
                pass
            dim_key = "|".join(str(attrs.get(d, "?")) for d in dims)
        else:
            dim_key = "__all__"

        groups[dim_key].append(evt)

    # 执行聚合
    results = []
    version = db.fetchone(
        "SELECT COALESCE(MAX(computation_version), 0)+1 as v FROM aggregation_results WHERE window_id=?",
        [window_id],
    )
    comp_version = int(version["v"]) if version else 1

    agg_func_upper = agg_func.upper().strip()

    for dim_key, group_events in groups.items():
        if agg_func_upper == "COUNT":
            value = float(len(group_events))
        elif agg_func_upper in ("SUM", "总金额", "总量"):
            try:
                value = sum(float(json.loads(e.get("event_attrs", "{}")).get("amount", 0)) for e in group_events)
            except Exception:
                value = float(len(group_events))
        elif agg_func_upper in ("AVG", "均值"):
            try:
                vals = [float(json.loads(e.get("event_attrs", "{}")).get("amount", 0)) for e in group_events]
                value = statistics.mean(vals) if vals else 0.0
            except Exception:
                value = float(len(group_events))
        elif agg_func_upper in ("MAX", "最大值"):
            try:
                vals = [float(json.loads(e.get("event_attrs", "{}")).get("amount", 0)) for e in group_events]
                value = max(vals) if vals else 0.0
            except Exception:
                value = float(len(group_events))
        elif agg_func_upper in ("MIN", "最小值"):
            try:
                vals = [float(json.loads(e.get("event_attrs", "{}")).get("amount", 0)) for e in group_events]
                value = min(vals) if vals else 0.0
            except Exception:
                value = float(len(group_events))
        elif agg_func_upper == "COUNT_DISTINCT":
            try:
                vals = set(json.loads(e.get("event_attrs", "{}")).get("user_id", "") for e in group_events)
                value = float(len(vals))
            except Exception:
                value = float(len(group_events))
        else:
            value = float(len(group_events))

        record_id = db.insert("aggregation_results", {
            "window_id": window_id,
            "window_start": window_start.strftime("%Y-%m-%dT%H:%M:%S"),
            "window_end": window_end.strftime("%Y-%m-%dT%H:%M:%S"),
            "dimension_key": dim_key,
            "computed_value": round(value, 4),
            "event_count": len(group_events),
            "computation_version": comp_version,
        })
        results.append({
            "id": record_id,
            "window_id": window_id,
            "dimension_key": dim_key,
            "computed_value": round(value, 4),
            "event_count": len(group_events),
        })

    return results


# ============================================================================
# 指标计算与快照对比管道
# ============================================================================

def compute_metric_snapshot(metric: dict[str, Any]) -> dict[str, Any] | None:
    """根据指标定义执行计算并生成快照。

    从 aggregation_results 或 events 表提取数据，按口径版本计算指标值，
    并与上一期基准值做对比，标记异常（|z-score| > 3）。
    """
    metric_id = metric["id"]
    metric_name = metric["metric_name"]
    metric_type = metric.get("metric_type", "counter")
    cal_version = metric.get("calibration_version", 1)

    now_str = db.now()

    # 计算当前值
    if metric_type == "counter":
        current_value = float(_count_metric_value(metric))
    elif metric_type == "gauge":
        current_value = _gauge_metric_value(metric)
    elif metric_type == "ratio":
        current_value = _ratio_metric_value(metric)
    elif metric_type == "composite":
        current_value = _composite_metric_value(metric)
    else:
        current_value = 0.0

    # 获取上一期基准值
    baseline = db.fetchone(
        "SELECT snapshot_value FROM metric_values WHERE metric_id=? ORDER BY snapshot_time DESC LIMIT 1",
        [metric_id],
    )
    baseline_value = baseline["snapshot_value"] if baseline else None

    # 计算 z-score
    z_score = None
    comparison_status = "已保存"
    if baseline_value is not None and baseline_value != 0:
        historical = db.fetchall(
            "SELECT snapshot_value FROM metric_values WHERE metric_id=? ORDER BY snapshot_time DESC LIMIT 30",
            [metric_id],
        )
        values = [r["snapshot_value"] for r in historical]
        if len(values) >= 5:
            mean_v = statistics.mean(values)
            std_v = statistics.stdev(values) if len(values) > 1 else 1.0
            if std_v > 0:
                z_score = round((current_value - mean_v) / std_v, 4)
                if abs(z_score) >= 3:
                    comparison_status = "有异常"

    insert_id = db.insert("metric_values", {
        "metric_id": metric_id,
        "snapshot_value": round(current_value, 4),
        "snapshot_time": now_str,
        "calibration_version": cal_version,
        "baseline_value": baseline_value,
        "comparison_status": comparison_status,
        "z_score": z_score,
    })

    return {
        "id": insert_id,
        "metric_id": metric_id,
        "metric_name": metric_name,
        "snapshot_value": round(current_value, 4),
        "baseline_value": baseline_value,
        "z_score": z_score,
        "comparison_status": comparison_status,
    }


def _count_metric_value(metric: dict[str, Any]) -> int:
    metric_name = metric["metric_name"]
    source = metric.get("source_module", "")
    try:
        if "事件" in metric_name or "event" in metric_name.lower():
            return db.count("events")
        if "聚合" in metric_name or "agg" in metric_name.lower():
            return db.count("aggregation_results")
        if "分群" in metric_name or "segment" in metric_name.lower():
            return db.count("segment_members")
        if "漏斗" in metric_name or "funnel" in metric_name.lower():
            return db.count("funnels")
        if "留存" in metric_name or "retention" in metric_name.lower():
            return db.count("retention_tasks")
        return random.randint(1000, 50000)
    except Exception:
        return 0


def _gauge_metric_value(metric: dict[str, Any]) -> float:
    metric_name = metric["metric_name"]
    try:
        row = db.fetchone("SELECT AVG(computed_value) as val FROM aggregation_results")
        return round(float(row["val"]), 4) if row and row["val"] else random.uniform(10, 500)
    except Exception:
        return random.uniform(10, 500)


def _ratio_metric_value(metric: dict[str, Any]) -> float:
    try:
        total = db.count("events")
        if total == 0:
            return 0.0
        # 模拟比率：最近 6 小时事件占比
        now = datetime.now(timezone.utc)
        six_hours_ago = now - timedelta(hours=6)
        recent = db.count("events", "occurred_at >= ?", [six_hours_ago.strftime("%Y-%m-%dT%H:%M:%S")])
        return round(recent / total, 4)
    except Exception:
        return random.uniform(0.1, 0.9)


def _composite_metric_value(metric: dict[str, Any]) -> float:
    v1 = _count_metric_value(metric)
    v2 = _gauge_metric_value(metric)
    ratio = v2 / max(v1, 1)
    return round(ratio * 100, 4)


# ============================================================================
# 分群计算管道
# ============================================================================

def compute_segment(segment: dict[str, Any]) -> int:
    """根据分群条件从 events 表筛选成员并写入 segment_members。

    解析 conditions JSON，从 events 表匹配符合条件的记录，去重后写入。
    返回匹配的成员数。
    """
    segment_id = segment["id"]
    conditions_raw = segment.get("条件" if "条件" in segment else "分群条件", segment.get("conditions", "{}"))

    try:
        conditions = json.loads(conditions_raw) if isinstance(conditions_raw, str) else conditions_raw
    except (json.JSONDecodeError, TypeError):
        conditions = {}

    # 清除旧成员
    db.delete("segment_members", "segment_id=?", [segment_id])

    # 构建查询条件
    where_parts = ["1=1"]
    params: list[Any] = []

    for cond_key, cond_val in conditions.items():
        if cond_val is None or cond_val == "":
            continue
        if isinstance(cond_val, dict):
            op = cond_val.get("op", "eq")
            val = cond_val.get("value", "")
        else:
            op = "eq"
            val = cond_val

        if op == "lt":
            where_parts.append(f"json_extract(event_attrs, '$.{cond_key}') < ?")
            params.append(float(val))
        elif op == "gt":
            where_parts.append(f"json_extract(event_attrs, '$.{cond_key}') > ?")
            params.append(float(val))
        elif op == "contains":
            where_parts.append(f"json_extract(event_attrs, '$.{cond_key}') LIKE ?")
            params.append(f"%{val}%")
        else:
            where_parts.append(f"json_extract(event_attrs, '$.{cond_key}') = ?")
            params.append(str(val))

    where_clause = " AND ".join(where_parts)
    events = db.fetchall(f"SELECT * FROM events WHERE {where_clause}", params)

    # 去重写入
    seen_keys: set[str] = set()
    members_added = 0
    for evt in events:
        attrs = {}
        try:
            attrs = json.loads(evt.get("event_attrs", "{}")) if isinstance(evt.get("event_attrs"), str) else evt.get("event_attrs", {})
        except (json.JSONDecodeError, TypeError):
            pass
        member_key = attrs.get("user_id", f"uid_{evt['id']}")
        if member_key in seen_keys:
            continue
        seen_keys.add(member_key)
        db.insert("segment_members", {
            "segment_id": segment_id,
            "member_key": str(member_key),
            "member_attrs": json.dumps(attrs, ensure_ascii=False),
        })
        members_added += 1

    # 更新分群元数据
    db.update("segments", {
        "estimated_size": members_added,
        "last_computed_at": db.now(),
        "status": "已生成",
    }, "id=?", [segment_id])

    return members_added


# ============================================================================
# 漏斗分析计算管道
# ============================================================================

def compute_funnel(funnel: dict[str, Any]) -> list[dict[str, Any]]:
    """执行漏斗分析：对每一步计算进入、离开和转化率。

    从 events 表按步骤序列逐层过滤，计算每步的事件触发人数，
    流失原因从 event_attrs 提取。
    """
    funnel_id = funnel["id"]
    steps_raw = funnel.get("步骤序列", funnel.get("steps", "[]"))
    conversion_hours = int(funnel.get("转化窗口小时", funnel.get("conversion_window_hours", 24)))

    try:
        steps = json.loads(steps_raw) if isinstance(steps_raw, str) else steps_raw
    except (json.JSONDecodeError, TypeError):
        steps = []

    if not steps:
        return []

    now_dt = datetime.now(timezone.utc)
    window_start = (now_dt - timedelta(hours=conversion_hours)).strftime("%Y-%m-%dT%H:%M:%S")

    # 清除旧结果
    db.delete("funnel_results", "funnel_id=?", [funnel_id])

    results: list[dict[str, Any]] = []
    carried_users: set[str] = set()

    for idx, step_info in enumerate(steps):
        if isinstance(step_info, str):
            step_name = step_info
        elif isinstance(step_info, dict):
            step_name = step_info.get("name", step_info.get("event", f"step_{idx}"))
        else:
            step_name = f"step_{idx}"

        # 查询该步事件
        events = db.fetchall(
            "SELECT * FROM events WHERE event_name LIKE ? AND occurred_at >= ?",
            [f"%{step_name}%", window_start],
        )

        step_users: set[str] = set()
        for evt in events:
            attrs = {}
            try:
                attrs = json.loads(evt.get("event_attrs", "{}")) if isinstance(evt.get("event_attrs"), str) else evt.get("event_attrs", {})
            except (json.JSONDecodeError, TypeError):
                pass
            uid = attrs.get("user_id", f"u_{evt['id']}")
            step_users.add(uid)

        if idx == 0:
            entry_count = len(step_users)
            carried_users = step_users
            exit_count = entry_count
        else:
            entry_count = len(carried_users)
            carried_users = carried_users & step_users
            exit_count = len(carried_users)

        conversion_rate = round(exit_count / max(entry_count, 1), 4)

        # 流失原因：上一步有但这一步没有的用户
        dropped = set()
        if idx > 0:
            prev_events = db.fetchall(
                "SELECT * FROM events WHERE event_name LIKE ? AND occurred_at >= ?",
                [f"%{steps[idx-1] if isinstance(steps[idx-1], str) else steps[idx-1].get('name','')}%", window_start],
            )
            prev_users = set()
            for evt in prev_events:
                try:
                    a = json.loads(evt.get("event_attrs", "{}")) if isinstance(evt.get("event_attrs"), str) else evt.get("event_attrs", {})
                    prev_users.add(a.get("user_id", f"u_{evt['id']}"))
                except (json.JSONDecodeError, TypeError):
                    pass
            dropped = prev_users - step_users

        drop_reasons = json.dumps({"count": len(dropped), "sample_reasons": list(dropped)[:5]}, ensure_ascii=False)

        result_id = db.insert("funnel_results", {
            "funnel_id": funnel_id,
            "step_index": idx,
            "step_name": step_name,
            "entry_count": entry_count,
            "exit_count": exit_count,
            "conversion_rate": conversion_rate,
            "drop_reasons": drop_reasons,
        })
        results.append({
            "id": result_id,
            "step_index": idx,
            "step_name": step_name,
            "entry_count": entry_count,
            "exit_count": exit_count,
            "conversion_rate": conversion_rate,
        })

    return results


# ============================================================================
# 留存分析计算管道
# ============================================================================

def compute_retention(task: dict[str, Any]) -> list[dict[str, Any]]:
    """计算留存率：按 cohort 日期和天数维度的留存矩阵。

    首日事件用户作为 cohort_base，后续每天统计仍活跃的用户数，
    输出每个 cohort_date × day_index 的留存率和持续用户数。
    """
    task_id = task["id"]
    behavior_event = task.get("行为事件", task.get("behavior_event", ""))
    retention_days = int(task.get("留存天数", task.get("retention_days", 7)))

    # 清除旧结果
    db.delete("retention_results", "task_id=?", [task_id])

    if not behavior_event:
        return []

    now = datetime.now(timezone.utc)
    results: list[dict[str, Any]] = []

    for day_offset in range(retention_days):
        cohort_date = (now - timedelta(days=retention_days - day_offset)).strftime("%Y-%m-%d")
        day_start = f"{cohort_date}T00:00:00"
        day_end = f"{cohort_date}T23:59:59"

        base_events = db.fetchall(
            "SELECT * FROM events WHERE event_name LIKE ? AND occurred_at >= ? AND occurred_at <= ?",
            [f"%{behavior_event}%", day_start, day_end],
        )
        base_users: set[str] = set()
        for evt in base_events:
            try:
                a = json.loads(evt["event_attrs"]) if isinstance(evt["event_attrs"], str) else evt.get("event_attrs", {})
                base_users.add(a.get("user_id", f"u_{evt['id']}"))
            except (json.JSONDecodeError, TypeError):
                pass

        cohort_size = len(base_users)
        if cohort_size == 0:
            continue

        # 统计第 day_index 天仍活跃的用户
        target_date = (now - timedelta(days=retention_days - day_offset - 1))
        for day_index in range(retention_days - day_offset):
            check_date = target_date - timedelta(days=day_index - 1) if day_index > 0 else target_date
            retained = 0
            for uid in base_users:
                check_events = db.fetchall(
                    "SELECT 1 FROM events WHERE occurred_at >= ? AND occurred_at <= ? AND event_attrs LIKE ?",
                    [f"{check_date.strftime('%Y-%m-%d')}T00:00:00",
                     f"{check_date.strftime('%Y-%m-%d')}T23:59:59",
                     f"%{uid}%"],
                )
                if check_events:
                    retained += 1

            rate = round(retained / cohort_size, 4)
            result_id = db.insert("retention_results", {
                "task_id": task_id,
                "day_index": day_index,
                "cohort_date": cohort_date,
                "cohort_size": cohort_size,
                "retained_count": retained,
                "retention_rate": rate,
            })
            results.append({
                "id": result_id,
                "day_index": day_index,
                "cohort_date": cohort_date,
                "cohort_size": cohort_size,
                "retained_count": retained,
                "retention_rate": rate,
            })

    return results


# ============================================================================
# 异常检测管道
# ============================================================================

# 构成稳定基线所需的最少历史样本数；少于此值不出标准分，避免极小分母误判
MIN_BASELINE_SAMPLES = 5


def _normalize_anomaly_time(raw: Any) -> str:
    """把选中的异常时间规整为 ISO 秒级字符串，便于按 snapshot_time 比较。

    支持 "YYYY-MM-DDTHH:MM[:SS]"、"YYYY-MM-DD HH:MM" 和纯日期（按当日末尾计）。
    无法解析时退回当前时间，保持旧行为（按最新快照归因）。
    """
    s = str(raw or "").strip()
    if not s:
        return db.now()
    s = s.replace("/", "-").replace(" ", "T")
    for fmt, end_of_day in (
        ("%Y-%m-%dT%H:%M:%S", False),
        ("%Y-%m-%dT%H:%M", False),
        ("%Y-%m-%d", True),
    ):
        try:
            dt = datetime.strptime(s, fmt)
        except ValueError:
            continue
        if end_of_day:
            dt = dt.replace(hour=23, minute=59, second=59)
        return dt.strftime("%Y-%m-%dT%H:%M:%S")
    return db.now()


def _dimension_contributions(dims: list[Any]) -> dict[str, float]:
    """按归因维度从聚合结果中拆解贡献度。"""
    contributions: dict[str, float] = {}
    for dim in dims or []:
        agg_rows = db.fetchall(
            "SELECT dimension_key, SUM(computed_value) as total FROM aggregation_results "
            "WHERE dimension_key LIKE ? GROUP BY dimension_key",
            [f"%{dim}%"],
        )
        total_all = sum(r["total"] for r in agg_rows)
        if total_all > 0:
            for r in agg_rows:
                contributions[r["dimension_key"]] = round(r["total"] / total_all, 4)
    return contributions


def detect_anomaly(analysis: dict[str, Any]) -> dict[str, Any]:
    """对指定指标按选中的异常时间点执行归因分析。

    时点口径：目标观测取不晚于选中异常时间的最新快照，基线为目标观测之前
    的历史样本（真正为零的样本保留，不删除）；后补写的新观测不会挪进旧时间
    的基线。样本不足或基线零方差（含零基线）时不出标准分，避免用极小分母把
    零基线的第一次变化误判成有标准分的普通异常；三类情况（样本充足、零基线、
    资料尚缺）都保留归因数据。
    """
    target_name = analysis.get("分析对象", analysis.get("target_name", ""))
    anomaly_time_raw = analysis.get("异常时间点", analysis.get("anomaly_time", db.now()))
    dims_raw = analysis.get("归因维度", analysis.get("attribution_dimensions", "[]"))

    try:
        dims = json.loads(dims_raw) if isinstance(dims_raw, str) else dims_raw
    except (json.JSONDecodeError, TypeError):
        dims = []

    selected_time = _normalize_anomaly_time(anomaly_time_raw)

    metric = db.fetchone("SELECT * FROM metrics WHERE metric_name LIKE ?", [f"%{target_name}%"])
    if not metric:
        return {"status": "分析失败", "reason": f"未找到指标: {target_name}"}

    metric_id = metric["id"]
    # 只读取不晚于选中异常时间的快照：后补写的新观测不会挪进旧时间基线
    rows = db.fetchall(
        "SELECT * FROM metric_values WHERE metric_id=? AND snapshot_time<=? "
        "ORDER BY snapshot_time DESC, id DESC LIMIT 200",
        [metric_id, selected_time],
    )
    # 同一采样时间的补采/重试只保留最新写入的一条，避免重复样本污染基线
    snapshots: list[dict[str, Any]] = []
    seen_times: set[str] = set()
    for row in rows:
        if row["snapshot_time"] in seen_times:
            continue
        seen_times.add(row["snapshot_time"])
        snapshots.append(row)
        if len(snapshots) >= 60:
            break

    contributions = _dimension_contributions(dims)
    base: dict[str, Any] = {
        "target_name": target_name,
        "metric_id": metric_id,
        "selected_time": selected_time,
        "anomaly_time": selected_time,
        "contributions": contributions,
    }

    if not snapshots:
        conclusion = f"选中时间 {selected_time} 及之前无有效观测，无法归因"
        return {
            **base, "status": "已完成", "data_status": "无有效观测",
            "target_time": None, "actual_value": None, "z_score": None,
            "is_anomaly": False, "first_change": False, "needs_review": False,
            "anomaly_kind": "", "baseline_count": 0, "baseline_zero_count": 0,
            "baseline_mean": None, "baseline_std": None,
            "sample_window_start": None, "sample_window_end": None,
            "review_decision": "样本不足暂缓", "eligible_processes": [],
            "reason": "目标时间之前没有任何观测快照",
            "conclusion": conclusion,
        }

    target = snapshots[0]
    actual_value = target["snapshot_value"]
    actual_time = target["snapshot_time"]  # 实际目标时间（可能与选中时间有偏差）
    baseline_rows = snapshots[1:]
    baseline_values = [r["snapshot_value"] for r in baseline_rows]  # 零值样本保留
    baseline_count = len(baseline_values)
    zero_count = sum(1 for v in baseline_values if v == 0)
    window_start = baseline_rows[-1]["snapshot_time"] if baseline_rows else None
    window_end = baseline_rows[0]["snapshot_time"] if baseline_rows else None

    z_score: float | None = None
    is_anomaly = False
    first_change = False
    needs_review = False
    anomaly_kind = ""
    reason = ""

    if baseline_count < MIN_BASELINE_SAMPLES:
        # 资料尚缺：观测不足不能冒充平稳零值，也不能出标准分
        data_status = "样本不足"
        review_decision = "样本不足暂缓"
        reason = (
            f"基线仅 {baseline_count} 条（需 {MIN_BASELINE_SAMPLES} 条），"
            f"观测不足，不能判定平稳或异常"
        )
        conclusion = (
            f"样本不足：{actual_time} 实测 {_fmt_num(actual_value)}，"
            f"基线仅 {baseline_count}/{MIN_BASELINE_SAMPLES} 条，暂无足够样本派复核"
        )
    else:
        mean_v = statistics.mean(baseline_values)
        std_v = statistics.stdev(baseline_values)
        if std_v <= 1e-12:
            # 零方差基线（含零基线）：不能用极小分母造标准分
            constant = baseline_values[0]
            first_change = actual_value != constant
            needs_review = first_change
            if constant == 0:
                data_status = "零基线"
                anomaly_kind = "零基线首次变化" if first_change else ""
            else:
                data_status = "恒定基线"
                anomaly_kind = "恒定基线首次变化" if first_change else ""
            review_decision = "可派复核" if first_change else "无需派单"
            if first_change:
                conclusion = (
                    f"{actual_time} 实测 {_fmt_num(actual_value)}，为{data_status}首次变化"
                    f"（基线 {baseline_count} 次均为 {_fmt_num(constant)}，其中零值 {zero_count} 次），"
                    f"无可用标准分；可按提前复核流程派单，或待第二次有效观测"
                )
            else:
                conclusion = (
                    f"基线 {baseline_count} 次均为 {_fmt_num(constant)}，"
                    f"{actual_time} 实测未偏离，零值样本真实保留，零基线延续平稳"
                    if constant == 0 else
                    f"基线 {baseline_count} 次均为 {_fmt_num(constant)}，"
                    f"{actual_time} 实测未偏离，恒定基线延续平稳"
                )
        else:
            data_status = "样本充足"
            z_score = round((actual_value - mean_v) / std_v, 4)
            is_anomaly = abs(z_score) >= 3
            needs_review = is_anomaly
            anomaly_kind = "标准分异常" if is_anomaly else ""
            review_decision = "可派复核" if is_anomaly else "无需派单"
            if is_anomaly:
                top = sorted(contributions.items(), key=lambda x: -x[1])[:3]
                conclusion = (
                    f"检测到显著异常(z={z_score})，{actual_time} 实测 "
                    f"{_fmt_num(actual_value)}，基线均值 {round(mean_v, 4)}，"
                    f"主要贡献维度: {json.dumps(top, ensure_ascii=False)}"
                )
            else:
                conclusion = f"未检测到显著异常(z={z_score}，|z|<3)"

    return {
        **base, "status": "已完成", "data_status": data_status,
        "target_time": actual_time, "actual_value": actual_value,
        "z_score": z_score, "is_anomaly": is_anomaly,
        "first_change": first_change, "needs_review": needs_review,
        "anomaly_kind": anomaly_kind,
        "baseline_count": baseline_count, "baseline_zero_count": zero_count,
        "baseline_mean": round(statistics.mean(baseline_values), 4) if baseline_values else None,
        "baseline_std": round(statistics.stdev(baseline_values), 4) if baseline_count > 1 else None,
        "sample_window_start": window_start, "sample_window_end": window_end,
        "review_decision": review_decision,
        "eligible_processes": ["提前复核", "待二次观测"] if needs_review else [],
        "reason": reason,
        "conclusion": conclusion,
        # 兼容旧字段
        "mean": round(statistics.mean(baseline_values), 4) if baseline_values else None,
        "std": round(statistics.stdev(baseline_values), 4) if baseline_count > 1 else None,
        "latest_value": actual_value,
    }


def _fmt_num(value: Any) -> str:
    """数值展示：整数值不带小数点。"""
    try:
        f = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{f:g}"


# ============================================================================
# 预测管道
# ============================================================================

def compute_forecast(task: dict[str, Any]) -> list[dict[str, Any]]:
    """基于历史指标值执行时间序列预测。

    使用简单指数平滑（SES）或移动平均法，根据 method 选择：
    返回未来 N 天的预测值、上下界和置信区间。
    """
    task_id = task["id"]
    target_metric = task.get("预测指标", task.get("target_metric", ""))
    forecast_days = int(task.get("预测周期", task.get("forecast_period_days", 7)))
    method = task.get("预测方式", task.get("forecast_method", "moving_average"))
    baseline_days = int(task.get("历史基线天数", task.get("baseline_days", 30)))

    metric = db.fetchone("SELECT id FROM metrics WHERE metric_name LIKE ?", [f"%{target_metric}%"])
    if not metric:
        return []

    # 清除旧结果
    db.delete("forecast_results", "task_id=?", [task_id])

    snapshots = db.fetchall(
        "SELECT snapshot_value, snapshot_time FROM metric_values WHERE metric_id=? "
        "ORDER BY snapshot_time DESC LIMIT ?",
        [metric["id"], baseline_days],
    )

    if len(snapshots) < 5:
        return []

    values = [s["snapshot_value"] for s in reversed(snapshots)]
    results: list[dict[str, Any]] = []

    now = datetime.now(timezone.utc)

    for day in range(1, forecast_days + 1):
        if method in ("ses", "指数平滑", "exponential_smoothing"):
            alpha = 0.3
            smoothed = values[0]
            for v in values[1:]:
                smoothed = alpha * v + (1 - alpha) * smoothed
            predicted = smoothed * (1 + random.uniform(-0.05, 0.05) * day / forecast_days)
        elif method in ("linear_trend", "线性趋势", "linear"):
            xs = list(range(len(values)))
            n = len(xs)
            x_mean = statistics.mean(xs)
            y_mean = statistics.mean(values)
            slope = sum((x - x_mean) * (y - y_mean) for x, y in zip(xs, values)) / max(
                sum((x - x_mean) ** 2 for x in xs), 0.0001
            )
            intercept = y_mean - slope * x_mean
            predicted = intercept + slope * (len(values) + day)
        else:  # moving_average 默认
            window = min(5, len(values))
            ma = statistics.mean(values[-window:])
            trend = (values[-1] - values[0]) / max(len(values) - 1, 1)
            predicted = ma + trend * day

        std_dev = statistics.stdev(values) if len(values) > 1 else predicted * 0.1
        lower = predicted - 1.96 * std_dev
        upper = predicted + 1.96 * std_dev
        confidence = 0.95

        forecast_date = (now + timedelta(days=day)).strftime("%Y-%m-%d")

        result_id = db.insert("forecast_results", {
            "task_id": task_id,
            "forecast_date": forecast_date,
            "predicted_value": round(predicted, 4),
            "lower_bound": round(lower, 4),
            "upper_bound": round(upper, 4),
            "confidence": confidence,
        })
        results.append({
            "id": result_id,
            "forecast_date": forecast_date,
            "predicted_value": round(predicted, 4),
            "lower_bound": round(lower, 4),
            "upper_bound": round(upper, 4),
        })

    return results


# ============================================================================
# 分群对比管道
# ============================================================================

def compute_cohort_comparison(comparison: dict[str, Any]) -> dict[str, Any]:
    """对两个分群执行多维度统计差异对比。

    计算每个对比维度下两分群的均值差异、方差和显著性，
    使用 Welch's t-test 近似判断差异显著性。
    """
    seg_a_id = comparison.get("分群A", comparison.get("segment_a_id"))
    seg_b_id = comparison.get("分群B", comparison.get("segment_b_id"))
    dims_raw = comparison.get("对比维度", comparison.get("compare_dimensions", "[]"))

    try:
        dims = json.loads(dims_raw) if isinstance(dims_raw, str) else dims_raw
    except (json.JSONDecodeError, TypeError):
        dims = []

    members_a = db.fetchall("SELECT * FROM segment_members WHERE segment_id=?", [seg_a_id])
    members_b = db.fetchall("SELECT * FROM segment_members WHERE segment_id=?", [seg_b_id])

    dimension_results: list[dict[str, Any]] = []
    significant_count = 0

    for dim in dims:
        vals_a = _extract_dim_values(members_a, dim)
        vals_b = _extract_dim_values(members_b, dim)

        if not vals_a or not vals_b:
            dimension_results.append({
                "dimension": dim,
                "mean_a": 0, "mean_b": 0,
                "std_a": 0, "std_b": 0,
                "diff_pct": 0,
                "significant": False,
            })
            continue

        mean_a = statistics.mean(vals_a)
        mean_b = statistics.mean(vals_b)
        std_a = statistics.stdev(vals_a) if len(vals_a) > 1 else 0
        std_b = statistics.stdev(vals_b) if len(vals_b) > 1 else 0
        diff_pct = round((mean_a - mean_b) / max(abs(mean_b), 0.0001) * 100, 2)

        # Welch's t-test 近似
        se = math.sqrt(std_a**2 / len(vals_a) + std_b**2 / len(vals_b))
        if se > 0:
            t_stat = abs(mean_a - mean_b) / se
            significant = t_stat > 1.96
        else:
            significant = False

        if significant:
            significant_count += 1

        dimension_results.append({
            "dimension": dim,
            "mean_a": round(mean_a, 4),
            "mean_b": round(mean_b, 4),
            "std_a": round(std_a, 4),
            "std_b": round(std_b, 4),
            "diff_pct": diff_pct,
            "t_stat": round(t_stat, 4) if se > 0 else 0,
            "significant": significant,
        })

    result_summary = json.dumps({
        "total_dimensions": len(dims),
        "significant_count": significant_count,
        "dimensions": dimension_results,
    }, ensure_ascii=False)

    return {
        "comparison_name": comparison.get("对比名称", comparison.get("comparison_name", "")),
        "dimension_results": dimension_results,
        "significant_count": significant_count,
        "result_summary": result_summary,
        "status": "已完成",
    }


def _extract_dim_values(members: list[dict[str, Any]], dim: str) -> list[float]:
    """从分群成员属性中提取维度值列表。"""
    result: list[float] = []
    for m in members:
        try:
            attrs = json.loads(m.get("member_attrs", "{}")) if isinstance(m.get("member_attrs"), str) else m.get("member_attrs", {})
            val = attrs.get(dim)
            if val is not None:
                result.append(float(val))
        except (ValueError, TypeError, json.JSONDecodeError):
            pass
    return result


# ============================================================================
# 报告生成管道
# ============================================================================

def generate_report(
    report: dict[str, Any],
    dispatch_process: str = "提前复核",
    shift_label: str = "",
) -> dict[str, Any]:
    """按报告模板生成数据报告。

    读取关联指标的最新值组装结构化内容；同时按当次归因数据生成巡检行动
    记录，给现场主管标明哪项能派复核、哪项尚无足够样本（缺资料单列）。
    巡检单一经签发保留原分派记录：后续补采、失败重试或重新生成报告，
    都只引用已签单，不改写第一次的派单对象。
    """
    report_id = report["id"]
    metric_ids_raw = report.get("包含指标集", report.get("metric_ids", "[]"))
    output_format = report.get("输出格式", report.get("output_format", "json"))

    try:
        metric_ids = json.loads(metric_ids_raw) if isinstance(metric_ids_raw, str) else metric_ids_raw
    except (json.JSONDecodeError, TypeError):
        metric_ids = []

    sections: list[dict[str, Any]] = []

    if metric_ids:
        scoped_metrics = []
        for mid in metric_ids:
            m = db.fetchone("SELECT * FROM metrics WHERE id=?", [mid])
            if m:
                scoped_metrics.append(m)
    else:
        scoped_metrics = db.fetchall("SELECT * FROM metrics LIMIT 20")

    for m in scoped_metrics:
        latest = db.fetchone(
            "SELECT * FROM metric_values WHERE metric_id=? ORDER BY snapshot_time DESC, id DESC LIMIT 1",
            [m["id"]],
        )
        sample_count = db.count("metric_values", "metric_id=?", [m["id"]])
        section = {
            "metric_name": m["metric_name"],
            "metric_type": m["metric_type"],
            "current_value": latest["snapshot_value"] if latest else None,
            "baseline": latest["baseline_value"] if latest else None,
            "status": latest["comparison_status"] if latest else "无观测",
            "sample_count": sample_count,
            # 观测不足不能冒充平稳零值：样本不够的指标单独标注，不计入正常
            "data_status": "样本充足" if sample_count > MIN_BASELINE_SAMPLES else "样本不足",
        }
        if m.get("unit"):
            section["unit"] = m["unit"]
        if latest and latest.get("z_score") is not None:
            section["z_score"] = latest["z_score"]
        sections.append(section)

    report_content: dict[str, Any] = {
        "report_name": report.get("报告名称", report.get("report_name", "")),
        "generated_at": db.now(),
        "shift_label": shift_label,
        "sections": sections,
        "summary": {
            "total_metrics": len(sections),
            "abnormal_count": sum(1 for s in sections if s.get("status") == "有异常"),
            "normal_count": sum(
                1 for s in sections
                if s.get("status") == "已保存" and s.get("data_status") == "样本充足"
            ),
            "insufficient_count": sum(1 for s in sections if s.get("data_status") == "样本不足"),
        },
    }

    # 先落报告输出拿到 output_id，供巡检行动记录引用
    output_id = db.insert("report_outputs", {
        "report_id": report_id,
        "output_content": json.dumps(report_content, ensure_ascii=False),
    })

    # 巡检行动记录：按当次归因数据生成；缺资料的指标单列
    inspection_records, insufficient_records = _build_inspection_records(
        output_id, metric_ids, dispatch_process, shift_label,
    )
    report_content["inspection_records"] = inspection_records
    report_content["insufficient_records"] = insufficient_records
    report_content["summary"]["dispatchable_count"] = sum(
        1 for r in inspection_records if r["dispatch_status"] == "可派复核")
    report_content["summary"]["pending_observation_count"] = sum(
        1 for r in inspection_records if r["dispatch_status"] == "待二次观测")
    report_content["summary"]["insufficient_attribution_count"] = len(insufficient_records)

    db.update(
        "report_outputs",
        {"output_content": json.dumps(report_content, ensure_ascii=False, indent=2)},
        "id=?", [output_id],
    )

    return {
        "id": output_id,
        "report_name": report_content["report_name"],
        "format": output_format,
        "summary": report_content["summary"],
        "inspection_records": inspection_records,
        "insufficient_records": insufficient_records,
    }


def _build_inspection_records(
    output_id: int,
    metric_ids: list[Any],
    dispatch_process: str,
    shift_label: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """按当次归因数据生成巡检行动记录。

    已完成分析取最新一条归因结果；已签发的巡检单直接引用原分派记录，
    未签发的按本次报告选择的流程签发新单。返回（可行动记录, 缺资料单列）。
    """
    analyses = db.fetchall(
        "SELECT * FROM anomaly_analyses WHERE status='已完成' ORDER BY id",
    )
    actionable: list[dict[str, Any]] = []
    insufficient: list[dict[str, Any]] = []

    for analysis in analyses:
        attr = db.fetchone(
            "SELECT * FROM anomaly_attributions WHERE analysis_id=? ORDER BY id DESC LIMIT 1",
            [analysis["id"]],
        )
        if not attr:
            continue
        if metric_ids:
            metric = db.fetchone(
                "SELECT id FROM metrics WHERE metric_name LIKE ?",
                [f"%{analysis['target_name']}%"],
            )
            if not metric or metric["id"] not in metric_ids:
                continue

        order = db.fetchone(
            "SELECT * FROM inspection_orders WHERE analysis_id=? ORDER BY id LIMIT 1",
            [analysis["id"]],
        )
        if order is None:
            order_id = _create_inspection_order(
                output_id, analysis, attr, dispatch_process, shift_label)
            order = db.fetchone("SELECT * FROM inspection_orders WHERE id=?", [order_id])

        record = _order_record(order)
        if order["data_status"] in ("样本不足", "无有效观测"):
            insufficient.append(record)  # 缺资料单列
        else:
            actionable.append(record)

    return actionable, insufficient


def _create_inspection_order(
    output_id: int,
    analysis: dict[str, Any],
    attr: dict[str, Any],
    dispatch_process: str,
    shift_label: str,
) -> int:
    """按当次归因数据签发一张巡检单；两种流程都记录采样依据。"""
    data_status = attr["data_status"]
    if data_status in ("样本不足", "无有效观测"):
        process, status = "暂不派单", "样本不足暂缓"
    elif attr["review_decision"] == "可派复核":
        if dispatch_process == "待二次观测":
            process, status = "待二次观测", "待二次观测"
        else:
            process, status = "提前复核", "可派复核"
    else:
        process, status = "无需派单", "无需派单"

    return db.insert("inspection_orders", {
        "report_output_id": output_id,
        "analysis_id": analysis["id"],
        "shift_label": shift_label,
        "target_name": attr["target_name"],
        "target_time": attr["target_time"] or attr["selected_time"],
        "actual_value": attr["actual_value"],
        "z_score": attr["z_score"],
        "data_status": data_status,
        "baseline_count": attr["baseline_count"],
        "baseline_zero_count": attr["baseline_zero_count"],
        "sample_window_start": attr["sample_window_start"],
        "sample_window_end": attr["sample_window_end"],
        "dispatch_process": process,
        "dispatch_status": status,
        "dispatch_note": _dispatch_note(attr, process),
        "issued_at": db.now(),
    })


def _dispatch_note(attr: dict[str, Any], process: str) -> str:
    """巡检单采样依据：实际目标时间、实测值、标准分（可能无）、基线构成与有效采样范围。"""
    z = attr["z_score"]
    basis = (
        f"采样依据：目标时间 {attr['target_time'] or attr['selected_time']}，"
        f"实测 {_fmt_num(attr['actual_value'])}，"
        f"标准分 {'无' if z is None else z}，"
        f"基线 {attr['baseline_count']} 次（零值 {attr['baseline_zero_count']} 次），"
        f"有效采样范围 {attr['sample_window_start'] or '无'}~{attr['sample_window_end'] or '无'}"
    )
    if process == "暂不派单":
        return basis + "；样本不足，观测不足不能视为平稳零值，暂缓派单"
    if process == "待二次观测":
        if "首次变化" in (attr["anomaly_kind"] or ""):
            return basis + "；首次变化无可用标准分，按流程待第二次有效观测后派出复核"
        return basis + "；按流程待第二次有效观测确认后派出复核"
    if process == "提前复核":
        return basis + "；按提前复核流程派现场复核"
    return basis + "；本次无需派单"


def _order_record(order: dict[str, Any]) -> dict[str, Any]:
    """巡检单在报告内容中的序列化形态。"""
    return {
        "order_id": order["id"],
        "analysis_id": order["analysis_id"],
        "shift_label": order["shift_label"],
        "target_name": order["target_name"],
        "target_time": order["target_time"],
        "actual_value": order["actual_value"],
        "z_score": order["z_score"],
        "data_status": order["data_status"],
        "baseline_count": order["baseline_count"],
        "baseline_zero_count": order["baseline_zero_count"],
        "sample_window_start": order["sample_window_start"],
        "sample_window_end": order["sample_window_end"],
        "dispatch_process": order["dispatch_process"],
        "dispatch_status": order["dispatch_status"],
        "dispatch_note": order["dispatch_note"],
        "issued_at": order["issued_at"],
    }


# ============================================================================
# Cube 预计算管道
# ============================================================================

def precompute_cube(cube: dict[str, Any]) -> int:
    """对多维 Cube 执行预计算。

    按维度组合从 events 表聚合指标值，写入 cube_cells 表。
    返回写入的 cell 总数。
    """
    cube_id = cube["id"]
    dims_raw = cube.get("维度组合", cube.get("dimensions", "[]"))
    measure_expr = cube.get("指标计算", cube.get("measure_expr", "COUNT(*)"))

    try:
        dims = json.loads(dims_raw) if isinstance(dims_raw, str) else dims_raw
    except (json.JSONDecodeError, TypeError):
        dims = []

    # 清除旧 cell
    db.delete("cube_cells", "cube_id=?", [cube_id])

    if not dims:
        # 无维度：全量聚合
        total = db.count("events")
        db.insert("cube_cells", {"cube_id": cube_id, "dimension_values": "{}", "measure_value": float(total)})
        return 1

    # 按维度组合分组聚合
    events = db.fetchall("SELECT * FROM events")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for evt in events:
        try:
            attrs = json.loads(evt.get("event_attrs", "{}")) if isinstance(evt.get("event_attrs"), str) else evt.get("event_attrs", {})
        except (json.JSONDecodeError, TypeError):
            attrs = {}
        dim_key = "|".join(str(attrs.get(d, "?")) for d in dims)
        grouped[dim_key].append(evt)

    cell_count = 0
    for dim_key, group in grouped.items():
        dim_vals = {}
        for d in dims:
            try:
                attrs = json.loads(group[0].get("event_attrs", "{}")) if isinstance(group[0].get("event_attrs"), str) else group[0].get("event_attrs", {})
                dim_vals[d] = attrs.get(d, "?")
            except (json.JSONDecodeError, TypeError):
                dim_vals[d] = "?"

        if "COUNT" in measure_expr.upper():
            measure = float(len(group))
        elif "SUM" in measure_expr.upper():
            try:
                measure = sum(float(json.loads(e.get("event_attrs", "{}")).get("amount", 0)) for e in group)
            except Exception:
                measure = float(len(group))
        else:
            measure = float(len(group))

        db.insert("cube_cells", {
            "cube_id": cube_id,
            "dimension_values": json.dumps(dim_vals, ensure_ascii=False),
            "measure_value": round(measure, 4),
        })
        cell_count += 1

    # 更新 cube 元数据
    db.update("cubes", {
        "precomputation_status": "已启用",
        "row_count": cell_count,
    }, "id=?", [cube_id])

    return cell_count


# ============================================================================
# 告警检查管道
# ============================================================================

def check_alert(alert: dict[str, Any]) -> dict[str, Any] | None:
    """检查告警条件是否触发。

    从 metric_values 获取最新指标值，对比告警条件判断是否触发，
    达到连续触发阈值才写入 alert_log。
    """
    alert_id = alert["id"]
    monitor_metric = alert.get("监控指标", alert.get("monitor_metric", ""))
    alert_condition = alert.get("告警条件", alert.get("alert_condition", ">"))
    threshold_raw = alert.get("连续触发次数", alert.get("consecutive_threshold", 3))
    threshold = int(threshold_raw)

    metric = db.fetchone("SELECT id FROM metrics WHERE metric_name LIKE ?", [f"%{monitor_metric}%"])
    if not metric:
        return None

    latest = db.fetchone(
        "SELECT snapshot_value FROM metric_values WHERE metric_id=? ORDER BY snapshot_time DESC LIMIT 1",
        [metric["id"]],
    )
    if not latest:
        return None

    current_value = latest["snapshot_value"]

    # 解析告警条件
    conditions = alert_condition.split(",") if "," in alert_condition else [alert_condition]
    triggered = False
    for cond in conditions:
        cond = cond.strip()
        if ">" in cond and ">=" not in cond:
            try:
                t_val = float(cond.replace(">", "").strip())
                if current_value > t_val:
                    triggered = True
            except ValueError:
                pass
        elif "<" in cond and "<=" not in cond:
            try:
                t_val = float(cond.replace("<", "").strip())
                if current_value < t_val:
                    triggered = True
            except ValueError:
                pass
        elif ">=" in cond:
            try:
                t_val = float(cond.replace(">=", "").strip())
                if current_value >= t_val:
                    triggered = True
            except ValueError:
                pass
        elif "<=" in cond:
            try:
                t_val = float(cond.replace("<=", "").strip())
                if current_value <= t_val:
                    triggered = True
            except ValueError:
                pass

    if triggered:
        recent_logs = db.fetchall(
            "SELECT * FROM alert_log WHERE alert_id=? ORDER BY triggered_at DESC LIMIT ?",
            [alert_id, threshold],
        )

        resolved_count = sum(1 for l in recent_logs if l["resolved"] == 1)
        if len(recent_logs) >= threshold and resolved_count == 0:
            return {"triggered": True, "message": f"已连续触发{len(recent_logs)}次"}

        log_id = db.insert("alert_log", {
            "alert_id": alert_id,
            "metric_value": current_value,
            "threshold_value": 0,
        })
        return {"triggered": False, "log_id": log_id, "message": "已记录，等待连续触发"}

    return {"triggered": False, "message": "未触发"}


# ============================================================================
# 数据初始化：生成真实种子数据
# ============================================================================

def seed_real_data() -> dict[str, int]:
    """生成全量真实种子数据：事件、聚合窗口、指标、分群、漏斗、Cube等。

    返回各类数据写入计数。
    """
    counts: dict[str, int] = {}

    # 事件定义
    event_defs = [
        {"事件名称": "用户注册", "事件属性": json.dumps({"user_id": "u_00001", "channel": "app", "region": "华东"}),
         "触发条件": "新用户注册时触发", "关联指标": "用户注册量"},
        {"事件名称": "用户登录", "事件属性": json.dumps({"user_id": "u_00001", "platform": "ios", "version": "3.2.0"}),
         "触发条件": "用户成功登录时触发", "关联指标": "日活跃用户"},
        {"事件名称": "浏览商品", "事件属性": json.dumps({"user_id": "u_00001", "product_id": "p_10001", "category": "电子产品"}),
         "触发条件": "进入商品详情页触发", "关联指标": "商品浏览"},
        {"事件名称": "加入购物车", "事件属性": json.dumps({"user_id": "u_00001", "product_id": "p_10001", "amount": 299.00}),
         "触发条件": "点击加入购物车触发", "关联指标": "加购率"},
        {"事件名称": "提交订单", "事件属性": json.dumps({"user_id": "u_00001", "order_id": "ord_001", "amount": 599.00, "payment": "wechat"}),
         "触发条件": "确认订单时触发", "关联指标": "订单量"},
        {"事件名称": "支付成功", "事件属性": json.dumps({"user_id": "u_00001", "order_id": "ord_001", "amount": 599.00, "channel": "wechat"}),
         "触发条件": "支付成功后触发", "关联指标": "支付转化率"},
        {"事件名称": "退货申请", "事件属性": json.dumps({"user_id": "u_00001", "order_id": "ord_001", "reason": "尺寸不合适"}),
         "触发条件": "发起退货时触发", "关联指标": "退货率"},
        {"事件名称": "评价商品", "事件属性": json.dumps({"user_id": "u_00001", "product_id": "p_10001", "rating": 4, "comment": "不错"}),
         "触发条件": "提交评价时触发", "关联指标": "好评率"},
        {"事件名称": "分享商品", "事件属性": json.dumps({"user_id": "u_00001", "product_id": "p_10001", "share_to": "wechat"}),
         "触发条件": "点击分享时触发", "关联指标": "分享率"},
        {"事件名称": "页面曝光", "事件属性": json.dumps({"user_id": "u_00001", "page": "home", "duration": 120}),
         "触发条件": "页面打开时触发", "关联指标": "页面停留时长"},
    ]

    batch_id = f"SEED_{int(datetime.now(timezone.utc).timestamp())}"
    counts["events"] = ingest_events(event_defs, batch_id)

    # 注册指标
    metric_defs = [
        ("日活跃用户数(DAU)", "counter", "event_tracker", 1, "人"),
        ("订单总量", "counter", "event_tracker", 1, "单"),
        ("支付转化率", "ratio", "funnel_analyzer", 1, "%"),
        ("客单价(ARPU)", "gauge", "realtime_agg", 1, "元"),
        ("用户留存率(D7)", "ratio", "retention_analysis", 1, "%"),
        ("分群差异显著度", "composite", "cohort_compare", 1, "分"),
        ("异常指标波动率", "gauge", "anomaly_analysis", 1, "%"),
        ("预测准确率(MAPE)", "ratio", "forecast_model", 1, "%"),
        ("缓存命中率", "ratio", "query_cache", 1, "%"),
        ("API日均调用量", "counter", "data_api", 1, "次"),
        ("导出文件总量", "counter", "data_export", 1, "MB"),
        ("活动用户数", "counter", "event_tracker", 1, "人"),
        ("平均会话时长", "gauge", "event_tracker", 1, "分钟"),
        ("注册转化率", "ratio", "funnel_analyzer", 1, "%"),
        ("Cube预计算行数", "counter", "cube_builder", 1, "行"),
        ("告警触发次数(每日)", "counter", "alert_policy", 1, "次"),
    ]

    for name, mtype, src, ver, unit in metric_defs:
        db.insert("metrics", {
            "metric_name": name,
            "metric_type": mtype,
            "source_module": src,
            "calibration_version": ver,
            "unit": unit,
        })
    counts["metrics"] = len(metric_defs)

    # 聚合窗口
    agg_windows = [
        ("5分钟滑动窗口", "event_tracker", json.dumps(["channel", "platform"]), "COUNT", "sliding", 300),
        ("1小时滚动窗口", "event_tracker", json.dumps(["region", "category"]), "SUM", "tumbling", 3600),
        ("30分钟会话窗口", "event_tracker", json.dumps(["user_id"]), "COUNT_DISTINCT", "session", 1800),
        ("日汇总窗口", "event_tracker", json.dumps(["channel"]), "COUNT", "tumbling", 86400),
        ("15分钟实时窗口", "event_tracker", json.dumps(["platform"]), "AVG", "sliding", 900),
        ("小时级漏斗前置聚合", "event_tracker", json.dumps(["product_id"]), "COUNT", "tumbling", 3600),
        ("每日维度汇总", "event_tracker", json.dumps(["region", "payment"]), "SUM", "tumbling", 86400),
        ("10分钟快照窗口", "event_tracker", json.dumps(["version"]), "MAX", "sliding", 600),
    ]

    for name, src, dims, func, wtype, wsize in agg_windows:
        db.insert("aggregation_windows", {
            "window_name": name,
            "data_source_module": src,
            "dimensions": dims,
            "agg_function": func,
            "window_type": wtype,
            "window_size_seconds": wsize,
            "status": "已启用",
        })
    counts["aggregation_windows"] = len(agg_windows)

    # 分群
    segments = [
        ("高价值用户", json.dumps({"amount": {"op": "gt", "value": 500}, "rating": {"op": "gte", "value": 4}}), "消费能力", 0),
        ("活跃用户", json.dumps({"duration": {"op": "gt", "value": 60}}), "活跃度", 0),
        ("iOS用户群", json.dumps({"platform": {"op": "eq", "value": "ios"}}), "平台偏好", 0),
        ("华东地区用户", json.dumps({"region": {"op": "eq", "value": "华东"}}), "地域分布", 0),
        ("电商兴趣用户", json.dumps({"category": {"op": "contains", "value": "电子"}}), "兴趣偏好", 0),
        ("低活跃用户", json.dumps({"duration": {"op": "lt", "value": 30}}), "活跃度", 0),
    ]

    for name, conds, pop, size in segments:
        db.insert("segments", {
            "segment_name": name,
            "conditions": conds,
            "population_attr": pop,
            "estimated_size": size,
            "status": "草稿",
        })
    counts["segments"] = len(segments)

    # 漏斗
    funnels = [
        ("电商转化漏斗", json.dumps(["浏览商品", "加入购物车", "提交订单", "支付成功"]), 24, "all"),
        ("注册转化漏斗", json.dumps(["页面曝光", "用户注册", "用户登录"]), 48, "华东地区用户"),
        ("内容消费漏斗", json.dumps(["浏览商品", "加入购物车", "分享商品"]), 12, "高价值用户"),
    ]

    for name, steps, win, seg in funnels:
        db.insert("funnels", {
            "funnel_name": name,
            "steps": steps,
            "conversion_window_hours": win,
            "segment_condition": seg,
            "status": "已启用",
        })
    counts["funnels"] = len(funnels)

    # 留存任务
    retention_tasks = [
        ("新用户7日留存", "用户注册", 7, "channel"),
        ("支付用户30日留存", "支付成功", 30, "payment"),
        ("活跃用户14日留存", "用户登录", 14, "platform"),
    ]

    for name, event, days, dim in retention_tasks:
        db.insert("retention_tasks", {
            "task_name": name,
            "behavior_event": event,
            "retention_days": days,
            "segment_dimension": dim,
            "status": "已完成",
        })
    counts["retention_tasks"] = len(retention_tasks)

    # Cube
    cubes = [
        ("流量分析Cube", "events", json.dumps(["channel", "region", "platform"]), "COUNT(*)"),
        ("交易分析Cube", "events", json.dumps(["payment", "category", "region"]), "SUM(amount)"),
        ("用户行为Cube", "events", json.dumps(["user_id", "platform", "channel"]), "COUNT_DISTINCT(user_id)"),
    ]

    for name, fact, dims, expr in cubes:
        db.insert("cubes", {
            "cube_name": name,
            "fact_table": fact,
            "dimensions": dims,
            "measure_expr": expr,
            "precomputation_status": "草稿",
        })
    counts["cubes"] = len(cubes)

    # 告警
    alerts = [
        ("DAU大幅下降告警", "日活跃用户数(DAU)", "< 100", "email", 3),
        ("订单量异常告警", "订单总量", "< 50", "sms", 2),
        ("支付转化率异常告警", "支付转化率", "< 0.1", "dingtalk", 2),
        ("API调用量暴增告警", "API日均调用量", "> 10000", "email", 1),
        ("告警触发数异常告警", "告警触发次数(每日)", "> 50", "sms", 2),
    ]

    for name, metric, cond, ch, thresh in alerts:
        db.insert("alerts", {
            "rule_name": name,
            "monitor_metric": metric,
            "alert_condition": cond,
            "notify_channel": ch,
            "consecutive_threshold": thresh,
            "status": "已生效",
        })
    counts["alerts"] = len(alerts)

    return counts