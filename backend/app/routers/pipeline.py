"""数据分析管道路由：事件采集、聚合执行、指标快照、分群计算等核心操作入口。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body

from app.database import db
from app.pipeline import (
    _normalize_anomaly_time, check_alert, compute_aggregation,
    compute_cohort_comparison, compute_forecast, compute_funnel,
    compute_metric_snapshot, compute_retention, compute_segment,
    generate_report, ingest_events, precompute_cube, seed_real_data,
)
from app.services import service_for

router = APIRouter(prefix="/api/pipeline", tags=["分析管线"])


@router.post("/seed")
def api_seed_real_data() -> dict:
    """初始化全部种子数据：事件、指标、聚合窗口、分群、漏斗等。"""
    counts = seed_real_data()
    return {"ok": True, "counts": counts}


@router.post("/events/ingest")
def api_ingest_events() -> dict:
    """对所有已启用的事件定义批量摄入模拟事件。"""
    svc = service_for("event_tracker")
    result = svc.ingest_all_enabled()
    return result


@router.post("/aggregation/run-all")
def api_run_all_aggregations() -> dict:
    """对所有已启用聚合窗口执行计算。"""
    svc = service_for("realtime_agg")
    result = svc.run_all_active()
    return result


@router.post("/aggregation/{window_id}/run")
def api_run_aggregation(window_id: int) -> dict:
    """对指定聚合窗口执行单次计算。"""
    svc = service_for("realtime_agg")
    return svc.run_aggregation(window_id)


@router.post("/metrics/compute-all")
def api_compute_all_metrics() -> dict:
    """对所有指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_all()


@router.post("/metrics/register")
def api_register_metric(payload: dict[str, Any] = Body(default={})) -> dict:
    """登记一个指标定义（幂等），供采样与归因使用。"""
    name = str(payload.get("metric_name") or "").strip()
    if not name:
        return {"ok": False, "message": "缺少 metric_name"}
    existing = db.fetchone("SELECT * FROM metrics WHERE metric_name=?", [name])
    if existing:
        return {"ok": True, "message": "指标已存在", "metric": existing}
    metric_type = str(payload.get("metric_type") or "counter")
    if metric_type not in ("counter", "gauge", "ratio", "composite"):
        metric_type = "counter"
    metric_id = db.insert("metrics", {
        "metric_name": name,
        "metric_type": metric_type,
        "source_module": str(payload.get("source_module") or "metric_store"),
        "calibration_version": int(payload.get("calibration_version") or 1),
        "unit": str(payload.get("unit") or ""),
    })
    return {"ok": True, "metric": db.fetchone("SELECT * FROM metrics WHERE id=?", [metric_id])}


@router.post("/metrics/{metric_id}/observe")
def api_observe_metric(metric_id: int, payload: dict[str, Any] = Body(default={})) -> dict:
    """记录一次观测快照（补采/重试都走这里）。

    显式给定快照时间的补采只影响该时间口径的归因；已签发的巡检单不受
    后续补采、失败或重试影响。观测本身不打标准分，异常判断由归因管线完成。
    """
    metric = db.fetchone("SELECT * FROM metrics WHERE id=?", [metric_id])
    if metric is None:
        return {"ok": False, "message": f"指标 {metric_id} 不存在"}
    if "value" not in payload:
        return {"ok": False, "message": "缺少观测值 value"}
    try:
        value = float(payload.get("value"))
    except (TypeError, ValueError):
        return {"ok": False, "message": "观测值 value 不是数字"}

    snapshot_time = _normalize_anomaly_time(payload.get("snapshot_time")) \
        if payload.get("snapshot_time") else db.now()
    baseline = db.fetchone(
        "SELECT snapshot_value FROM metric_values WHERE metric_id=? AND snapshot_time<? "
        "ORDER BY snapshot_time DESC, id DESC LIMIT 1",
        [metric_id, snapshot_time],
    )
    row_id = db.insert("metric_values", {
        "metric_id": metric_id,
        "snapshot_value": value,
        "snapshot_time": snapshot_time,
        "calibration_version": metric.get("calibration_version", 1),
        "baseline_value": baseline["snapshot_value"] if baseline else None,
        "comparison_status": "已保存",
        "z_score": None,
    })
    return {
        "ok": True,
        "observation": db.fetchone("SELECT * FROM metric_values WHERE id=?", [row_id]),
    }


@router.post("/metrics/{metric_id}/snapshot")
def api_compute_snapshot(metric_id: int) -> dict:
    """对指定指标执行快照计算。"""
    svc = service_for("metric_store")
    return svc.compute_snapshot(metric_id)


@router.post("/segments/{segment_id}/compute")
def api_compute_segment(segment_id: int) -> dict:
    """对指定分群执行计算。"""
    svc = service_for("segment_engine")
    return svc.run_computation(segment_id)


@router.post("/funnels/{funnel_id}/compute")
def api_compute_funnel(funnel_id: int) -> dict:
    """对指定漏斗执行分析计算。"""
    svc = service_for("funnel_analyzer")
    return svc.run_computation(funnel_id)


@router.post("/retention/{task_id}/compute")
def api_compute_retention(task_id: int) -> dict:
    """对指定留存任务执行计算。"""
    svc = service_for("retention_analysis")
    return svc.run_computation(task_id)


@router.post("/forecast/{task_id}/compute")
def api_compute_forecast(task_id: int) -> dict:
    """对指定预测任务执行计算。"""
    svc = service_for("forecast_model")
    return svc.run_forecast(task_id)


@router.post("/comparison/{comparison_id}/compute")
def api_compute_comparison(comparison_id: int) -> dict:
    """对指定分群对比执行计算。"""
    svc = service_for("cohort_compare")
    return svc.run_comparison(comparison_id)


@router.post("/cube/{cube_id}/precompute")
def api_precompute_cube(cube_id: int) -> dict:
    """对指定Cube执行预计算。"""
    svc = service_for("cube_builder")
    return svc.run_precomputation(cube_id)


@router.post("/anomaly/{analysis_id}/run")
def api_run_anomaly_analysis(analysis_id: int) -> dict:
    """对指定归因分析器执行一次按选中异常时间的归因分析。"""
    svc = service_for("anomaly_analysis")
    return svc.run_analysis(analysis_id)


@router.get("/anomaly/{analysis_id}/attribution")
def api_anomaly_attribution(analysis_id: int) -> dict:
    """重新打开某次分析的归因数据（时点口径，不受后续补采影响）。"""
    svc = service_for("anomaly_analysis")
    return svc.latest_attribution(analysis_id)


@router.get("/inspection/orders")
def api_inspection_orders(analysis_id: int | None = None) -> dict:
    """查询已签发的巡检行动记录（已签单保留原分派记录）。"""
    if analysis_id is not None:
        rows = db.fetchall(
            "SELECT * FROM inspection_orders WHERE analysis_id=? ORDER BY id",
            [analysis_id],
        )
    else:
        rows = db.fetchall("SELECT * FROM inspection_orders ORDER BY id DESC LIMIT 100")
    return {"ok": True, "total": len(rows), "orders": rows}


@router.post("/report/{report_id}/generate")
def api_generate_report(
    report_id: int,
    payload: dict[str, Any] = Body(default={}),
) -> dict:
    """对指定报告模板执行生成，并按当次归因数据签发本班巡检单。

    可选 body：dispatch_process（提前复核 / 待二次观测）、shift_label（班次）。
    """
    svc = service_for("data_report")
    return svc.generate(
        report_id,
        dispatch_process=str(payload.get("dispatch_process") or "提前复核"),
        shift_label=str(payload.get("shift_label") or ""),
    )


@router.post("/alerts/check-all")
def api_check_all_alerts() -> dict:
    """检查所有已生效告警规则。"""
    svc = service_for("alert_policy")
    return svc.check_all()


@router.get("/analytics/full-pipeline")
def api_full_pipeline() -> dict:
    """执行完整分析链路：事件采集→聚合→指标→分群→漏斗→留存→预测→报告。"""
    steps = {}

    # 1. 事件采集
    svc_events = service_for("event_tracker")
    steps["events"] = svc_events.ingest_all_enabled()

    # 2. 聚合
    svc_agg = service_for("realtime_agg")
    steps["aggregation"] = svc_agg.run_all_active()

    # 3. 指标快照
    svc_metrics = service_for("metric_store")
    steps["metrics"] = svc_metrics.compute_all()

    # 4. 分群
    segments = db.fetchall("SELECT id FROM segments LIMIT 6")
    seg_results = []
    for s in segments:
        r = service_for("segment_engine").run_computation(s["id"])
        seg_results.append(r)
    steps["segments"] = {"processed": len(segments), "results": seg_results}

    # 5. 漏斗
    funnels = db.fetchall("SELECT id FROM funnels LIMIT 3")
    funnel_results = []
    for f in funnels:
        r = service_for("funnel_analyzer").run_computation(f["id"])
        funnel_results.append(r)
    steps["funnels"] = {"processed": len(funnels), "results": funnel_results}

    # 6. 留存
    ret_tasks = db.fetchall("SELECT id FROM retention_tasks LIMIT 3")
    ret_results = []
    for t in ret_tasks:
        r = service_for("retention_analysis").run_computation(t["id"])
        ret_results.append(r)
    steps["retention"] = {"processed": len(ret_tasks), "results": ret_results}

    # 7. 预测
    forecast_tasks = db.fetchall("SELECT id FROM forecast_tasks LIMIT 2")
    fc_results = []
    for t in forecast_tasks:
        r = service_for("forecast_model").run_forecast(t["id"])
        fc_results.append(r)
    steps["forecast"] = {"processed": len(forecast_tasks), "results": fc_results}

    # 8. 报告
    reports = db.fetchall("SELECT id FROM reports LIMIT 2")
    report_results = []
    for rep in reports:
        r = service_for("data_report").generate(rep["id"])
        report_results.append(r)
    steps["reports"] = {"processed": len(reports), "results": report_results}

    return {"ok": True, "pipeline": steps}