"""异常归因领域服务：对指标异常波动自动执行多维度归因和贡献度分析。

真实数据流：确定分析对象和异常时间点 → pipeline.detect_anomaly 按选中时间
读取时点快照（样本充足 / 零基线 / 样本不足三类都保留归因数据）→ 按归因维度
解构贡献度 → 输出分析结论 → 写入 anomaly_analyses 与 anomaly_attributions 表。
"""
from __future__ import annotations

import json
from typing import Any

from app.database import db
from app.pipeline import detect_anomaly
from app.services.base import BaseModuleService


class AnomalyAnalysisService(BaseModuleService):
    MODULE = "anomaly_analysis"
    ENTITY = "归因分析器"
    OPERATION_TERM = "分析"
    LIST_FIELDS = ["分析对象", "异常时间点", "归因维度", "贡献度排序", "分析结论"]
    REQUIRED_FIELDS = ["分析对象", "异常时间点", "归因维度"]
    STATUS_ORDER = ["待分析", "分析中", "已完成", "分析失败"]
    ACTION_RULES = {"启动分析": "已完成", "取消分析": "分析失败"}
    NEGATIVE_ACTIONS = []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        """启动分析时真正执行归因管线，而不是只翻转状态。"""
        if action == "启动分析":
            result = self.run_analysis(entry_id)
            entry = self.get_entry(entry_id)
            if entry is None:
                return None, f"{self.ENTITY} {entry_id} 不存在或已归档"
            message = result.get("conclusion") or result.get("message") or "分析完成"
            return entry, message
        return super().run_action(entry_id, action)

    def run_analysis(self, entry_id: int) -> dict[str, Any]:
        """执行异常归因分析。先加载记录，再调用管线，最后落归因快照。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return {"ok": False, "message": f"分析记录 {entry_id} 不存在"}

        # 标记为分析中
        db.update("anomaly_analyses", {"status": "分析中"}, "id=?", [entry_id])

        analysis_data = {
            "分析对象": entry.get("target_name") or entry.get("分析对象"),
            "异常时间点": entry.get("anomaly_time") or entry.get("异常时间点"),
            "归因维度": entry.get("attribution_dimensions") or entry.get("归因维度"),
        }
        result = detect_anomaly(analysis_data)

        if result.get("status") == "分析失败":
            db.update("anomaly_analyses", {
                "status": "分析失败",
                "conclusion": result.get("reason", "分析失败"),
            }, "id=?", [entry_id])
            return {"ok": False, "message": result.get("reason", "分析失败"), **result}

        # 回写结论
        conclusion = result.get("conclusion", "分析完成")
        contributions = result.get("contributions", {})
        ranking_json = json.dumps(
            sorted(contributions.items(), key=lambda x: -x[1])[:5],
            ensure_ascii=False,
        )

        # 落归因快照：样本充足、零基线、资料尚缺三类都保留
        db.insert("anomaly_attributions", {
            "analysis_id": entry_id,
            "target_name": result.get("target_name", ""),
            "selected_time": result.get("selected_time", ""),
            "target_time": result.get("target_time"),
            "actual_value": result.get("actual_value"),
            "z_score": result.get("z_score"),
            "data_status": result.get("data_status", ""),
            "baseline_count": result.get("baseline_count", 0),
            "baseline_zero_count": result.get("baseline_zero_count", 0),
            "baseline_mean": result.get("baseline_mean"),
            "baseline_std": result.get("baseline_std"),
            "sample_window_start": result.get("sample_window_start"),
            "sample_window_end": result.get("sample_window_end"),
            "anomaly_kind": result.get("anomaly_kind", ""),
            "review_decision": result.get("review_decision", ""),
            "attribution_json": json.dumps(result, ensure_ascii=False),
        })

        db.update("anomaly_analyses", {
            "status": "已完成",
            "contribution_ranking": ranking_json,
            "conclusion": conclusion,
        }, "id=?", [entry_id])

        return {"ok": True, "target": result.get("target_name"), **result}

    def latest_attribution(self, entry_id: int) -> dict[str, Any]:
        """读取分析记录最新一次归因快照（重新打开旧时间归因时用）。"""
        entry = self.get_entry(entry_id)
        if entry is None:
            return {"ok": False, "message": f"分析记录 {entry_id} 不存在"}
        attr = db.fetchone(
            "SELECT * FROM anomaly_attributions WHERE analysis_id=? ORDER BY id DESC LIMIT 1",
            [entry_id],
        )
        if not attr:
            return {"ok": False, "message": "尚无归因数据，请先启动分析", "entry": entry}
        try:
            attr["attribution"] = json.loads(attr.get("attribution_json") or "{}")
        except json.JSONDecodeError:
            attr["attribution"] = {}
        return {"ok": True, "entry": entry, "attribution": attr}

    def recent_anomalies(self, limit: int = 20) -> list[dict[str, Any]]:
        """获取最近的分析结果。"""
        return db.fetchall(
            "SELECT * FROM anomaly_analyses WHERE status='已完成' ORDER BY id DESC LIMIT ?",
            [limit],
        )

    def domain_snapshot(self) -> dict[str, Any]:
        snapshot = super().domain_snapshot()
        snapshot["total_analyses"] = db.count("anomaly_analyses")
        snapshot["completed_analyses"] = db.count("anomaly_analyses", "status='已完成'")
        snapshot["total_attributions"] = db.count("anomaly_attributions")
        snapshot["insufficient_attributions"] = db.count(
            "anomaly_attributions", "data_status IN ('样本不足','无有效观测')")
        return snapshot
