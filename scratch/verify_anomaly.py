"""巡检场景验证：零基线首次变化、样本不足单列、补采不改签、时点基线。"""
import json
import os
import sys

sys.path.insert(0, "/workspace/backend")

from app.database import db  # noqa: E402
from app.services import service_for  # noqa: E402

PASS, FAIL = "PASS", "FAIL"
checks = []


def check(name, cond, detail=""):
    checks.append((name, bool(cond), detail))
    print(f"[{PASS if cond else FAIL}] {name}" + (f" | {detail}" if detail else ""))


def observe(metric_id, value, t):
    db.insert("metric_values", {
        "metric_id": metric_id, "snapshot_value": value, "snapshot_time": t,
        "calibration_version": 1, "baseline_value": None,
        "comparison_status": "已保存", "z_score": None,
    })


# ---------- 场景数据 ----------
old_id = db.insert("metrics", {"metric_name": "老渠道退款笔数", "metric_type": "counter",
                               "source_module": "metric_store", "calibration_version": 1, "unit": "笔"})
new_id = db.insert("metrics", {"metric_name": "新渠道退款笔数", "metric_type": "counter",
                               "source_module": "metric_store", "calibration_version": 1, "unit": "笔"})
var_id = db.insert("metrics", {"metric_name": "波动渠道订单量", "metric_type": "counter",
                               "source_module": "metric_store", "calibration_version": 1, "unit": "单"})

# 老渠道：9 次按时采样都为 0，09:00 第一次测到 2
for h in range(9):
    observe(old_id, 0, f"2026-11-24T0{h}:00:00")
observe(old_id, 2, "2026-11-24T09:00:00")
# 新渠道：只到了一次 0
observe(new_id, 0, "2026-11-24T09:00:00")
# 波动渠道：有真实方差的基线 + 一个显著异常
for i, v in enumerate([10, 12, 11, 13, 12, 11]):
    observe(var_id, v, f"2026-11-24T0{i}:00:00")
observe(var_id, 40, "2026-11-24T09:00:00")

an_svc = service_for("anomaly_analysis")
rep_svc = service_for("data_report")


def make_analysis(target, time_str):
    entry, missing = an_svc.create_entry({"分析对象": target, "异常时间点": time_str, "归因维度": "[]"})
    assert not missing, missing
    return entry["id"]


a_old = make_analysis("老渠道退款笔数", "2026-11-24T09:00:00")
a_new = make_analysis("新渠道退款笔数", "2026-11-24T09:00:00")
a_var = make_analysis("波动渠道订单量", "2026-11-24T09:00:00")
a_void = make_analysis("老渠道退款笔数", "2026-11-20T09:00:00")  # 选中时间之前无观测

# ---------- 1. 零基线首次变化：无标准分，基线九次零可查 ----------
r_old = an_svc.run_analysis(a_old)
check("老渠道 data_status=零基线", r_old.get("data_status") == "零基线", r_old.get("data_status"))
check("老渠道 z_score 为 None（无标准分）", r_old.get("z_score") is None)
check("老渠道实测=2", r_old.get("actual_value") == 2)
check("老渠道实际目标时间=09:00", r_old.get("target_time") == "2026-11-24T09:00:00")
check("老渠道基线 9 次全零", r_old.get("baseline_count") == 9 and r_old.get("baseline_zero_count") == 9)
check("老渠道有效采样范围记录", r_old.get("sample_window_start") == "2026-11-24T00:00:00"
      and r_old.get("sample_window_end") == "2026-11-24T08:00:00")
check("老渠道可派复核", r_old.get("review_decision") == "可派复核")
check("老渠道两种流程可选", set(r_old.get("eligible_processes", [])) == {"提前复核", "待二次观测"})
check("老渠道首次变化标记", r_old.get("first_change") is True and r_old.get("anomaly_kind") == "零基线首次变化")
check("老渠道不算普通标准分异常", r_old.get("is_anomaly") is False)

# ---------- 2. 新渠道资料尚缺：不能冒充平稳 ----------
r_new = an_svc.run_analysis(a_new)
check("新渠道 data_status=样本不足", r_new.get("data_status") == "样本不足")
check("新渠道 z_score 为 None", r_new.get("z_score") is None)
check("新渠道暂缓派单", r_new.get("review_decision") == "样本不足暂缓")
check("新渠道结论未宣称平稳", "延续平稳" not in r_new.get("conclusion", "")
      and "未检测到显著异常" not in r_new.get("conclusion", ""), r_new.get("conclusion"))
check("新渠道空态说明原因", "样本不足" in r_new.get("conclusion", ""))

# ---------- 3. 样本充足：正常出标准分 ----------
r_var = an_svc.run_analysis(a_var)
check("波动渠道 data_status=样本充足", r_var.get("data_status") == "样本充足")
check("波动渠道有标准分且显著", r_var.get("z_score") is not None and abs(r_var["z_score"]) >= 3,
      f"z={r_var.get('z_score')}")
check("波动渠道可派复核", r_var.get("review_decision") == "可派复核")

# ---------- 4. 无有效观测：空态记录目标时间 ----------
r_void = an_svc.run_analysis(a_void)
check("无观测 data_status=无有效观测", r_void.get("data_status") == "无有效观测")
check("无观测记录选中时间", r_void.get("selected_time") == "2026-11-20T09:00:00")
check("无观测暂缓派单", r_void.get("review_decision") == "样本不足暂缓")

# 三类归因数据都保留
attrs = db.fetchall("SELECT * FROM anomaly_attributions")
check("归因快照已保留(>=4条)", len(attrs) >= 4, f"{len(attrs)} 条")

# ---------- 5. 本班巡检单（提前复核流程） ----------
rep_entry, missing = rep_svc.create_entry({
    "报告名称": "早班巡检报告", "包含指标集": json.dumps([old_id, new_id, var_id]), "报告周期": "班次"})
assert not missing
report_id = rep_entry["id"]

gen1 = rep_svc.generate(report_id, dispatch_process="提前复核", shift_label="早班")
check("报告生成 ok", gen1.get("ok") is True)
orders_old = db.fetchall("SELECT * FROM inspection_orders WHERE analysis_id=?", [a_old])
orders_new = db.fetchall("SELECT * FROM inspection_orders WHERE analysis_id=?", [a_new])
orders_var = db.fetchall("SELECT * FROM inspection_orders WHERE analysis_id=?", [a_var])
check("老渠道已签巡检单", len(orders_old) == 1)
o_old = orders_old[0]
check("老渠道派单=可派复核/提前复核", o_old["dispatch_status"] == "可派复核"
      and o_old["dispatch_process"] == "提前复核")
check("老渠道巡检单记录 09:00 采样依据", "2026-11-24T09:00:00" in o_old["dispatch_note"]
      and "实测 2" in o_old["dispatch_note"] and "标准分 无" in o_old["dispatch_note"],
      o_old["dispatch_note"])
check("老渠道巡检单记录九次零基线", "基线 9 次（零值 9 次）" in o_old["dispatch_note"])
check("新渠道巡检单=样本不足暂缓", len(orders_new) == 1
      and orders_new[0]["dispatch_status"] == "样本不足暂缓"
      and orders_new[0]["dispatch_process"] == "暂不派单")
check("波动渠道巡检单=可派复核", len(orders_var) == 1
      and orders_var[0]["dispatch_status"] == "可派复核")
check("缺资料单列", any(r["target_name"] == "新渠道退款笔数" for r in gen1["insufficient_records"]))
check("缺资料未混入可行动记录", all(r["target_name"] != "新渠道退款笔数" for r in gen1["inspection_records"]))
check("汇总含可派/暂缓计数", gen1["summary"].get("dispatchable_count") == 2
      and gen1["summary"].get("insufficient_attribution_count") == 2, json.dumps(gen1["summary"], ensure_ascii=False))

# ---------- 6. 补采 10:00=3，重开 09:00 归因 ----------
observe(old_id, 3, "2026-11-24T10:00:00")
r_old2 = an_svc.run_analysis(a_old)
check("补采后 09:00 仍实测 2", r_old2.get("actual_value") == 2)
check("补采后 09:00 仍无标准分", r_old2.get("z_score") is None)
check("补采后基线仍九次零（10:00 未挪入）", r_old2.get("baseline_count") == 9
      and r_old2.get("baseline_zero_count") == 9)
check("补采后采样范围不变", r_old2.get("sample_window_end") == "2026-11-24T08:00:00")

# ---------- 7. 重新生成报告：已签巡检保留原分派 ----------
gen2 = rep_svc.generate(report_id, dispatch_process="待二次观测", shift_label="早班")
orders_old2 = db.fetchall("SELECT * FROM inspection_orders WHERE analysis_id=?", [a_old])
check("重出报告不重复签单", len(orders_old2) == 1)
check("第一次派单对象不被补写改变", orders_old2[0]["dispatch_process"] == "提前复核"
      and orders_old2[0]["dispatch_status"] == "可派复核")
rec_old2 = [r for r in gen2["inspection_records"] if r["target_name"] == "老渠道退款笔数"]
check("重开报告引用原巡检单", len(rec_old2) == 1 and rec_old2[0]["dispatch_process"] == "提前复核")

# ---------- 8. 待二次观测流程：新分析签新单，同样记录 09:00 依据 ----------
a_old2 = make_analysis("老渠道退款笔数", "2026-11-24T09:00:00")
an_svc.run_analysis(a_old2)
rep2, _ = rep_svc.create_entry({
    "报告名称": "二班巡检报告", "包含指标集": json.dumps([old_id]), "报告周期": "班次"})
gen3 = rep_svc.generate(rep2["id"], dispatch_process="待二次观测", shift_label="二班")
o_wait = db.fetchone("SELECT * FROM inspection_orders WHERE analysis_id=?", [a_old2])
check("待二次观测流程签单", o_wait is not None and o_wait["dispatch_status"] == "待二次观测"
      and o_wait["dispatch_process"] == "待二次观测")
check("待二次观测单同样记录 09:00 采样依据", "2026-11-24T09:00:00" in o_wait["dispatch_note"]
      and "标准分 无" in o_wait["dispatch_note"], o_wait["dispatch_note"])

# ---------- 9. 通用动作入口：启动分析真正跑归因 ----------
a_act = make_analysis("波动渠道订单量", "2026-11-24T09:00:00")
entry, msg = an_svc.run_action(a_act, "启动分析")
check("动作入口启动分析完成", entry is not None and entry["status"] == "已完成", msg)
check("动作入口产出归因快照", db.count("anomaly_attributions", "analysis_id=?", [a_act]) == 1)

# ---------- 10. 补采重试：同一时间重写不污染基线 ----------
observe(old_id, 2, "2026-11-24T09:00:00")  # 09:00 重试，值不变
r_retry = an_svc.run_analysis(a_old)
check("重试后 09:00 实测仍为 2", r_retry.get("actual_value") == 2)
check("重试后基线仍九次零（重复时间已折叠）", r_retry.get("baseline_count") == 9
      and r_retry.get("baseline_zero_count") == 9)
check("重试后仍无标准分", r_retry.get("z_score") is None)
observe(old_id, 5, "2026-11-24T09:00:00")  # 09:00 重试修正为 5
r_fix = an_svc.run_analysis(a_old)
check("重试修正后取最新写入值 5", r_fix.get("actual_value") == 5)
check("修正后基线仍九次零", r_fix.get("baseline_count") == 9
      and r_fix.get("baseline_zero_count") == 9)
orders_old3 = db.fetchall("SELECT * FROM inspection_orders WHERE analysis_id=?", [a_old])
check("补采重试后已签巡检仍保留原分派", len(orders_old3) == 1
      and orders_old3[0]["dispatch_process"] == "提前复核"
      and orders_old3[0]["dispatch_status"] == "可派复核")

# ---------- 11. 真零延续：足够零样本才算平稳，零样本不被删 ----------
observe(new_id, 0, "2026-11-24T09:30:00")  # 新渠道第二次观测仍为 0
r_new2 = an_svc.run_analysis(a_new)
check("新渠道第二次观测后仍样本不足", r_new2.get("data_status") == "样本不足")
check("新渠道仍不能显示已证实平稳", "平稳" not in r_new2.get("conclusion", "").replace("不能判定平稳或异常", "")
      or "不能" in r_new2.get("conclusion", ""), r_new2.get("conclusion"))
zero_id = db.insert("metrics", {"metric_name": "静默通道心跳", "metric_type": "counter",
                                "source_module": "metric_store", "calibration_version": 1, "unit": "次"})
for h in range(10):
    observe(zero_id, 0, f"2026-11-24T0{h}:00:00" if h < 10 else f"2026-11-24T{h}:00:00")
a_zero = make_analysis("静默通道心跳", "2026-11-24T09:00:00")
r_zero = an_svc.run_analysis(a_zero)
check("真零延续 data_status=零基线", r_zero.get("data_status") == "零基线")
check("真零延续无需派单", r_zero.get("review_decision") == "无需派单")
check("真零样本全部保留", r_zero.get("baseline_count") == 9 and r_zero.get("baseline_zero_count") == 9)
check("真零延续结论确认平稳", "平稳" in r_zero.get("conclusion", ""), r_zero.get("conclusion"))

# ---------- 12. 取消分析 ----------
a_cancel = make_analysis("波动渠道订单量", "2026-11-24T09:00:00")
entry_c, _ = an_svc.run_action(a_cancel, "取消分析")
check("取消分析流转为分析失败", entry_c is not None and entry_c["status"] == "分析失败")

print()
failed = [c for c in checks if not c[1]]
print(f"共 {len(checks)} 项检查，失败 {len(failed)} 项")
sys.exit(1 if failed else 0)
