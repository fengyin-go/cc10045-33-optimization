"""HTTP 层端到端验证：本班巡检单 → 补采 → 重开归因与报告。"""
import json
import urllib.request

B = "http://127.0.0.1:8019"


def req(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    r = urllib.request.Request(B + path, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(r) as resp:
        return json.loads(resp.read())


def post(path, payload=None):
    return req("POST", path, payload)


def get(path):
    return req("GET", path)


old = post("/api/pipeline/metrics/register", {"metric_name": "老渠道退款笔数"})["metric"]["id"]
new = post("/api/pipeline/metrics/register", {"metric_name": "新渠道退款笔数"})["metric"]["id"]

for h in range(9):
    post(f"/api/pipeline/metrics/{old}/observe", {"value": 0, "snapshot_time": f"2026-11-24T0{h}:00:00"})
post(f"/api/pipeline/metrics/{old}/observe", {"value": 2, "snapshot_time": "2026-11-24T09:00:00"})
post(f"/api/pipeline/metrics/{new}/observe", {"value": 0, "snapshot_time": "2026-11-24T09:00:00"})

a1 = post("/api/anomaly_analysis", {"values": {"分析对象": "老渠道退款笔数", "异常时间点": "2026-11-24T09:00:00", "归因维度": "[]"}})["entry"]["id"]
a2 = post("/api/anomaly_analysis", {"values": {"分析对象": "新渠道退款笔数", "异常时间点": "2026-11-24T09:00:00", "归因维度": "[]"}})["entry"]["id"]

r1 = post(f"/api/pipeline/anomaly/{a1}/run")
r2 = post(f"/api/pipeline/anomaly/{a2}/run")
print("老渠道归因:", r1["data_status"], "| z:", r1["z_score"], "| 实测:", r1["actual_value"],
      "| 基线:", r1["baseline_count"], "零:", r1["baseline_zero_count"], "|", r1["review_decision"])
print("新渠道归因:", r2["data_status"], "| z:", r2["z_score"], "|", r2["conclusion"])

rid = post("/api/data_report", {"values": {"报告名称": "早班巡检", "包含指标集": "[]",
                                           "报告周期": "班次", "输出格式": "json", "自动发送": "0"}})["entry"]["id"]

print("\n--- 本班巡检单（提前复核） ---")
g1 = post(f"/api/pipeline/report/{rid}/generate", {"dispatch_process": "提前复核", "shift_label": "早班"})
for r in g1["inspection_records"]:
    print("可行动:", r["target_name"], "|", r["dispatch_status"], "|", r["dispatch_process"])
    print("   依据:", r["dispatch_note"])
for r in g1["insufficient_records"]:
    print("单列:", r["target_name"], "|", r["dispatch_status"])
    print("   依据:", r["dispatch_note"])
print("汇总:", json.dumps(g1["summary"], ensure_ascii=False))

print("\n--- 补采 10:00=3，重试 09:00=2 ---")
post(f"/api/pipeline/metrics/{old}/observe", {"value": 3, "snapshot_time": "2026-11-24T10:00:00"})
post(f"/api/pipeline/metrics/{old}/observe", {"value": 2, "snapshot_time": "2026-11-24T09:00:00"})

att = get(f"/api/pipeline/anomaly/{a1}/attribution")["attribution"]
print("重开归因:", att["target_time"], "实测", att["actual_value"], "z=", att["z_score"],
      "基线", att["baseline_count"], "零", att["baseline_zero_count"])

print("\n--- 重新生成报告（换待二次观测流程） ---")
g2 = post(f"/api/pipeline/report/{rid}/generate", {"dispatch_process": "待二次观测", "shift_label": "早班"})
for r in g2["inspection_records"]:
    print("重出报告:", r["target_name"], "|", r["dispatch_status"], "|", r["dispatch_process"], "| issued", r["issued_at"])

orders = get(f"/api/pipeline/inspection/orders?analysis_id={a1}")
print("已签巡检:", orders["total"], "张 |", orders["orders"][0]["dispatch_process"],
      "/", orders["orders"][0]["dispatch_status"])
