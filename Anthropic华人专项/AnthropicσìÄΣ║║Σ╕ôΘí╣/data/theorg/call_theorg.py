#!/usr/bin/env python3
"""
The Org API — Org Chart 安全调用脚本（数据原子落盘）
用法：cd /home/node/.openclaw/workspace/Anthropic华人专项 && python3 data/theorg/call_theorg.py

⚠️ 教训：2026-04-05 首次调用使用 curl 直接输出到终端，终端截断，
   导致 orgchart_raw_2026-04-05.json 仅保留 81 个节点（实际约 450）。
   本脚本通过先写 .tmp 再 rename 的原子操作保证数据完整性。
"""
import json, sys, os, requests
from datetime import date

# ── 配置 ──────────────────────────────────────────────────────
ACCOUNTS_FILE = "data/theorg/theorg_accounts.json"
DOMAIN        = "anthropic.com"
SLUG          = "anthropic"
OUT_DIR       = "data/theorg"

# ── 额度检查（强制，不通过则退出）────────────────────────────
with open(ACCOUNTS_FILE) as f:
    config = json.load(f)
acc = config["accounts"][0]
used  = acc["total_credits_used"]
limit = acc["monthly_limit_credits"]
if used >= limit:
    print(f"🚫 本月额度已耗尽 ({used}/{limit} credits)，下次可用: {acc.get('reset_date','月初')}")
    sys.exit(0)

# ── 幂等性检查：该公司本月是否已有存档 ───────────────────────
today = str(date.today())
out_file = f"{OUT_DIR}/orgchart_{SLUG}_{today}.json"
if os.path.exists(out_file):
    print(f"✅ 已有存档 {out_file}，跳过 API 调用")
    sys.exit(0)

# ── 实际调用 ─────────────────────────────────────────────────
print(f"🔄 调用 The Org API (domain={DOMAIN})...")
resp = requests.get(
    "https://api.theorg.com/v1.2/companies/org-chart",
    params={"domain": DOMAIN},
    headers={
        "X-Api-Key": acc["api_key"],
        "Content-Type": "application/json",
    },
    timeout=60,
)
resp.raise_for_status()
data = resp.json()

# ── 立即原子落盘（先写临时文件再 rename，防止写入中途崩溃）──
os.makedirs(OUT_DIR, exist_ok=True)
tmp_file = out_file + ".tmp"
with open(tmp_file, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)
os.rename(tmp_file, out_file)   # 原子替换，保证文件完整

# ── 统计节点 ─────────────────────────────────────────────────
nodes = data.get("data", [])
positions = [n for n in nodes if n.get("nodeType") == "position"]
print(f"✅ 存档完成: {out_file}")
print(f"   总节点: {len(nodes)} | position节点: {len(positions)}")

# ── 更新账号使用记录 ──────────────────────────────────────────
acc["total_credits_used"]    += 10
acc["total_org_chart_calls"] += 1
acc["usage_log"].append({
    "date": today, "credits": 10,
    "endpoint": f"GET /v1.2/companies/org-chart?domain={DOMAIN}",
    "result_nodes": len(nodes),
    "result_positions": len(positions),
    "out_file": out_file,
    "note": "Python 原子落盘",
})
if used + 10 >= limit:
    today_d = date.today()
    next_reset = f"{today_d.year}-{today_d.month+1:02d}-01" if today_d.month < 12 \
                 else f"{today_d.year+1}-01-01"
    acc["status"]     = "exhausted_this_month"
    acc["reset_date"] = next_reset
with open(ACCOUNTS_FILE, "w") as f:
    json.dump(config, f, ensure_ascii=False, indent=2)
print(f"   账号记录已更新: {used+10}/{limit} credits 已用")
print(f"\n📌 下一步：运行华人初筛脚本")
print(f"   python3 data/theorg/screen_chinese.py {out_file}")
