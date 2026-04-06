#!/usr/bin/env python3
"""
Hunter.io Domain Search — 全量拉取脚本（原子落盘）
用法：cd /home/node/.openclaw/workspace/Anthropic华人专项 && python3 data/hunter/call_hunter.py

设计原则（吸取 The Org API 2026-04-05 数据丢失教训）：
1. 先写 .tmp，再 os.rename() 原子替换，防止写入中途崩溃导致文件损坏
2. 每翻一页立即追加到内存列表，全部完成后一次性写盘（不边翻页边写文件）
3. 调用前检查额度，调用后立即更新账号记录
4. 幂等性检查：同一域名同一天已有存档则跳过
"""
import json, sys, os, time, requests
from datetime import date

# ── 配置 ──────────────────────────────────────────────────────
ACCOUNTS_FILE = "data/hunter/hunter_accounts.json"
DOMAIN        = "anthropic.com"
SLUG          = "anthropic"
OUT_DIR       = "data/hunter"
PER_PAGE      = 100
SEARCH_COST   = 1               # 每次翻页消耗 1 search credit
SLEEP_BETWEEN = 1.0             # 翻页间隔（秒），避免触发限流

# ── 读取账号配置 ──────────────────────────────────────────────
with open(ACCOUNTS_FILE) as f:
    config = json.load(f)
acc = config["accounts"][0]
API_KEY = acc["api_key"]

# ── 额度检查 ─────────────────────────────────────────────────
used  = acc["total_searches_used"]
limit = acc["monthly_search_limit"]
available = limit - used
print(f"📊 当前 search 额度: 已用 {used}/{limit}，剩余 {available}")

# ── 幂等性检查：今天是否已有存档 ─────────────────────────────
today = str(date.today())
out_file = f"{OUT_DIR}/domain_search_{SLUG}_{today}.json"
if os.path.exists(out_file):
    print(f"✅ 已有今日存档 {out_file}，跳过 API 调用")
    with open(out_file) as f:
        saved = json.load(f)
    print(f"   存档记录数: {len(saved.get('emails', []))}")
    sys.exit(0)

# ── 第一次调用：获取总记录数 ─────────────────────────────────
print(f"\n🔍 查询 {DOMAIN} 总记录数...")
resp = requests.get(
    "https://api.hunter.io/v2/domain-search",
    params={"domain": DOMAIN, "api_key": API_KEY, "limit": 1, "offset": 0},
    timeout=30,
)
resp.raise_for_status()
meta = resp.json().get("meta", {})
total = meta.get("results", 0)
import math
total_pages = math.ceil(total / PER_PAGE)
calls_needed = total_pages  # 每页1次
print(f"   总记录: {total} | 需翻页: {total_pages} 次 | 消耗额度: {calls_needed}")

if calls_needed > available - 2:  # 保留 2 次余量
    print(f"⚠️  额度不足（需 {calls_needed}，剩余 {available}），中止")
    sys.exit(1)

# ── 全量翻页拉取 ──────────────────────────────────────────────
all_emails = []
pattern    = None
organization = None
errors_occurred = []

for page in range(total_pages):
    offset = page * PER_PAGE
    print(f"  📄 第 {page+1}/{total_pages} 页 (offset={offset})...", end=" ", flush=True)
    
    try:
        r = requests.get(
            "https://api.hunter.io/v2/domain-search",
            params={
                "domain": DOMAIN,
                "api_key": API_KEY,
                "limit": PER_PAGE,
                "offset": offset,
            },
            timeout=30,
        )
        r.raise_for_status()
        page_data = r.json()
        
        if page_data.get("errors"):
            print(f"❌ API Error: {page_data['errors']}")
            errors_occurred.append({"page": page, "error": page_data["errors"]})
            break
        
        emails = page_data.get("data", {}).get("emails", [])
        all_emails.extend(emails)
        
        if page == 0:
            pattern = page_data.get("data", {}).get("pattern")
            organization = page_data.get("data", {}).get("organization")
        
        print(f"✅ 获取 {len(emails)} 条（累计 {len(all_emails)}）")
        
        if len(emails) < PER_PAGE:
            print(f"  ℹ️  最后一页（{len(emails)} < {PER_PAGE}），停止翻页")
            break
            
    except requests.RequestException as e:
        print(f"❌ 网络错误: {e}")
        errors_occurred.append({"page": page, "error": str(e)})
        break
    
    if page < total_pages - 1:
        time.sleep(SLEEP_BETWEEN)

# ── 原子落盘 ─────────────────────────────────────────────────
payload = {
    "_meta": {
        "domain": DOMAIN,
        "organization": organization,
        "email_pattern": pattern,
        "fetched_at": today,
        "total_reported": total,
        "total_fetched": len(all_emails),
        "pages_fetched": page + 1,
        "errors": errors_occurred,
        "account_id": acc["id"],
    },
    "emails": all_emails,
}

os.makedirs(OUT_DIR, exist_ok=True)
tmp_file = out_file + ".tmp"
with open(tmp_file, "w", encoding="utf-8") as f:
    json.dump(payload, f, ensure_ascii=False, indent=2)
os.rename(tmp_file, out_file)   # 原子替换

print(f"\n✅ 原子落盘完成: {out_file}")
print(f"   总记录: {len(all_emails)} 条 | organization: {organization} | pattern: {pattern}")

# ── 更新账号使用记录 ──────────────────────────────────────────
pages_called = page + 1
acc["total_searches_used"] += pages_called
acc["usage_log"].append({
    "date": today,
    "action": "domain_search_full",
    "domain": DOMAIN,
    "searches_used": pages_called,
    "records_fetched": len(all_emails),
    "out_file": out_file,
    "note": f"全量拉取 {DOMAIN}，{total_pages} 页原子落盘",
})
acc["archived_domains"].append({
    "domain": DOMAIN,
    "date": today,
    "records": len(all_emails),
    "file": out_file,
})

with open(ACCOUNTS_FILE, "w", encoding="utf-8") as f:
    json.dump(config, f, ensure_ascii=False, indent=2)
print(f"   账号记录已更新: {acc['total_searches_used']}/{acc['monthly_search_limit']} searches 已用")
print(f"\n📌 下一步：运行华人初筛")
print(f"   python3 data/hunter/screen_chinese.py {out_file}")
