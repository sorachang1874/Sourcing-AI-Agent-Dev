#!/usr/bin/env python3
"""
fetch_ack.py — 批量下载 Anthropic 论文 HTML，提取 Acknowledgements，保存可审计数据
用法: python3 fetch_ack.py
输出:
  html_raw/<pid>.html     — 原始 HTML 全文
  ack_extracted/<pid>.txt — 提取出的 ACK 文本（或 NO_ACK）
  ack_summary.json        — 汇总结果
"""

import urllib.request, re, json, time, os, sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
HTML_DIR = os.path.join(BASE_DIR, "html_raw")
ACK_DIR  = os.path.join(BASE_DIR, "ack_extracted")
SUMMARY  = os.path.join(BASE_DIR, "ack_summary.json")
DB_PATH  = os.path.join(BASE_DIR, "arxiv_scan_db.json")

os.makedirs(HTML_DIR, exist_ok=True)
os.makedirs(ACK_DIR, exist_ok=True)

# ── 目标论文（上次检测 HTML 可用的 9 篇）────────────────────────────────────────
TARGETS = [
    "2401.05566",
    "2411.07494",
    "2412.13678",
    "2412.14093",
    "2503.10965",
    "2505.05410",
    "2601.04603",
    "2601.10387",
    "2601.20245",
]

# ── 工具函数 ──────────────────────────────────────────────────────────────────

def fetch_html(pid: str) -> str:
    """下载完整 HTML，优先读本地缓存。"""
    local = os.path.join(HTML_DIR, f"{pid}.html")
    if os.path.exists(local):
        with open(local, encoding="utf-8", errors="ignore") as f:
            print(f"  [cache] {pid}")
            return f.read()

    url = f"https://arxiv.org/html/{pid}"
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (compatible; research-bot/1.0)"})
    with urllib.request.urlopen(req, timeout=30) as r:
        content = r.read().decode("utf-8", errors="ignore")

    with open(local, "w", encoding="utf-8") as f:
        f.write(content)
    print(f"  [download] {pid} → {len(content)//1024}KB")
    return content


def extract_ack(html: str) -> str | None:
    """从 HTML 中提取 Acknowledgements 章节文本。"""
    # 匹配 Acknowledgements/Acknowledgments/Funding 标题后的段落
    patterns = [
        # h2/h3/h4 标题后的内容，直到下一个同级标题或 </article>
        r'(?i)<h[2-4][^>]*>[^<]*(?:acknowledgements?|acknowledgments?|funding acknowledgement)[^<]*</h[2-4]>\s*(.*?)(?=<h[1-4][\s>]|</article|</section|</body)',
        # section id 含 ack 的
        r'(?i)<section[^>]+id="[^"]*ack[^"]*"[^>]*>(.*?)</section>',
        # div class 含 ack
        r'(?i)<div[^>]+class="[^"]*ack[^"]*"[^>]*>(.*?)</div>',
    ]
    for pat in patterns:
        m = re.search(pat, html, re.DOTALL)
        if m:
            raw = m.group(1)
            text = re.sub(r'<[^>]+>', ' ', raw)
            text = re.sub(r'\s+', ' ', text).strip()
            if len(text) > 40:
                return text
    return None


def chinese_names_in_ack(ack_text: str) -> list[str]:
    """简单规则：找出 ACK 中可能是华人的名字。
    策略：找连续两段首字母大写的英文单词对，再看是否有常见华人姓氏。
    """
    CHINESE_SURNAMES = {
        "Bai","Chen","Cheng","Chu","Deng","Ding","Du","Fan","Feng","Fu",
        "Gao","Gong","Gu","Guo","Han","He","Hong","Hou","Hu","Huang",
        "Ji","Jia","Jiang","Jin","Kong","Li","Liang","Lin","Liu","Long",
        "Lou","Lu","Luo","Ma","Mao","Meng","Mo","Ni","Ning","Pan",
        "Peng","Qi","Qian","Qiao","Qin","Qiu","Qu","Ran","Ren","Shao",
        "Shen","Shi","Song","Su","Sun","Tang","Tao","Tian","Tong","Tu",
        "Wan","Wang","Wei","Wen","Wu","Xia","Xiao","Xie","Xu","Xue",
        "Yan","Yang","Yao","Ye","Yi","Yin","You","Yu","Yuan","Yue",
        "Zeng","Zhang","Zhao","Zheng","Zhou","Zhu","Zhuang","Zou",
        "Hua","Mu","Jing","Kai","Lei","Mei","Nan","Rong","Shan","Shu",
        "Wei","Xin","Yan","Yun","Zhen","Jun","Bin","Fang","Jian","Kun",
        "Liang","Ming","Ping","Qiang","Rui","Sheng","Ting","Xiang","Yong","Zhong",
        # 少数不那么常见但出现过的
        "Bao","Cai","Cao","Ce","Cui","Dai","Dong","Duan","Fei","Ge",
        "Gen","Gui","Hai","Hang","Hao","Heng","Jie","Jing","Juan","Ke",
        "Kuo","Lai","Lang","Lao","Le","Lie","Liu","Mao","Men","Mi",
        "Miao","Nan","Nie","Nong","Ou","Pai","Pang","Pei","Piao","Pu",
        "Ruan","Run","Sha","Shang","Shao","She","Shui","Si","Sui","Suo",
        "Tan","Teng","Ti","Tiao","Tie","Tong","Tou","Wa","Wai","Wan",
        "Xun","Yan","Yao","Ying","Yue","Yun","Ze","Zhai","Zhan","Zhe",
    }
    # 找 "Firstname Lastname" 格式（不含介词）
    tokens = re.findall(r'\b([A-Z][a-z]+)\s+([A-Z][a-z]+)\b', ack_text)
    hits = []
    seen = set()
    for first, last in tokens:
        if last in CHINESE_SURNAMES and first not in {"The","This","In","For","We","Our","Their","With","From","All","These","Such","Some"}:
            name = f"{first} {last}"
            if name not in seen:
                seen.add(name)
                hits.append(name)
    return hits


# ── 主流程 ────────────────────────────────────────────────────────────────────

def main():
    summary = {}

    # 加载已有 summary
    if os.path.exists(SUMMARY):
        with open(SUMMARY) as f:
            summary = json.load(f)

    for pid in TARGETS:
        if pid in summary:
            print(f"  [skip] {pid} already processed")
            continue

        print(f"\n── {pid} ──")
        try:
            html = fetch_html(pid)
        except Exception as e:
            print(f"  ❌ fetch failed: {e}")
            summary[pid] = {"status": "fetch_error", "error": str(e)}
            continue

        ack = extract_ack(html)
        ack_file = os.path.join(ACK_DIR, f"{pid}.txt")

        if ack:
            with open(ack_file, "w", encoding="utf-8") as f:
                f.write(ack)
            names = chinese_names_in_ack(ack)
            print(f"  ✅ ACK found ({len(ack)} chars)")
            print(f"  🔍 候选华人名: {names}")
            summary[pid] = {
                "status": "ack_found",
                "ack_chars": len(ack),
                "ack_preview": ack[:300],
                "candidate_chinese_names": names,
            }
        else:
            with open(ack_file, "w", encoding="utf-8") as f:
                f.write("NO_ACK")
            print(f"  ⚠️  no ACK section found")
            summary[pid] = {"status": "no_ack"}

        # 保存进度（每篇完成后即写入）
        with open(SUMMARY, "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        time.sleep(0.5)

    print("\n\n═══════════════════════════════")
    print(f"完成: {len(summary)} 篇")
    found = [pid for pid, v in summary.items() if v.get("status") == "ack_found"]
    print(f"有 ACK: {len(found)} 篇")
    for pid in found:
        names = summary[pid].get("candidate_chinese_names", [])
        print(f"  {pid}: {names}")
    print("═══════════════════════════════")


if __name__ == "__main__":
    main()
