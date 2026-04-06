#!/usr/bin/env python3
"""
LinkedIn Profile Fetcher — for biz-visit-onepager skill
用途：在本地（家庭/办公室网络）拉取 LinkedIn profile，输出 JSON 供 OpenClaw 生成 one-pager
作者：OpenClaw AI
版本：1.1
"""

import os, sys, json, argparse, textwrap, time

# ── 依赖检查 ────────────────────────────────────────────────────────────────
REQUIRED = ["linkedin_api", "requests"]
missing = []
for pkg in REQUIRED:
    try:
        __import__(pkg)
    except ImportError:
        missing.append(pkg)

if missing:
    print(f"[ERROR] 缺少依赖包：{', '.join(missing)}")
    print(f"[FIX]  请先运行：pip install {' '.join(p.replace('linkedin_api','linkedin-api') for p in missing)}")
    sys.exit(2)

from linkedin_api import Linkedin
from requests.cookies import RequestsCookieJar
import requests as req_lib


# ── Cookie 获取说明 ──────────────────────────────────────────────────────────
COOKIE_HELP = textwrap.dedent("""
  如何获取 LinkedIn Cookie（一次性操作）：
  1. 在 Chrome/Edge 中打开并登录 https://www.linkedin.com
  2. 按 F12 打开开发者工具 → 点击 "Application" 标签
  3. 左侧展开 "Cookies" → 点击 "https://www.linkedin.com"
  4. 找到以下两个 cookie，复制其 Value：
     - li_at      （很长的字符串，以 AQED 开头）
     - JSESSIONID （格式为 "ajax:数字"，带引号）
  5. 将两个值粘贴到下方提示或通过命令行参数传入

  ⚠️  注意：请在本地网络（家庭/办公室）运行此脚本，不要在服务器/VPN 环境下运行，
     否则会触发 LinkedIn 风控导致 cookie 失效。
""")


# ── 工具函数 ─────────────────────────────────────────────────────────────────
def err(msg, hint=None, code=1):
    print(f"\n[ERROR] {msg}", file=sys.stderr)
    if hint:
        print(f"[HINT]  {hint}", file=sys.stderr)
    sys.exit(code)


def warn(msg):
    print(f"[WARN]  {msg}", file=sys.stderr)


def info(msg):
    print(f"[INFO]  {msg}", file=sys.stderr)


def build_api(li_at: str, jsessionid: str) -> Linkedin:
    """构建 Linkedin API 实例，注入 cookie 到真实 requests.Session"""
    # linkedin-api 2.x 用 dict 存 cookies，会导致 requests 内部报错
    # 正确做法：先创建实例，再替换为真实 requests.Session
    try:
        api = Linkedin("", "", authenticate=False)
    except Exception as e:
        err(f"linkedin-api 初始化失败：{e}",
            "请确认 linkedin-api 版本：pip install --upgrade linkedin-api")

    real_session = req_lib.Session()
    real_session.cookies.set("li_at", li_at, domain=".www.linkedin.com")
    real_session.cookies.set("JSESSIONID", jsessionid, domain=".www.linkedin.com")
    real_session.headers.update({
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ),
        "accept": "application/vnd.linkedin.normalized+json+2.1",
        "accept-language": "en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7",
        "x-li-lang": "en_US",
        "x-restli-protocol-version": "2.0.0",
        "csrf-token": jsessionid.strip('"'),
        "x-li-track": json.dumps({
            "clientVersion": "1.13.5",
            "mpVersion": "1.13.5",
            "osName": "web",
            "timezoneOffset": 8,
            "timezone": "Asia/Shanghai",
            "deviceFormFactor": "DESKTOP",
            "mpName": "voyager-web",
        }),
        "referer": "https://www.linkedin.com/feed/",
    })
    api.client.session = real_session
    return api


def verify_cookie(api: Linkedin) -> dict:
    """验证 cookie 有效性，返回当前登录用户信息"""
    try:
        me = api.get_user_profile()
        if not me or "miniProfile" not in me:
            err(
                "Cookie 验证失败：返回数据异常",
                "请重新从 LinkedIn 网页端获取最新的 li_at 和 JSESSIONID"
            )
        return me
    except Exception as e:
        msg = str(e)
        if "403" in msg or "CSRF" in msg:
            err("Cookie 无效或已过期（403/CSRF）",
                "请重新登录 LinkedIn 网页端，重新获取 cookie")
        elif "999" in msg or "451" in msg:
            err("被 LinkedIn 识别为非正常终端（999/451）",
                "请确认在本地网络运行，不要使用 VPN 或服务器 IP")
        else:
            err(f"Cookie 验证失败：{msg}",
                "请重新获取 cookie 后重试")


def extract_photo_url(profile: dict) -> str:
    """从 profile 数据中提取最高清的头像 URL"""
    try:
        pics = profile.get("profilePicture", {}) or {}
        artifacts = (
            pics.get("displayImageReference", {})
                .get("vectorImage", {})
                .get("artifacts", [])
        ) or []
        if artifacts:
            best = max(artifacts, key=lambda a: a.get("width", 0))
            root = (
                pics.get("displayImageReference", {})
                    .get("vectorImage", {})
                    .get("rootUrl", "")
            )
            fid = best.get("fileIdentifyingUrlPathSegment", "")
            if root and fid:
                return root + fid
    except Exception:
        pass
    return ""


def fetch_profile(api: Linkedin, slug: str, include_skills: bool = True) -> dict:
    """拉取完整 profile，包含经历、教育、技能"""
    info(f"正在拉取 profile: {slug} ...")

    try:
        raw = api.get_profile(slug)
    except Exception as e:
        msg = str(e)
        if "999" in msg or "451" in msg:
            err(f"拉取 profile 时被 LinkedIn 封锁（{msg}）",
                "请确认在本地网络运行，不要使用 VPN 或服务器 IP")
        elif "Not Found" in msg or "404" in msg:
            err(f"未找到 LinkedIn 用户：{slug}",
                "请检查 slug 是否正确（LinkedIn URL 中 /in/ 后面的部分）")
        else:
            err(f"拉取 profile 失败：{msg}")

    if not raw:
        err(f"Profile 为空：{slug}", "请确认该账号存在且可公开访问")

    result = {
        "slug": slug,
        "name": f"{raw.get('firstName','')} {raw.get('lastName','')}".strip(),
        "headline": raw.get("headline", ""),
        "summary": raw.get("summary", ""),
        "location": raw.get("locationName", ""),
        "country": raw.get("geoCountryName", ""),
        "industry": raw.get("industryName", ""),
        "public_id": raw.get("public_id", slug),
        "profile_url": f"https://www.linkedin.com/in/{slug}/",
        "photo_url": extract_photo_url(raw),
        "education": [],
        "experience": [],
        "certifications": [],
        "languages": [],
        "skills": [],
        "_raw_keys": list(raw.keys()),
    }

    # 教育经历
    for edu in raw.get("education", []):
        tp = edu.get("timePeriod", {})
        result["education"].append({
            "school": edu.get("schoolName", ""),
            "degree": edu.get("degreeName", ""),
            "field": edu.get("fieldOfStudy", ""),
            "description": edu.get("description", ""),
            "activities": edu.get("activities", ""),
            "grade": edu.get("grade", ""),
            "start_year": tp.get("startDate", {}).get("year"),
            "end_year": tp.get("endDate", {}).get("year"),
        })

    # 工作经历
    for exp in raw.get("experience", []):
        tp = exp.get("timePeriod", {})
        start = tp.get("startDate", {})
        end = tp.get("endDate", {})
        result["experience"].append({
            "company": exp.get("companyName", ""),
            "title": exp.get("title", ""),
            "location": exp.get("locationName", ""),
            "description": exp.get("description", ""),
            "start": f"{start.get('year','?')}-{str(start.get('month','?')).zfill(2)}" if start else "",
            "end": f"{end.get('year','?')}-{str(end.get('month','?')).zfill(2)}" if end else "至今",
            "company_urn": exp.get("companyUrn", ""),
        })

    # 证书
    for cert in raw.get("certifications", []):
        result["certifications"].append({
            "name": cert.get("name", ""),
            "authority": cert.get("authority", ""),
            "year": cert.get("timePeriod", {}).get("startDate", {}).get("year"),
        })

    # 语言
    for lang in raw.get("languages", []):
        result["languages"].append({
            "name": lang.get("name", ""),
            "proficiency": lang.get("proficiency", ""),
        })

    # 技能（需要单独请求）
    if include_skills:
        try:
            info("正在拉取技能列表 ...")
            time.sleep(0.5)  # 礼貌性延迟，避免频率过高
            skills = api.get_profile_skills(slug)
            result["skills"] = [
                {"name": s.get("name", ""), "endorsements": s.get("endorsementCount", 0)}
                for s in (skills or [])
            ]
        except Exception as e:
            warn(f"技能列表拉取失败（非致命）：{e}")

    return result


def search_people(api: Linkedin, query: str, limit: int = 5) -> list:
    """搜索 LinkedIn 用户"""
    info(f"正在搜索：{query} ...")
    try:
        results = api.search_people(keywords=query, limit=limit)
        return [
            {
                "name": r.get("name", ""),
                "public_id": r.get("public_id", ""),
                "title": r.get("jobtitle", ""),
                "location": r.get("location", ""),
                "profile_url": f"https://www.linkedin.com/in/{r.get('public_id', '')}/" if r.get("public_id") else "",
                "urn": r.get("urn_id", ""),
            }
            for r in results
        ]
    except Exception as e:
        msg = str(e)
        if "999" in msg or "451" in msg:
            err("搜索时被 LinkedIn 封锁", "请确认在本地网络运行")
        warn(f"搜索失败：{e}")
        return []


# ── 主程序 ───────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(
        description="LinkedIn Profile Fetcher — 为 OpenClaw one-pager 生成准备数据",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=COOKIE_HELP,
    )
    parser.add_argument(
        "--slug", "-s",
        help="LinkedIn 公开 ID（URL 中 /in/ 后面的部分，如 haochenzhang）"
    )
    parser.add_argument(
        "--search", "-q",
        help="搜索关键词（如 'Haochen Zhang Anthropic'），输出候选列表"
    )
    parser.add_argument(
        "--li-at",
        default=os.environ.get("LINKEDIN_LI_AT", ""),
        help="li_at cookie 值（也可设为环境变量 LINKEDIN_LI_AT）"
    )
    parser.add_argument(
        "--jsessionid",
        default=os.environ.get("LINKEDIN_JSESSIONID", ""),
        help='JSESSIONID cookie 值（也可设为环境变量 LINKEDIN_JSESSIONID）'
    )
    parser.add_argument(
        "--no-skills",
        action="store_true",
        help="跳过技能列表拉取（速度更快，减少被检测风险）"
    )
    parser.add_argument(
        "--out", "-o",
        help="输出到文件（默认输出到 stdout）"
    )
    parser.add_argument(
        "--interactive", "-i",
        action="store_true",
        help="交互模式：若未提供 cookie，从命令行提示输入"
    )

    args = parser.parse_args()

    # ── 参数校验 ──────────────────────────────────────────────────────────
    if not args.slug and not args.search:
        parser.print_help()
        print("\n[ERROR] 请提供 --slug 或 --search 参数", file=sys.stderr)
        sys.exit(2)

    li_at = args.li_at.strip()
    jsessionid = args.jsessionid.strip()

    # 交互模式补全 cookie
    if args.interactive and (not li_at or not jsessionid):
        print(COOKIE_HELP)
        if not li_at:
            li_at = input("请输入 li_at 的值：").strip()
        if not jsessionid:
            jsessionid = input('请输入 JSESSIONID 的值（含引号，如 "ajax:123"）：').strip()

    if not li_at:
        err(
            "缺少 li_at cookie",
            "通过 --li-at 参数传入，或设置环境变量 LINKEDIN_LI_AT，或使用 -i 交互模式"
        )
    if not jsessionid:
        err(
            "缺少 JSESSIONID cookie",
            "通过 --jsessionid 参数传入，或设置环境变量 LINKEDIN_JSESSIONID，或使用 -i 交互模式"
        )

    # ── 建立连接 ──────────────────────────────────────────────────────────
    info("正在初始化 LinkedIn API ...")
    api = build_api(li_at, jsessionid)

    info("正在验证 cookie 有效性 ...")
    me = verify_cookie(api)
    login_name = me.get("miniProfile", {}).get("firstName", "未知")
    login_id = me.get("miniProfile", {}).get("publicIdentifier", "未知")
    info(f"✅ Cookie 有效，当前登录账号：{login_name}（{login_id}）")

    # ── 执行操作 ──────────────────────────────────────────────────────────
    if args.search:
        results = search_people(api, args.search)
        output = {
            "action": "search",
            "query": args.search,
            "results": results,
            "count": len(results),
            "_note": "复制目标用户的 public_id，用 --slug 参数拉取完整 profile",
        }
    else:
        profile = fetch_profile(api, args.slug, include_skills=not args.no_skills)
        output = {
            "action": "profile",
            "profile": profile,
            "_note": "将此 JSON 粘贴给 OpenClaw AI 生成 one-pager",
        }

    # ── 输出 ──────────────────────────────────────────────────────────────
    out_json = json.dumps(output, ensure_ascii=False, indent=2)

    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(out_json)
        info(f"✅ 结果已写入：{args.out}")
        info(f"   将文件内容粘贴给 OpenClaw AI，或上传文件即可生成 one-pager")
    else:
        print(out_json)

    info("完成。")


if __name__ == "__main__":
    main()
