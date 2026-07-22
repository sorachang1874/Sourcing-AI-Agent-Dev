#!/bin/bash
# ============================================================
# LinkedIn Profile Fetcher — 一键运行脚本
# 用法：bash run.sh
# ============================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_SCRIPT="$SCRIPT_DIR/linkedin_fetch.py"

# ── 颜色输出 ─────────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo -e "${BLUE}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BLUE}║   LinkedIn Profile Fetcher for OpenClaw      ║${NC}"
echo -e "${BLUE}╚══════════════════════════════════════════════╝${NC}"
echo ""

# ── 检查 Python ───────────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
    echo -e "${RED}[ERROR] 未找到 python3，请先安装 Python 3.8+${NC}"
    echo "  macOS: brew install python3"
    echo "  官网:  https://www.python.org/downloads/"
    exit 1
fi

PYTHON_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
echo -e "${GREEN}✓ Python $PYTHON_VER${NC}"

# ── 检查并安装依赖 ────────────────────────────────────────────
echo -e "\n${YELLOW}[1/3] 检查依赖...${NC}"
if ! python3 -c "import linkedin_api" &>/dev/null; then
    echo "  正在安装 linkedin-api ..."
    pip3 install linkedin-api --quiet
    echo -e "  ${GREEN}✓ linkedin-api 已安装${NC}"
else
    echo -e "  ${GREEN}✓ linkedin-api 已存在${NC}"
fi

# ── 读取 Cookie ───────────────────────────────────────────────
echo -e "\n${YELLOW}[2/3] 输入 LinkedIn Cookie${NC}"
echo ""
echo "  获取方式："
echo "  1. Chrome 登录 https://www.linkedin.com"
echo "  2. F12 → Application → Cookies → https://www.linkedin.com"
echo "  3. 分别复制 li_at 和 JSESSIONID 的 Value"
echo ""
echo -e "  ${RED}⚠️  请确保使用本地网络（家庭/办公室），不要开 VPN${NC}"
echo ""

# 检查是否已有环境变量
if [ -n "$LINKEDIN_LI_AT" ] && [ -n "$LINKEDIN_JSESSIONID" ]; then
    echo -e "  ${GREEN}✓ 检测到环境变量，使用已有 cookie${NC}"
    LI_AT="$LINKEDIN_LI_AT"
    JSESSIONID="$LINKEDIN_JSESSIONID"
else
    read -p "  请输入 li_at 的值：" LI_AT
    echo ""
    read -p '  请输入 JSESSIONID 的值（含引号，如 "ajax:123"）：' JSESSIONID
    echo ""

    if [ -z "$LI_AT" ] || [ -z "$JSESSIONID" ]; then
        echo -e "${RED}[ERROR] Cookie 不能为空${NC}"
        exit 1
    fi
fi

# ── 选择操作 ──────────────────────────────────────────────────
echo -e "\n${YELLOW}[3/3] 选择操作${NC}"
echo ""
echo "  1) 通过 LinkedIn Slug 直接拉取 profile（已知链接，推荐）"
echo "  2) 搜索用户名，获取候选列表后再拉取"
echo ""
read -p "  请选择 [1/2]（默认 1）：" CHOICE
CHOICE="${CHOICE:-1}"
echo ""

if [ "$CHOICE" = "2" ]; then
    read -p "  请输入搜索关键词（如：Haochen Zhang Anthropic）：" QUERY
    echo ""
    echo -e "${BLUE}► 开始搜索...${NC}"
    python3 "$PYTHON_SCRIPT" \
        --search "$QUERY" \
        --li-at "$LI_AT" \
        --jsessionid "$JSESSIONID"
    echo ""
    echo -e "${YELLOW}► 从上方结果中找到目标用户的 public_id，然后重新运行选择选项 1${NC}"
else
    echo "  LinkedIn Profile URL 示例：https://www.linkedin.com/in/haochenzhang/"
    read -p "  请输入 Slug（/in/ 后面的部分，如 haochenzhang）：" SLUG
    echo ""

    # 询问是否输出到文件
    read -p "  是否保存到文件？[y/N]：" SAVE_FILE
    echo ""

    if [[ "$SAVE_FILE" =~ ^[Yy]$ ]]; then
        OUT_FILE="${SLUG}_linkedin_$(date +%Y%m%d_%H%M%S).json"
        echo -e "${BLUE}► 开始拉取 profile，结果将保存到：$OUT_FILE${NC}"
        python3 "$PYTHON_SCRIPT" \
            --slug "$SLUG" \
            --li-at "$LI_AT" \
            --jsessionid "$JSESSIONID" \
            --out "$OUT_FILE"
        echo ""
        echo -e "${GREEN}══════════════════════════════════════════${NC}"
        echo -e "${GREEN}✅ 完成！${NC}"
        echo -e "${GREEN}   文件：$OUT_FILE${NC}"
        echo -e "${GREEN}   下一步：将文件内容粘贴给 OpenClaw AI 生成 one-pager${NC}"
        echo -e "${GREEN}══════════════════════════════════════════${NC}"
    else
        echo -e "${BLUE}► 开始拉取 profile...${NC}"
        python3 "$PYTHON_SCRIPT" \
            --slug "$SLUG" \
            --li-at "$LI_AT" \
            --jsessionid "$JSESSIONID"
        echo ""
        echo -e "${GREEN}✅ 完成！将上方 JSON 粘贴给 OpenClaw AI 生成 one-pager${NC}"
    fi
fi
