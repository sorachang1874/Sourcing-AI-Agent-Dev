#!/bin/bash
# fetch_one.sh <arxiv_id>
# 下载单篇论文 HTML 到本地，最多等 60 秒
PID=$1
OUT="/home/node/.openclaw/workspace/Anthropic华人专项/data/arxiv_scan/html_raw/${PID}.html"
if [ -f "$OUT" ]; then
  echo "[cache] $PID"
  exit 0
fi
echo "[fetch] $PID ..."
curl -sL --max-time 60 \
  -H "User-Agent: Mozilla/5.0 (compatible; research-bot/1.0)" \
  "https://arxiv.org/html/${PID}" \
  -o "$OUT"
CODE=$?
SIZE=$(wc -c < "$OUT" 2>/dev/null || echo 0)
echo "exit=$CODE size=${SIZE}"
