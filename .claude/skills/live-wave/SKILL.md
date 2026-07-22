---
name: live-wave
description: 跑一轮付费 live 采集/判定波次（Harvest/Apify 采集、Grok 采集、DeepSeek judge、CSV 导出）。当用户要求 live 抓取、补 profile、跑 judge 批次、重出交付 CSV，或提到 salvage/slot-fill/former lane 时使用。
---

# Live 波次运维

前提门（任一不满足即停）：配额未耗尽（chshapi / HarvestAPI 月度墙先查 PROGRESS.md）；
操作者明确批准付费范围；live 三重门 env 只在批准后设置。

## 顺序（每步的完整纪律见引用文档）

1. **盘点先行**：本地已有 dataset/已下载/已合并 + 远端 Apify run 历史，打印 delta；
   delta=0 不提交。已付 dataset 是收据——先 salvage（`live_apify_dataset_salvage.py`）
   再考虑重抓。
2. **进程 vintage**：任何长驻服务驱动付费流程前，`ps -o lstart -p <pid>` 对比关键代码
   mtime；不新鲜就先重启或改走 library/脚本路径。
3. **派发**：只用 committed 脚本（注册表 = `sourcing-ai-agent/scripts/README.md`）；
   批次几何 400–800 url/run、4–8 并发；per-function functionIds 永不合并；dry-run 先
   验证 payload 形状。
4. **中止**：kill driver ≠ abort run——先列 run id，调 `/v2/actor-runs/{id}/abort`；
   cancel 后手动清 `runtime_provider_limiter_leases` 并取消其 worker。
5. **judge/导出**：`live_grok_collection_run.py` → `live_luna_judge_run.py`（--limit
   先 smoke）→ `live_xfirst_export_csv.py`（确定性，可先复现基线再改输入）。

## References

- 脚本注册表与家族：`sourcing-ai-agent/scripts/README.md`
- Provider 知识与现场纪律全文：`sourcing-ai-agent/docs/HARVESTAPI_PLAYBOOK.md`
- 付费纪律与禁令（僵尸命令永不 retry/resume）：workspace `CLAUDE.md` + `PROGRESS.md` hazards
