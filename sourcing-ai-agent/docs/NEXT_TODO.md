# Next TODO

## Goal

这份清单记录当前工作 backlog。它不是默认 onboarding 文档；接手项目时应先读 `docs/INDEX.md` 和 `../PROGRESS.md`，再回到这里看待办。

## Highest Priority

### 1. Prepare the repo for clean GitHub sync and collaboration

- 持续清理 README / onboarding / docs map，确保 GitHub 上不会出现失效链接和过时入口。
- 让当前实现、当前 provider 策略、当前资产治理规则都能在 repo 内自解释。
- push 前至少再做一轮 markdown link audit 和回归测试。

### 2. Run a new end-to-end company test on top of the stabilized workflow

- 优先选择一个新的真实组织做完整测试，而不是继续只围绕 TML 调参。
- 当前候选测试方向是 `Humans&`，目标是从用户 query 出发，验证：
  - 意图识别
  - asset reuse / refresh 判定
  - acquisition
  - retrieval
  - manual review routing
  - result delivery

### 3. Productize asset governance and snapshot promotion

- 把 `draft / partial / canonical / superseded / archived` 状态落成可执行规则，而不是只停留在文档。
- 为 company asset / scoped asset / retrieval artifact 建立更稳定的 registry 和 default pointer。
- 让上传新资产、替换旧 canonical、保留历史版本的流程标准化。

## Lower Priority

### 4. Continue service evolution work

- 把当前 hybrid 方向继续推进为：
  - cloud control plane
  - cloud asset plane
  - local acquisition-capable runner
- 先服务化 retrieval-only / asset-backed 请求，再考虑托管 acquisition。

## Resume Checklist

切换账号之后，先做：

1. 阅读 `docs/INDEX.md`
2. 阅读 `../PROGRESS.md`
3. 阅读最近的 canonical asset / validation 文档
4. 检查 provider 和 object storage 配置
5. 再回到这份 `docs/NEXT_TODO.md`

## Useful Commands

```bash
cd "sourcing-ai-agent"

PYTHONPATH=src python3 -m sourcing_agent.cli build-company-candidate-artifacts --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli complete-company-assets --company thinkingmachineslab --profile-detail-limit 12 --exploration-limit 2
PYTHONPATH=src python3 -m sourcing_agent.cli export-company-handoff-bundle --company thinkingmachineslab
PYTHONPATH=src python3 -m sourcing_agent.cli upload-asset-bundle --manifest runtime/asset_exports/<bundle>/bundle_manifest.json
```
