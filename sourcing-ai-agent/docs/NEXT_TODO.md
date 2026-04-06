# Next TODO

## Goal

这份清单用于在切换账号、切换设备或开启新的 AI session 后，直接继续当前项目，而不依赖聊天上下文。

## Highest Priority

### 1. Continue Thinking Machines Lab asset accumulation

当前已完成：

- current roster
- prioritized current detail
- former fallback
- publication supplementation
- manual review resolution
- handoff bundle durable sync to R2

下一步继续补：

- former leads
- unresolved publication leads
- corner-case web exploration assets
- 可复用的 normalized candidate artifacts

### 2. Strengthen object storage sync operability

当前已完成：

- 并发 upload/download
- per-object retry
- local/remote bundle index
- local/remote sync run manifest
- Thinking Machines Lab handoff bundle R2 `upload -> download -> restore`

下一步要补：

- failed upload/download 的 resume strategy
- 更细粒度 progress summary
- “最近可恢复 bundle” / “最近成功 sync run” 的快捷入口
- 针对大 bundle 的 selective restore / partial pull

### 3. Prepare server-oriented runtime model

后续目标已经明确：

- Sourcing AI Agent 运行在长期在线服务器
- provider secrets 由 secret manager 注入
- 高价值 runtime 资产进入 object storage
- 相似 query 尽量复用既有资产，而不是重复 acquisition

下一步建议：

- 定义 server deployment checklist
- 定义 object storage prefix / environment layout
- 定义 server-side runtime bootstrap
- 定义 asset reuse / asset refresh policy

## Lower Priority

### 4. Improve object storage provider compatibility

当前 provider：

- `filesystem`
- `s3_compatible`

后续可补：

- direct OSS compatibility tuning
- richer object metadata
- lifecycle / retention policy

### 5. Add restore convenience tooling

后续可补：

- `bootstrap-device`
- `pull-latest-company-handoff`
- `restore-latest-sqlite`
- “仅恢复某公司某 snapshot” 的快捷命令

## Resume Checklist

切换账号之后，先做：

1. 阅读 `docs/HANDOFF_2026-04-06.md`
2. 阅读 `docs/RECOVERY_TUTORIAL.md`
3. 阅读这份 `docs/NEXT_TODO.md`
4. 检查 object storage 配置
5. 继续做 `Thinking Machines Lab asset accumulation` 和 `server-oriented runtime model`
