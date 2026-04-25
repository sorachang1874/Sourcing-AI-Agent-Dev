# Cloudflare Same-Origin API Proxy

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


## Goal

把前端从“跨域直连 `https://api.111874.xyz`”升级成“同域访问 `/api/*`”。

这样前端可以：

- 不再依赖额外的 CORS/runtime backend URL 配置
- 直接用 `same-origin` 模式构建
- 后续无论后端挂在 ECS `nginx/caddy`，还是 Cloudflare Worker 反代，前端代码都不需要再改

## Frontend Build Mode

现在 hosted 前端支持两种正式模式：

1. `VITE_API_BASE_URL=same-origin`
   - 推荐长期模式
   - 前端直接请求 `https://demo.111874.xyz/api/*`
   - 要求同域 ingress 已经把 `/api/*` 代理到真实 backend

2. `VITE_API_BASE_URL=https://api.111874.xyz`
   - 兼容过渡模式
   - 仍然是前端跨域直连独立 API 域名

仓库当前默认 production 值已经改成：

```text
VITE_API_BASE_URL=same-origin
```

## Current Hosted Path

当前已经落地并验证通过的不是“单独 Worker route”，而是：

- Cloudflare Pages static frontend
- Pages Functions 负责同域代理：
  - `frontend-demo/functions/api/[[path]].js`
  - `frontend-demo/functions/health.js`
- shared proxy helper：
  - `frontend-demo/cloudflare_api_proxy.mjs`

也就是说，当前 `wrangler pages deploy dist ...` 时会一并上传 Functions bundle，
让以下路径直接同域代理到 `https://api.111874.xyz`：

- `https://demo.111874.xyz/api/*`
- `https://demo.111874.xyz/health`

这条路的好处是：

- 不依赖单独的 Workers deploy token 权限
- 直接复用当前 Pages deploy token
- 发布前端时同域 proxy 跟着一起更新，不再分成两次部署

## Worker Template

仓库内提供了一个 Cloudflare Worker 模板：

- `configs/cloudflare/demo_api_proxy_worker.mjs`
- `configs/cloudflare/wrangler.demo_api_proxy.toml.example`

它的职责很简单：

- 接收 `demo.111874.xyz/api/*`
- 转发到 `API_UPSTREAM_BASE`
- 保留请求方法、body、query string
- 透传常见转发头，并避免把前端域名错误地回指成 upstream 形成循环

## Recommended Setup

### Option A: Pages Functions

当前推荐的最小闭环方案：

1. Pages 继续托管静态前端
2. Pages Functions 处理：
   - `demo.111874.xyz/api/*`
   - `demo.111874.xyz/health`
3. Functions upstream 指向 `https://api.111874.xyz`
4. Pages build env 设置：

```text
VITE_API_BASE_URL=same-origin
VITE_USE_MOCK=false
VITE_USE_LOCAL_ASSETS=false
```

### Option B: Pages + Worker Route

仍可作为替代方案，但要求额外的 Workers deploy 权限：

1. Pages 继续托管静态前端
2. 给 `demo.111874.xyz/api/*` 挂 Cloudflare Worker route
3. Worker upstream 指向 `https://api.111874.xyz`
4. Pages build env 设置：

```text
VITE_API_BASE_URL=same-origin
VITE_USE_MOCK=false
VITE_USE_LOCAL_ASSETS=false
```

### Option C: ECS Same-Origin Ingress

适合后续进一步收敛基础设施：

1. 前端静态文件由 ECS 上的 `nginx/caddy` 托管
2. `/api/*` 同站反代到 Python backend
3. Cloudflare 只做 TLS / CDN
4. 前端仍然保持：

```text
VITE_API_BASE_URL=same-origin
```

## Why This Is Better Than Direct Cross-Origin

- 前端配置更稳定，不再需要给用户暴露 backend URL 语义
- CORS、mixed-content、runtime override 这些额外分叉可以逐步退出主路径
- 未来把后端从 `api.111874.xyz` 换到别的 ingress，前端构建不用跟着改
- 更符合服务级产品的同域入口习惯

## Validation

切到 same-origin 之后，上线前至少验证：

1. `https://demo.111874.xyz/api/runtime/health`
2. `https://demo.111874.xyz/api/providers/health`
3. 一个已有历史记录链接的恢复
4. 一个已有 job 的 dashboard 加载

如果这些路径在 `demo.111874.xyz` 下通了，前端就不再需要直接知道 `api.111874.xyz`。
