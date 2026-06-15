# Track C — C2: Minimal Auth + Tenant-Scoping (design for owner review)

> Status: design for owner review (2026-06-15). Built from a 5-reader read-only
> understand-workflow of the current auth/identity/ownership state. NOT yet
> implemented. Line refs anchored to `src/sourcing_agent/`.

C2 北极星(`SERVING_EXECUTION_NORTH_STAR.md` §C2):单 org / 静态 per-user bearer /
无登录 UI;**收紧 user-private 读路径**(scope by authenticated owner),提交端
**服务端覆盖** body 的身份字段;shared-canonical(company_assets/projections/...)
**不** scope。

## 1. 现状(understand-workflow 核验)

- **零用户鉴权**:全 app 唯一 auth 是 provider webhook token,只守一条路由
  (`api.py` post_apify_webhook)。其余 ~60 路由全开。所有 handler 已收到 live
  `Request`(`_make_endpoint` `api.py:360`),可读 `request.headers` / `request.state`;
  目前无任何 `request.state.user/identity`。中间件栈三层
  (`_RawPathTargetMiddleware` → `_RequestConcurrencyMiddleware` → `_CorsHeaderMiddleware`
  → routing),auth gate 自然插在 `create_app` `api.py:62` 的 add_middleware 序列里。
- **所有权数据模型大部分已在**(C2 ≈ 「加 WHERE」,非全量 schema 改):
  - `jobs`:有 `requester_id` + `tenant_id`(`storage.py:1494`),但 list/get 路径不 scope;
    仅 query_dispatches 的 reuse/join 路径用它。
  - `crm_records`:有 `workspace_id`(indexed UNIQUE)+ `owner_user_id`;list/by-person
    已按 workspace_id scope,仅 get-by-id(PK)不 scope。
  - ~30 control-plane 表有 `workspace_id` 且已 filter。
  - `frontend_history_links`:**无任何所有权列**——唯一需 schema 改 + backfill 的表。
- **关键告警(真正的 long pole)**:这些所有权列**当前 collapse 成 `'default'`/空**
  (workspace_id `'default'` 出现 ~150 处;dispatch 把 user_id→requester_id、
  workspace_id→tenant_id 但都映射到空/default)。即:**列在、值不在**——app 实质
  single-tenant。所以 WHERE-scope 要真正区分,必须先有 ingress 身份注入(让新行带真
  owner)+ 对存量行的一次性归因 backfill(否则存量全是 'default')。
- **租户语义分裂**:`tenant_id`(jobs/dispatch)vs `workspace_id`(CRM/control-plane)
  ——rollout 前需统一。`requester_id` 只在 2 张表(jobs、query_dispatches)。
- **over-reach 面**:每个 `{id}` 读路由取裸 id 无所有权校验(get_job + 全部 10 个子资源、
  get_crm_record、frontend-history/{id}、export poll/artifact、target-candidate profile、
  persons/{key})。**无 `GET /api/jobs` list 路由**(C2 设计里的 "list_jobs" 无 HTTP 面)。
- **客户端身份字段(须服务端派生)**:(A) **决定所有权/scope**(spoof 可越权/复用他人 job):
  CRM 提交的 `workspace_id`(`_require_crm_public_web_body_workspace_id`
  `crm_public_web_owner.py:2480`)、dispatch 的 `requester_id`/`tenant_id`
  (`_build_query_dispatch_context` `orchestrator.py:52740`);(B) **仅 attribution**:
  reviewer/actor(plan-review/manual-review/operation-action)。plan 提交本身不读
  requester——身份在 dispatch-context 构建时才进入,故覆盖点在 `_build_query_dispatch_context`。
- **shared-canonical(C2 不 scope)**:company-assets/evidence/assertions、projection 读族
  (projection/{id}/candidates/search/persons)、persons、collections、media、operations
  ——表按 company_key/projection_id/person_identity_key 键,无 requester 列;三份设计文档
  (`SERVING_MESH_OWNERSHIP_BOUNDARY.md` §2c/§4 等)明确禁止 C2 scope 它们(会破坏单 org dedup/reuse)。

## 2. 设计(深重构:auth + 服务端派生身份 + 收紧读侧)

1. **认证**:`_AuthMiddleware` 插在中间件栈(CORS 之后、routing 之前),从
   `Authorization: Bearer <token>` 解析 token → 查 per-user token→identity 映射 →
   设 `request.state.identity = {user_id, ...}`;无/错 token → 401。webhook 路由 +
   health 等少数公开路由豁免。单 org、静态 token(无登录 UI)。
2. **服务端派生身份(覆盖客户端)**:在提交边界用 `request.state.identity` **覆盖**
   并 strip 客户端发来的 requester_id/tenant_id/workspace_id/user_id/org_id;reviewer/actor
   也设为 authenticated identity。覆盖点:plan/workflow 路径在 `_build_query_dispatch_context`
   前注入;CRM/company/profile public-web + operation-action 在 handler 边界用派生
   workspace_id/requested_by/actor。下游 scope-match 代码不变(值已服务端派生后即正确)。
3. **收紧 user-private 读**:get_job(+10 子资源,全部 key off `store.get_job(job_id)`)、
   get_crm_record(by-id)、export poll/artifact、frontend-history、target-candidate detail
   ——加 authenticated-owner 谓词;**mismatch 返回 404**(非 403,防 id 枚举)。
   shared-canonical 读族**不动**(冻结 mesh 边界)。
4. **frontend_history**:无所有权列;从关联 job 的 requester_id 派生 owner(免 schema 改),
   或加列 + backfill。
5. **8 槽护栏 / 中间件顺序**:auth 在 concurrency 之内(已过 CORS preflight)、routing 之前。

## 3. Owner 决策点（2026-06-15 已批）

**RATIFIED**:(a) per-user bearer token,env JSON map `SOURCING_API_BEARER_TOKENS={"<token>":"<user_id>"}`;(b) **forward-only**(建立 auth+注入+读侧 scoping 机制;新行带真 owner;存量 'default' 行视为 pre-auth legacy 可读;不 backfill);(c) **按 user scope**(jobs.requester_id / crm.owner_user_id;tenant_id/workspace_id 由 auth identity 派生为该 user 的固定 namespace);(d) **hard 401 + 前端同批加 bearer**(后端无 token 即 401;frontend-demo fetchJson 加 env-注入 static bearer);(e) frontend_history owner 从关联 job 派生(免 schema);(f) mismatch 返回 **404**(防枚举);shared-canonical 读族不 scope。

实现增量序:**C2.1 auth 基座**(middleware + identity + env token config + 公开路由豁免 + 测试)→ **C2.2 服务端派生身份**(提交边界覆盖+strip 客户端身份)→ **C2.3 收紧 user-private 读**(get_job+子资源/get_crm_record/exports/frontend-history,owned-mismatch→404,legacy 'default' 可读)→ **C2.4 前端 bearer** → 评审。

### 原始决策点（保留供追溯）

- **(a) Auth token 模型**:per-user bearer token,如何配置 token→user_id? 建议 env JSON map
  (`SOURCING_API_BEARER_TOKENS={"<token>":"<user_id>",...}`)——最简、无登录 UI、per-user
  可 scope。(单一共享 token 则无法 per-user scope。)
- **(b) scope 深度 vs `'default'` collapse**:存量行 owner 全是 'default'/空。
  - 建议 **forward-only**:新行带真 owner(身份注入后);存量 'default' 行视为 pre-auth
    legacy(对任何 authenticated user 可读 OR admin-only),不做高风险 backfill。
  - 备选:对存量做归因 backfill(从 job 的既有 requester_id;但多数本就空)——成本高、可疑。
- **(c) 租户语义**:单 org 下「owner」=**user**。建议按 **user**(jobs.requester_id /
  crm.owner_user_id)scope;tenant_id/workspace_id 由 auth identity 派生为该 user 的固定
  namespace(统一二者),而非 org 级。请确认按 user-scope(非 org-scope)。
- **(d) 强制度 / rollout**:hard 401(无 token 即拒)是安全正解,但会**立刻打断当前前端**
  (frontend-demo 不发 Authorization,需先加 static bearer)。建议:后端 hard 401 +
  前端同批加 bearer(env 注入 fetchJson);或短暂 `SOURCING_API_AUTH_ENFORCE` 开关灰度。
- **(e) frontend_history**:派生 owner(免 schema)vs 加列 + backfill。建议派生(免 schema 改)。
