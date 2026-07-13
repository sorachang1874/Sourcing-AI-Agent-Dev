# Track D 机制×不变量矩阵（DESIGN_INVARIANT_CHECKLIST v2 协议产物）

> Status: Matrix evidence record（v2 2026-07-13：v7 文档 + 清单 v2【十类】的完整 180 格矩阵，
> 每格由独立代理产出、非作者自评——满足 R6#11 对完整矩阵的要求。v1 History：v6 前的九类
> violations-only 扫描（74 项）见 git 历史）。obligation 格 = 实施批开工义务（与上层计划 §6
> 义务清单并轨）；na = 该类不适用；satisfied 格附 § 证据。


## 1-单写者/聚合所有权（satisfied 17/18）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b owner 矩阵(verification_state owner=验证 owner，readers 永不直写)+§2.2 v5 单写者拆分：验证聚合只由验证 owner 写，gate 只由 plan review owner 写，域事件+reducer 连接；状态迁移表全部为类型化 owner 命令触发。 |
| verification_intent(+epoch/claim gen/phase gen) | satisfied | D3 §4c：intent 迁移全部经 事件→reducer→验证 owner 域命令；generic 控制面只动 runtime 现态、域侧 supersession 不由控制面直写；§4b 矩阵 decision_generation/intent 禁止一切读方消费写路径。 |
| identity_search_budget_grant | satisfied | D3 §2.1(v6 R5#4)：revoke 走单写者路径——非 plan-review-owner 的转移经 事件→reducer→plan-review owner 的 grant 命令收敛，不跨 owner 直写；grant 进 §4b owner 矩阵(owner=plan review owner，消费方=Tier-2 命令的 pre-transport CAS 扣减)。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：apply owner=plan review owner，gate 只由其写(v5 单写者拆分)；watermark 在 apply owner 单 UoW 内单调推进；record not_applied 永不触发 gate 更新——验证 owner 不越权写 gate。 |
| company.identity.verification.record(CAS) | satisfied | D3 §2.2/§4c：record owner=验证 owner，单 UoW『只写自己的聚合』(verification 行+intent 迁移+证据)，跨聚合的 gate 更新经 recorded 域事件→reducer→apply 命令(plan review owner)。 |
| expiry 定时/expire 命令 | satisfied | D3 §4b 迁移表 expire 行(v5 round4#4)：接受时同 UoW 持久化 not-before 定时事件，定期域 owner 扫描→reducer 计划 expire 命令；读路径只做 fail-closed 派生，『过期行按 needs_human 消费但零写零 enqueue』——读者去写化明文。 |
| 迁移桥(文件注册表+快照) | obligation | D3 §4a 已定 PG canonical 单 owner+全部写入方迁移/停写+决定永不回写文件；但单写者成立依赖写入方全量 inventory(计划 §6.1 实施义务#1，round-4 仍在点名新快照写方)，穷举前文件侧多写者未消除。 |
| Tier-1/Tier-2 命令+续跑编舞 | satisfied | D3 §2.3(v7 R6#4)：phase 结果事件→reducer 计划 search.expand/后继 verify.evidence 子命令(各绑自己 claim/attempt)，无命令内等待、不跨 owner；仅显式 final_adjudication 可达 record，中间 phase 结构上到不了。 |
| 证据 bundle+adjudication manifest | satisfied | D3 §6：bundle 不可变、owner 侧 resolve，模型只引用 evidence_ids、provenance 全服务端派生；§4c(R6#6)：manifest 由验证 owner 在裁决开始前持久化，record CAS 校验全终态聚合 hash——单一写方即验证 owner。 |
| ModelInvocationEnvelope | satisfied | D0 §2.2(v6 R5#8)：单一物理 schema、canonical 字段清单唯一定义处，D3 §6 引用不复述(sweep 修正双定义)；信封为服务端不可变结果侧契约，由 turn/命令 owner 在调用点铸造，模型不可自证任何字段。 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3 budget_reservation_ref：『单一 PG cost-ledger owner』，父行预留+子行暴露，状态机 prepared→dispatching→…；D3 §8 逐字对齐同一台账口径，无第二记账写方。 |
| 逻辑结果槽(+消费 CAS) | satisfied | D0 §2.5(v5+sweep blocker 修正)：『槽状态转移的写者 = turn owner 的域命令』，generic 控制面经 事件→reducer→turn owner 命令收敛，不跨 owner 直写；消费 CAS 为完整终局结果的原子消费 UoW，与 D3 §4c 同构。 |
| ModelTurnExecutionContext | satisfied | D0 §2.3：frozen 不可变值对象，workspace/actor/permission 由 owner 铸造(v3#14)；§0：只有 typed model-turn owner 可提供 context，缺任一字段构造期 raise——无第二铸造方、无可变共享状态。 |
| ActionRequestSpec+schema pins | satisfied | D0 §3.1：request_schema 单一事实源、三方(提交/planner/dispatch)消费同一份；pin=AgentAction 行上 owner 写入的不可变列，OperationRun 在实际创建点(approve/retry)copy+verify，不静默沿用任一侧。 |
| agent tool registry/agent_tool_enabled | satisfied | D0 §3.2-3.3：served 集从 ActionRegistry 的 dispatch_adapter 字段派生(替换 orchestrator 硬编码字面集合的第二事实源)；端点只读 GET，command 级字段复用 command_type_manifest() 序列化、不第三处重组。 |
| agent_events 投影 | satisfied | 计划 §2 D2(v2/v3)：『投影 ownership = turn owner 单写者』、禁止独立推进 workflow 状态、terminal 真值只在 canonical workflow 表、可从 canonical 事件重放重建；精确投影契约细化为实施义务#2(计划 §6.2)。 |
| 路由注册表+有效快照 | satisfied | D0 §4：checked-in、版本化、product-owned，路由变更=配置提交+审计非运行时行为，与 reviewer 路由表零耦合；§2.3：有效快照在 action/approval/command 创建点由 owner 固化为不可变引用，随 retry 传播、无人改写。 |
| scripted 回放转写治理 | satisfied | D0 §2.8：转写唯一写方=录制管线(包装 live session、仅 owner 显式授权运行)，落 runtime 命名空间不入库；转写 tenant-bound、只存归一化脱敏事件，写后即证据 artifact 不可变，读方(回放)fail-closed 校验指纹不回写。 |

## 2-租户键（satisfied 12/18）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b：唯一键 (runtime_namespace, workspace_id, company_fingerprint)，workspace 在行身份与索引；owner 矩阵限定消费方，公共 ingress 禁入。 |
| verification_intent(+epoch/claim gen/phase gen) | satisfied | D3 §4c：(workspace_id, intent_id) 身份，v5 明文「workspace 等式进入索引、命令/事件引用、证据 bundle、repository 授权、幂等 scope 与下述 CAS 的每一条」（round4#3 修复）。 |
| identity_search_budget_grant | satisfied | D3 §2.1：grant 键 =（workspace_id, review_session_id, intent generation, policy_revision）；pre-transport 授权 CAS fence 绑 workspace 键的 intent/grant 精确签发身份。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：幂等键 = apply:<workspace_id>:<session_id>:<源域事件 id>；apply owner UoW「CAS 于 {workspace + session 当前 revision + source 事件 id + generation 规则}」。 |
| company.identity.verification.record(CAS) | satisfied | D3 §2.2 幂等键 record:<workspace_id>:<intent_id>（R6#1）；§4c 全条件 CAS 首条即「workspace_id 匹配 AND intent_id 匹配 …」。 |
| expiry 定时/expire 命令 | obligation | D3 §2.2/§4b 只声明定时事件携带 generation 并 CAS 于当前 generation；expire 命令的幂等键与 CAS 谓词未显式列 workspace（record/apply 均已列）——实施批比照命名空间化并入竞态电池。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a：无 workspace 的全局记录进 quarantine 映射表（显式映射或 shared-origin，不静默落 workspace）；bridge 命中无显式 workspace 映射 ⇒ diagnostic-only needs_human；workspace 决定永不回写全局文件。 |
| Tier-1/Tier-2 命令+续跑编舞 | obligation | intent/grant 绑定均 workspace 键（D3 §4c/§2.1），但 verify.evidence 各 phase 子命令与 search.expand 的幂等键未像 record/apply 那样显式含 workspace——实施批按 R6#1 同法命名空间化。 |
| 证据 bundle+adjudication manifest | satisfied | D3 §4c：manifest 键 = workspace/intent/phase generation；bundle 不可变绑 workspace 键 intent/attempt，bundle hash 入信封与 record CAS（§6）；上层 §6 义务#5 另补 judge_call_key 的 workspace 维度。 |
| ModelInvocationEnvelope | satisfied | D0 §2.2 canonical 清单含 workspace/actor/permission 身份，ToolTurnResult 冗余镜像 workspace_id/actor_id「信封为权威」；D3 §6 同一物理定义含 tenant/permission/policy 身份，经接受 CAS 全链绑定。 |
| 成本台账(预留+暴露行) | obligation | D0 §2.3：子行身份 = (reservation, activity_attempt, physical_call_index)，父行挂 operation——workspace 仅经 operation 间接，台账行身份/索引与释放 CAS 未显式声明租户列；实施批 DDL 落表时补。 |
| 逻辑结果槽(+消费 CAS) | satisfied | D0 §2.5：owner 维护「workspace-scoped 逻辑结果槽」，槽行身份即租户界定；消费 CAS 谓词绑 canonical operation/turn/command pins，§2.1 把 workspace/actor 绑入 ToolTurnResult/journal 身份。 |
| ModelTurnExecutionContext | satisfied | D0 §2.3：workspace_id/actor_id/permission_scope 显式字段（v3#14 租户/主体绑定、owner 铸造）；缺任一字段 live 构造期 raise；三者并入 §2.8 request hash。 |
| ActionRequestSpec+schema pins | na | schema 注册表是 product-owned 全局契约配置，非租户级 durable 状态；pin 作为不可变列落在已带租户身份的 AgentAction/OperationRun 行上（D0 §3.1），自身无租户键可言。 |
| agent tool registry/agent_tool_enabled | na | served 子集是全租户一致的只读目录投影（D0 §3.2/§3.3），非租户数据；新路由的 token 鉴权属 C2 hosted 激活依赖（上层计划 §3 D1 行）。 |
| agent_events 投影 | satisfied | 上层计划 §2 D2（v3 存储契约）：三表 PG-only，agent_events 带「workspace 租户列与授权 scope」+ per-stream sequence_number/idempotency_key；cursor 授权细则 = 义务清单#2。 |
| 路由注册表+有效快照 | na | ModelRouteRegistry 为 checked-in、product-owned 版本化配置（D0 §4），快照为非密钥配置内容（§2.3）——非租户状态；其 digest 与租户身份在 §2.8 request hash 中共同绑定。 |
| scripted 回放转写治理 | satisfied | D0 §2.8（v7 R6#10）：request hash 含 workspace_id/actor_id/permission_scope/transcript_digest；live 转写 tenant-bound 跨租户不可回放，仅显式 synthetic fixture 可复用且接受时校验信封租户等式。 |

## 3-世代与物理围栏(含物理约束交互/同步围栏)（satisfied 15/18）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b decision_generation 单调+状态迁移全走类型化 owner 命令+superseded 现态由新 generation 整行替换；写入经 §4c 全条件 CAS（decision_generation=<expected>+claim gen+control epoch+runtime_namespace 模式失配即拒）。 |
| verification_intent(+epoch/claim gen/phase gen) | obligation | D3 §4c 围栏设计完备（claim_generation+durable control epoch 封 requeue→重claim ABA，requeue 同 UoW 先递增 epoch；§2.3 phase_generation），但依赖的永不重置 claim generation 物理列今日不存在——已诚实化为 D3 批前置 migration（计划 §6.6）。 |
| identity_search_budget_grant | satisfied | D3 §2.1：不可变签发身份（grant_id+issuance_generation+授予事件id），终态永不复活、再授予=新行；supersede-with-transfer 原子迁余额；pre-transport CAS 含 claim generation/control epoch/attempt/精确签发（R5#3/#4）。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：watermark 单调、每次成功 apply（含 blocking）恒推进，clearing 仅 generation>watermark，晚到旧 clearing 永拒；CAS 于 session revision+源事件 id；幂等键 apply:<ws>:<session>:<源事件id> 按 v7 R6#1 命名空间化避开 (workflow_run_id,idempotency_key) 互撞。 |
| company.identity.verification.record(CAS) | satisfied | D3 §4c 单 UoW 全条件 CAS 含 workspace+intent pending+claim_generation+attempt+预期终态事件+decision_generation+pins+bundle watermark+control epoch+OperationRun 非终态+manifest hash；控制转移在各自 commit 同 UoW 同步推进围栏（v7 R6#5 同步围栏）；键 record:<ws>:<intent> 命名空间化（R6#1）。 |
| expiry 定时/expire 命令 | satisfied | D3 §2.2 反向失效：expiry 定时事件携带 generation、触发时 CAS 于当前 generation（旧定时器不可失效新决定）；接受时同 UoW 持久化 not-before 定时事件，读者零写零 enqueue（§4b 迁移表 expire 行）。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a：bridge 命中且无显式 workspace 映射=diagnostic-only needs_human，不得授权任何决定；backfill 行永不 verified 态；precedence preflight 保证 PG 行在位时文件/快照不可能胜出——无版本文件数据结构上进不了接受路径。 |
| Tier-1/Tier-2 命令+续跑编舞 | satisfied | D3 §2.3（v7 R6#4）：后继 verify.evidence 子命令携 root intent+单调 phase_generation+1，每个子命令各绑自己的 claim/attempt；只有显式 final_adjudication 能计划 record，中间 phase 结构上到不了 record。 |
| 证据 bundle+adjudication manifest | satisfied | D3 §6 不可变 bundle hash 绑入调用信封；§4c（v7 R6#6）manifest 键=workspace/intent/phase generation，record CAS 要求全终态聚合 hash 匹配——缺席兄弟候选⇒聚合不成立⇒needs_human，幸存者唯一有效被结构性堵死。 |
| ModelInvocationEnvelope | satisfied | D0 §2.2 单一物理 schema（canonical 定义一处），含 command/attempt 因果+快照 ref+digest+terminal_reason，经 action/command/attempt/结果槽/journal 与两侧接受 CAS 全链绑定；D3 §6 CAS 同时比对 result artifact/provider call/bundle hash/attempt 四方一致。 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3：子行键=(reservation,activity_attempt,physical_call_index) 绑物理 attempt 身份；prepared→dispatching→sent→confirmed\|uncertain\|no_call 状态机（v6 R5#6），对账后经 CAS 释放预留；扣减绑精确 grant 签发+claim gen/epoch（D3 §2.1 pre-transport CAS）。 |
| 逻辑结果槽(+消费 CAS) | satisfied | D0 §2.5：接受 CAS 全条件含 durable control epoch+claim/attempt+pins（sweep 修 ABA）；cancel/supersession 原子 open→closed、accepted→superseded 抢占接受后未消费窗口；消费 CAS 含槽 generation+epoch；有意重生成必须显式推进槽 generation；槽写者单 owner 化。 |
| ModelTurnExecutionContext | satisfied | D0 §2.3 提供 claim/attempt/因果身份与 revisions，但权威判定不信载荷：接受方 CAS 以存储的 intent/槽/epoch 值为准（D0 §2.5、D3 §4c）；deadline_at 持久化墙钟、claim 后派生 attempt-local 单调钟；缺任一字段构造期 raise。 |
| ActionRequestSpec+schema pins | satisfied | D0 §3.1（v4 R3#12）：pin=AgentAction 行不可变列（提交时写）；OperationRun 在实际创建点 copy+verify（审批型 approve 时才建 :813-827、retry 子 run :1042-1061 两处都执行）；幂等重放 pin 不匹配 fail-closed，digest 不匹配转人工——正面回应 R4#12 创建点教训。 |
| agent tool registry/agent_tool_enabled | na | 只读注册谓词与 serve 投影（D0 §3.2/3.3），无接受/应用类写、无控制转移可推进围栏；schema/pin 的世代围栏由 §3.1（pin 列+copy verify）与 §2.8 请求 hash 承担。 |
| agent_events 投影 | obligation | 计划 §2 D2 给出形态（per-stream sequence_number+idempotency_key、terminal 真值在 canonical 表、可重放重建），但 rebuild 的世代/围栏细节（stream 身份公式、rebuild owner/顺序、replay parity preflight）明列为义务清单#2，随 D2 批落定。 |
| 路由注册表+有效快照 | satisfied | D0 §2.3（v5 R4#10）：effective_route_snapshot 在 action/approval/command 创建点固化（明拒 claim 时固化的太晚窗口——部署后首 claim 拿新配置），digest 计入 canonical request hash 并随 retry 全链传播；route_revision 全链持久化；§4 rollout_state 显式。 |
| scripted 回放转写治理 | satisfied | D0 §2.8：canonical request hash 含 route_revision/快照 digest/schema+policy revisions/租户（v7 R6#10），任一字段变化回放 fail-closed；§2.5 runtime_namespace+provider_mode 使 scripted 结果模式失配即被接受 CAS 拒，结构上进不了 live 授权路径。 |

## 4-生命周期完备性（satisfied 15/18）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b 状态迁移表全量含反向行：verified_accepted→needs_human(expire 命令)、shadow→superseded(过期/policy 升版/新 intent)、superseded→pending(重验)、failed/timed_out/needs_human→pending；历史/现态分离，superseded 行不原地复活；注册期校验、readers 永不直写。 |
| verification_intent(+epoch/claim gen/phase gen) | satisfied | D3 §4c intent_state 五枚举{pending,applied,cancelled,timed_out,superseded}；retry/resume/cancel/timeout/重编译/人工每种转移经事件→reducer→域命令原子 supersede；requeue 同 UoW 递增 control epoch；后继 intent 只在新 claim+新 attempt 事务铸造；§2.3 phase_generation 单调。 |
| identity_search_budget_grant | satisfied | D3 §2.1：状态枚举 active/revoked/exhausted/superseded/reconciled；不可变签发身份(grant_id+issuance_generation+授予事件 id)、终态永不复活、再授予=新行；supersede-with-transfer 余额继承(恰好剩余额度绑新 intent generation)；revoke 单写者路径。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：watermark 单调、每次成功 apply(含 blocking)恒推进；反向失效——expire/supersede 域事件→reducer→apply(blocking 方向)重开已清除的阻塞；幂等键含源事件 id，各次转移各自成键互不去重；not_applied 永不触发 gate 更新。 |
| company.identity.verification.record(CAS) | satisfied | D3 §4c：全条件 CAS 全过则原子 intent→applied+行迁移+applied 证据，任一失配⇒全不动+not_applied 显式 no-op 终态证据事件；§2.3 只有 final_adjudication phase 结果结构上可达 record，中间 phase 到不了。 |
| expiry 定时/expire 命令 | satisfied | D3 §4b：接受时同 UoW 持久化 expiry not-before 定时事件，定期域 owner 扫描发类型化事件→reducer 计划 expire 命令(定时收敛有 owner、非读者驱动)；读路径 fail-closed 派生但零写零 enqueue；§2.2 定时事件携带 generation，触发时 CAS 于当前 generation(旧定时器不可失效新决定)。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a 四步收编含桥的退役生命周期：backfill=needs_human+quarantine 映射；bridge 命中记指标+diagnostic-only 语义；deletion preflight 覆盖全部 resolver 含快照重扫；精确写方 inventory=实施义务(上层 §6#1)但退役条件(零命中窗口)已在设计层成文。 |
| Tier-1/Tier-2 命令+续跑编舞 | satisfied | D3 §2.3：类型化 phase 结果三枚举 final_adjudication\|evidence_insufficient\|needs_human_budget，续跑 durable 化(结果事件→reducer 计划后继子命令，phase_generation+1，各绑自己 claim/attempt)；预算/步数耗尽/超时⇒needs_human 显式出口；控制转移经 §4c intent supersession 收敛。 |
| 证据 bundle+adjudication manifest | satisfied | D3 §4c：manifest 裁决开始前持久化，键=(workspace,intent,phase generation)，要求全部 expected candidates 全终态+聚合 hash 匹配——crash/过滤/未跑的兄弟候选缺席⇒聚合不成立⇒needs_human；§6 bundle 为不可变 intent/attempt 证据，hash 绑入信封，随 intent generation 换代。 |
| ModelInvocationEnvelope | satisfied | D0 §2.2/D3 §6：不可变结果侧契约，单一物理 schema 一处定义；签发身份=provider call id+attempt+command 因果，per-call 不可变、无复活路径；含 terminal_reason，非可授权终态在 D3 接受谓词 fail-closed 转 needs_human；经接受 CAS 全链绑定。 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3+D3 §8：暴露行状态机 prepared→dispatching→sent→confirmed\|uncertain\|no_call 全量，crash 停在 dispatching/sent 一律 uncertain 按 worst-case 计直至 provider 对账或保守消耗；confirmed/uncertain 对账后 CAS 释放未用预留；orphan 回收在 v2 覆盖映射(§7 #15)点名为台账契约组成。 |
| 逻辑结果槽(+消费 CAS) | satisfied | D0 §2.5：slot_state 五态{open,accepted,consumed,closed,superseded}；cancel/supersession 原子关槽(open→closed、accepted→superseded 含接受后未消费窗口)；end_turn 零 action 也走 accepted→consumed 不滞留；有意重新生成=显式推进槽 generation(新 step)；晚到 terminal 一律 quarantine。 |
| ModelTurnExecutionContext | na | 请求侧 frozen 不可变值对象，非 durable 状态机——无迁移表可言；生命周期由其引用对象承担(budget→台账状态机、deadline_at 持久化墙钟、快照/pin 各有创建点契约)；缺任一字段构造期 raise(D0 §2.3)。 |
| ActionRequestSpec+schema pins | satisfied | D0 §3.1：pin=AgentAction 行不可变列(提交时写)；OperationRun 在实际创建点(approve 时建 run、retry 子 run)各自 copy+verify，失配 fail-closed 不静默沿用；宽松迁移桥有非同义反复退役条件(全部 API 可提交 action 零命中一个发布窗口)+residual 台账(上层 §6#3)。 |
| agent tool registry/agent_tool_enabled | satisfied | D0 §3.2 五条谓词定义准入/退出(缺 schema/adapter/model-safe schema 即自然退出 served 集)+§3.3 双向守卫 served≡agent_tool_enabled+simulate dispatch preflight；在途引用由 §3.1 schema pin 失配 fail-closed 兜底(工具退役不影响已 pin 的 durable 对象)。 |
| agent_events 投影 | obligation | 计划 §2 D2 声明可重建(从 canonical workflow 事件重放)+投影单写者=turn owner，但 rebuild owner/顺序、cursor 授权、replay parity preflight 明列为实施义务(计划 §6 #2，D2 批)——定时/重建收敛的 owner 细节由该批 gate 落地。 |
| 路由注册表+有效快照 | satisfied | D0 §4 rollout_state 枚举 draft\|canary\|active\|retired 为路由全生命周期；路由变更=配置提交+审计非运行时行为；§2.3 快照在 action/approval/command 创建点固化并随 retry 全链传播——retired/变更路由不影响已排队命令语义(digest 计入 request hash)。 |
| scripted 回放转写治理 | obligation | D0 §2.8 声明每份转写带 schema_version、大小上限、保留 TTL，但 TTL 执行的定时清扫 owner(谁扫谁删、非读者驱动)未指定——转写为 runtime 文件不入库、风险低，清扫机制随 D0 批转写治理落地时补。 |

## 5-晚到与部分结果（satisfied 16/18）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4c：CAS 任一失配 ⇒ 全不动 + not_applied 显式 no-op 证据事件；§8「晚到结果按 §4c 隔离」；§4b 迁移表含 cancel/timeout/rebuild 控制路径行；§4c 八项竞态电池覆盖各类晚到。 |
| verification_intent(+epoch/claim gen/phase gen) | satisfied | D3 §4c：requeue 同 UoW 先递增 durable control epoch，「已 requeue、未重 claim」窗口旧结果 epoch 失配即拒；每种控制转移原子 supersede 旧 intent；失配落 not_applied 显式终态。 |
| identity_search_budget_grant | satisfied | D3 §2.1：不可变签发身份——旧授予事件驱动的 stale Tier-2 命令不能消费新 grant；pre-transport 授权 CAS fence 非终态+claim gen/control epoch+attempt+精确签发，cancel 后异步收敛窗口内调用被 epoch 失配挡住。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：watermark 单调、每次成功 apply（含 blocking 方向）都推进；clearing 仅当 generation > watermark——「晚到旧 clearing 永被拒」；not_applied 永不触发 gate 更新；幂等键含源域事件 id。 |
| company.identity.verification.record(CAS) | satisfied | D3 §4c 全条件 CAS（含 workspace/claim gen/attempt/control epoch/bundle watermark/manifest 全终态 hash）；cancel/timeout/requeue/rebuild 各自 commit UoW 同步推进围栏，旧 record 在异步 supersession 落地前即被挡；失配 ⇒ not_applied。 |
| expiry 定时/expire 命令 | satisfied | D3 §2.2 反向失效：expiry 定时事件携带 generation，触发时 CAS 于当前 generation——「旧定时器不可失效新决定」；§4b：读路径对已过期行 fail-closed 派生（按 needs_human 消费、零写零 enqueue），定时事件晚到也无放行窗口。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a：bridge 命中无显式 workspace 映射 ⇒ diagnostic-only needs_human，不得授权身份/gate 清除/付费抑制（部分/陈旧结果结构性不可授权）；precedence preflight——PG 行在位时文件注册表与快照不可能胜出。 |
| Tier-1/Tier-2 命令+续跑编舞 | satisfied | D3 §2.3：只有显式 final_adjudication 才能计划 record——中间 phase 结果结构上到不了 record；每 phase 子命令绑自己的 claim/attempt + phase_generation；§2.1 控制政策按 provider 能力派生（poll_cancel_late_result_quarantine / fail_closed_until_terminal）。 |
| 证据 bundle+adjudication manifest | satisfied | D3 §4c 多候选完备性证明（R6#6）：record CAS 要求 manifest 全终态且聚合 hash 匹配——crash/过滤/未跑的兄弟候选缺席 ⇒ 聚合不成立 ⇒ needs_human，部分结果「幸存者显得唯一有效」被结构性堵死；bundle 不可变、hash 绑入信封。 |
| ModelInvocationEnvelope | satisfied | D0 §2.2 canonical 字段必含 terminal_reason；D3 §6：{end_turn, tool_calls} 之外的终态在 D3 接受谓词 fail-closed 转 needs_human——截断/过滤的 judge 输出不可参与 auto-confirm；信封与 result artifact ref+digest 绑定防配错输出。 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3 / D3 §8：任何 wire 写前先落 dispatching；停在 dispatching/sent 的 crash 一律按 uncertain 以 worst-case 预留计，直至 provider 对账或保守消耗（晚到对账不假设未花费）；confirmed/uncertain 对账后经 CAS 释放未用预留。 |
| 逻辑结果槽(+消费 CAS) | satisfied | D0 §2.5：slot_state 含 superseded；cancel 落在「接受后、消费前」窗口时 accepted→superseded 抢先（正是本类的『已接受未消费』窗口）；消费 = 完整终局结果原子消费（部分持久化整 UoW 回滚）；晚到 terminal 结果一律 quarantine 证据。 |
| ModelTurnExecutionContext | na | 不适用：context 是请求侧不可变准入信封、非异步结果通道；晚到/部分结果由其携带的 idempotency/attempt/epoch 喂入 D0 §2.5 槽 CAS 与 §2.6 状态机（超时 ⇒ 失败终止 + incomplete attempt）处置。 |
| ActionRequestSpec+schema pins | satisfied | D0 §3.1：pin 在 AgentAction 提交点不可变写入，OperationRun 在实际创建点（approve/retry-child）copy+verify；幂等重放 pin 不匹配 ⇒ fail-closed 拒绝（不静默沿用任一侧）；晚到 dispatch 遇 schema 演化 digest 不匹配 ⇒ 转人工。 |
| agent tool registry/agent_tool_enabled | na | 不适用：registry 是同步只读配置投影（GET /api/agent/tool-registry + serve 谓词），自身无异步结果路径；工具执行结果的晚到/部分归 D0 §2.5 结果槽与消费 CAS 管辖。 |
| agent_events 投影 | satisfied | Plan §2 D2：agent_events 禁止独立推进 workflow 状态、terminal 真值属 canonical 表——晚到/重复投影行不可授权；per-stream sequence_number + idempotency_key；可从 canonical 事件重放重建。精确 replay parity 契约 = Plan §6 义务#2（D2 批）。 |
| 路由注册表+有效快照 | satisfied | D0 §2.3：快照在 action/approval/command 创建点固化并随 retry 全链传播——部署改路由后晚执行/晚重试的命令保持旧语义；snapshot digest 计入 §2.8 request hash 与 D3 §4c pins 匹配谓词，pin 失配即拒。 |
| scripted 回放转写治理 | satisfied | D0 §2.8：canonical request hash 任一字段变化 ⇒ 回放 fail-closed（陈旧转写不可静默命中）；转写带 TTL；§2.4 scripted 回放 canonical ToolTurnResult（无部分流逃逸）；§2.6 消费方放弃迭代 ⇒ 关连接 + 记 incomplete attempt。 |

## 6-成本诚实性（satisfied 7/18）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | na | 读模型行不发起付费物理调用；行存「预算 grant/信封引用」列供对账回溯（D3 §4b），支出记账在 §8 台账侧 |
| verification_intent(+epoch/claim gen/phase gen) | na | 围栏/身份机制，自身零付费调用；intent supersession 时在途调用成本由台账 dispatching/sent→uncertain 态按 worst-case 兜底（D0 §2.3、D3 §8），不假设未花费 |
| identity_search_budget_grant | satisfied | D3 §2.1：命令/attempt/扣减/transport 绑精确签发身份 + pre-transport 授权 CAS（R5#4）；supersede-with-transfer 携带恰好剩余额度不回满；计划 §5 TD-5：attempt 创建同事务原子扣减、跨 retry/resume 不重置 |
| plan_review.identity_result.apply(watermark) | na | gate 写命令，零 provider 调用；付费步骤成本在验证/搜索命令侧经 §8 台账入账 |
| company.identity.verification.record(CAS) | na | 纯域写命令无物理支出；其 CAS 要求 manifest 全终态（D3 §4c）间接保证入账完备，记账本体在 §8/D0 §2.3 |
| expiry 定时/expire 命令 | na | 零成本域控制路径（定时事件+域 owner 扫描，D3 §4b 迁移表行），不发起 provider 调用 |
| 迁移桥(文件注册表+快照) | na | 读降级桥无付费调用；bridge 命中 diagnostic-only、不得授权付费分支抑制（D3 §4a v5），不产生支出面 |
| Tier-1/Tier-2 命令+续跑编舞 | satisfied | D3 §2.1（Tier-2 缺 grant 引用 fail-closed）+ §8：worst-case 预留 + 每物理调用一行暴露（prepared→dispatching→sent→confirmed\|uncertain\|no_call）、信封耗尽→needs_human 永不静默超支；§2.3 每 phase 子命令各绑 attempt，重跑各自入账 |
| 证据 bundle+adjudication manifest | satisfied | D3 §4c：manifest 持久化逐 call 信封/结果/状态 + all-terminal 聚合 hash 先于 record——crash/未跑调用缺席即聚合不成立，构成终局对账钩子；信封含成本暴露行引用（§6/D0 §2.2） |
| ModelInvocationEnvelope | satisfied | D0 §2.2 canonical 字段清单显式含「成本暴露行引用」+ usage/usage_status；D3 §6 同一物理定义一处落库，接受=两半同过绑成本行 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3（v6 R5#6）：父行 worst-case 预留+子行(reservation,attempt,call_index)；任何 wire 写前先落 dispatching；crash 停 dispatching/sent 按 uncertain worst-case 计至 provider 对账；no_call 仅限可证明 pre-transport 中止；对账后 CAS 释放；D3 §8 同口径 |
| 逻辑结果槽(+消费 CAS) | na | 结果授权/去重机制不改变计费口径；重试/显式再生成（新 step）的每次物理调用各有独立暴露行（D0 §2.3），槽 join/quarantine 不吞成本 |
| ModelTurnExecutionContext | satisfied | D0 §2.3：budget(ModelTurnBudget)+budget_reservation_ref 必填，缺任一构造期 raise（pre-transport，不污染熔断）；usage_status≠reported 按 monetary_ceiling 保守保留额记账，缺失 usage 永不静默视为零成本 |
| ActionRequestSpec+schema pins | na | 输入 schema 事实源与 pin 生命周期（D0 §3.1）无付费调用面；预算政策由 route budget_class/grant 承担 |
| agent tool registry/agent_tool_enabled | na | 只读 serve 面（D0 §3.2/3.3）不发起支出；ToolSpec.budget_required 仅服务端元数据（§2.2），执行期成本走 owner 侧信封/台账 |
| agent_events 投影 | na | turn 粒度投影/明细表（计划 §2 D2）零付费调用；turn 级 max_cost/max_wall fail-closed 属 D2 loop 预算机制，模型调用成本经 D0 §2.3 台账入账 |
| 路由注册表+有效快照 | satisfied | D0 §4：budget_class 映射默认 ModelTurnBudget 与审批政策；§2.3 effective_route_snapshot 在 action/approval/command 创建点固化含定价类、digest 计入 request hash——排队重试沿创建点定价配置，部署改价不悄改已排队调用口径 |
| scripted 回放转写治理 | na | scripted/simulate 回放零 live 付费调用（D0 §2.8；录制 live 仅 owner 显式授权、fail-closed 纪律原样适用）；模式隔离防污染属第 10 类 |

## 7-物理身份绑定（satisfied 17/18）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b 行含物理因果列(source workflow_command_id/activity_run_id/attempt_id/entity_delta_id)+result_artifact_ref+grant/信封引用+accepted_policy_version；§6/§4c CAS 四方一致(result artifact/provider call/bundle hash/attempt) |
| verification_intent(+epoch/claim gen/phase gen) | satisfied | D3 §4c intent 存 plan bundle hash+fingerprint+policy/schema/route revisions+effective_route_snapshot digest+expected generation，claim 完成且 ActivityAttempt 创建后绑定 claim_generation/attempt/epoch；新列=义务(计划 §6.6) |
| identity_search_budget_grant | satisfied | D3 §2.1 不可变签发身份(grant_id+issuance_generation+授予事件 id)，键含 intent generation+policy_revision；命令/attempt/扣减/transport 绑精确签发，stale 命令不能消费新 grant |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2 apply CAS 于 {workspace+session 当前 revision+source 事件 id 匹配+generation/watermark}；幂等键含源域事件 id(各转移各自成键)；消费域事件非 provider 结果，无需 provider 快照 |
| company.identity.verification.record(CAS) | satisfied | D3 §4c 全条件 CAS：claim_generation+activity_attempt_id+预期终态事件身份+policy/schema/route/snapshot pins+control epoch+manifest hash；§6 四方一致(artifact/provider call id/bundle hash/attempt) |
| expiry 定时/expire 命令 | satisfied | D3 §4b 接受时同 UoW 持久化 expiry not-before 定时事件；§2.2 定时事件携带 generation，触发时 CAS 于当前 generation——旧定时器不可失效新决定 |
| 迁移桥(文件注册表+快照) | satisfied | legacy 数据无物理身份可绑，D3 §4a 以降权补偿：backfill 恒 decision_source=legacy_registry_import+needs_human(永不 verified)；bridge 命中无 workspace 映射=diagnostic-only，不授权身份/gate 清除 |
| Tier-1/Tier-2 命令+续跑编舞 | satisfied | D3 §2.3 每个后继 verify.evidence 子命令各绑自己的 claim/attempt，root intent+单调 phase_generation；§8 fetch_key/judge_call_key 项目身份；judge_call_key 补维(route/schema/policy revision)=计划 §6.5 义务 |
| 证据 bundle+adjudication manifest | satisfied | D3 §6 bundle 不可变、hash 绑入调用信封，evidence_ids 服务端 resolve；§4c manifest 键=workspace/intent/phase generation，逐 call 信封/结果/状态+all-terminal 聚合 hash 入 record CAS |
| ModelInvocationEnvelope | satisfied | D0 §2.2 canonical 字段清单：快照 ref+digest、permission/outbound/model-safe revisions、command/attempt 因果、provider call id+响应身份、terminal_reason、result artifact ref+digest、成本行引用；D3 §6 单一物理定义，经接受 CAS 全链绑定 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3 暴露子行身份=(reservation, activity_attempt, physical_call_index)，每物理调用一行；§2.2 信封含成本暴露行引用互绑；定价类在 effective_route_snapshot 于创建点固化；D3 §8 同口径+对账 parity |
| 逻辑结果槽(+消费 CAS) | satisfied | D0 §2.5 接受 CAS=claim/attempt 身份+durable control epoch+schema/route/policy pins；action 身份=(result_slot_id, slot_generation, tool_name, canonical_args_digest)；消费 CAS 谓词含同一全集，approve/dispatch 复查 |
| ModelTurnExecutionContext | satisfied | D0 §2.3 route_revision+effective_route_snapshot_ref 在 action/approval/command 创建点固化(非 claim 时)，含 endpoint/timeout/定价/circuit，digest 计入 §2.8 request hash；workspace/actor/attempt/因果 id/三 revision 全绑 |
| ActionRequestSpec+schema pins | satisfied | D0 §3.1 pin(version+digest)=AgentAction 提交时不可变列；OperationRun 在实际创建点 copy+verify(approve 时建 run :813-827、retry 子 run :1042-1061 两处)；幂等重放 pin 失配 fail-closed——即 v2 定义的 R4#12 处方 |
| agent tool registry/agent_tool_enabled | obligation | tool-schema 版本/digest 在 turn 创建点钉住并贯穿 terminal result/journal→AgentAction→approve/retry run = 计划 §6 义务 4(D0/D2 批)；D0 §2.8 已把 tools_schema_digest 入 request hash 但 turn 级创建点 pin 未在 D0 文内落定 |
| agent_events 投影 | satisfied | 计划 §2 D2：每行带物理因果列(workflow_event_id/workflow_command_id/operation_run_id/turn_id/step_id)+per-stream sequence_number+idempotency_key；精确投影契约细化=计划 §6 义务 2 |
| 路由注册表+有效快照 | satisfied | D0 §4 ModelRouteSpec 版本化 checked-in；§2.3 route_revision(内容 digest)+effective_route_snapshot(endpoint/base_url digest、timeout、api_style、定价类、circuit policy id)——正是 v2 要求的非仅逻辑版本的有效配置快照，随 retry 全链传播 |
| scripted 回放转写治理 | satisfied | D0 §2.8 canonical request hash 覆盖 route_id/route_revision/snapshot digest/tools_schema_digest/三 policy revision/messages_digest/租户/transcript_digest，任一变化即回放 fail-closed；转写 tenant-bound 且带 schema_version |

## 8-provenance 与信任边界（satisfied 16/18）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b decision_source∈{machine,human,legacy_registry_import} 分通道 + owner 矩阵禁「意图抽取/公共 ingress」消费；§3 服务端引用、ingress 携带即拒；§5 human_confirmed 跨 plan 复用永不重标为机器验证 |
| verification_intent(+epoch/claim gen/phase gen) | satisfied | D3 §4c intent 由 owner 在 claim+attempt 创建后铸造，pins/epoch 全部服务端存储值（非载荷携带）；人工决定经 §5 独立 UoW→reducer→supersession 命令，人机转移通道不混 |
| identity_search_budget_grant | satisfied | D3 §2.1 grant=人在 review 卡的类型化部分决定（人通道），不可变签发身份；Tier-2 审批证据=精确 grant 签发引用，缺引用 fail-closed——机器不能自授予预算 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2 apply 只由 recorded 域事件（仅 applied）或人工决定事件经 reducer 计划，幂等键含源事件 id；机器结果不经人工字段回流 gate，人工链 §5 单独 UoW+事件 |
| company.identity.verification.record(CAS) | satisfied | D3 §4c 全条件 CAS 含 verification_state NOT IN (human_confirmed)——机器结果结构上不能覆盖人工决定；§6 接受=模型半+服务端信封半同过，模型不可自证 |
| expiry 定时/expire 命令 | na | 纯服务端生命周期机制（接受时同 UoW 持久化定时事件→域 owner 扫描→expire 命令，D3 §4b），无人/机通道混淆面、无模型自证面、无 ingress 面——属第 1/4 类关注 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a backfill 行 decision_source=legacy_registry_import 永不 verified/accepted 态；bridge 命中 diagnostic-only needs_human，不得授权身份/gate 清除/付费抑制——import provenance 不冒充验证 |
| Tier-1/Tier-2 命令+续跑编舞 | satisfied | D3 §2.3 phase 结果类型化，只有显式 final_adjudication 可达 record（中间态结构上到不了）；§2.1 Tier-2 绑精确 grant 签发（人授权证据）；控制政策 provider 能力派生非模型声明 |
| 证据 bundle+adjudication manifest | obligation | 主体满足（D3 §6 模型只引用 bundle 内 evidence_ids，URL/域/kind 服务端 resolve，模型输出 provenance 字段忽略+记偏差；§4c manifest 服务端枚举），但「official domain 归属」服务端证明规则成文为实施义务（Plan §6#5） |
| ModelInvocationEnvelope | satisfied | D0 §2.2/D3 §6 信封=服务端不可变结果侧契约，provider 响应身份/usage/fallback/circuit 证据全部服务端派生，「身份/usage/fallback 模型不可自证」明文；单一物理 schema 定义于 D0 §2.2 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3 usage_status!=reported 按保守预留计、缺失 usage 永不静默视为零成本；暴露行由单一 PG cost-ledger owner 服务端记录（D3 §8 同口径），非模型自报 |
| 逻辑结果槽(+消费 CAS) | satisfied | D0 §2.5 provider tool_call_id 仅证据，accepted_result_id 由 owner 铸造；槽写者=turn owner 域命令；scripted/simulate 结果携带 runtime_namespace 不可流入 live 授权路径 |
| ModelTurnExecutionContext | satisfied | D0 §2.3 workspace/actor/permission_scope 由 owner 铸造进 context；调用方只传 route_id 不接受裸 model 字符串；缺 context/budget 构造期 raise 拒 live——模型/客户端无法自供身份 |
| ActionRequestSpec+schema pins | satisfied | D0 §3.1 target_ref 只含 owner 铸造的资源身份，模型/客户端提供 target 字段一律拒，input 别名覆盖 owner target 提交即拒——模型不可伪造服务端引用；schema 服务端单一事实源 |
| agent tool registry/agent_tool_enabled | satisfied | D0 §3.2 谓词4：缺注册的 revisioned model_safe_result_schema+校验器 owner 不 serve；§2.1 出站校验覆盖全部消息角色（system/user/assistant/tool_result），approval/budget 元数据永不上 wire |
| agent_events 投影 | satisfied | Plan §2 D2：每行带物理因果列（workflow_event/command/operation_run/turn/step id），terminal 真值属 canonical workflow 表、禁止独立推进状态——投影不可冒充权威 provenance；带 workspace 授权 scope |
| 路由注册表+有效快照 | satisfied | D0 §4 checked-in product-owned，调用方只传 route_id、model 服务端钉死；fallback_policy=fail_closed 无静默改道、「model_usage 永非路由输入」；快照在 owner 创建点服务端固化，digest 入 request hash |
| scripted 回放转写治理 | satisfied | D0 §2.8 转写只存归一化事件永不存 raw provider payload、字段级脱敏；live 转写 tenant-bound 跨租户不可回放，合成 fixture 必须显式标注 synthetic 且接受时校验信封租户等式；录制仅 owner 显式授权 |

## 9-自包含与跨文档一致（satisfied 13/18）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b 唯一定义（PG-only DDL+全量迁移表+owner 矩阵）；上层 §2 D3/§5 TD-6/TD-7 术语一致（shadow_would_verify/verified_accepted/needs_human 逐字同枚举） |
| verification_intent(+epoch/claim gen/phase gen) | obligation | D3 §2.3 声明 intent 升级为 root-intent+per-phase 子绑定，但 §4c 行契约未同步列 phase_generation（仅出现在 §4c manifest 键）——DDL 收口归 D3 实施批 |
| identity_search_budget_grant | blocker | 额度单位跨处矛盾：上层 §5 TD-5「3 次/plan」vs 上层 §2 D3 与 D3 §2.1「3 次/次授予」——再授予（新 grant 新额度）机制下 per-plan 与 per-grant 语义分歧未消解 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2 apply UoW：watermark 单调恒推进、clearing 仅 gen>watermark；上层 §2 D3 同名同语义（generation watermark 围栏、阻塞方向恒占优），链条逐字对齐 |
| company.identity.verification.record(CAS) | satisfied | 事件链 plan §2 D3 与 D3 §2.2 逐字一致（仅 applied 触发 apply、not_applied 显式终态）；全条件 CAS 唯一定义于 D3 §4c，幂等键命名空间化自包含 |
| expiry 定时/expire 命令 | satisfied | D3 §4b 迁移表行（接受时同 UoW 落定时事件、域 owner 扫描、读者零写零 enqueue）+ §2.2 反向失效（定时事件携带 generation 的 CAS）自包含；上层 v5 注记「expiry 去读者化」一致 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a 收编四步+bridge diagnostic-only+删除条件自包含；上层 §0 结构性事实与 §6 义务#1 点名同一批写方（company_asset_supplement/asset_sync、:641-681 更正为读方），两文同步 |
| Tier-1/Tier-2 命令+续跑编舞 | satisfied | D3 §2.3 phase 结果枚举（final_adjudication/evidence_insufficient/needs_human_budget）+durable 续跑编舞自包含全文；上层 §2 D3「审批与预算」bullet 术语一致，无矛盾描述 |
| 证据 bundle+adjudication manifest | satisfied | D3 §6 evidence_ids-only+bundle hash、§4c manifest（workspace/intent/phase generation 键+all-terminal hash）单处定义；D0 §2.2 信封字段清单含 evidence bundle hash，跨文一致 |
| ModelInvocationEnvelope | satisfied | 本类的正面样板（R5#8 修复）：canonical 字段清单唯一定义于 D0 §2.2；D3 §6 明示「以 D0 §2.2 为唯一定义处」只补裁决侧语义、不复述 schema |
| 成本台账(预留+暴露行) | obligation | 状态机 prepared→dispatching→sent→confirmed\|uncertain\|no_call 在 D0 §2.3 与 D3 §8 双处逐字枚举（当前一致但无单一 canonical 定义处）——与信封 R5#8 同型重复，待文档批收敛为一处定义 |
| 逻辑结果槽(+消费 CAS) | satisfied | D0 §2.5 唯一定义（slot_state 五枚举+接受/消费全条件 CAS+epoch）；D3 §10 覆盖映射（R5#5→D0 §2.5）仅交叉引用不复述，无第二定义 |
| ModelTurnExecutionContext | satisfied | D0 §2.3 唯一定义；D3 §6 显式区分「请求执行上下文（D0 §2.3）」与结果侧信封防混指；上层 §2 D0 同名引用（因果 id+审批/预算信封+idempotency+route_id）一致 |
| ActionRequestSpec+schema pins | blocker | 上层 §2 D1 仍写「ActionSpec 扩展版本化 input_schema」，未同步 D0 §3.1 的 ActionRequestSpec/request_schema（input+target 双段，target 旁路修复 R2#10）——上层计划与详设未同步修订 |
| agent tool registry/agent_tool_enabled | satisfied | D0 §3.2 五条谓词+§3.3 复用 command_type_manifest 序列化（不第三处重组）；上层 §2 D1 同词汇（agent_tool_enabled 子集、12/15 不可 dispatch、语义断言）逐字一致 |
| agent_events 投影 | obligation | 上层 §2 D2 只给形态（PG-only、sequence_number+idempotency_key、可重建、禁独立推进 workflow）；精确投影契约（stream 身份/ordinal/唯一约束/rebuild/replay parity）明示为上层 §6 义务#2，随 D2 批定 |
| 路由注册表+有效快照 | satisfied | D0 §4 ModelRouteSpec 唯一定义、字段与上层 §5 TD-4 清单一致（与 reviewer 路由表零耦合两文同述）；快照契约 D0 §2.3 声明为「共享模型调用契约」，D3 §4c/§6 以 digest 引用不复述 |
| scripted 回放转写治理 | satisfied | D0 §2.8 唯一定义（canonical request hash v1 字段清单+租户绑定回放+脱敏/TTL 治理）；上层 §4（语义结果等价 A/B）与 D3 §9（scripted 转写回放集）仅引用语义，跨文一致 |

## 10-运行时/模式隔离（satisfied 12/18）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b 运行时隔离段：唯一键 (runtime_namespace, workspace_id, fingerprint)，provider_mode 不可变列；非 live 证据 diagnostic-only，live 命名空间结构上不可能产生 shadow/verified 行；跨模式污染 preflight 进 contract lane |
| verification_intent(+epoch/claim gen/phase gen) | obligation | D3 §4b 总括称幂等 scope/每条接受 CAS 全链携带 namespace+mode，但 §4c intent 身份仍写为 (workspace_id, intent_id) 且全条件 CAS 枚举未逐字列 namespace/provider_mode 等式——实施批需在 intent DDL 与 CAS 谓词显式落列 |
| identity_search_budget_grant | obligation | D3 §2.1 grant 键 = (workspace_id, review_session_id, intent generation, policy_revision)，无 runtime_namespace/provider_mode；§4b 隔离总括未点名 grant——scripted 链消费 grant 额度的隔离需实施批补键/补 CAS 谓词 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：apply 仅由 record applied 域事件触发，而 record CAS 模式失配即拒（§4b「模式不匹配即 CAS 拒」）——scripted 证据结构上到不了 gate 清除；幂等键含 workspace（apply:<workspace_id>:<session_id>:<源事件 id>） |
| company.identity.verification.record(CAS) | satisfied | D3 §4b：每条接受 CAS 全链携带不可变 runtime_namespace+provider_mode，模式不匹配即 CAS 拒；非 live 证据不可能产生 live 命名空间 shadow_would_verify/verified_accepted 行，Phase-2 promotion 只认 live 证据 |
| expiry 定时/expire 命令 | satisfied | D3 §4b 迁移表 + §2.2 反向失效：定时事件在接受 UoW 内挂到带 (runtime_namespace,…) 键的验证行，expire 命令 CAS 于该行 generation；且 expire 仅 blocking 方向（verified→needs_human），无跨模式授权面 |
| 迁移桥(文件注册表+快照) | obligation | D3 §4a：bridge 命中恒 diagnostic-only needs_human、导入行永不 verified/accepted、无映射进 quarantine——授权路径已结构性挡住；但 backfill 行的 runtime_namespace/provider_mode 赋值未规定，随 D3 批 inventory/backfill 落定 |
| Tier-1/Tier-2 命令+续跑编舞 | satisfied | D3 §4b：operation/command/attempt/幂等 scope 全链携带不可变 namespace+provider_mode；phase 子命令各绑 claim/attempt（§2.3），Tier-2 pre-transport CAS（§2.1）落在该链内，scripted 结果只能产 diagnostic-only 证据 |
| 证据 bundle+adjudication manifest | satisfied | D3 §4b 明列「证据 bundle」在 namespace+mode 携带链内；manifest 聚合 hash 进 record 全条件 CAS（§4c R6#6），该 CAS 模式失配即拒——非 live 证据集不可能凑成 live applied |
| ModelInvocationEnvelope | satisfied | D0 §2.5 运行时隔离（R6#2）：信封携带不可变 runtime_namespace+provider_mode，scripted/simulate 结果不可流入 live 命名空间任何授权路径；D3 §6 同一物理 schema、两侧接受 CAS 全链绑定 |
| 成本台账(预留+暴露行) | obligation | D0 §2.3/D3 §8 台账行键 = (reservation, activity_attempt, physical_call_index)，未显式声明 namespace/provider_mode 列——可经 attempt 因果继承（§4b 链含 attempt），但 simulate 调用不得写 live 台账需实施批显式落列+preflight |
| 逻辑结果槽(+消费 CAS) | satisfied | D0 §2.5（R6#2）：槽/journal/action 携带不可变 runtime_namespace+provider_mode，scripted/simulate 结果不可流入 live 命名空间授权路径；接受/消费 CAS 全条件含 pins+epoch，模式在链内 |
| ModelTurnExecutionContext | obligation | live 门每次 transport 前调 assert_live_provider_access_allowed（D0 §2.7，含契约文档扩展到模型中继），但 §2.3 字段清单与 §2.8 canonical request hash 均未列 runtime_namespace/provider_mode——幂等 scope 的模式维度需实施批补入 |
| ActionRequestSpec+schema pins | satisfied | D0 §2.5：action（AgentAction 行）在 namespace+provider_mode 携带清单内；pin 为提交时不可变列，OperationRun 实际创建点（approve/retry-child）copy+verify、失配 fail-closed（§3.1），无跨模式沿用窗口 |
| agent tool registry/agent_tool_enabled | satisfied | D0 §3.2 谓词第 5 条：每个 served tool 须过 simulate dispatch preflight 且实际行使 model-safe 序列化器（contract lane 守卫）；registry 为静态配置投影，无 durable 模式态，served≡enabled 双向守卫 |
| agent_events 投影 | obligation | 计划 §2 D2 只定了 PG-only、sequence+idempotency_key、workspace 租户列与可重建性，未提 runtime_namespace/provider_mode；计划 §6 义务 2（agent_events 精确投影契约）随 D2 批落定，模式列应并入该义务 |
| 路由注册表+有效快照 | satisfied | D0 §4：每条 route 显式 simulate_mapping（simulate/scripted 替身实现），fallback_policy 唯一初始值 fail_closed，owner 终审前 live 一律 fail-closed；§2.3 快照在创建点固化、digest 计入 request hash |
| scripted 回放转写治理 | satisfied | D0 §2.8（v7 R6#10）：canonical request hash 含 workspace/actor/permission/transcript_digest——回放身份含租户；live 转写 tenant-bound 跨租户不可回放，仅显式 synthetic fixture 可跨租户且接受时校验信封租户等式；配合 §2.5 scripted 结果不入 live 授权路径 |
