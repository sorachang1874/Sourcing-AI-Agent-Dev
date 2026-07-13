# Track D 机制×不变量矩阵（DESIGN_INVARIANT_CHECKLIST v2 协议产物）

> Status: Matrix evidence record（**v3 2026-07-13：v8 文档的 200 格全新矩阵**【10 类 × 20 机制，
> 机制清单已含 human_transition_pending 编舞、awaiting_budget、隔离机制本身——R7#6 修复】，
> 每格由独立代理产出、非作者自评，随 v8 设计重生成【R7#5/#8 的过时格规则】。obligation 格
> 带稳定 ID（OB-类.序，R7#9），= 对应实施批开工义务，与上层计划 §6 义务清单并轨。
> 早期版本（v1 九类 violations-only / v2 180 格）见 git 历史。


## 1-单写者/聚合所有权（satisfied 19/20）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b「readers 永不直写」+ owner 矩阵（verification_state/generation owner=验证 owner）；§2.2 v5 单写者拆分「验证状态/intent/generation 只由验证 owner 写，gate 只由 plan review owner 写」。 |
| verification_intent(+epoch/claim gen/phase gen/awaiting_budget) | satisfied | D3 §4c：intent owner=验证 owner（§4b 矩阵）；supersession 每种经「事件→reducer→域命令」；generic 控制面「只动 runtime 现态」（epoch/claim 属命令行）；awaiting_budget 迁移由 record 非授权分支（验证 owner）执行（§2.2）。 |
| identity_search_budget_grant | obligation **OB-1.1** | 生命周期单写者已成文（§2.1 revoke「经事件→reducer→plan-review owner 的 grant 命令收敛，不跨 owner 直写」），但 Tier-2 消费方 pre-transport 扣减/exhausted 转移的字段级写者归属未成文——属计划 §6#7「§4b 矩阵扩展到全部共享字段」实施义务。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：「gate 只由 plan review owner 写」（v5 round4#1 单写者拆分）；apply owner 单 UoW 推进 watermark；expiry/supersession/human 路径全部经域事件→reducer→apply 命令收口，not_applied 永不触发 gate 更新。 |
| company.identity.verification.record(三分支 CAS) | satisfied | D3 §2.2 v8：「record 是这些迁移的唯一 owner，非授权结果也是真实域状态」；record owner 单 UoW「只写自己的聚合（verification 行 + intent 迁移 + not_applied/applied 证据）」，gate 更新经 recorded 域事件→reducer→apply。 |
| human_transition_pending 态与人工决定编舞 | satisfied | D3 §5：review owner 人工决定 UoW「在自己的聚合上装 fail-closed 的 human_transition_pending 态」；验证聚合改写经 reducer 计划验证 owner supersession 命令；pending 态由同 owner 的 apply 命令清除——无跨聚合直写。 |
| expiry 定时/expire 命令 | satisfied | D3 §4b 迁移表：「读者不得 enqueue：接受时同 UoW 持久化 expiry not-before 定时事件，定期域 owner 扫描发类型化事件→reducer 计划 expire 命令；读路径只做 fail-closed 派生……零写零 enqueue」；expire 后 gate 重开走 §2.2 反向失效链。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a：「PG 表 canonical」；step 3「全部写入方迁移或停写……经 PG owner 写入」；step 4「workspace 决定永不回写全局文件」+ precedence preflight；桥命中 owner=bridge 指标表（§4b 矩阵）。写点全量 inventory 为批 step 1 硬项。 |
| Tier-1/Tier-2 命令+判别化续跑编舞 | satisfied | D3 §2.2/§2.3 v8：phase 结果为类型化事件，reducer 只计划后继子命令（各绑自己的 claim/attempt）；「只有显式 final_adjudication 结果才允许计划 verification.record」——中间 phase 结构上到不了 record，reducer 永不直写域状态。 |
| 证据 bundle+adjudication manifest(预创建+终态化) | satisfied | D3 §6「不可变的 intent/attempt 证据 bundle」；§4c v8：owner 裁决前预创建 manifest 全部条目，「未决条目由 owner 的超时/对账命令终态化（标 unresolved）」——条目终态化写者收敛于验证 owner 命令，needs_human 有明确产生者（record 非授权分支）。 |
| ModelInvocationEnvelope | satisfied | D3 §6：「独立命名的不可变结果侧契约，D0 与 D3 同一物理定义一处落库」；canonical 字段清单以 D0 §2.2 为唯一定义处；不可变即创建点单次写入，经全链绑定只被读取比对。 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3 budget_reservation_ref：「单一 PG cost-ledger owner；父行 = worst-case 预留(挂 operation)；子行 = (reservation, activity_attempt, physical_call_index) 暴露行」，状态机与对账释放均经该 owner CAS。 |
| 逻辑结果槽(+outcome digest/occurrence/消费 CAS) | satisfied | D0 §2.5 v6 sweep：「槽状态转移的写者 = turn owner 的域命令（generic 控制面经 事件→reducer→turn owner 命令收敛，与 D3 §4c 同构，不跨 owner 直写）」；owner 为每个 (turn_id, step_id) 维护槽，首成功 CAS 占槽。 |
| ModelTurnExecutionContext | satisfied | D0 §2.3：frozen dataclass、租户/主体字段「owner 铸造」（v3#14）；快照/revision 在 action/approval/command 创建点固化后不可变随链传播——构造后无第二写者，缺任一字段构造期 raise。 |
| ActionRequestSpec+schema pins | satisfied | D0 §3.1：request_schema「单一事实源，三方消费同一份」；pin 为「AgentAction 行上 owner 写入的不可变列（提交时写）」，OperationRun 在实际创建点（approve/retry）由各自 owner copy+verify，幂等重放 pin 不匹配 fail-closed。 |
| agent tool registry/agent_tool_enabled(5 条件) | satisfied | D0 §3.3：serve 端点为「新只读 GET /api/agent/tool-registry」，复用 command_type_manifest() 不第三处重组；served 集从 ActionSpec.dispatch_adapter 注册字段派生（§3.2 条 2），注册表为单一事实源投影，无运行时写。 |
| agent_events 投影 | satisfied | 计划 §2 D2：「投影 ownership = turn owner 单写者」；「terminal 真值属 workflow 事件/命令表，agent_events 是 turn 粒度的投影/明细表，禁止独立推进 workflow 状态」；可从 canonical 事件重放重建。精确投影契约=义务 §6#2。 |
| 路由注册表+有效快照(含 TD-4 初始表) | satisfied | D0 §4：「checked-in、版本化、product-owned」，「路由变更 = 配置提交 + 审计，不是运行时行为」；有效快照在 action/approval/command 创建点固化为不可变引用随 retry 全链传播（§2.3），运行时无写者。 |
| scripted 回放转写治理 | satisfied | D0 §2.8：转写唯一产生者=录制管线（「录制器包装 live session，仅 owner 显式授权时运行」），落 runtime/ 命名空间不入库；入库 fixture 必须合成数据、显式标注 synthetic——回放路径只读，任一指纹字段变化 fail-closed。 |
| runtime_namespace/provider_mode 隔离本身 | satisfied | D3 §4b：全链携带「不可变 runtime_namespace + provider_mode」，行创建点一次写入后无写者；模式不匹配即 CAS 拒（结构上不可能产生 live 命名空间授权行）；D0 §2.5 信封/槽/journal/action 同款不可变携带。 |

## 2-租户键（satisfied 15/20）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b：唯一键 (runtime_namespace, workspace_id, company_fingerprint)，v7 隔离键前置；行身份即含租户，PG-only + repository owner。 |
| verification_intent(+epoch/claim gen/phase gen/awaiting_budget) | satisfied | D3 §4c：(workspace_id, intent_id) 身份，v5 恢复租户键；「workspace 等式进入索引、命令/事件引用、证据 bundle、repository 授权、幂等 scope 与下述 CAS 的每一条」。 |
| identity_search_budget_grant | satisfied | D3 §2.1：grant 键 =（workspace_id, review_session_id, verification_intent generation, policy_revision）+ 不可变签发身份；命令/attempt/扣减/transport 绑定精确签发身份，键内含租户；grant 进 §4b owner 矩阵。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：幂等键 apply:<runtime_namespace>:<workspace_id>:<session_id>:<源域事件 id>；apply owner CAS = {workspace + session 当前 revision + source 事件 id + generation 规则}，v8 键绑入 CAS 每一条含 human/expiry 路径。 |
| company.identity.verification.record(三分支 CAS) | satisfied | D3 §2.2/§4c：幂等键 record:<runtime_namespace>:<workspace_id>:<intent_id>:<phase_generation>；全条件 CAS 首条即「workspace_id 匹配」，非授权分支同样过身份/围栏 CAS。 |
| human_transition_pending 态与人工决定编舞 | satisfied | D3 §5：pending 态装在 review owner 自己的聚合（session 为租户键载体，grant 键证明 session 与 workspace_id 成对）；清除经 §2.2 apply 命令，其幂等键与 CAS 均含 workspace 且明示覆盖 human 路径。 |
| expiry 定时/expire 命令 | obligation **OB-2.1** | D3 §2.2 反向失效只写「触发时 CAS 于当前 generation」——目标行身份虽含 workspace（§4b 唯一键），但 expire 命令 CAS 谓词与定时事件身份未逐字列 workspace 等式，按清单口径在 D3 实施批补明。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a：backfill 无 workspace 的全局记录进 quarantine 映射表（显式映射或标 shared-origin，不静默落入任一 workspace）；bridge 命中无显式 workspace 映射 = diagnostic-only needs_human；workspace 决定永不回写全局文件。 |
| Tier-1/Tier-2 命令+判别化续跑编舞 | satisfied | D3 §2.2/§2.3/§4c：verify.evidence/search.expand/后继子命令全部绑 workspace 键的 intent（workspace 等式进命令/事件引用与每条 CAS）；Tier-2 绑精确 grant 签发（键含 workspace_id）；record 幂等键含 workspace。 |
| 证据 bundle+adjudication manifest(预创建+终态化) | satisfied | D3 §4c：「workspace 等式进入…证据 bundle」；manifest 预创建条目键 = workspace/intent/phase generation；§6 bundle hash 绑入调用信封并入 §4c CAS 四方一致比对。 |
| ModelInvocationEnvelope | satisfied | D0 §2.2 canonical 字段清单含 workspace/actor/permission revisions；D3 §6 明示「tenant/permission/policy 身份」入信封；ToolTurnResult 冗余镜像 workspace_id/actor_id，信封为权威。 |
| 成本台账(预留+暴露行) | obligation **OB-2.2** | D0 §2.3：父行挂 operation、子行 = (reservation, activity_attempt, physical_call_index)——租户仅经 operation 传递，台账行身份与「CAS 释放未用预留」谓词未逐字含 workspace 键，实施批（live 付费路径落地前）补列。 |
| 逻辑结果槽(+outcome digest/occurrence/消费 CAS) | satisfied | D0 §2.5：「owner 为每个 (turn_id, step_id) 维护 workspace-scoped 逻辑结果槽」——槽身份含租户；接受/消费 CAS 目标即该租户行并绑 canonical operation/turn/command/claim pins + 不可变 runtime_namespace。 |
| ModelTurnExecutionContext | satisfied | D0 §2.3：workspace_id/actor_id/permission_scope 字段明注「租户/主体绑定，owner 铸造」；缺任一字段 live 构造期 raise；三 revision 与租户身份同绑结果槽身份与 §2.8 request hash。 |
| ActionRequestSpec+schema pins | na | schema/version/digest 是 checked-in 注册表元数据而非租户 durable 状态；pin 落点（AgentAction 不可变列、OperationRun copy+verify、accepted-action journal）均为已租户化对象（§2.1 workspace 绑入 journal 身份）。 |
| agent tool registry/agent_tool_enabled(5 条件) | na | served 集是全局注册表配置投影（只读 GET /api/agent/tool-registry），无租户行/租户 CAS；hosted 多用户暴露的鉴权归 C2 激活依赖（计划 §3 D1 行明示）。 |
| agent_events 投影 | satisfied | 计划 §2 D2（v3 补全存储契约）：agent_events 带 per-stream sequence_number + idempotency_key、「workspace 租户列与授权 scope」；cursor 授权列入义务清单 #2。 |
| 路由注册表+有效快照(含 TD-4 初始表) | na | ModelRouteRegistry 为 checked-in、product-owned 配置（D0 §4），非租户数据；有效快照 ref/digest 固化在租户化 durable 对象创建点并计入含 workspace_id 的 §2.8 request hash。 |
| scripted 回放转写治理 | satisfied | D0 §2.8：canonical request hash 显式含 workspace_id/actor_id/permission_scope（v7 修 R6#10 回放身份含租户）；live 转写 tenant-bound 跨租户不可回放，仅显式 synthetic fixture 可跨租户且接受时校验信封租户等式。 |
| runtime_namespace/provider_mode 隔离本身 | satisfied | 隔离键与租户键成对出现：§4b 唯一键 (runtime_namespace, workspace_id, fingerprint)、§2.2 两个幂等键同时含 namespace+workspace、§2.8 回放身份含租户——namespace 未替代或吞并 workspace 维度。 |

## 3-世代与物理围栏(含物理约束交互/同步围栏)（satisfied 18/20）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b：decision_generation 单调+迁移表全量 owner 事件触发；§4b 历史/现态分离——superseded 行由新 generation 决定整行替换，不原地复活；§2.2 expiry 定时事件携带 generation、触发时 CAS 于当前 generation。 |
| verification_intent(+epoch/claim gen/phase gen/awaiting_budget) | satisfied | D3 §4c：绑 claim_generation+activity_attempt_id+control epoch；requeue 同一控制 UoW 先递增 durable control epoch 封 ABA；每种控制转移原子 supersede 旧 intent；claim gen 物理列诚实化=计划§6#6 migration 前置。 |
| identity_search_budget_grant | satisfied | D3 §2.1：不可变签发身份（grant_id+单调 issuance_generation），终态永不复活；supersede-with-transfer 绑新 intent generation；pre-transport CAS fence OperationRun/命令非终态+claim generation/control epoch+attempt+精确签发。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：CAS 于 workspace+session revision+源事件 id+generation 规则；watermark 单调、blocking 也推进——晚到旧 clearing 永被拒；v8 namespace/provider_mode 绑入 CAS 每一条（含 expiry/supersession/human 路径）。 |
| company.identity.verification.record(三分支 CAS) | satisfied | D3 §4c 全条件 CAS：claim_generation+attempt+control epoch+bundle watermark+terminal winner+manifest 聚合 hash；v8 三分支：非授权分支同样过身份/围栏 CAS，not_applied 只留给 stale generation/epoch/hash 失配。 |
| human_transition_pending 态与人工决定编舞 | satisfied | D3 §5：人工决定 UoW 原子推进 review/gate control epoch + 装 fail-closed human_transition_pending（本身 blocking）——正是同步围栏：控制转移已 commit、域侧 supersession 未落地的窗口内终审/commit 不可能凭旧 canonical 通过。 |
| expiry 定时/expire 命令 | satisfied | D3 §4b/§2.2：接受时同 UoW 持久化 expiry not-before 定时事件，域 owner 扫描→reducer→expire 命令；定时事件携带 generation，触发时 CAS 于当前 generation——旧定时器不可失效新决定；读路径零写零 enqueue。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a：bridge 命中无映射时 diagnostic-only needs_human——不得授权身份/gate 清除/付费抑制；backfill 行永不 verified 态；precedence preflight 保证 PG 行在位时文件/快照不可能胜出——无授权写面故无世代竞态。 |
| Tier-1/Tier-2 命令+判别化续跑编舞 | satisfied | D3 §2.3：后继 verify.evidence 子命令 = root intent + 单调 phase_generation+1，每个子命令各绑自己的 claim/attempt；§2.2 record 幂等键含 namespace/workspace/intent/phase_generation；中间 phase 结果结构上到不了 record。 |
| 证据 bundle+adjudication manifest(预创建+终态化) | satisfied | D3 §4c(v8)：manifest 预创建键=workspace/intent/phase generation；未决条目由 owner 超时/对账命令终态化；完整 terminal bitmap 入聚合 hash 且 record CAS 比对该 hash；bundle hash 绑入信封与 §4c CAS 四方一致。 |
| ModelInvocationEnvelope | satisfied | D0 §2.2：信封含 command/attempt 因果+快照 ref/digest+terminal_reason，经 action/command/attempt/结果槽/journal 与接受 CAS 全链绑定；D3 §6：CAS 同时比对 result artifact/provider call/bundle hash/attempt 身份四方一致。 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3：暴露子行键=(reservation, activity_attempt, physical_call_index) 绑物理 attempt；wire 前先落 dispatching；D3 §2.1 扣减/授权 CAS 在 transport 前且 fence claim generation/control epoch/attempt/精确 grant 签发。 |
| 逻辑结果槽(+outcome digest/occurrence/消费 CAS) | satisfied | D0 §2.5：接受 CAS 全条件含 claim/attempt 匹配+durable control epoch（明示 requeue→重 claim 窗口 ABA 适用于槽）+pins；消费 CAS 含槽 generation+epoch；cancel 原子 open→closed / accepted→superseded；重生显式推进槽 generation。 |
| ModelTurnExecutionContext | satisfied | D0 §2.3：activity_attempt_id 物理 attempt 身份（替代裸 int）；effective_route_snapshot 在 action/approval/command 创建点固化（非 claim 时）、随 retry 全链传播、digest 入 request hash；deadline_at 持久化、claim 后派生 attempt-local 单调钟。 |
| ActionRequestSpec+schema pins | satisfied | D0 §3.1(v4 round3#12)：pin 为 AgentAction 不可变列；OperationRun 在实际创建点（approve 时建 run :813-827、retry 子 run :1042-1061）各自 copy+verify——直击 R4#12；幂等重放 pin 不匹配 fail-closed；保留元数据键防碰撞。 |
| agent tool registry/agent_tool_enabled(5 条件) | na | 静态派生的只读 serve 谓词，无异步接受/应用写与控制转移；注册表随时间漂移的世代问题由 schema pin 机制（D0 §3.1 digest 不匹配⇒转人工）另行围栏，本机制自身无 class-3 面。 |
| agent_events 投影 | obligation **OB-3.1** | 投影可重建且 terminal 真值归 canonical 表（计划 §2 D2），但 rebuild 作为控制转移的 owner/顺序/与在线写的围栏、replay parity preflight 显式列为计划 §6#2 实施批义务（D2 批）。 |
| 路由注册表+有效快照(含 TD-4 初始表) | satisfied | D0 §2.3/§4：route_revision（条目内容 digest）随 action/command/attempt/请求 hash/result 全链持久化——部署改路由不影响已排队重试；快照创建点固化修 R4#10（claim 时太晚）；rollout_state=draft，变更=配置提交非运行时行为。 |
| scripted 回放转写治理 | satisfied | D0 §2.8：canonical request hash 含 route_revision+effective_route_snapshot_digest+permission/outbound/model-safe 三 revision+租户，任一字段变化⇒回放 fail-closed——转写不可能跨配置世代/跨租户命中。 |
| runtime_namespace/provider_mode 隔离本身 | satisfied | D3 §4b(R6#2)：行身份/幂等 scope/每条接受 CAS 全链携带不可变 namespace+mode，模式不匹配即 CAS 拒——非 live 证据结构上不可能产生 live 授权行；v8 R7#2 把传播补到 apply 的事件/命令/幂等/CAS 每一条；D0 §2.5 同构覆盖信封/槽/journal/action。 |

## 4-生命周期完备性（satisfied 14/20）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b 全量迁移表含反向路径：expire 命令、policy 升版、supersession、重验(failed/timed_out/needs_human→pending、superseded→pending)、human_confirmed 行；§2.2 反向失效重开 gate；历史/现态分离防 superseded 原地复活。 |
| verification_intent(+epoch/claim gen/phase gen/awaiting_budget) | **blocker** | D3 §2.2 声明 intent 迁入 durable `awaiting_budget` 态，但 §4c intent_state 枚举仍为 {pending, applied, cancelled, timed_out, superseded}——新态不在状态机/迁移规则内，其进入/退出(含永无 grant 时的收敛)无迁移表覆盖，自相矛盾。 |
| identity_search_budget_grant | satisfied | D3 §2.1：不可变签发身份(grant_id+单调 issuance_generation+授予事件 id)、终态 grant 永不复活(再授予=新行)、supersede-with-transfer 恰好剩余额度继承、状态枚举 active/revoked/exhausted/superseded/reconciled、revoke 单写者收敛——逐项命中第 4 类 grant 规则。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：watermark 单调且每次成功 apply(含 blocking)恒推进；晚到旧 clearing 永被拒；反向失效(expire/supersede→blocking apply)显式重开已清除的阻塞；expiry 定时事件携带 generation CAS。 |
| company.identity.verification.record(三分支 CAS) | satisfied | D3 §2.2 v8 三分支：授权→shadow_would_verify；非授权=真实域写 needs_human/failed/timed_out(record 为这些迁移唯一 owner)；not_applied 只留给 stale 围栏失配——§4b pending 行迁移全部有明确产生者(含 R7#4 的 manifest-incomplete 分支)。 |
| human_transition_pending 态与人工决定编舞 | obligation **OB-4.1** | D3 §5 定义装态(人工决定 UoW)与清态(apply 命令)，fail-closed 方向安全；但该态未入 §4b 迁移表、无 stuck-pending 定时收敛条目——计划 §5a 已把 R7#6(补进矩阵机制清单)列为下一迭代簿记项。 |
| expiry 定时/expire 命令 | satisfied | D3 §4b：接受时同 UoW 持久化 expiry not-before 定时事件，定期域 owner 扫描发类型化事件→reducer 计划 expire 命令；读路径 fail-closed 派生零写零 enqueue；§2.2 定时器携带 generation，旧定时器不可失效新决定——定时收敛有 owner 且去读者化。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a 全生命周期：backfill(needs_human+quarantine 映射)→降级 bridge(命中记指标、diagnostic-only)→precedence preflight→deletion preflight+非同义反复退役条件(计划 §6.3 residual 台账)；写入方逐点迁移或停写。 |
| Tier-1/Tier-2 命令+判别化续跑编舞 | satisfied | D3 §2.2/§2.3 v8：phase 结果判别化路由(final_adjudication/evidence_insufficient/needs_human_budget/失败超时)全部有终点；后继子命令绑 root intent+单调 phase_generation+1 各带自己的 claim/attempt；grant 余额有界终止；中间 phase 结构上到不了 record 授权分支。 |
| 证据 bundle+adjudication manifest(预创建+终态化) | satisfied | D3 §4c v8：owner 裁决前预创建全部条目(键=workspace/intent/phase generation)，未决条目由 owner 的超时/对账命令终态化(标 unresolved)——定时收敛有 owner；unresolved⇒record 非授权分支落 needs_human，完整 terminal bitmap 入聚合 hash。 |
| ModelInvocationEnvelope | satisfied | D0 §2.2 单一物理 schema；D3 §6 定性为『独立命名的不可变结果侧契约』——不可变签发、含 command/attempt 因果与 result artifact ref+digest，经全链绑定与两侧接受 CAS 消费；不可变对象无复活面。 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3/D3 §8：状态机 prepared→dispatching→sent→confirmed\|uncertain\|no_call 全量，crash 可区分(停在 dispatching/sent 按 uncertain worst-case 计)，对账后 CAS 释放未用预留；单一 PG cost-ledger owner；§9 验收含预留-实际-对账三方 parity。 |
| 逻辑结果槽(+outcome digest/occurrence/消费 CAS) | satisfied | D0 §2.5：slot_state {open,accepted,consumed,closed,superseded} 全枚举；cancel/supersession 原子关槽(open→closed、accepted→superseded 抢先消费窗口)；有意重新生成显式推进槽 generation(不复活)；写者=turn owner 域命令单写者。 |
| ModelTurnExecutionContext | na | frozen 每调用构造的请求侧上下文，非 durable 有状态机制，无迁移/过期状态机；其唯一时间语义(deadline_at 墙钟持久化+claim 后派生 attempt-local 单调钟)已在 D0 §2.3/§2.6 规定。 |
| ActionRequestSpec+schema pins | satisfied | D0 §3.1：pin 为 AgentAction 不可变列(创建点写入)，OperationRun 在实际创建点(approve/retry-child)copy+verify，幂等重放 pin 失配 fail-closed；digest 失配转人工；宽松迁移桥有非同义反复退役条件(全部可提交 action 零命中一个发布窗口)。 |
| agent tool registry/agent_tool_enabled(5 条件) | satisfied | D0 §3.2 五条谓词派生 served 集 + §3.3 双向守卫(served≡enabled)；条件失守即出集；在途对象由 §3.1 schema pin fail-closed 兜底(不静默沿用)；simulate preflight 为 contract lane 级持续守卫防注册与 adapter 再脱节。 |
| agent_events 投影 | obligation **OB-4.2** | 计划 §2 D2 已声明可重建(canonical workflow 事件重放)、重放语义随 DDL、turn owner 单写者，但精确投影契约(rebuild owner/顺序、cursor 授权、replay parity preflight)在计划 §6.2 显式转 D2 实施批义务。 |
| 路由注册表+有效快照(含 TD-4 初始表) | satisfied | D0 §4：rollout_state draft\|canary\|active\|retired 全生命周期枚举，初始表全 draft、逐条 owner 拨动升级；路由变更=配置提交+审计非运行时行为；§2.3 快照在 action/approval/command 创建点固化并随 retry 全链传播，部署改路由不影响已排队重试。 |
| scripted 回放转写治理 | obligation **OB-4.3** | D0 §2.8 声明每份转写带 schema_version、大小上限、保留 TTL 与租户绑定，但 TTL 到期清扫的执行 owner(定时事件/扫描)未指定——按第 4 类『定时收敛有 owner』属 D0 实施批落地细节。 |
| runtime_namespace/provider_mode 隔离本身 | na | 两者是行/幂等 scope/CAS 上的不可变属性(D3 §4b『不可变 runtime_namespace + provider_mode』)，无状态迁移、过期或复活语义可言；其覆盖完备性归第 10 类(运行时/模式隔离)自查。 |

## 5-晚到与部分结果（satisfied 14/20）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4c「任一失配 ⇒ 全不动 + not_applied no-op 证据事件」+ §8「cancel 前置停止；晚到结果按 §4c 隔离」；§4b 过期行读路径 fail-closed 按 needs_human 消费、零写零 enqueue。 |
| verification_intent(+epoch/claim gen/phase gen/awaiting_budget) | satisfied | D3 §4c：requeue 同 UoW 递增 durable control epoch——「已 requeue、未重 claim」窗口旧结果 epoch 失配即拒；八项竞态电池含 cancel/timeout/generic-retry 后晚到 apply；awaiting_budget 为显式 durable 态（§2.2）。 |
| identity_search_budget_grant | satisfied | D3 §2.1：精确签发身份绑定——「旧授予事件驱动的 stale Tier-2 命令不能消费新 grant」；pre-transport 授权 CAS fence epoch/claim/attempt，「cancel 后的异步收敛窗口内 provider 调用被 epoch 失配挡住」；终态 grant 永不复活。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：watermark 单调、每次成功 apply 含 blocking 均推进；「clearing 仅当 generation > watermark 生效……晚到旧 clearing 永被拒」；not_applied 永不触发 gate 更新；各次转移各有源事件成键。 |
| company.identity.verification.record(三分支 CAS) | satisfied | D3 §2.2/§4c：not_applied 显式终态「只保留给 stale 身份/generation/epoch/hash 失配」；全条件 CAS 含 claim generation+attempt+control epoch+预期终态事件身份，晚到/失配结果结构性不落域写。 |
| human_transition_pending 态与人工决定编舞 | satisfied | D3 §5：人工决定 UoW 原子推进 review/gate control epoch + 装 blocking 的 human_transition_pending——「窗口期内终审/commit 不可能凭旧 canonical 通过」；§4c CAS 含 verification_state NOT IN (human_confirmed)，晚到机器结果被挡。 |
| expiry 定时/expire 命令 | satisfied | D3 §2.2 反向失效：「expiry 定时事件携带 generation，触发时 CAS 于当前 generation（旧定时器不可失效新决定）」——晚触发旧定时器即晚到结果，被 generation CAS 拒。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a：无 workspace 映射的 bridge 命中 = diagnostic-only needs_human「不得授权身份/gate 清除/付费分支抑制」；precedence preflight 保证 PG 行在位时文件/快照不可能胜出——迟来的 legacy 读写结构性不可授权。 |
| Tier-1/Tier-2 命令+判别化续跑编舞 | satisfied | D3 §2.3：phase 结果判别化（final_adjudication\|evidence_insufficient\|needs_human_budget），「只有显式 final_adjudication 才允许计划 record——中间 phase 结果结构上到不了 record」；每子命令各绑 claim/attempt+phase_generation。 |
| 证据 bundle+adjudication manifest(预创建+终态化) | obligation **OB-5.1** | D3 §4c：未决条目由 owner 超时/对账命令终态化(unresolved)、不完整 bitmap 走非授权分支 needs_human；但「超时终态化之后晚到的裁决调用结果」的条目级落点（首写 CAS+quarantine 证据）未成文——实施批（八项竞态电池同族）固化。 |
| ModelInvocationEnvelope | satisfied | D0 §2.2 canonical 字段含 terminal_reason；D3 §6：「非可授权终态（{end_turn, tool_calls} 之外）在 D3 接受谓词同样 fail-closed 转 needs_human（截断/过滤的 judge 输出不可参与 auto-confirm）」。 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3/D3 §8：wire 写前先落 dispatching；「停在 dispatching/sent 的 crash 一律按 uncertain 以 worst-case 预留计，直至 provider 对账或保守消耗」——发送未见结果（晚到确认）显式态，缺 usage 永不视为零成本。 |
| 逻辑结果槽(+outcome digest/occurrence/消费 CAS) | satisfied | D0 §2.5：slot_state 含 accepted→superseded——「cancel 落在接受后、消费前窗口时抢先」（正是「已接受未消费」窗口）；消费为完整终局原子 UoW；重试仅 outcome digest 精确相等 join，子集/超集/重排 quarantine；「晚到 terminal 结果一律 quarantine 证据」。 |
| ModelTurnExecutionContext | na | 请求侧 owner 铸造的不可变上下文，自身不产生异步结果；晚到/部分结果由结果槽、信封与 §2.6 状态机（超时 ⇒ 失败终止 + incomplete attempt）承接。 |
| ActionRequestSpec+schema pins | na | 静态 schema/pin 契约无异步结果；晚到幂等重放的 pin 失配已在 §3.1 fail-closed 拒绝（copy+verify，类 3/7 关注面）。 |
| agent tool registry/agent_tool_enabled(5 条件) | na | 静态 serve 谓词与注册表只读投影，不产生异步结果；served 结果的晚到/截断语义由结果槽与 terminal_reason 授权集合承接。 |
| agent_events 投影 | satisfied | Plan §2 D2：「terminal 真值属 workflow 事件/命令表，agent_events 是投影/明细，禁止独立推进 workflow 状态」+ 可重建——晚到/缺失投影行结构性不可授权；精确重放 parity 契约已列义务 #2（D2 批）。 |
| 路由注册表+有效快照(含 TD-4 初始表) | na | checked-in 配置注册表 + action/approval/command 创建点固化快照均为静态输入，无异步结果；快照使晚排队重试语义稳定（类 3/7 承接，D0 §2.3）。 |
| scripted 回放转写治理 | na | 回放为确定性同步重放 canonical ToolTurnResult、任一指纹字段变化 fail-closed（D0 §2.8），无晚到窗口；截断/不完整流由 §2.6 协议失败状态机兜底。 |
| runtime_namespace/provider_mode 隔离本身 | satisfied | D3 §4b：namespace/mode 进「每条接受 CAS」，「模式不匹配即 CAS 拒」——跨模式晚到结果结构性不可能落 live 授权路径；D0 §2.5 信封/槽/journal/action 同携带；R7#2 已把 apply 每条 CAS（含 expiry/supersession/human 路径）纳入。 |

## 6-成本诚实性（satisfied 8/20）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | na | 该表为记录聚合、不发起付费调用；成本由台账/信封承载，行仅存「预算 grant/信封引用」列（D3 §4b）供追溯。 |
| verification_intent(+epoch/claim gen/phase gen/awaiting_budget) | satisfied | D3 §2.1：intent supersession 时 grant owner 原子 supersede-with-transfer，「新 grant 携带恰好剩余额度（不清零、不回满）」——重跑不假设未花费；awaiting_budget 后追加=再授予新额度，非复活。 |
| identity_search_budget_grant | satisfied | D3 §2.1：维度化信封+扣减原子（TD-5）、pre-transport 授权 CAS（session+intent+grant 三活跃+epoch/claim/精确签发）在 provider 调用前；耗尽⇒terminal needs_human「永不静默超支」（§8）。 |
| plan_review.identity_result.apply(watermark) | na | gate 写命令，不发起任何付费 provider/模型调用，无成本面。 |
| company.identity.verification.record(三分支 CAS) | na | 纯域状态写（验证聚合迁移），本身零 provider 调用；其消费的付费结果的成本已由 §8 每物理调用一行入账。 |
| **human_transition_pending 态与人工决定编舞** | na | 人工决定 UoW 为纯 PG 写+事件编舞（D3 §5），不含付费调用。 |
| expiry 定时/expire 命令 | na | 定时事件+expire 命令为域状态迁移（D3 §4b），零付费调用；触发的重验走新 intent+新 grant 正常计账路径。 |
| 迁移桥(文件注册表+快照) | na | bridge 为读回退（PG miss 后读文件，D3 §4a），零 provider 调用；bridge 命中且无映射时明文「不得授权…付费分支抑制」，不产生成本失真。 |
| Tier-1/Tier-2 命令+判别化续跑编舞 | satisfied | D3 §8：「worst-case 预留+每次物理调用一行暴露记录 prepared→dispatching→sent→confirmed\|uncertain\|no_call」；Tier-2 审批证据=grant 事件引用缺即 fail-closed（§2.1）；每 phase 子命令各绑 attempt 各入账。 |
| 证据 bundle+adjudication manifest(预创建+终态化) | satisfied | 逐候选裁决调用各带信封含「成本暴露行引用」（D3 §6/D0 §2.2）；未决条目由 owner 超时/对账命令终态化，对应 crash/放弃调用按 §8 uncertain 以 worst-case 预留计，不假设未花费。 |
| ModelInvocationEnvelope | satisfied | D0 §2.2 canonical 字段清单显式含「usage+usage_status」与「成本暴露行引用」，D3 §6 同一物理定义——每次付费调用的结果侧证据与台账行绑定。 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3：父行 worst-case 预留、子行=(reservation,attempt,physical_call_index)；「任何 wire 写之前先落 dispatching」、crash 停 dispatching/sent 按 uncertain 以 worst-case 计、仅可证明 pre-transport 中止转 no_call、对账后 CAS 释放。 |
| 逻辑结果槽(+outcome digest/occurrence/消费 CAS) | na | 槽只管结果去重/接受/消费（D0 §2.5），不发起付费调用；重试再调用的成本由台账每物理调用一行独立入账，槽 join/quarantine 不影响计账。 |
| ModelTurnExecutionContext | satisfied | D0 §2.3：budget+budget_reservation_ref 必填，缺任一构造期 raise（pre-transport、不发网络请求）；「usage_status != reported 按 monetary_ceiling 保守保留额记账，缺失 usage 永不静默视为零成本」。 |
| ActionRequestSpec+schema pins | na | 输入 schema 事实源与 pin 生命周期（D0 §3.1）为校验/身份机制，零付费调用。 |
| agent tool registry/agent_tool_enabled(5 条件) | na | serve 谓词与只读注册表路由不发起付费调用；simulate dispatch preflight 走 simulate 模式无 wire 成本；budget_required 为服务端元数据交 owner/信封执行。 |
| agent_events 投影 | na | turn 粒度投影/明细表（计划 §2 D2），禁止推进 workflow 状态、零 provider 调用，无成本面。 |
| 路由注册表+有效快照(含 TD-4 初始表) | satisfied | D0 §2.3：有效路由快照在 action/approval/command 创建点固化且内容含「定价类」，digest 计入 request hash——部署改价不改已排队重试的计费口径；§4 budget_class 映射默认 ModelTurnBudget 与审批政策。 |
| scripted 回放转写治理 | na | 回放 fail-closed 无网络 I/O、零付费；录制器包装的 live session 仅 owner 显式授权且走正常 live context+台账路径（D0 §2.8）。 |
| runtime_namespace/provider_mode 隔离本身 | na | 隔离标记为身份/围栏机制，不发起付费调用；simulate/scripted 不产生 wire 成本，live 成本行经携带 namespace 的信封引用可追溯（D0 §2.5/D3 §4b）。 |

## 7-物理身份绑定（satisfied 20/20）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b：行含物理因果列（source workflow_command_id/activity_run_id/attempt_id/entity_delta_id）+ accepted_policy_version/valid_until + fingerprint_version + schema 版本化 result_artifact_ref + 预算 grant/信封引用 + canonical_url 推导规则版本。 |
| verification_intent(+epoch/claim gen/phase gen/awaiting_budget) | satisfied | D3 §4c：intent 绑 operation_run+session+command+claim_generation+activity_attempt_id，存 plan bundle hash+fingerprint+policy/route/schema revisions+effective_route_snapshot digest+control epoch；claim gen 物理列诚实标注为 migration 义务（计划 §6#6）。 |
| identity_search_budget_grant | satisfied | D3 §2.1：每次授予=新 grant_id+scoped 单调 issuance_generation+授予事件 id 的不可变签发身份；命令/attempt/扣减/transport 绑定精确签发身份；pre-transport CAS 加 claim generation/control epoch/attempt。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：apply 幂等键=apply:<namespace>:<workspace>:<session>:<源域事件 id>，各次转移各有源事件各自成键；CAS 于 session revision+source 事件 id+generation watermark；expiry 定时事件携带 generation。 |
| company.identity.verification.record(三分支 CAS) | satisfied | D3 §4c：全条件 CAS 含 claim_generation+activity_attempt_id+预期终态事件身份+fingerprint+decision_generation+policy/schema/route/snapshot pins+control epoch+manifest 聚合 hash；§6：同时比对 result artifact/provider call/bundle hash/attempt 四方一致。 |
| human_transition_pending 态与人工决定编舞 | satisfied | D3 §5：人工决定 UoW 原子记录决定+发决定事件+推进 review/gate control epoch；commit owner canonical 复查比对最新人工决定事件 id 与验证行 provenance 一致；human_confirmed 行带 decision source=human+review session ref。 |
| expiry 定时/expire 命令 | satisfied | D3 §2.2 反向失效：expiry 定时事件携带 generation，触发时 CAS 于当前 generation（旧定时器不可失效新决定）；§4b：接受时同 UoW 持久化 not-before 定时事件，expire 经域 owner 命令写。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a：backfill 行 decision_source=legacy_registry_import + verification_state=needs_human（永不 verified/accepted）；无 workspace 映射的 bridge 命中= diagnostic-only needs_human，不得授权身份/gate 清除——无物理绑定的遗留产物结构性不可授权。 |
| Tier-1/Tier-2 命令+判别化续跑编舞 | satisfied | D3 §2.3：每个后继 verify.evidence 子命令绑 root intent+单调 phase_generation+1 且各绑自己的 claim/attempt；record 幂等键含 phase_generation。judge_call_key 追加 workspace/intent gen/路由/schema/policy revision 维度=计划 §6 义务#5。 |
| 证据 bundle+adjudication manifest(预创建+终态化) | satisfied | D3 §6：evidence_ids 对照不可变 intent/attempt 证据 bundle resolve，bundle hash 绑入调用信封与 §4c CAS；§4c：manifest 键=workspace/intent/phase generation，完整 terminal bitmap 参与聚合 hash 入 record CAS。 |
| ModelInvocationEnvelope | satisfied | D0 §2.2 canonical 单一定义：快照 ref+digest、workspace/permission/outbound/model-safe revisions、command/attempt 因果、provider 响应身份（requested/response/effective+provenance+call id）、terminal_reason、usage、result artifact ref+digest、成本暴露行引用，经全链与接受 CAS 绑定。 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3：暴露子行键=(reservation, activity_attempt, physical_call_index)，状态 prepared→dispatching→sent→confirmed\|uncertain\|no_call；信封含成本暴露行引用与 provider call id 互链；D3 §8 同口径+对账 parity 验收。 |
| 逻辑结果槽(+outcome digest/occurrence/消费 CAS) | satisfied | D0 §2.5：action 身份=(result_slot_id, slot_generation, tool_name, canonical_args_digest, occurrence_ordinal)；接受/消费 CAS 含 claim/attempt 身份+durable control epoch+schema/route/policy pins；provider tool_call_id 仅证据；canonical outcome digest 精确相等才 join。 |
| ModelTurnExecutionContext | satisfied | D0 §2.3：route_revision（内容 digest）+effective_route_snapshot_ref 在 action/approval/command 创建点固化（非 claim 时），内容=endpoint/base_url digest、timeout、api_style、定价、circuit policy id，随 retry 全链传播且 digest 计入 §2.8 request hash；activity_attempt_id 物理身份。 |
| ActionRequestSpec+schema pins | satisfied | D0 §3.1（v4 round3#12）：pin=AgentAction 行上提交时写入的不可变列；OperationRun 在实际创建点 copy+verify——审批型 run 在 approve 时才建（:813-827）、retry 子 run 在 :1042-1061 才建，两处都执行；幂等重放 pin 失配 fail-closed。直接落实 R4#12 规则。 |
| agent tool registry/agent_tool_enabled(5 条件) | satisfied | D0 §3.1/§2.8：schema 版本+digest 作为不可变 pin 落 durable 对象，tools_schema_digest 计入 canonical request hash；谓词第 4 条要求 revisioned model_safe_result_schema。turn 创建点钉 tool-schema digest 并贯穿 journal/approve/retry=计划 §6 义务#4。 |
| agent_events 投影 | satisfied | 计划 §2 D2：每行带物理因果列（workflow_event_id/workflow_command_id/operation_run_id/turn_id/step_id）+ per-stream sequence_number+idempotency_key；精确投影契约（stream 身份公式/source-event ordinal/replay parity）=计划 §6 义务#2。 |
| 路由注册表+有效快照(含 TD-4 初始表) | satisfied | D0 §4：调用方只传 route_id、model 显式钉死、rollout_state=draft；§2.3：route_revision 单独不足（base_url/timeout 可变、熔断键含 base_url），故有效配置快照（endpoint/timeout/定价/circuit）在创建点固化——正是本类'非仅逻辑路由版本'要求。 |
| scripted 回放转写治理 | satisfied | D0 §2.8：canonical request hash v1 含 route_id/route_revision/effective_route_snapshot_digest/三 revision/messages_digest/tools_schema_digest/workspace/actor/transcript_digest，任一字段变化即回放 fail-closed；转写 tenant-bound、synthetic 才可跨租户。 |
| runtime_namespace/provider_mode 隔离本身 | satisfied | D3 §4b：operation/command/attempt/信封/证据 bundle/行身份/幂等 scope/每条接受 CAS 全链携带不可变 runtime_namespace+provider_mode；§2.2 v8：record/apply 幂等键均以 namespace 前缀并绑入 CAS 每一条；D0 §2.5 同构——隔离键本身即物理身份维度并逐点持久化。 |

## 8-provenance 与信任边界（satisfied 20/20）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b `decision_source ∈ {machine, human, legacy_registry_import}` 显式列；§3 human_confirmed 带 decision source=human+review session ref、跨 plan 永不重标为机器验证；§4a 导入行永不 verified/accepted。 |
| verification_intent(+epoch/claim gen/phase gen/awaiting_budget) | satisfied | D3 §4c 全条件 CAS 含 `verification_state NOT IN (human_confirmed)`——机器结果结构上不能覆盖人工决定；人工路径经 §5 独立 supersession 命令（generation+1），两通道命令/字段分立。 |
| identity_search_budget_grant | satisfied | D3 §2.1：grant = review 卡上人工类型化部分决定，不可变签发身份（grant_id+issuance_generation+授予事件 id）；Tier-2 命令审批证据 = grant 事件引用、缺引用 fail-closed——人工授权通道不可被机器伪造。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2：apply 只由 record 域事件（授权/非授权分支，不含 not_applied）经 reducer 触发写 gate reason，不写人工字段；§5 人工决定走独立 UoW+事件链后才经 apply 清 pending；commit owner 复查 canonical 不信 gate 快照。 |
| company.identity.verification.record(三分支 CAS) | satisfied | D3 §2.2/§4c：record 是机器迁移唯一 owner；CAS 排除 human_confirmed；§4b 迁移表 human_confirmed 只经「人工确认 UoW」进入、只被新 generation 人工决定 supersede——机器结果永不经人工字段回流。 |
| human_transition_pending 态与人工决定编舞 | satisfied | D3 §5：人工决定 UoW 原子记录决定+发事件+推进 control epoch+装 blocking 的 human_transition_pending；commit owner 比对最新人工决定事件 id 与验证行 provenance 一致——人机通道分立且「人永远赢」窗口关死。 |
| expiry 定时/expire 命令 | satisfied | D3 §4b：expire 只作用于 verified_accepted→needs_human（机器态）；human_confirmed 仅可被新 generation 人工决定 superseded——定时器/机器路径不能降级或伪装人工决定；读路径 fail-closed 派生零写。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a：backfill 行 decision_source=legacy_registry_import 且恒 needs_human（永不 verified/accepted）；无 workspace 映射的 bridge 命中 = diagnostic-only needs_human，不得授权身份/gate 清除/付费分支抑制——legacy 数据不可冒充已验证。 |
| Tier-1/Tier-2 命令+判别化续跑编舞 | satisfied | D3 §2.3：只有显式 final_adjudication 可计划 verification.record，中间 phase 结果结构上到不了授权路径；needs_human_budget 转人工（gate 提示授予预算），机器只建议、授予恒为人工部分决定（§2.1）。 |
| 证据 bundle+adjudication manifest(预创建+终态化) | satisfied | D3 §6：模型只引用 owner 提供的不可变 bundle 内 evidence_ids，URL/可注册域/kind 全部服务端派生，越界 id⇒needs_human；§4c manifest 条目由服务端预枚举——模型不能自证证据 provenance 或塑造完备性。official-domain 证明规则成文列为义务#5。 |
| ModelInvocationEnvelope | satisfied | D0 §2.2 canonical 定义+D3 §6：provider 响应身份/usage/fallback/circuit 证据全部服务端信封记录，「身份/usage/fallback 模型不可自证」；模型输出中出现 provider/usage/fallback/URL/domain 字段一律忽略并记协议偏差。 |
| 成本台账(预留+暴露行) | satisfied | D0 §2.3/D3 §8：暴露行由服务端 transport 状态机（prepared→dispatching→sent→…）驱动而非模型申报；usage_status != reported 按保守保留额记账、缺失 usage 永不静默视为零——成本证据全为服务端派生。 |
| 逻辑结果槽(+outcome digest/occurrence/消费 CAS) | satisfied | D0 §2.5：provider tool_call_id 仅证据、槽/action 身份由 owner 铸造（result_slot_id+occurrence+args digest）；仅验证过的 terminal 结果且 terminal_reason∈{end_turn,tool_calls} 可占槽授权——模型不可自证结果身份。 |
| ModelTurnExecutionContext | satisfied | D0 §2.3：workspace/actor/permission_scope「owner 铸造」，route_id 不接受裸 model 字符串，permission/outbound-policy/model-safe revisions 服务端绑入——调用主体与策略身份全部服务端提供，模型/客户端不可注入。 |
| ActionRequestSpec+schema pins | satisfied | D0 §3.1：target 段服务端绑定，「模型提供的 target 字段一律拒绝，target 由 owner 从会话上下文铸造」；同名/别名字段试图覆盖 owner target ⇒ 提交即拒——ingress 不可伪造服务端资源引用。 |
| agent tool registry/agent_tool_enabled(5 条件) | satisfied | D0 §3.2 条件4：缺已注册 revisioned model_safe_result_schema 即不 serve（敏感输出上 wire 结构性关死）；§2.1 出站校验覆盖全部消息角色（system/user/assistant/tool_result），条件5 preflight 实际行使 model-safe 序列化器。 |
| agent_events 投影 | satisfied | Plan §2 D2：每行带物理因果列（workflow_event_id/command_id/operation_run_id/turn_id/step_id），terminal 真值属 canonical workflow 表、投影禁止独立推进状态，带 workspace 租户列与授权 scope——provenance 服务端派生且不可反向污染真值。 |
| 路由注册表+有效快照(含 TD-4 初始表) | satisfied | D0 §4：调用方永远只传 route_id、model 显式钉死、fallback_policy=fail_closed 无静默改道；§2.6 provenance 只在真实拿到 response 身份时标 provider_response、fallback 场景永不伪造；与 reviewer 表/CRM 锁互不引用。 |
| scripted 回放转写治理 | satisfied | D0 §2.8：转写只存归一化事件永不存 raw payload、管线内置脱敏；live 转写 tenant-bound 跨租户不可回放，仅显式 synthetic fixture 可跨租户（接受时校验信封租户等式）；录制器仅 owner 显式授权运行——回放证据不可冒充 live provenance。 |
| runtime_namespace/provider_mode 隔离本身 | satisfied | D3 §4b：非 live 证据 diagnostic-only，结构上不可能产生 live 命名空间的 shadow_would_verify/verified_accepted（模式失配即 CAS 拒），Phase-2 promotion 只认 live 证据；D0 §2.5 scripted/simulate 结果不可流入 live 授权路径——测试证据不可变成生产决定。 |

## 9-自包含与跨文档一致（satisfied 16/20）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b 唯一定义（DDL+全量状态迁移表）；plan §2 D3 用同一枚举词 shadow_would_verify/verified_accepted/needs_human，§2.2 record 非授权分支落 needs_human/failed/timed_out 与迁移表首行逐字对应。 |
| verification_intent(+epoch/claim gen/phase gen/awaiting_budget) | **blocker** | 状态枚举跨节不一致：D3 §2.2 令 intent 迁入 durable `awaiting_budget` 态，但 §4c `intent_state ∈ {pending, applied, cancelled, timed_out, superseded}` 未含该值，违反状态枚举逐字一致。 |
| identity_search_budget_grant | satisfied | 单一定义 D3 §2.1（状态 active/revoked/exhausted/superseded/reconciled）；plan §5 TD-5 与之口径一致（每次授予默认 3 次、维度化、supersede-with-transfer 恰好剩余额度），v6 已显式消解与上层「不重置」的矛盾。 |
| plan_review.identity_result.apply(watermark) | satisfied | watermark 规则单处定义于 D3 §2.2（单调恒推进、clearing 仅 >、blocking ≥ 并推进）；plan §2 D3「generation watermark 围栏，阻塞方向恒占优」与之语义一致；v8 幂等键含 runtime_namespace。 |
| company.identity.verification.record(三分支 CAS) | **blocker** | 上层计划未同步 v8：plan §2 D3 链条仍写「recorded 域事件（仅 applied）→ apply」，而 D3 v8 §2.2 三分支下非授权分支（needs_human/failed/timed_out）同为真实域写并触发 apply；plan §5a 还把 R7#4 列为待修。 |
| human_transition_pending 态与人工决定编舞 | obligation **OB-9.1** | 编舞文本自包含且一致（D3 §5 单 UoW 装态→reducer→supersession→apply 清态），但按 plan §5a R7#6 该新机制尚待补进机制×不变量矩阵并随矩阵重生成。 |
| expiry 定时/expire 命令 | satisfied | D3 §4b 迁移表行（verified_accepted→needs_human 经 `company.identity.verification.expire`，接受时同 UoW 持久化定时事件、读者零写零 enqueue）与 §2.2 反向失效（定时事件携带 generation、CAS 于当前 generation）互相一致。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a 与 plan §0/§6#1 写入方清单逐点一致（acquisition:1017-1031、company_registry:492-539/554-594、:641-681 两文档同为「更正为读方」；round-4 点名 company_asset_supplement/asset_sync 同载）。 |
| Tier-1/Tier-2 命令+判别化续跑编舞 | **blocker** | plan §2 D3 链条写「验证 terminal 事件 → reducer 计划 record」无判别化分支，且 §5a 仍把 R7#3 列为下一迭代待修，与 D3 v8 §2.2/§2.3 已落的判别化路由（evidence_insufficient→search.expand/awaiting_budget）不同步。 |
| 证据 bundle+adjudication manifest(预创建+终态化) | satisfied | D3 §4c 单处定义（预创建 manifest、超时/对账命令终态化、unresolved⇒record 非授权分支 needs_human 而非 not_applied），与 §2.2 三分支语义及 §6 bundle-hash 绑定表述一致。 |
| ModelInvocationEnvelope | satisfied | 一个物理 schema 一处定义：D0 §2.2 明示「本注释即 canonical 字段清单(D3 §6 引用不复述)」，D3 §6 对称声明「canonical 字段清单以 D0 §2.2 为唯一定义处」且补充字段（terminal_reason/digest 等）均在 D0 清单内。 |
| 成本台账(预留+暴露行) | satisfied | 状态机跨文档逐字一致：D0 §2.3 与 D3 §8 均为 prepared→dispatching→sent→confirmed\|uncertain\|no_call，且「任何 wire 写之前先落 dispatching」「crash 停在 dispatching/sent 按 uncertain 计」两处同词。 |
| 逻辑结果槽(+outcome digest/occurrence/消费 CAS) | satisfied | 单处定义 D0 §2.5（slot_state 五值枚举、outcome digest 精确相等 join、occurrence 序号、消费 CAS），跨文档引用显式共享机制（「与 D3 §4c 同一 epoch 机制」「与 D3 §4c 同构」），无第二定义。 |
| ModelTurnExecutionContext | satisfied | 单处定义 D0 §2.3；D3 §6 显式消歧「那里定义的是请求执行上下文，本信封是独立命名的不可变结果侧契约」，请求侧/结果侧两契约边界跨文档一致；plan §2 D0 引用同名。 |
| ActionRequestSpec+schema pins | satisfied | D0 §3.1 单一事实源（input+target 两段、三方同源消费、pin 为不可变列+创建点 copy+verify）；plan §2 D1 标注「v7 与 D0 §3.1 同步」且措辞（target 服务端绑定、修复 submit_action 零校验）逐条对应。 |
| agent tool registry/agent_tool_enabled(5 条件) | satisfied | plan §2 D1 五条件与 D0 §3.2 谓词 1-5 逐条对应（schema 在/dispatch adapter 在/activity-spine 语义断言/revisioned model_safe_result_schema【R7#10 已同步】/simulate preflight 含行使序列化器）。 |
| agent_events 投影 | satisfied | plan §2 D2 为唯一定义处（无 D2 详设文档）且自包含：非第二事件源、物理因果列、PG-only、sequence_number+idempotency_key、可重建；精确投影契约以稳定义务形式在 §6#2 挂 D2 批，无跨文档矛盾。 |
| 路由注册表+有效快照(含 TD-4 初始表) | satisfied | D0 §4 单处定义 ModelRouteSpec+初始两行；plan §5 TD-4 摘要（gpt-5.6-sol/chat_completions/fail_closed/draft）与表逐字一致并回指 D0 §4；快照契约在 D0 §2.3 声明为共享契约、D3 §4c/§6 同名引用。 |
| scripted 回放转写治理 | satisfied | D0 §2.8 单处定义（canonical request hash 含租户、tenant-bound 转写、synthetic fixture 例外、脱敏管线）；plan §4「语义结果等价而非事件逐帧等价」与 D0 §2.4 等价测试表述一致；D3 §9 仅引用 scripted 转写。 |
| runtime_namespace/provider_mode 隔离本身 | satisfied | D3 §4b 与 D0 §2.5 同词汇同规则（不可变 runtime_namespace+provider_mode 全链携带、非 live 证据 diagnostic-only、scripted/simulate 结构上不入 live 授权路径、跨模式污染 preflight 进 contract lane），并同引 RUNTIME_ENVIRONMENT_ISOLATION 契约。 |

## 10-运行时/模式隔离（satisfied 14/20）

| 机制 | 状态 | 证据/缺口 |
|---|---|---|
| company_identity_verifications 表 | satisfied | D3 §4b：唯一键 (runtime_namespace, workspace_id, company_fingerprint)+provider_mode 不可变列；非 live 证据 diagnostic-only，模式不匹配即 CAS 拒，结构上不可能产生 live shadow/verified 行；跨模式污染 preflight 进 contract lane。 |
| verification_intent(+epoch/claim gen/phase gen/awaiting_budget) | obligation **OB-10.1** | D3 §4c intent 行身份仅 (workspace_id, intent_id)，未显式列 runtime_namespace/provider_mode 不可变列；隔离仅经 §4b 全链 blanket 与 namespaced record 幂等键传递——实施批应把两列落为 intent 行显式列并进其 CAS 谓词。 |
| identity_search_budget_grant | obligation **OB-10.2** | D3 §2.1 grant 键 =(workspace_id, review_session_id, intent generation, policy_revision)+签发身份，无 runtime_namespace/provider_mode 列；pre-transport CAS 经命令/epoch/attempt 间接隔离——实施批应加显式模式列防非 live grant 资助 live 检索。 |
| plan_review.identity_result.apply(watermark) | satisfied | D3 §2.2（v8 修 R7#2）：幂等键 apply:<runtime_namespace>:<workspace_id>:<session_id>:<源域事件 id>；namespace+provider_mode 绑入 apply 的事件/命令身份/幂等 scope/gate 行与 CAS 每一条（含 expiry/supersession/human 路径），跨模式污染 preflight 覆盖 apply。 |
| company.identity.verification.record(三分支 CAS) | satisfied | D3 §2.2：幂等键 record:<runtime_namespace>:<workspace_id>:<intent_id>:<phase_generation>（v8 补 namespace/phase）；§4b：每条接受 CAS 携带不可变 namespace+provider_mode，模式不匹配即拒，非 live 结果到不了 live 授权态。 |
| human_transition_pending 态与人工决定编舞 | satisfied | D3 §5 编舞收敛于 apply 命令与 gate 行，而 §2.2 v8 明文 namespace+provider_mode 绑入 gate 行与 CAS 每一条『含 human 路径』；supersession 写 namespaced 验证行（§4b 键含 runtime_namespace），人工决定不跨命名空间生效。 |
| expiry 定时/expire 命令 | satisfied | D3 §2.2 v8：expiry 路径显式列入 namespace 绑定的 apply CAS（『含 expiry/supersession/human 路径』）；expire 命令的目标验证行键含 runtime_namespace（§4b），定时事件携带 generation、触发时 CAS 于当前 generation——跨模式与旧定时器双重被挡。 |
| 迁移桥(文件注册表+快照) | satisfied | D3 §4a：backfill 行恒 needs_human 永不 verified/accepted；无 workspace 映射的 bridge 命中 = diagnostic-only needs_human，不得授权身份/gate 清除/付费分支抑制；precedence preflight——legacy 全局产物结构上无法进入任何模式的授权路径。 |
| Tier-1/Tier-2 命令+判别化续跑编舞 | satisfied | D3 §4b：operation/command/attempt/信封/证据 bundle/幂等 scope/接受 CAS 全链携带不可变 namespace+provider_mode；续跑子命令经 namespaced record 幂等键（§2.2）收口；Tier-2 transport 前 CAS（§2.1）+ D0 §2.7 逐次 live 门覆盖付费调用。 |
| 证据 bundle+adjudication manifest(预创建+终态化) | satisfied | D3 §4b 全链明文含『证据 bundle』携带不可变 namespace+provider_mode；manifest 键 = workspace/intent/phase generation（§4c），其聚合 hash 只经 namespaced record CAS 消费——非 live bundle/manifest 无法喂进 live 授权分支。 |
| ModelInvocationEnvelope | satisfied | D0 §2.5（R6#2）：『信封/槽/journal/action 携带不可变 runtime_namespace + provider_mode，scripted/simulate 结果不可流入 live 命名空间的任何授权路径』；D3 §6 同一物理 schema 一处定义，经两侧接受 CAS 全链绑定。 |
| 成本台账(预留+暴露行) | obligation **OB-10.3** | D0 §2.3 台账行键 =(reservation, activity_attempt, physical_call_index)，未声明 runtime_namespace/provider_mode 列；simulate/scripted 调用与 live 预留/对账的分账口径未明——实施批（live 付费路径落地前台账必须在位）应补模式列与对账隔离。 |
| 逻辑结果槽(+outcome digest/occurrence/消费 CAS) | satisfied | D0 §2.5：槽明文列入『信封/槽/journal/action 携带不可变 runtime_namespace + provider_mode』；槽 workspace-scoped，接受/消费 CAS 全条件含 control epoch+pins，scripted/simulate 结果不可流入 live 命名空间授权路径。 |
| ModelTurnExecutionContext | obligation **OB-10.4** | D0 §2.3 字段清单（workspace/actor/permission/route/revisions/因果 id/idempotency_key）不含 runtime_namespace/provider_mode；其铸造的 idempotency_key 与 §2.8 request hash 亦未列该两维——下游 durable 产物靠环境隐式取值，实施批应显式入字段与 hash。 |
| ActionRequestSpec+schema pins | satisfied | spec 为 checked-in 静态契约、无模式态；pin 落点 = AgentAction/OperationRun 不可变列（D0 §3.1），而 action/journal 行按 §2.5 携带不可变 runtime_namespace+provider_mode——pin 所在 durable 对象已被命名空间隔离覆盖。 |
| agent tool registry/agent_tool_enabled(5 条件) | na | registry 为静态 checked-in 投影/只读 serve（D0 §3.2-3.3），自身无 per-mode durable 行或接受 CAS；simulate dispatch preflight 是 contract-lane 守卫非运行时授权，运行时隔离由 AgentAction/槽/信封（§2.5）承担。 |
| agent_events 投影 | obligation **OB-10.5** | 计划 §2 D2 声明 workspace 租户列+sequence+idempotency_key+可重建，但未声明行/幂等 scope 的 runtime_namespace/provider_mode；精确投影契约本就是 D2 批义务（计划 §6 义务 2），应把两列并入 stream 身份公式与 rebuild 语义。 |
| 路由注册表+有效快照(含 TD-4 初始表) | satisfied | D0 §4：每条 route 显式 simulate_mapping（simulate/scripted 替身实现）；初始表 rollout_state 全 draft、live 启用逐条 owner 拨动；§0/§2.7 live 一律 fail-closed 且 transport 前逐次过 assert_live_provider_access_allowed——模式路径在路由层即分叉。 |
| scripted 回放转写治理 | satisfied | D0 §2.8（v7 修 R6#10）：canonical request hash 含 workspace_id/actor_id/permission_scope/transcript_digest；live 转写 tenant-bound 跨租户不可回放，仅显式 synthetic fixture 可跨租户（接受时校验信封租户等式）；真实数据禁作入库 fixture。 |
| runtime_namespace/provider_mode 隔离本身 | satisfied | 机制健全：D3 §4b（不可变两列+全链携带+模式失配 CAS 拒+Phase-2 只认 live 证据+跨模式 preflight 进 lane）+ D0 §2.5（信封/槽/journal/action）+ §2.7（低层门扩展到模型中继，含 RUNTIME_ENVIRONMENT_ISOLATION 契约文档同批更新）。 |
