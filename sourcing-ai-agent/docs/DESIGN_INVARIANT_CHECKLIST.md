# 设计不变量清单（对抗设计评审的前置自查协议）

> Status: Active engineering protocol（2026-07-13，起草 = Claude Fable 5，从 Track D 设计 v1-v5
> 五轮对抗评审的 80+ findings 实证蒸馏；owner 采纳后与 `INDEPENDENT_REVIEW_GATE.md` 配套使用；
> 经验待回写 ai-assisted-engineering-playbook）。

## 0. 要解决的问题（实证）

Track D 设计五轮评审（R1 24 → R2 16 → R3 17 → R4 阻断 6 → R5 阻断 5）呈现固定动力学：**每轮修复
N 层机制，评审即在 N+1 层产出新 findings**——修复引入的新机制（表/命令/CAS/槽/grant）自身又
缺围栏/生命周期/租户键，循环不在文档层收敛。复盘全部 findings 后的关键事实：**它们几乎全部是
下述不变量类对"新引入机制"的重复应用**。逐层下潜不是评审能力问题，而是评审自然聚焦于
"上一轮修复面"；解法是把全部不变量类做成**机制×不变量矩阵**在首轮评审前由作者自查穷举，
把 N+1、N+2 层的问题在第 0 轮暴露。

## 1. 十类设计不变量（v2 起为十类；每个新机制逐类过，机制清单本身随每版设计刷新——
新引入的状态/机制必须同批进矩阵，R7#6 教训）

对设计中**每一个新引入的机制**（durable 表/行、命令类型、CAS/围栏、结果槽、信封/grant、
投影、schema/pin）逐类回答「满足于§X / 不适用因为… / 开放为实施义务#N」：

1. **单写者与聚合所有权**：该状态恰有一个 owner 命令/模块写；跨聚合更新走
   事件→reducer→对方 owner 命令（reducer 永不直写域存储）；读者永不写、永不 enqueue
   （含"读到过期就顺手修"类隐性写）。
2. **租户键**：workspace/tenant 键在行身份、索引、**每一条 CAS 谓词**、幂等 scope、证据
   bundle、授权检查中（重写文档时最易静默丢失——R4#3 即 v4 重写回归）。
3. **世代与物理围栏**：接受/应用类写必须 CAS 于**存储的**（非随载荷携带的）generation +
   物理执行身份（claim generation/lease token、attempt id）；每种控制转移
   （cancel/retry/resume/timeout/rebuild/人工决定）都显式推进围栏，**且区分同步围栏与异步收敛
   ——控制转移已 commit、域侧 supersession 未落地的窗口内旧结果必须已被挡**；专查 **ABA 窗口**：
   requeue 之后、下次 claim 之前，旧结果是否仍能匹配旧围栏？依赖的物理列真的存在于
   pinned schema 吗（R4#2：claim generation 列并不存在）？**（v2 强化，R6#1 教训）新键/新命令
   必须对照 pinned 物理约束检查交互**：共享唯一约束（如 workflow_commands 的
   (workflow_run_id, idempotency_key)）下，两个不同命令/两次不同转移的键会不会互撞？
4. **生命周期完备性**：状态迁移表全量（含反向路径：过期、policy 失效、supersession、重验、
   "谁重开一个已被清除的阻塞"）；定时收敛有 owner（定时事件/扫描 owner，非读者驱动）；
   grant/信封类有不可变签发身份 + 终态不复活 + 余额继承规则。
5. **晚到与部分结果**：每个异步结果都可能在 cancel/supersession/timeout **之后**到达——落在哪
   （quarantine 显式终态）？"已接受未消费"窗口（accepted→persist 间被 cancel）有消费 CAS 吗？
   截断/过滤/异常终止的输出是否结构性不可授权？
6. **成本诚实性**：worst-case 预留 + **每次物理调用一笔**暴露记录
   （prepared→sent→confirmed/uncertain/no_call——crash 可区分未发送与发送未见结果）+ 终局
   对账；重跑的钱显式入账不假设未花费；扣减与授权 CAS 在 transport **之前**且同事务。
7. **物理身份绑定**：结果绑 provider call id + response/result digest + attempt + 有效配置快照
   （endpoint/timeout/定价/circuit，非仅逻辑路由版本）+ schema/policy revisions；pin 在 durable
   对象的**实际创建点**逐点持久化（submit/approve/retry-child 各自 copy+verify——先查清对象
   到底在哪一步才被创建，R4#12：审批型 action 的 run 在 approve 时才建）。
8. **Provenance 与信任边界**：人/机通道永不混淆（机器结果不得经人工字段回流）；模型输出永不
   自证 transport 元数据或证据 provenance（provider/usage/fallback/域名归属全部服务端派生）；
   公共 ingress 不可伪造服务端引用；出站 model-safe 白名单覆盖全部消息角色。
9. **自包含与跨文档一致**：无"同 vN"式历史引用（Git 历史不是契约）；跨文档共享的契约 =
   一个物理 schema 一处定义；术语/命令名/状态枚举/链条描述逐字一致；上层计划与详设同步修订。
10. **运行时/模式隔离**（v2 新增，R6#2 教训——首版清单完全缺失，而这是仓库
    RUNTIME_ENVIRONMENT_ISOLATION 契约的核心）：每个新 durable 行/幂等 scope/接受 CAS 是否携带
    不可变 `runtime_namespace` + `provider_mode`？simulate/scripted/回放产物是否**结构上不可能**
    进入 live 命名空间的授权路径（污染 = 测试证据变成生产决定）？跨模式污染 preflight 在吗？
    回放身份是否含租户（跨租户复用只允许显式合成 fixture）？

> 清单本身随每轮 findings 的类分布迭代：某类反复出现 → 该列自查失效，先补列再补文档
> （v1→v2：R6 暴露第 10 类缺失与第 3 类物理交互盲区）。

## 2. 使用协议

1. **作者侧（首轮评审前，硬前置）**：设计文档附「机制×不变量矩阵」（机制行 × **全部十列**，
   格内 §引用/N-A/**稳定义务 ID**——义务必须有可引用的编号并映射进计划义务清单，R7#9）；矩阵
   随每版设计**重生成**（修复后的过时格 = 评审可抓的事实错误，R7#8）；矩阵本身接受评审。
   用并行子代理自查：每类一个 agent 扫全部机制，格子由证据填充而非作者自评。
2. **评审侧（prompt 模板）**：要求 reviewer **checklist 驱动的单遍全深度扫描**（对每个机制逐
   9 类核对，而非自然逐层下潜）；同时给出校准：「实施批协议（Scout/characterize-first/
   per-batch gate）会解决的规格细节 = 实施义务清单项，非设计阻断；阻断 = 事实错误、契约违反、
   机制不健全、自相矛盾」。
3. **终止规则**：连续一轮的阻断集全部属于矩阵已标注的「实施义务」类 ⇒ 设计定型，走
   owner-accepted exception（引 artifact），剩余 findings 转实施批开工义务；出现新的事实/
   契约/所有权类错误 ⇒ 继续设计轮。
4. **度量**：记录每轮 findings 的不变量类分布——若某类反复出现，说明作者矩阵那一列自查失效，
   下轮先补该列。

## 3. 适用范围

分布式/并发/付费/多 owner 语义的设计输入文档（Track D 类）。纯重构迁移批（Track B 类）沿用
handbook §4 既有协议，不强制本清单。
