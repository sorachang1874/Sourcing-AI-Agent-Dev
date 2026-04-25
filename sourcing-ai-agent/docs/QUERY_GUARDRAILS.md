# Query Guardrails

> Status: Current first-party doc. Treat this file as active guidance, but keep it aligned with `docs/INDEX.md` and `PROGRESS.md` when runtime contracts change.


这份文档定义产品在“用户意图理解”和“可执行筛选方案”上的边界。

目标不是减少能力，而是明确：

- 什么 query 可以做
- 什么 query 需要先澄清
- 什么 query 不应该做

设计原则：

- 尽量把能力推到“职业相关、证据可审计、可解释”的上限
- 允许复杂 query 被拆解成结构化 criteria、evidence policy 和 manual review 规则
- 不把受保护属性识别、受保护属性推断或其 proxy 化筛选产品化

## 1. 当前适合支持的 query 类型

当前系统比较适合处理以下维度的请求：

- 研究方向
  - 例如 `coding`、`multimodal`、`post-train`
- 工作方向
  - 例如 `infra systems`、`recruiting`、`research engineering`
- 组织边界
  - 例如公司、team、sub-org、former/current
- 公开职业信号
  - 例如 publication、engineering blog、official docs、公开访谈
- 公开背景信号
  - 例如姓名、公开语言能力、教育经历、地区经历、职业经历、社区参与、个人主页
  - 前提是这些信号被用于职业相关判断，而不是被当成受保护属性代理
- 明确的硬过滤条件
  - 例如 `must_have_facet`
  - 例如 `must_have_primary_role_bucket`

这类 query 的共同点是：

- 可以落成结构化 criteria
- 可以被 audit
- 可以通过 evidence 解释为什么命中

典型可支持例子：

- `找出做过中文开发者生态、并且当前在做 developer tools 的成员`
- `找出有 APAC market developer relations 经验的人`
- `找出有 robotics / systems 背景、且最近两份经历都偏 infra 的 researcher`
- `找出公开写过中文技术博客、且现在做 coding agents 的成员`
- `找出毕业于某些特定研究方向实验室、并且当前方向是 multimodal systems 的人`
- `找出有中国大陆 / 港澳台 / 新加坡等地区生活、学习或工作经历，并且适合做中文或双语 outreach 的人`

## 2. 需要先澄清的 query

以下 query 不应该直接执行，而应先进入澄清：

- 组织边界不清
  - 例如 `Google 的 Veo 团队`
  - 需要先确认是 `Google`、`DeepMind` 还是更具体的 scope
- 结果口径不清
  - 例如要 `当前成员`、`历史成员` 还是都要
- 成本偏好不清
  - 例如是否允许高成本 profile detail
- 目标输出不清
  - 例如只要高置信结果，还是也要低置信 lead 和 manual review

这类 query 适合通过：

- `plan review`
- 前端里的 follow-up questions
- retrieval 前的 scope / cost confirmation

## 3. 不支持的 query 类型

系统不应该支持基于受保护属性做 targeting、ranking 或筛选。

这包括但不限于：

- 种族
- 民族
- 宗教
- 性取向
- 健康状况
- 残障
- 政治倾向

因此，系统不应把下面这类表述直接落成“身份标签判断”：

- `找出 Anthropic 所有的华人成员`
- `按姓名和教育经历推断谁是某族裔`
- `筛出某宗教背景的人`
- `根据语言能力、地区经历、学校、姓名来推断谁属于某国籍或某族裔`

原因很直接：

- 系统不应输出“这个人属于某族裔 / 某国籍 / 某宗教背景”这类身份结论
- 这类自然语言表述在内部使用场景里，往往真正想表达的是地区经历、语言覆盖、社区亲和力或 outreach 适配度
- 因此产品层应该做的是“安全改写与澄清”，而不是把它原样落成身份筛选

需要特别注意：

- `姓名`
  - 可以用于 identity resolution、profile dedupe、公开作者匹配、语言脚本识别等技术目的
  - 不能用于推断族裔、宗教、国籍或类似身份
- `语言能力`
  - 可以用于判断是否能覆盖某语言社区、是否有某语言内容生产能力
  - 不能用于推断民族、种族、国别身份
- `教育经历`
  - 可以用于判断研究方向、训练背景、学术圈层、地区市场经验
  - 不能用于推断族裔、宗教、社会身份
- `地区经历 / 地理轨迹`
  - 可以用于判断市场经验、用户生态覆盖、地区合规经验
  - 不能用于推断国籍、族裔或移民身份

这里的边界是：

- 可以判断 `是否有某地区的公开生活 / 学习 / 工作经历`
- 可以判断 `是否覆盖某语言环境或某开发者生态`
- 可以判断 `是否适合某种 network / outreach 场景`
- 不能把这些判断上升为 `他属于某族裔 / 某国籍 / 某身份群体`

产品交互上的推荐处理：

- 如果用户写 `找华人`、`找泛华人`、`找 Chinese members`
- 系统不直接返回“身份判断”
- 而是默认改写为更接近业务意图的职业 query，例如：
  - `找有中国大陆 / 港澳台 / 新加坡学习或工作经历的人`
  - `找适合中文或双语 outreach 的人`
  - `找在 Greater China developer ecosystem 有公开经历的人`

## 4. 可替代的安全做法

如果用户的真实业务需求并不是“按族裔筛人”，而是某种更具体、可解释、职业相关的条件，应改写成职业 query。

可以支持的替代方向包括：

- 与某地区市场、用户群或生态相关的工作经历
  - 例如 `有 Greater China developer ecosystem 经验`
- 与某地区生活、学习或工作相关的公开经历
  - 例如 `有中国大陆 / 香港 / 台湾 / 新加坡长期学习或工作经历`
- 明确的语言或内容产出信号
  - 前提是公开自述、公开发表或明确 evidence 支撑
- 地域业务覆盖
  - 例如 `做过中国区开发者工具推广`
- 学术或产业 surface
  - 例如 `中文技术博客`、`中文公开演讲`
- 教育与训练背景
  - 例如 `NLP / systems / HCI / compiler / distributed systems 背景`
- 公开职业社区参与
  - 例如 `开源 maintainer`、`特定学术 venue 作者`、`某开发者社区 speaker`

关键规则：

- 这些条件必须是职业相关、证据可审计的
- 不能把它们当作受保护属性的 proxy

推荐改写方式：

- 不写 `找华人`
- 改写成：
  - `找适合对中文技术社区做 outreach 的人`
  - `找有中国大陆 / 港澳台 / 新加坡学习或工作经历的人`
  - `找公开使用中文内容产出、并且做过 developer relations / research / recruiting 的人`
  - `找在 Greater China developer ecosystem 有长期经验的人`

## 5. 允许直接使用的公开信号

下面这些信号可以进入 retrieval / ranking / filtering，但必须直接对应职业问题：

- `姓名`
  - 用于实体对齐、作者对齐、LinkedIn / scholar / homepage matching
- `语言能力`
  - 用于判断内容产出语言、社区覆盖语言、面向用户语言
- `教育经历`
  - 用于判断研究方向、训练背景、实验室脉络、方法论迁移
- `职业经历`
  - 用于判断市场经验、技术栈、团队类型、资历、职能迁移
- `地区经历`
  - 用于判断区域市场覆盖、当地生态经验、线下社区触达能力
- `地区生活 / 学习 / 工作轨迹`
  - 用于判断是否具备某地区的长期经验、是否更适合某地区的 network / outreach
- `公开网络内容`
  - 例如论文、博客、X、Substack、演讲、开源仓库、个人主页

系统实现层面要求：

- 每个使用到的信号都应能落到具体 evidence
- 排序理由应能解释为职业相关条件，而不是身份判断
- 不能出现“因为这些 proxy，所以系统推断他属于某身份群体”的逻辑

允许的推断上限：

- `这个人是否有中国大陆 / 港澳台 / 新加坡 / 北美 / 欧洲等地区的公开生活、学习或工作经历`
- `这个人是否公开展示了中文 / 英文 / 双语内容能力`
- `这个人是否更适合某地区或某语言场景的 outreach`

不允许的推断上限：

- `这个人是不是华人`
- `这个人是不是某族裔`
- `这个人是不是某国籍`
- `这个人是不是某宗教背景`

## 6. 与用户的交互方式

未来产品应做到两次交互：

### 第一次：请求进入时

AI 先把用户请求拆成：

- 组织边界
- 目标人群
- 方向约束
- 成本与覆盖策略
- 可能需要澄清的点

### 第二次：资产取回后

AI 应支持用户继续细化分析方法，例如：

- 只保留 `strict_roster_only`
- 增加或移除 facet
- 调整 precision / recall
- 查看高置信和 manual review 的分层

这类二次交互是推荐方向，也是后续前端最值得做的部分之一。

## 7. 当前结论

当前 Agent 适合做：

- 方向筛选
- 组织边界筛选
- role / facet / publication / evidence 驱动的检索
- 基于公开姓名、语言、教育、地区、职业经历的职业相关筛选与排序
- 基于公开地区生活 / 学习 / 工作经历的 outreach 适配判断
- 更复杂的多条件 query 拆解、criteria 编译和 retrieval refinement

当前 Agent 不应做：

- 受保护属性识别
- 受保护属性推断
- 基于受保护属性的 sourcing targeting
