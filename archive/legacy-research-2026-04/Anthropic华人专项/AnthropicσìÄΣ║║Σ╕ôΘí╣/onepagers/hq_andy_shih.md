# Andy Shih · Member of Technical Staff · Anthropic

> 数据来源：LinkedIn / 个人主页 andyshih.com / Google Scholar｜更新：2026-03-31

---

## 人物简介

![Andy Shih](https://raw.githubusercontent.com/kasuganosora1874/one-pager-photos/main/andy-shih.jpg)

**Andy Shih**（推测约 30 岁），Anthropic Member of Technical Staff，2024 年 4 月加入。斯坦福大学计算机科学博士（2024 年毕业），师从 Stefano Ermon 和 Dorsa Sadigh，UCLA 本科 & 硕士。他的研究横跨**扩散模型（Diffusion Models）采样加速**与**序列模型推理**两大方向——代表作"Parallel Sampling of Diffusion Models"（NeurIPS 2023 Spotlight）提出并行采样框架，大幅提升扩散模型的推理速度，在生成式 AI 的效率优化领域具有较高影响力。本科和硕士期间在 UCLA 自动推理组（Automated Reasoning Group）深耕形式化推理，为后来研究 LLM 推理打下基础。

---

## 履历介绍

### 工作履历

- **【2024.04–至今】Anthropic** | Member of Technical Staff
  - 具体项目未公开，推测方向：Claude 的推理能力研究，以及生成模型的采样效率优化
  - 博士毕业后直接加入，是 Anthropic 在扩散模型与推理交叉方向的重要技术人员

- **【2019–2024】Stanford University SAIL** | 博士研究员
  - 导师：Stefano Ermon（生成模型权威）、Dorsa Sadigh（机器人学习）
  - 核心成果：并行扩散模型采样（NeurIPS 2023 Spotlight）、长时间尺度温度缩放（ICML 2023）

- **【UCLA 本科 & 硕士期间】UCLA 自动推理组** | 研究助理
  - 导师：Arthur Choi、Adnan Darwiche
  - 研究形式化推理与知识表示（Any-Order Autoregressive Models，NeurIPS 2022）

### 学业背景

- **斯坦福大学（Stanford University）** | 博士，计算机科学｜2019–2024
  - 所在实验室：Stanford AI Lab（SAIL）& Stanford ML Group
  - 导师：Stefano Ermon + Dorsa Sadigh（联合指导）
- **加州大学洛杉矶分校（UCLA）** | 本科 + 硕士，计算机科学

### 个人风格

- **以效率为切入点的系统级研究者**：扩散模型领域的主流工作长期聚焦于生成质量，Andy 选择从采样并行化切入——这不是热点赛道，而是一个对产业落地至关重要、但理论难度被低估的方向。并行采样方案需要同时处理马尔可夫链的时序依赖与并行计算调度，本质上是在概率模型和系统优化之间搭桥，这正是他形式化推理背景所赋予的独特切入角。

- **形式化推理背景对深度学习研究的实质性迁移**：UCLA 自动推理组的训练（SAT、知识编译、Any-Order 自回归建模）强调模型结构的可控性与可验证性。这套思维在扩散模型采样和 LLM 推理研究中均有直接体现——他的论文倾向于给出理论保证或严格的边界分析，而非单纯的经验性 benchmark 刷分，这在当前 LLM 研究风气中属于稀缺品质。

---

## 社交媒体

- 🌐 个人主页：[andyshih.com](https://www.andyshih.com/)
- 🐦 X：[@andyshih_](https://twitter.com/andyshih_)
- 🔗 LinkedIn：[linkedin.com/in/andy-shih](https://www.linkedin.com/in/andy-shih/)
- 💻 GitHub：[github.com/andyshih12](https://github.com/andyshih12)
- 📚 Google Scholar：[scholar.google.com/citations?user=G85kxUUAAAAJ](https://scholar.google.com/citations?hl=en&user=G85kxUUAAAAJ)

---

## 学术成果

**研究方向**：扩散模型 / 生成式模型采样 / 序列建模 / LLM 推理

| 论文 | 发表 | 核心贡献 |
|---|---|---|
| [Parallel Sampling of Diffusion Models](https://arxiv.org/abs/2305.16317) | NeurIPS 2023 **Spotlight** | **第一作者**。并行化扩散模型的反向去噪过程，大幅提升采样速度 |
| [Long Horizon Temperature Scaling](https://arxiv.org/abs/2302.03686) | ICML 2023 | **第一作者**。针对长时间序列预测的温度缩放方法，改善自回归模型的长程生成质量 |
| [Training and Inference on Any-Order Autoregressive Models the Right Way](https://arxiv.org/abs/2205.13554) | NeurIPS 2022 | **第一作者**。任意顺序自回归模型的统一训练与推理框架 |

---

*本 one-pager 由 OpenClaw AI 生成，数据来源于 andyshih.com、LinkedIn、Google Scholar 等公开信息。*
