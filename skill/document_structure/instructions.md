# Document Structure

当任务是创建项目文档、把松散草稿改成高密度版本，或把混合型长文拆成主文档加子文档时，使用这个 skill。

核心规则：

- 主文档必须可独立阅读
- 每节先直接总结关键内容
- 需要时每节只放一个强支撑工件：表格、流程图、样例数据、关键代码
- 需要图时优先用 `mermaid`
- 子文档只做展开，不替代主文档内容

## 项目参考索引

开始起草前，先读项目参考索引：

- [文档规范索引](https://www.feishu.cn/docx/UwTWd6vHlo9lPMxmQB9cSjWvnUd)

用索引决定当前任务该遵循哪些项目参考文档。
不要凭记忆硬写项目命名规则。
特别是这些固定命名，若项目参考里有定义，必须直接使用：

- module names
- object names
- pipeline names
- output artifact names
- other fixed project terms

如果索引没覆盖当前主题，可以继续结构化处理，但要显式指出项目参考覆盖缺失。

## 先判断文档形态

先判断当前任务更适合哪种形态：

- 单个短文档
- 主文档 + 子文档
- 把已有长文重写成以上结构

当源文档同时混入两类及以上信息时，优先拆组：

- cognition
- design
- output specification
- execution
- assets

如果一个文档同时写背景、系统设计、任务追踪、字段 schema、代码，应该拆。

## 主文档写法

把主文档当作完整文档的压缩版，而不是导航页。

每节都按这个方式写：

1. 直接开门见山：`We are doing X, because of Y, and the key point is Z.`
2. 总结必须具体、高密度
3. 只在有帮助时放一个最小支撑工件
4. 如需展开，结尾加 `See:` 指向子文档

不要写这类空框架句：

- this section explains...
- this document is responsible for...
- the value of this section is...

要直接总结内容本身。

## 选择支撑工件

用最小但最有效的工件：

- 字段、角色、优先级、输出、对比：用表格
- 顺序流程、agent chain、依赖、决策流：用 `mermaid`
- 不要用 ASCII 图或伪代码块假装流程图
- schema、payload、执行样例需要可复制代码时：用短 JSON 或代码块
- 结构已经很明显时，才用短 bullet list

一个强工件优于多个弱工件。

## 按信息类型拆分

默认子文档类型：

- `cognition`
- `design`
- `output-spec`
- `execution`
- `assets`

不需要的类型不要硬建。

## 写作密度要求

优先暴露这些信息：

- explicit goals
- explicit reasons
- explicit key objects
- explicit next steps
- important fields
- important code path
- important libraries or tools
- important example rows
- important flow

避免：

- 文档管理语言
- 泛泛而谈的开场
- 重复解释子文档是干什么的
- 冗长软引导

## PRD 类硬约束

1. heading depth 不超过三级
2. 一个文档只解决一个主问题
3. 用表格、样例、规则、示例代码提升密度
4. 直接写对象、规则、要求
5. 标题硬、开头短
6. 不保留内部讨论痕迹
7. 同 schema 的字段定义或样例数据尽量合并成一张表
8. 需要图时直接用 `mermaid`

## 默认工作流

1. read the source document or prompt
2. read the project reference index
3. choose the relevant linked project references
4. identify the one main problem
5. mark mixed content by information type
6. decide one doc or a doc group
7. write or rewrite the main doc as a compressed readable version
8. create child docs only where needed
9. add `See:` links from the main doc
10. make sure the main doc still works alone

如果用户明确要求走 review workflow：

1. 先用这个 skill 写本地 markdown draft
2. self-check 一次
3. 把本地 draft 交给 `$document-review`
4. 根据 review findings 修订
5. 本地稿干净后再发布飞书版

## 质量标准

好结果必须满足：

- 只读主文档也能理解全局
- 每节都在说关键内容，不是在讲“本节做什么”
- 主文档里至少有一些具体支撑工件
- 子文档按信息类型展开
- 长文拆分是因为混合目的，不是因为字数

需要模板、拆分清单和固定 workflow 别名时，查看：

- `references/patterns.md`
- `references/workflow-aliases.md`
