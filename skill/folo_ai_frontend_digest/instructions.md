# Folo AI FrontEnd Digest

当用户要从 Folo 分类 `AI FrontEnd` 生成 AI 日报时，使用这个 skill。

核心要求：

- 用脚本做确定性抓取、过滤、补全
- 只把论文最终选题和中文洞察留给模型判断
- markdown 准备好之后再发送到飞书

## 工作流

1. 运行 `scripts/build_digest.py`：
   - 抓取指定 Folo 分类
   - 翻页获取过去 24 小时条目
   - 拆分 News / Papers
   - 对 arXiv 论文应用固定过滤规则
   - 拉取 arXiv 摘要
2. 读取输出目录里的 JSON 和 markdown scaffold。
3. 从过滤后的论文池里选择：
   - `5` 篇 `new`
   - `5` 篇 `classic`
4. 写出最终 markdown 日报。
5. 如果用户要求发送，运行 `scripts/send_lark_markdown.py` 发到私聊或群聊。

## 固定规则

这些规则已经由构建脚本实现：

- 时间窗口：过去 `24h`
- 论文源：`cs.AI updates on arXiv.org`
- 允许分类：`cs.LG`、`cs.CL`、`cs.SE`
- 黑名单关键词：
  - `clinical`
  - `psychiatric`
  - `lung cancer`
  - `biomechanical`
  - `traffic`
  - `driving`
  - `emboli`
  - `field medicine`
  - `legal`
  - `graph`

除非用户明确改规则，否则不要手工重写这些过滤逻辑。

## 输出约定

最终日报必须包含：

### 1. `News`

- 保留所有非论文条目
- 每条包含：
  - `title`
  - `Original Content in English`
  - `Original Link`
- 不要输出窗口统计、计数、候选数量等内部信息
- 所有非论文源统一走 `News` 格式
- 编号从 `1` 开始

### 2. `Paper - New Signals`

- 可用时精确输出 `5` 篇
- 每条包含：
  - `title`
  - `Original Content in English`
  - `Insights in Chinese`
  - `Original Link`
- 最终稿里不要单独加 abstract 字段

### 3. `Paper - Core Themes`

- 可用时精确输出 `5` 篇
- 每条包含：
  - `title`
  - `Original Content in English`
  - `Insights in Chinese`
  - `Original Link`
- 如果最终合并成单个 `Paper` 段落，编号从 `1` 开始

如果某个桶不足 `5` 篇，只能在用户允许时从另一个桶补位；否则明确说明缺口。

## 选题指引

把脚本标签当起点，不当作最终答案。

默认归入 `classic` 的主题：

- agent
- multi-agent
- reasoning
- RAG
- retrieval
- memory
- coding agents
- safety
- alignment
- efficiency

更适合归入 `new` 的情况：

- 新产品形态
- 新交互界面或模态
- 新执行边界
- 新工作流模式
- 新部署路径或 UX 含义

优先选择能支撑“未来 AI 应用会怎么变”的论文。

## 发送

私聊测试时，若用户未指定目标用户或群聊，默认发给当前认证用户。

最终发送必须用 `scripts/send_lark_markdown.py`，原因：

- 仅在必要时用 `lark-cli` 解析当前用户
- 实际消息发送走飞书 OpenAPI
- 避免 Windows 下 `lark-cli --content` 把中文打成 `?`

示例：

```powershell
python scripts/send_lark_markdown.py --markdown-file <path>
python scripts/send_lark_markdown.py --markdown-file <path> --chat-id oc_xxx
```

## 资源

- `scripts/build_digest.py`：抓取、翻页、过滤、补全、生成 scaffold
- `scripts/send_lark_markdown.py`：发送 markdown 到飞书
- `references/selection-notes.md`：论文分桶和过滤规则说明
- `agents/openai.yaml`：当前 skill 的 OpenAI 调用元数据
