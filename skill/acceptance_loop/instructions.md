# Acceptance Loop

当用户希望按稳定、可追踪的执行闭环推进任务，而不是临时式改代码时，使用这个 skill。

核心前提：

- 用户能提供清晰的验收标准

如果验收标准不够清楚，先把它改写成机器可检查的规则，再进入循环。
写 `acceptance.json.checks` 时，用 `references/check-key-mapping.md` 把用户语言归一成稳定的 check key。

## 开始对齐

进入执行循环前，先用紧凑 intake 模板把 case 说清楚。

如果用户还没提供足够上下文，先补齐这些最小输入：

- background
- target task
- acceptance standards
- runtime location
- boundaries or safety constraints

使用内置模板：

- `assets/case_intake.template.md`

在 case 不足以定义下面 3 件事之前，不要开始实现：

- one task
- one target
- one acceptance checklist

## 核心规则

只允许按这个顺序推进：

1. define one task
2. implement the smallest change
3. run the target command or test
4. write structured acceptance result
5. if failed, debug and retry
6. if passed, record progress and commit

不要跳过 acceptance。
不要把失败的工作当作已完成进度提交。
不要把多个无关任务混进同一个 loop。

## 工作单元

每个 loop 只处理一个 task、一个 target。

例子：

- one crawler source
- one parser fix
- one acceptance script
- one retry/backoff fix
- one deployment check

## 必需产物

每轮 loop 都必须在 run 目录留下这些文件：

- `task.json`
- `crawl_result.json` 或同类 task-specific result file
- `acceptance.json`
- `debug.log`

如果任务不是 crawler，可以改结果文件名，但 `task.json`、`acceptance.json`、`debug.log` 必须保留。

创建首版文件时，优先复用这些模板：

- `assets/case_intake.template.md`
- `assets/task.template.json`
- `assets/acceptance.template.json`
- `assets/debug.template.log`
- `assets/case_acceptance_checklist.template.md`

## task.json 格式

必须包含：

- `task_id`
- `target_name`
- `task_type`
- `goal`
- `inputs`
- `done_when`
- `retry_limit`
- `created_at`

`done_when` 必须机器可检查，优先使用短条件：

- `acceptance overall_result = pass`
- `content_length > 0`
- `exit_code = 0`

## acceptance.json 格式

必须包含：

- `target_name`
- `task_id`
- `checks`
- `overall_result`
- `generated_at`

规则：

- 每个 check 的值只能是 `pass` 或 `fail`
- 只有所有 required check 都是 `pass` 时，`overall_result` 才能是 `pass`
- 不要用散文总结替代结构化 checks

## Debug Loop

当 acceptance 失败时：

1. identify the failed check
2. state the current suspected cause
3. patch the smallest relevant code or config
4. rerun only the necessary command
5. rewrite `acceptance.json`
6. append to `debug.log`

遇到下面情况就停止自动循环：

- `retry_limit` reached
- failure is environmental and cannot be fixed safely in code
- acceptance standard conflicts with current system design

## Commit Rule

只有 acceptance 通过后才能 commit。

提交时同时记录：

- task id
- target name
- acceptance result path
- short change summary

建议 commit 风格：

- `feat(scope): complete <task>`
- `fix(scope): repair <failed check>`

## 稳定性规则

- 一次只做一个 task
- 优先机器可检查的 acceptance
- retry 次数必须有限
- 所有失败都要能在日志里追溯
- 源码和 run artifacts 分离
- 把 acceptance 文件视为唯一 pass/fail 真相

## Local / Server 拆分

如果项目同时有本地开发和远端服务器：

- 默认在本地写代码
- 真实网络验证和最终 acceptance 放到服务器跑
- run artifacts 存服务器
- 代码目录和运行数据目录分开

## 最小启动步骤

1. read the acceptance standard
2. normalize it into check keys if needed
3. define the current task
4. identify the exact command to run
5. execute the task
6. write acceptance result
7. continue or stop based on `overall_result`

## 进度输出纪律

向用户汇报进度时，保持紧凑结构，只说：

- current task
- latest acceptance result
- latest debug finding if failed
- latest commit if passed

不要说模糊状态，例如 “almost done” 或 “looks good”。

## 用户验收交接

每次 loop 完成后，都要告诉用户怎么手工验证。

不能只依赖 `acceptance.json`。

交接内容必须包含：

- 用户应直接检查什么
- 应看哪些文件、命令、页面或输出
- 本轮 acceptance 的核心关注点
- 什么结果算 pass
- 哪些情况下机器结果可能误导

优先给一个短 checklist：

1. primary artifact to inspect
2. one or more direct commands or paths
3. exact pass condition in plain language
