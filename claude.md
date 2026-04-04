# 开发规范

当前仓库仍处于初始化阶段。以下内容默认解释为阶段一目标规范和落地约定；尚未实现的目录、依赖和命令，在项目骨架建立后按此执行。

## 阶段一目标技术栈

- **Web/API**: `FastAPI`
- **CLI**: `Typer`
- **包管理**: `uv`
- **ORM**: `SQLModel`
- **数据库迁移**: `Alembic`
- **开发期数据库**: `SQLite`
- **配置管理**: `pydantic-settings` + `.env`
- **数据校验**: `Pydantic`
- **日志**: `loguru`
- **测试**: `pytest`
- **代码检查**: `ruff`

## 目标项目结构

- 项目骨架建立后，接口层放在 `app/api/`
- 配置放在 `app/core/`
- 数据库连接与会话放在 `app/db/`
- SQLModel 模型放在 `app/models/`
- 请求/响应 schema 放在 `app/schemas/`
- 业务逻辑放在 `app/services/`
- 外部系统接入放在 `app/clients/` 或 `app/integrations/`
- 测试放在 `tests/`

## Python 代码规范

- Python 版本基线使用 `3.11+`
- 所有公开函数、方法、类属性必须写类型注解
- 禁止使用 `print`，统一走日志
- 模块职责保持单一，避免循环依赖
- 函数尽量短小，复杂逻辑拆到 service 层
- 常量统一集中定义，避免魔法值散落
- 新增配置项时，默认提供合理默认值和清晰注释

## FastAPI 约定

- 路由层只处理参数解析、鉴权、响应模型和 HTTP 状态码
- 业务逻辑不写在路由函数里，统一放到 `services/`
- 请求模型和响应模型使用 `Pydantic` schema，避免直接把表模型暴露给外部接口
- 每个请求使用独立数据库 session，不跨请求共享 session
- API 路由按领域拆分，不把所有接口堆到一个文件

## 数据库规范

- ORM 统一使用 `SQLModel`
- 需要复杂查询或底层能力时，允许下探到底层 `SQLAlchemy`
- 表结构变更必须通过 `Alembic` 管理，不手改线上或共享库结构
- 开发阶段使用 `SQLite`，数据库文件不提交到 Git
- SQLite 默认开启外键约束；需要并发读写时再评估 `WAL`
- 主键、唯一约束、索引显式声明，不依赖隐式行为
- 金额、价格、数量字段在入库前做显式类型转换和校验
- `date` / `datetime` 字段统一使用 ISO 格式或明确的 Python 时间类型

## 错误处理

- service / db 层发现异常时可以直接 `raise`
- API 入口层负责把业务异常转换成明确的 HTTP 响应
- CLI 入口层负责记录错误并返回非零退出码
- 不静默吞异常，不写裸 `except:`

## 配置规范

- 工具配置放在 `pyproject.toml`
- 运行时配置通过 `pydantic-settings` 管理
- 密钥、token、账号密码放在 `.env`
- `.env`、本地数据库、临时导出文件必须加入 `.gitignore`

## 测试与质量

- 单元测试使用 `pytest`
- 新增 service、parser、db 逻辑时，优先补对应测试
- 关键 API 至少覆盖基本的成功路径和失败路径
- 在相关工具接入后，提交前至少运行 `ruff check` 和 `pytest`

## Git 规范

- **Git 提交历史**: 尽量保持线性，合并前优先 `rebase`，避免无意义 merge commit
- **Git commit 粒度**: 尽量精简，一个 commit 只表达一个完整意图，避免把无关修改混在一起
- **Git commit 信息**: 简短清晰，优先使用 `feat:`、`fix:`、`refactor:`、`docs:`、`chore:` 这类前缀
- **提交前审查**: 默认执行仓库 pre-commit hook，在提交前调用 Codex agent 结合 review skill 审查 staged changes
- **Hook 安装**: 新 clone 的仓库需要先执行 `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/setup-git-hooks.ps1`
- **跳过方式**: 仅在紧急场景下允许使用 `git commit --no-verify` 或设置 `CU_SKIP_CODEX_REVIEW=1`
