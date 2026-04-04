# cu

铜相关数据拉取与建模实验项目。

当前状态：

- 仓库仍处于初始化阶段
- 当前已沉淀的内容主要是开发规范和阶段一字段文档
- `FastAPI` / `SQLModel` / `Alembic` 项目骨架尚未落地

阶段一规划：

- 语言：Python 3.11+
- 包管理：uv
- Web/API：FastAPI
- CLI：Typer
- ORM：SQLModel
- 开发期数据库：SQLite

文档：

- 阶段一 `market_daily` 字段定义：[docs/market-daily-stage1.md](docs/market-daily-stage1.md)
- 阶段一架构与 API 探针方案：[docs/stage1-architecture.md](docs/stage1-architecture.md)
- 字段来源飞书文档：<https://jcng9kabbb3s.feishu.cn/docx/Q6ted4WxJoRfBQx1Tdlcdyh0nId>
- 开发规范：[claude.md](claude.md)

Git Hook：

- 首次 clone 后执行 `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/setup-git-hooks.ps1`
