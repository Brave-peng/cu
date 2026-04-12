# cu

铜相关数据拉取与建模实验项目。

当前状态：

- 仓库仍处于初始化阶段
- 已有 `market_daily` 的 SQLModel 表模型、schema、SQLite 入库服务
- 已有 Typer 抓取命令，可从 AKShare 拉取 SHFE 日线并默认写入本地 SQLite
- `FastAPI` / `Alembic` 项目骨架尚未落地

阶段一规划：

- 语言：Python 3.11+
- 包管理：uv
- Web/API：FastAPI
- CLI：Typer
- ORM：SQLModel
- 开发期数据库：SQLite

抓取入库：

```bash
uv run python -m app.cli.market_crawler --start-date 2026-03-28
```

默认数据库为 `sqlite:///data/cu.db`。如只想打印样例、不写库：

```bash
uv run python -m app.cli.market_crawler --start-date 2026-03-28 --no-save
```

文档：

- 阶段一 `market_daily` 字段定义：[docs/market-daily-stage1.md](docs/market-daily-stage1.md)
- 阶段一架构与 API 探针方案：[docs/stage1-architecture.md](docs/stage1-architecture.md)
- 字段来源飞书文档：<https://jcng9kabbb3s.feishu.cn/docx/Q6ted4WxJoRfBQx1Tdlcdyh0nId>
- 开发规范：[claude.md](claude.md)

Git Hook：

- 首次 clone 后执行 `powershell -NoProfile -ExecutionPolicy Bypass -File scripts/setup-git-hooks.ps1`
