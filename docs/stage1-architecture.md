# Stage 1 Architecture

## 当前情况

- `market_daily` 的字段定义、校验规则和交付标准已经在 [market-daily-stage1.md](market-daily-stage1.md) 固化
- 开发规范已经在 [../claude.md](../claude.md) 固化
- 当前仓库已经有 `SQLModel` 表模型、schema、SQLite engine 初始化和 `market_daily` 入库服务
- 当前仓库已经有 Typer 抓取命令，通过 AKShare `get_futures_daily` 拉取 SHFE 日线
- 当前仓库还没有 `FastAPI` / `Alembic` 项目骨架

结论：

- 当前最小链路已推进到：`AKShare 拉取 -> 字段映射 -> SQLite 落库`
- 下一步更适合补查询服务、查询 API 和基础校验，而不是扩散到复杂报表

## 阶段一目标

第一阶段只解决一件事：

把 `SHFE CU` 的日度行情拉下来，映射成 `market_daily`，并稳定写入本地 `SQLite`。

不要一开始就做复杂 Web 接口、报表和多数据源聚合。

## 建议架构

```text
app/
  api/
    routes/
      health.py
      market_daily.py
  cli/
    __init__.py
    market_crawler.py
  core/
    config.py
    logging.py
  db/
    session.py
  models/
    market_daily.py
  schemas/
    market_daily.py
  services/
    market_daily_ingest.py
    market_daily_query.py
  clients/
    market_data_provider.py
    shfe_client.py
  parsers/
    shfe_market_daily.py
tests/
  clients/
  parsers/
  services/
```

## 各层职责

### `clients/`

负责请求外部 API，只做：

- 拼请求
- 发请求
- 处理超时、状态码、重试
- 返回原始响应或轻度标准化响应

不要在这里直接写入数据库。

### `parsers/`

负责把外部响应解析成内部结构。

建议先输出一个中间对象，例如：

```python
{
    "date": "2026-03-28",
    "symbol": "CU",
    "contract": "CU2505",
    "open": 81120,
    "high": 81380,
    "low": 80940,
    "close": 81230,
    "settlement": 81190,
    "volume": 156789,
    "open_interest": 198765,
    "source": "SHFE",
    "fetched_at": "2026-03-28T15:35:10+08:00",
    "note": ""
}
```

### `services/`

负责业务编排：

- 调用 client 拉数据
- 调用 parser 做字段映射
- 做字段校验和去重
- 调用数据库写入

服务层是整个阶段一的核心。

### `models/`

定义 `SQLModel` 表模型，例如：

- 表名：`market_daily`
- 唯一键：`date + symbol + contract`

### `api/`

第一阶段 Web API 只保留极小范围：

- `GET /health`
- `GET /market-daily`

这里只做查询，不承担抓取逻辑。

### `cli/`

第一阶段建议优先做 CLI，因为抓取、补抓、探针测试都更适合命令行。

建议命令：

- `probe-market-api`
- `fetch-market-daily`
- `load-market-daily`
- `backfill-market-daily`

## 最小开发顺序

### 第一步：API 探针

目标不是一开始写完整抓取器，而是回答 4 个问题：

1. 这个 API 现在能不能通
2. 能不能拿到 `CU` 数据
3. 返回里有没有真实合约维度
4. 字段能不能稳定映射到 `market_daily`

探针输出至少要包含：

- 请求 URL
- 请求参数
- HTTP 状态码
- 返回条数
- 原始响应样例
- 能识别出的日期和合约列表

### 第二步：解析器

把 API 返回解析成统一内部字段。

这一步不要直接写数据库，先做：

- 字段存在性检查
- 数值转换
- 时间转换
- 合约代码抽取

### 第三步：SQLite 落库

只做一张表：

- `market_daily`

先支持：

- 插入
- 按唯一键去重
- 按 `date / symbol / contract` 查询

### 第四步：补一个极小查询 API

等本地已经有数据后，再做 `FastAPI` 查询接口。

## API 探针测试设计

## 目标

先验证“有没有数据”，不是先验证“架构漂不漂亮”。

## 输入

至少支持：

- `symbol=CU`
- 指定一个交易日或日期区间
- 可选 `contract`

## 输出

探针命令应输出：

- 请求是否成功
- 返回是否为空
- 返回记录数
- 识别出的字段名
- 前 3 条样例
- 是否可以映射到 `market_daily`
- 哪些字段缺失

## 判定标准

### 成功

- 能拿到非空数据
- 能识别出至少一个 `CU` 真实合约
- 能映射出 `date/open/high/low/close`

### 部分成功

- API 可用，但字段不全
- API 可用，但没有真实合约代码
- API 可用，但返回为空

### 失败

- 请求失败
- 返回格式与预期完全不符
- 无法区分不同合约

## 建议先写的 3 个测试

### 1. 连通性测试

确认 API 能访问，状态码正常。

### 2. 最小样本测试

指定一个已知交易日，检查是否有 `CU` 数据。

### 3. 字段映射测试

从样本中验证以下字段能否稳定得到：

- `date`
- `contract`
- `open`
- `high`
- `low`
- `close`
- `settlement`
- `volume`
- `open_interest`

## 当前最缺的信息

为了把这份架构直接落成代码，现在还缺下面这些接口信息：

- API 地址
- 请求方法
- 请求参数
- 返回样例
- 是否需要鉴权
- 限流或频控要求

如果你把这些信息补进仓库，下一步就可以直接开始写：

1. `client`
2. `probe` 命令
3. `parser`
4. `SQLModel` 表模型
5. SQLite 落库
