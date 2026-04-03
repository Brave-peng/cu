# 飞书 CLI 使用说明

## 给人看的版本

`lark-cli` 是飞书官方 CLI。你可以把它理解成一个终端入口，用来搜索文档、读取文档、创建文档、更新文档，以及操作知识库、云空间、多维表格等资源。

先安装：

```bash
npm install -g @larksuite/cli --registry=https://registry.npmmirror.com
```

首次使用建议按这个顺序执行：

```bash
lark-cli config init
lark-cli auth login
lark-cli auth status
```

只要 `auth status` 显示当前身份正常，一般就可以开始使用。操作你自己的文档时，优先使用 `user` 身份。

最常用的是这几类命令：

- `lark-cli docs`：飞书文档
- `lark-cli wiki`：知识库
- `lark-cli drive`：云空间和文件
- `lark-cli sheets`：电子表格
- `lark-cli base`：多维表格

如果你只想操作文档，先记住下面四条就够了：

```bash
lark-cli docs +search --query 飞书CLI
lark-cli docs +fetch --doc DOC_URL_OR_TOKEN
lark-cli docs +create --title 示例文档 --markdown Hello
lark-cli docs +update --doc DOC_ID --mode append --markdown 新增一段内容
```

实际使用时，可以这样理解：

- `+search`：先找文档
- `+fetch`：把文档内容拉下来
- `+create`：新建一篇文档
- `+update`：追加或修改已有文档

有两个经验很重要：

- 看不到自己的文档时，先执行 `lark-cli auth status`，确认当前是不是 `user`
- 遇到 `/wiki/...` 这类链接时，不要默认把它当成文档本体，它经常只是一个知识库节点

如果命令报权限不足，通常不是 CLI 坏了，而是 scope 不够。这时重新授权一次最常见：

```bash
lark-cli auth login
```

如果你只是想快速上手，到这里已经够用。下面这部分是附给 AI / agent 阅读使用的。

---

## 给 AI / Agent 看的附录

### 1. 安装 CLI

```bash
npm install -g @larksuite/cli --registry=https://registry.npmmirror.com
```

### 2. 安装 skills

安装全部 skills：

```bash
npx skills add larksuite/cli --all -y -g
```

只安装文档相关：

```bash
npx skills add larksuite/cli -s lark-doc -s lark-drive -s lark-wiki -y -g
```

### 3. 推荐关注的 skills

- `lark-shared`：配置、登录、身份与 scope 处理
- `lark-doc`：文档搜索、创建、读取、更新
- `lark-drive`：云空间和文件
- `lark-wiki`：知识库空间和节点
- `lark-sheets`：电子表格
- `lark-base`：多维表格

### 4. Agent 侧使用原则

- 先读 `lark-shared`，再执行具体资源操作
- 用 `docs +search` 做资源发现
- 修改已有文档时优先局部更新，不默认整篇覆盖
- 遇到 wiki 链接时，不直接把 wiki token 当作文件 token
- 写入、删除、覆盖前先确认用户意图

### 5. 最小可用流程

```bash
lark-cli config init
lark-cli auth login
lark-cli auth status
lark-cli docs +search --query 示例
lark-cli docs +create --title 示例文档 --markdown Hello
```
