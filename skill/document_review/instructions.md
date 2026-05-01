# Document Review

当任务是在主写作完成后审查项目文档或 PRD 草稿时，使用这个 skill。

这个 skill 只负责 review，不默认重写全文。
除非用户明确要求 rewrite，否则只输出 findings 和 revision guidance。

## 项目参考索引

开始 review 前，先读：

- [文档规范索引](https://www.feishu.cn/docx/UwTWd6vHlo9lPMxmQB9cSjWvnUd)

用索引判断当前 draft 应对齐哪些项目参考文档。
不要凭记忆检查命名一致性。
如果索引没覆盖该主题，可以继续 review，但要明确写出参考覆盖缺失。

## Review Goal

检查草稿是否：

- structurally clean
- dense enough
- readable
- free of process noise
- appropriate for its target reader
- aligned with the relevant project references

输出 findings 和 revision guidance，不直接替作者重写。

## Review Workflow

如果用户明确要求走 review workflow，按下面顺序：

1. read the local markdown draft
2. read the project reference index
3. choose the relevant linked project references
4. ignore Feishu rendering issues unless final delivery QA is requested
5. review against writing constraints and PRD constraints
6. return a short list of findings
7. return revision guidance for the main agent

## Review Checklist

按这个顺序检查：

1. Structure
   - heading depth <= 3
   - one document solves one main problem
   - sections are not fragmented or redundant
2. Density
   - no empty framing
   - no document-management language
   - each section leads with a real conclusion
3. Noise
   - no internal discussion traces
   - no temporary debate notes
   - no unnecessary “not covered” statements
4. Readability
   - tables/examples/code/diagrams actually help
   - real flows use `mermaid`, not ASCII or fake diagrams
   - repeated tables with the same schema are merged
   - one logical schema should not be split across many tables
5. Target Fit
   - PRDs read like developer-facing requirements
   - rule docs read like executable rules
   - main docs can stand alone
6. Reference Alignment
   - fixed names match the relevant references
   - the same object is not renamed across sections
   - titles, body, tables, and examples stay consistent
   - findings point to concrete mismatches

## 输出格式

固定输出：

- `Findings`
- `Revision Guidance`

不要默认返回整篇重写稿。
不要只给表扬式反馈。

好 finding 的标准：

- concrete
- tied to a section
- easy to act on
- explicit when a flow should become `mermaid`

差的 finding：

- vague style opinions
- rewriting without diagnosis
- broad statements like “make it clearer”

## 严重度优先级

优先指出这些问题：

- wrong target reader
- mixed document purpose
- mismatch against project references
- low information density
- repeated or split schema tables
- process noise

轻微措辞问题放最后。

## PRD 专项检查

1. heading depth within three levels
2. one main problem per document
3. direct statements of object, rules, and requirements
4. short, hard titles and short opening sentences
5. no internal discussion traces
6. one schema table per logical section
7. one sample-data table per logical section

## 最终规则

像编辑一样诊断问题，不要默认变成第二作者。
