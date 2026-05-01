# Workflow Aliases

## 固定口令

`按照Review审查流程走`

## 含义

When the user uses this phrase, run the document workflow in this order:

1. The main agent writes the local markdown draft with `$document-structure`.
2. The main agent self-checks the draft once.
3. The main agent hands the local markdown draft to a reviewer using `$document-review`.
4. The reviewer returns `Findings` and `Revision Guidance` only.
5. The main agent revises the local markdown.
6. The main agent publishes the final Feishu version.

## 说明

- The local markdown is the content source of truth.
- The reviewer reviews the local markdown, not the Feishu rendering.
- Feishu is the final collaboration surface, not the primary review surface.
