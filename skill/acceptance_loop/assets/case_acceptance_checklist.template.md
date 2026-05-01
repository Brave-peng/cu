# Case Acceptance Checklist

Use this checklist before starting a new looped task.

Fill one row per acceptance check.

| check key | user standard | pass condition |
| --- | --- | --- |
| `replace_with_check_key` | replace with the user's original acceptance statement | replace with a machine-checkable pass condition |

## Rules

- every row must map to exactly one `checks` key
- every pass condition must be machine-checkable
- reuse existing check keys when the meaning is already the same
- create a new check key only when no existing key fits
- do not mix multiple conditions into one row

## Example

| check key | user standard | pass condition |
| --- | --- | --- |
| `content_fetch_success` | 能抓到正文 | every successful detail page record has `content` and `content_length > 0` |
| `dedupe_effective` | 去重生效 | rerunning the same batch does not create duplicate `dedupe_key` records |
