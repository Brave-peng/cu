# Check Key Mapping

Use this reference when converting user acceptance standards into `acceptance.json.checks` keys.

## 1. Naming Rules

- use English `snake_case`
- prefer `object_or_scope + condition`
- keep keys short and machine-readable
- reuse an existing key when the meaning is already the same
- create a new key only when no existing key fits

Good examples:

- `list_fetch_success`
- `detail_fetch_success`
- `content_fetch_success`
- `time_field_available`
- `dedupe_effective`
- `incremental_update_available`

Bad examples:

- `looks_good`
- `crawler_ok_now`
- `final_acceptance_check_for_list_page`

## 2. Common Mappings

| User Standard | Check Key |
| --- | --- |
| 能抓到列表 | `list_fetch_success` |
| 能抓到详情 | `detail_fetch_success` |
| 能抓到正文 | `content_fetch_success` |
| 正文是纯文本 | `content_clean_text` |
| 时间字段可用 | `time_field_available` |
| 时间优先级正确 | `time_priority_correct` |
| 去重生效 | `dedupe_effective` |
| URL 标准化正确 | `url_normalization_correct` |
| 来源字段正确 | `source_name_correct` |
| 失败可追溯 | `failure_traceable` |
| 可全量抓取 | `full_collection_capable` |
| 可增量更新 | `incremental_update_available` |
| 可补抓 | `refetch_interface_available` |
| 低压力抓取可用 | `low_pressure_crawling_available` |
| 失败退避可用 | `failure_backoff_available` |
| 安全切换接口可用 | `safe_fallback_interface_available` |
| 输出文件存在 | `output_file_generated` |
| 测试命令成功 | `test_command_success` |
| 退出码正确 | `exit_code_correct` |
| 数据库写入成功 | `db_write_success` |
| 配置加载成功 | `config_load_success` |

## 3. New Key Creation Rule

If no existing key matches:

1. identify the object or scope
2. identify the condition
3. combine them into one boolean-style key

Examples:

- screenshot generated -> `screenshot_generated`
- parser handles empty page -> `empty_page_parse_supported`
- retry limit enforced -> `retry_limit_enforced`

## 4. Conflict Rule

If two user statements mean the same thing, use one shared key.

Examples:

- `能抓到正文`
- `正文抓取成功`
- `详情页内容能拿到`

All should map to:

- `content_fetch_success`

## 5. Output Rule

Each key in `acceptance.json.checks` must map to only one pass/fail judgment.

Do not use one key to represent multiple unrelated conditions.
