# Case Intake Template

Use this template before starting a new case with the acceptance loop.

| Item | What to provide |
| --- | --- |
| Background | What this case is for and any necessary context |
| Target Task | The one thing to build, fix, or validate in this loop |
| Acceptance Standards | The exact pass/fail rules |
| Runtime Location | Local or server, plus working path if relevant |
| Inputs | Docs, URLs, code paths, samples, or configs to read |
| Boundaries | What must not be changed, performance limits, safety limits |
| Retry Limit | Maximum automatic retries allowed |

## Minimal Fill-In Form

```text
Background:
Target Task:
Acceptance Standards:
Runtime Location:
Inputs:
Boundaries:
Retry Limit:
```

## Start Rule

If any of these are missing or vague:

- target task
- acceptance standards
- runtime location

the loop should pause and ask for clarification first.
