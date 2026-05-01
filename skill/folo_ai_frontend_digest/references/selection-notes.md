# Selection Notes

Use this skill for a Folo digest where:

- non-paper items are kept in full
- arXiv papers come only from `cs.AI updates on arXiv.org`
- papers are filtered first by category and blacklist
- Codex then reads the filtered pool and picks `5 new + 5 classic`
- final output hides pipeline stats and shows only reader-facing digest content

## Paper Candidate Rules

- allowed categories: `cs.LG`, `cs.CL`, `cs.SE`
- blacklist terms:
  - `clinical`
  - `psychiatric`
  - `lung cancer`
  - `biomechanical`
  - `traffic`
  - `driving`
  - `emboli`
  - `field medicine`
  - `legal`
  - `graph`

## Bucket Intuition

`classic`:

- agent
- reasoning
- RAG / retrieval
- memory
- coding
- safety
- alignment
- efficiency

`new`:

- a new product angle
- a new interface or modality
- a new execution boundary
- a new workflow pattern
- a new deployment or UX implication

These are ranking hints, not hard inclusion rules.

## Final Presentation Rules

- `News` must work for every non-paper feed, not only blog/news feeds.
- `News` uses:
  - title
  - original content in English
  - original link
- `News` numbering restarts from `1`
- `Paper` uses:
  - title
  - original content in English
  - insights in Chinese
  - original link
- `Paper` numbering restarts from `1`
- Do not show a separate abstract section in the final digest.
- Do not show internal counters such as totals, candidate counts, or test-send notes in the final digest.
- Use the Feishu OpenAPI send path from `scripts/send_lark_markdown.py` for final delivery so Chinese text and rich formatting survive on Windows.
