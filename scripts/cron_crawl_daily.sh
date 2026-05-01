  #!/bin/bash

  cd /root/code/cu

  YESTERDAY=$(date -d "1 day ago" +%Y-%m-%d)
  START=$(date -d "7 days ago" +%Y-%m-%d)

  .venv/bin/python -m app.cli.market_crawler \
    --start-date "$START" \
    --end-date "$YESTERDAY" \
    --limit 1
