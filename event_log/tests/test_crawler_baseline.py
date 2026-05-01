from __future__ import annotations

import os
import unittest

from event_log_baseline.cli import run_smoke
from event_log_baseline.config import CrawlerConfig
from event_log_baseline.fetchers import BrowserFetcher, DefaultFetcher, ProxyFetcher, build_fetcher
from event_log_baseline.mining_com import parse_copper_term_id, parse_detail_page, parse_posts_api
from event_log_baseline.parsers import make_dedupe_key, parse_datetime_text, parse_detail_html, parse_list_html


class CrawlerBaselineTests(unittest.TestCase):
    def test_config_reads_request_controls(self) -> None:
        os.environ["REQUEST_INTERVAL_MS"] = "1700"
        os.environ["MAX_CONCURRENCY"] = "2"
        os.environ["MAX_RETRIES"] = "4"
        os.environ["REQUEST_TIMEOUT_SECONDS"] = "15"
        os.environ["ENABLE_FALLBACK_FETCHER"] = "false"
        config = CrawlerConfig.from_env()
        self.assertEqual(config.request_interval_ms, 1700)
        self.assertEqual(config.max_concurrency, 2)
        self.assertEqual(config.max_retries, 4)
        self.assertEqual(config.request_timeout_seconds, 15)
        self.assertFalse(config.enable_fallback_fetcher)

    def test_fetcher_factory_supports_all_modes(self) -> None:
        self.assertIsInstance(build_fetcher(CrawlerConfig(fetcher_mode="default")), DefaultFetcher)
        self.assertIsInstance(build_fetcher(CrawlerConfig(fetcher_mode="proxy")), ProxyFetcher)
        self.assertIsInstance(build_fetcher(CrawlerConfig(fetcher_mode="browser")), BrowserFetcher)

    def test_parser_pipeline_is_callable(self) -> None:
        list_items = parse_list_html('<a href="https://example.com/a">Item A</a>')
        detail = parse_detail_html(
            "<html><head><title>T</title></head><body><article class='entry-content'><time>2026-04-19 10:00:00</time><p>Body</p></article></body></html>"
        )
        parsed_dt = parse_datetime_text("2026-04-19 10:00:00")
        self.assertEqual(list_items[0].title, "Item A")
        self.assertEqual(detail.title, "T")
        self.assertEqual(detail.published_text, "2026-04-19 10:00:00")
        self.assertIsNotNone(parsed_dt)

    def test_dedupe_key_normalizes_tracking_bits(self) -> None:
        dedupe = make_dedupe_key("https://Example.com/article/?utm_source=x&gclid=y#frag")
        self.assertEqual(dedupe, "//example.com/article")

    def test_smoke_run_uses_default_fetcher(self) -> None:
        os.environ["FETCHER_MODE"] = "default"
        result = run_smoke()
        self.assertEqual(result["config"]["fetcher_mode"], "default")
        self.assertFalse(result["config"]["fallback_enabled"])
        self.assertEqual(result["fetch_status_code"], 200)
        self.assertEqual(result["dedupe_key"], "//example.com/article")

    def test_mining_com_term_and_posts_parsing(self) -> None:
        term_id = parse_copper_term_id('[{"id":21,"slug":"copper"},{"id":22,"slug":"gold"}]')
        posts = parse_posts_api(
            """
            [
              {
                "id": 1204162,
                "date": "2026-04-18T11:04:10",
                "link": "https://www.mining.com/example-copper-story/",
                "title": {"rendered": "Copper &amp; Demand"},
                "excerpt": {"rendered": "<p>Smelter demand stays strong.</p>"}
              }
            ]
            """
        )
        self.assertEqual(term_id, 21)
        self.assertEqual(posts[0].title, "Copper & Demand")
        self.assertEqual(posts[0].excerpt, "Smelter demand stays strong.")

    def test_mining_com_detail_parser_extracts_live_shape(self) -> None:
        detail = parse_detail_page(
            """
            <html>
              <head>
                <meta property="article:published_time" content="2026-04-18T11:04:10+00:00" />
              </head>
              <body>
                <article class="col-12 col-lg-8" data-post-id="1204162">
                  <h1 class="single-title mt-4 mb-2">Copper price jumps</h1>
                  <div class="post-meta mb-4">
                    <a href="/author/test/">Analyst</a> | April 18, 2026 | 4:04 am
                  </div>
                  <div class="post-inner-content">
                    <div class="content">
                      <p>First paragraph.</p>
                      <p>Second paragraph.</p>
                    </div>
                  </div>
                </article>
              </body>
            </html>
            """
        )
        self.assertEqual(detail.title, "Copper price jumps")
        self.assertEqual(detail.published_text, "2026-04-18T11:04:10+00:00")
        self.assertIn("First paragraph.", detail.content)
        self.assertIn("Second paragraph.", detail.content)

    def test_mining_com_detail_parser_trims_comments_and_footer_noise(self) -> None:
        detail = parse_detail_page(
            """
            <html>
              <body>
                <article>
                  <h1 class="single-title">Story</h1>
                  <div class="content">
                    <p>Core body paragraph.</p>
                    <p>Second body paragraph.</p>
                    <p>Share Comments</p>
                    <p>Cancel reply</p>
                    <p>More News</p>
                  </div>
                </article>
              </body>
            </html>
            """
        )
        self.assertIn("Core body paragraph.", detail.content)
        self.assertNotIn("Share Comments", detail.content)
        self.assertNotIn("Cancel reply", detail.content)
        self.assertNotIn("More News", detail.content)


if __name__ == "__main__":
    unittest.main()
