"""Unit tests for the webhook notifier."""

from __future__ import annotations

import pytest

from migrator.notifiers.webhook import (
    WebhookResult,
    notify_webhook,
    slack_payload_from_summary,
)


# ─────────────────────────────────────────────────────────────────────────────
# Fakes
# ─────────────────────────────────────────────────────────────────────────────


class _FakeResp:
    def __init__(self, status: int = 200, text: str = "ok") -> None:
        self.status_code = status
        self.text = text


class _SpySession:
    """Records every POST so tests can assert URL + payload + headers."""
    def __init__(self, status: int = 200, text: str = "ok") -> None:
        self.posts: list[tuple[str, dict]] = []
        self.headers: dict[str, str] = {}
        self._status = status
        self._text = text

    def post(self, url: str, *, json: dict, timeout: float, **kwargs):
        self.posts.append((url, json))
        return _FakeResp(self._status, self._text)


class _ExplodingSession:
    def post(self, url, **kwargs):
        raise ConnectionError("network is down")


# ─────────────────────────────────────────────────────────────────────────────
# Slack payload shape
# ─────────────────────────────────────────────────────────────────────────────


_SUCCESS_STEPS = [
    {"step": "extract_offsets", "status": "ok",      "detail": "watermark=2024..."},
    {"step": "plan_hybrid",     "status": "ok",      "detail": "wrote 6 files"},
    {"step": "deploy",          "status": "ok",      "detail": "2 created"},
    {"step": "backfill",        "status": "ok",      "detail": "5000 rows in 3 pages"},
    {"step": "parity",          "status": "ok",      "detail": "5/5 passed"},
]


_FAILED_STEPS = [
    {"step": "extract_offsets", "status": "ok",      "detail": "watermark=2024..."},
    {"step": "plan_hybrid",     "status": "ok",      "detail": "wrote 6 files"},
    {"step": "deploy",          "status": "error",   "detail": "boom"},
    {"step": "backfill",        "status": "skipped", "detail": "aborted"},
    {"step": "parity",          "status": "skipped", "detail": "aborted"},
]


class TestSlackPayload:
    def test_success_payload_uses_check_emoji(self):
        p = slack_payload_from_summary(
            datasource="events", pinot_table="events",
            success=True, steps=_SUCCESS_STEPS,
        )
        assert ":white_check_mark:" in p["text"]
        assert "Cutover succeeded" in p["text"]
        # Datasource + table both in the headline so receivers can
        # tell which migration this is from a glance.
        assert "events" in p["text"]

    def test_failure_payload_uses_x_emoji(self):
        p = slack_payload_from_summary(
            datasource="events", pinot_table="events",
            success=False, steps=_FAILED_STEPS,
        )
        assert ":x:" in p["text"]
        assert "FAILED" in p["text"]

    def test_each_step_renders_with_status_marker(self):
        p = slack_payload_from_summary(
            datasource="events", pinot_table="events",
            success=False, steps=_FAILED_STEPS,
        )
        # ok / error / skipped each get a distinct marker so the
        # operator can scan the bullet list quickly.
        assert ":white_check_mark:" in p["text"]   # the ok rows
        assert ":x:" in p["text"]                  # the deploy:error row
        assert ":heavy_minus_sign:" in p["text"]   # the skipped rows

    def test_long_step_detail_truncated(self):
        # Slack rejects messages over 40k chars; a single verbose
        # parity step alone can blow that. Locking in truncation.
        long_detail = "X" * 5000
        p = slack_payload_from_summary(
            datasource="x", pinot_table="x", success=True,
            steps=[{"step": "x", "status": "ok", "detail": long_detail}],
        )
        # ``…`` ellipsis appended after truncation marks where it cut.
        assert "…" in p["text"]
        # And the rendered text is well below the 40k limit.
        assert len(p["text"]) < 1000

    def test_parity_failed_count_appended(self):
        p = slack_payload_from_summary(
            datasource="x", pinot_table="x",
            success=False, steps=_FAILED_STEPS,
            parity_failed=3,
        )
        assert "Parity check: 3 failed" in p["text"]

    def test_parity_count_omitted_when_zero(self):
        p = slack_payload_from_summary(
            datasource="x", pinot_table="x",
            success=True, steps=_SUCCESS_STEPS,
            parity_failed=0,
        )
        assert "Parity check" not in p["text"]

    def test_out_dir_appended_when_set(self):
        p = slack_payload_from_summary(
            datasource="x", pinot_table="x",
            success=True, steps=_SUCCESS_STEPS,
            out_dir="/var/dpm/runs/abc123",
        )
        assert "/var/dpm/runs/abc123/cutover-report.json" in p["text"]


# ─────────────────────────────────────────────────────────────────────────────
# notify_webhook delivery
# ─────────────────────────────────────────────────────────────────────────────


class TestNotifyWebhookDelivery:
    def test_2xx_returns_ok(self):
        session = _SpySession(status=200)
        result = notify_webhook(
            "http://hooks.slack.com/x", {"text": "hi"}, session=session,
        )
        assert result.ok is True
        assert result.status_code == 200

    @pytest.mark.parametrize("status", [200, 201, 202, 204])
    def test_full_2xx_range_accepted(self, status: int):
        # Slack returns 200; PagerDuty events API returns 202; some
        # generic relays return 204. All 2xx must register as ok.
        session = _SpySession(status=status)
        result = notify_webhook("http://x", {"text": "hi"}, session=session)
        assert result.ok is True

    def test_4xx_returns_not_ok_with_status_and_detail(self):
        session = _SpySession(status=403, text="forbidden")
        result = notify_webhook(
            "http://x", {"text": "hi"}, session=session,
        )
        assert result.ok is False
        assert result.status_code == 403
        assert "forbidden" in result.detail

    def test_network_failure_returns_not_ok_no_status(self):
        # Connection error → ok=False, status_code=None, detail
        # carries the exception string. Webhook failure must NEVER
        # raise into the caller (cutover orchestrator).
        result = notify_webhook(
            "http://x", {"text": "hi"}, session=_ExplodingSession(),
        )
        assert result.ok is False
        assert result.status_code is None
        assert "network is down" in result.detail

    def test_payload_posted_as_json(self):
        # Spy session captures the json= kwarg the helper used; the
        # webhook receiver requires a JSON body, not form-encoded.
        session = _SpySession()
        notify_webhook(
            "http://hooks.slack.com/x",
            {"text": "hi", "channel": "#alerts"},
            session=session,
        )
        url, payload = session.posts[0]
        assert url == "http://hooks.slack.com/x"
        assert payload == {"text": "hi", "channel": "#alerts"}
