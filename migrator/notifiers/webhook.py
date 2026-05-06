"""
Webhook notifier — POSTs a cutover summary to a Slack/Discord/generic
incoming-webhook URL.

Why a single module rather than per-target classes: Slack's
incoming-webhook contract (POST JSON with a ``text`` field) is also
what Discord accepts (via its Slack-compat path) and what most
generic ChatOps relays expect. A custom payload shape is rare enough
that we can offer it as a future addon (``payload_template`` arg)
without complicating the v0 API.

Failure mode: a webhook delivery failure must never abort the
cutover. Operators set up notifications precisely so they don't have
to babysit the run; if the webhook is down, the right behaviour is
to log the issue and proceed. The orchestrator's report file remains
the source of truth.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass
class WebhookResult:
    """One-shot delivery outcome. ``ok=False`` when the webhook URL
    rejected the payload OR the request itself failed (DNS,
    connection, timeout). The orchestrator stamps this onto the
    final cutover report so the operator sees whether their
    notification got delivered."""
    ok: bool
    status_code: int | None = None
    detail: str = ""


def slack_payload_from_summary(
    *,
    datasource: str,
    pinot_table: str,
    success: bool,
    steps: list[dict],
    parity_failed: int = 0,
    out_dir: str | None = None,
) -> dict[str, Any]:
    """Build a Slack-incoming-webhook payload from a cutover summary.

    Body shape: a single ``text`` field with markdown-flavoured emojis
    (:white_check_mark: / :x:) that Slack and Discord both render.
    Plus a per-step bullet list so the operator can see at a glance
    which phase failed without opening the report file.
    """
    head = (
        f":white_check_mark: *Cutover succeeded* for `{datasource}` "
        f"→ `{pinot_table}`"
        if success
        else f":x: *Cutover FAILED* for `{datasource}` → `{pinot_table}`"
    )
    bullets = []
    for s in steps:
        marker = {
            "ok":      ":white_check_mark:",
            "skipped": ":heavy_minus_sign:",
            "error":   ":x:",
        }.get(s.get("status", ""), ":grey_question:")
        # Truncate very long detail strings — Slack rejects messages
        # over 40k chars and a verbose ``parity`` step alone can blow
        # past 10k.
        detail = s.get("detail", "")
        if len(detail) > 200:
            detail = detail[:200] + "…"
        bullets.append(f"  {marker} `{s.get('step', '')}`: {detail}")
    payload: dict[str, Any] = {
        "text": head + "\n" + "\n".join(bullets),
    }
    if parity_failed > 0:
        payload["text"] += f"\n_Parity check: {parity_failed} failed_"
    if out_dir:
        payload["text"] += f"\n_Report: `{out_dir}/cutover-report.json`_"
    return payload


def notify_webhook(
    url: str,
    payload: dict[str, Any],
    *,
    timeout: float = 10.0,
    session: Any = None,
) -> WebhookResult:
    """POST ``payload`` as JSON to ``url``. Returns a ``WebhookResult``;
    never raises (a notification failure is logged on the result, not
    propagated — the cutover already succeeded or failed for its own
    reasons before this is called).

    ``session`` is a duck-typed ``requests.Session``-shaped object so
    tests can inject a recording stub. ``None`` means the caller
    accepts the default (we lazily import ``requests`` and build a
    fresh session — keeps ``migrator.notifiers`` importable on a
    minimal install).
    """
    if session is None:
        try:
            import requests  # noqa: PLC0415 — lazy import is intentional
        except ImportError as exc:
            return WebhookResult(
                ok=False, detail=f"requests not installed: {exc}",
            )
        session = requests.Session()
        session.headers.update({"Content-Type": "application/json"})
    try:
        resp = session.post(url, json=payload, timeout=timeout)
    except Exception as exc:  # noqa: BLE001
        # Connection error, timeout, DNS failure — all surface here as
        # a single ok=False with the exception string. Operator's job
        # is to fix the URL or add retry, not for dpm to retry blindly.
        return WebhookResult(ok=False, detail=str(exc))
    status = resp.status_code
    # Slack returns 200 with body ``"ok"`` on success; many other
    # webhook receivers (PagerDuty events API, generic relays) return
    # 200/201/202/204. Accept the whole 2xx range.
    if 200 <= status < 300:
        return WebhookResult(ok=True, status_code=status)
    text = getattr(resp, "text", "")[:300]
    return WebhookResult(ok=False, status_code=status, detail=text)
