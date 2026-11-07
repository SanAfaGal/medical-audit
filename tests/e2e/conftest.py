"""Browser smoke tests (Chromium headless via Playwright) against a RUNNING server.

They are opt-in: nothing runs unless ``E2E_BASE_URL`` points at a dev/test server, e.g.

    docker compose up -d                       # dev stack on :8000
    uv run python -m playwright install --only-shell chromium   # once
    E2E_BASE_URL=http://localhost:8000 uv run pytest tests/e2e -m e2e

The server must have at least one institution with a period. A few tests create and delete a
throwaway record, so never point them at production.
"""

from __future__ import annotations

import json
import os
import urllib.request

import pytest

BASE_URL = os.environ.get("E2E_BASE_URL", "").rstrip("/")


def pytest_collection_modifyitems(config, items):
    if BASE_URL:
        return
    skip = pytest.mark.skip(reason="set E2E_BASE_URL to run the browser smoke tests")
    for item in items:
        if "e2e" in item.keywords:
            item.add_marker(skip)


def _get_json(path: str):
    with urllib.request.urlopen(f"{BASE_URL}{path}", timeout=10) as resp:  # noqa: S310 - trusted dev URL
        return json.load(resp)


@pytest.fixture(scope="session")
def base_url() -> str:
    return BASE_URL


@pytest.fixture(scope="session")
def context_ids(base_url) -> dict:
    """First institution that has a period: the page context (sidebar selection) the tests run under."""
    for inst in _get_json("/api/institutions"):
        periods = _get_json(f"/api/institutions/{inst['id']}/periods")
        if periods:
            return {
                "institutionId": inst["id"],
                "institution": inst,
                "periodId": periods[0]["id"],
                "period": periods[0],
            }
    pytest.skip("the server has no institution with a period to test against")


@pytest.fixture(scope="session")
def browser():
    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        try:
            b = p.chromium.launch(headless=True)
        except Exception as exc:  # noqa: BLE001 - missing browser binaries are a setup problem, not a failure
            pytest.skip(f"Chromium is not installed (run: playwright install --only-shell chromium): {exc}")
        yield b
        b.close()


class Session:
    """A page with the sidebar context preselected, collecting JS errors and API calls."""

    def __init__(self, page, base_url: str):
        self.page = page
        self.base_url = base_url
        self.page_errors: list[str] = []
        self.api_calls: list[str] = []
        page.on("pageerror", lambda e: self.page_errors.append(str(e)))
        page.on("request", lambda r: self.api_calls.append(f"{r.method} {r.url}") if "/api/" in r.url else None)

    def open(self, path: str):
        self.page.goto(f"{self.base_url}{path}", wait_until="networkidle")
        self.page.wait_for_timeout(500)  # let the Alpine init() chains settle

    def toasts(self) -> list[list[str]]:
        return self.page.evaluate("() => Alpine.store('toast').items.map(t => [t.type, t.message])")

    def component(self, js_predicate: str, js_body: str):
        """Evaluate ``js_body`` with ``d`` = the Alpine data of the first element matching ``js_predicate``."""
        return self.page.evaluate(
            f"""async () => {{
                const el = [...document.querySelectorAll('[x-data]')].find(e => {{ const d = Alpine.$data(e); return {js_predicate}; }});
                const d = Alpine.$data(el);
                {js_body}
            }}"""
        )


@pytest.fixture
def session(browser, base_url, context_ids):
    ctx = browser.new_context()
    ctx.add_init_script(f"sessionStorage.setItem('audit_ctx', {json.dumps(json.dumps(context_ids))});")
    page = ctx.new_page()
    yield Session(page, base_url)
    ctx.close()
