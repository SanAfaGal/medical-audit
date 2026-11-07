"""Smoke tests of the three pages: they load their data, and API failures reach the user as toasts."""

from __future__ import annotations

import json
import uuid

import pytest

pytestmark = pytest.mark.e2e


def _fulfill(status: int, detail: str):
    return lambda route: route.fulfill(
        status=status, content_type="application/json", body=json.dumps({"detail": detail})
    )


class TestShell:
    def test_context_store_loads_institutions_and_periods(self, session):
        session.open("/")
        state = session.page.evaluate(
            "() => ({inst: Alpine.store('ctx').institutions.length, ready: Alpine.store('ctx').ready})"
        )
        assert state["inst"] > 0 and state["ready"]
        assert not session.page_errors

    def test_institutions_500_becomes_a_toast_and_the_page_still_boots(self, session):
        session.page.route("**/api/institutions", _fulfill(500, "inst boom"))
        session.open("/")
        assert session.page.evaluate("() => Alpine.store('ctx').ready") is True
        assert any("inst boom" in message for _, message in session.toasts())
        assert not session.page_errors


class TestPipelinePage:
    def test_loads_doc_types(self, session):
        session.open("/")
        count = session.component(
            "d.docTypes !== undefined && d.stageStatuses !== undefined", "return d.docTypes.length;"
        )
        assert count > 0
        assert session.toasts() == []
        assert not session.page_errors

    def test_reattach_probe_survives_a_failing_active_endpoint(self, session):
        session.page.route("**/api/pipeline/active*", _fulfill(500, "active boom"))
        session.open("/")
        assert not session.page_errors


class TestAuditPage:
    def test_loads_lists_and_stats_without_errors(self, session):
        session.open("/audit")
        state = session.component(
            "d.folderStatuses !== undefined && d.findingsSummary !== undefined",
            "return {fs: d.folderStatuses.length, dt: d.docTypes.length, st: d.serviceTypes.length};",
        )
        assert state["fs"] > 0 and state["dt"] > 0 and state["st"] > 0
        assert session.toasts() == []
        assert not session.page_errors

    def test_api_failures_are_reported_and_fall_back_to_empty_lists(self, session):
        session.page.route("**/api/invoices/stats*", _fulfill(500, "boom stats"))
        session.page.route("**/api/settings/doc-types", lambda route: route.abort())
        session.open("/audit")

        messages = " | ".join(message for _, message in session.toasts())
        assert "boom stats" in messages
        assert "conectar" in messages  # the network failure message from api.js
        assert (
            session.component(
                "d.docTypes !== undefined && d.findingsSummary !== undefined", "return d.docTypes.length;"
            )
            == 0
        )
        assert not session.page_errors


class TestSettingsPage:
    @staticmethod
    def _settings(session, body: str):
        return session.component("d.folderStatuses !== undefined && d.addFolderStatus !== undefined", body)

    def test_loads_every_list(self, session):
        session.open("/settings")
        state = self._settings(
            session,
            "return {fs: d.folderStatuses.length, st: d.serviceTypes.length, dt: d.docTypes.length, inst: d.institutions.length};",
        )
        assert all(state.values()), state
        assert not session.page_errors

    def test_create_then_delete_a_folder_status(self, session):
        session.open("/settings")
        name = f"E2E_{uuid.uuid4().hex[:8].upper()}"
        result = self._settings(
            session,
            f"""
            const before = d.folderStatuses.length;
            d.newFS.status = '{name}';
            await d.addFolderStatus();
            const created = d.folderStatuses.find(f => f.status === '{name}');
            const afterCreate = d.folderStatuses.length;
            if (created) await d.deleteFolderStatus(created.id);   // always clean up
            return {{before, afterCreate, afterDelete: d.folderStatuses.length, created: !!created}};
            """,
        )
        assert result["created"], result
        assert result["afterCreate"] == result["before"] + 1
        assert result["afterDelete"] == result["before"]

    def test_server_errors_show_the_servers_message(self, session):
        session.open("/settings")
        self._settings(session, "await d.deleteFolderStatus(999999);")
        assert any("no encontrado" in message.lower() for _, message in session.toasts())

        duplicate = self._settings(
            session,
            "d.newFS.status = d.folderStatuses[0].status; await d.addFolderStatus(); "
            "return Alpine.store('toast').items.map(t => t.message);",
        )
        assert any("en uso" in message for message in duplicate)
        assert "Error al crear." not in duplicate  # the generic text was replaced by the server's message


class TestHelpPage:
    def test_loads_without_errors(self, session):
        session.open("/help")
        assert not session.page_errors


class TestScriptsAreServed:
    @pytest.mark.parametrize("path", ["/", "/audit", "/settings", "/help"])
    def test_every_script_loads_with_a_version(self, session, path):
        failed: list[str] = []
        session.page.on(
            "response",
            lambda r: failed.append(f"{r.status} {r.url}") if "/static/js/" in r.url and r.status >= 400 else None,
        )
        session.open(path)
        sources = session.page.evaluate(
            "() => [...document.scripts].map(s => s.src).filter(s => s.includes('/static/js/'))"
        )
        assert sources, "page loads no local scripts"
        assert all("?v=" in src for src in sources), sources
        assert failed == []
        assert not session.page_errors


class TestNoHalfBuiltPage:
    """The page stays hidden until Alpine and the fonts are ready: no flash of unstyled or hidden-then-gone content."""

    @pytest.mark.parametrize("path", ["/", "/audit", "/settings", "/help"])
    def test_page_is_revealed_with_alpine_started_and_the_font_loaded(self, session, path):
        session.open(path)
        state = session.page.evaluate(
            """() => ({
                revealed: !document.documentElement.classList.contains('preboot'),
                visible: getComputedStyle(document.body).visibility !== 'hidden',
                font: document.fonts.check('16px "Plus Jakarta Sans"'),
            })"""
        )
        assert state == {"revealed": True, "visible": True, "font": True}

    def test_x_show_elements_are_never_visible_before_alpine_starts(self, session):
        session.page.add_init_script(
            """
            window.__leaks = 0;
            window.__alpineUp = false;
            document.addEventListener('alpine:initialized', () => { window.__alpineUp = true; });
            (function sample() {
                if (document.body && !window.__alpineUp && getComputedStyle(document.body).visibility !== 'hidden') {
                    window.__leaks += [...document.querySelectorAll('[x-show]')].filter(
                        e => getComputedStyle(e).display !== 'none' && e.getClientRects().length > 0).length;
                }
                requestAnimationFrame(sample);
            })();
            """
        )
        session.open("/settings")
        assert session.page.evaluate("window.__leaks") == 0

    def test_a_blocked_alpine_does_not_leave_the_page_blank(self, session):
        session.page.route("**/vendor/alpine.min.js", lambda route: route.abort())
        session.page.goto(f"{session.base_url}/", wait_until="commit")
        session.page.wait_for_function(
            "() => document.body && getComputedStyle(document.body).visibility !== 'hidden'", timeout=6000
        )  # the 3 s safety timeout in boot.js reveals it
