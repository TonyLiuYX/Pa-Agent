from __future__ import annotations

import os
import re
from pathlib import Path

from playwright.sync_api import expect  # noqa: E402
from playwright.sync_api import sync_playwright  # noqa: E402

from .test_server import running_server


def _assert_no_horizontal_overflow(page, width: int) -> None:
    metrics = page.evaluate(
        "({width: window.innerWidth, body: document.body.scrollWidth, document: document.documentElement.scrollWidth})"
    )
    assert metrics["width"] == width
    assert metrics["body"] <= width
    assert metrics["document"] <= width


def test_local_fixture_browser_and_visual_qa(tmp_path: Path):
    with running_server(tmp_path) as (server, _supervisor):
        url = f"http://127.0.0.1:{server.server_port}/"
        with sync_playwright() as browser_api:
            candidates = (
                os.environ.get("G032_CHROMIUM_EXECUTABLE", ""),
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                "/Applications/Chromium.app/Contents/MacOS/Chromium",
            )
            executable = next((candidate for candidate in candidates if candidate and Path(candidate).exists()), None)
            launch_options = {"headless": True}
            if executable:
                launch_options["executable_path"] = executable
            browser = browser_api.chromium.launch(**launch_options)
            page = browser.new_page(viewport={"width": 1440, "height": 900})
            page.goto(url + "trade", wait_until="networkidle")
            assert page.locator("text=PAPER TRADING ONLY").count() == 1
            assert page.locator("#start-run").is_visible()
            page.locator("#trade-symbol").focus()
            page.keyboard.press("Tab")
            assert page.evaluate("document.activeElement && document.activeElement.id") == "trade-timeframe"

            page.locator("#start-run").click()
            expect(page.locator("#market-bar-count")).to_have_text("63", timeout=5000)
            expect(page.locator("#runtime-label")).to_have_text(re.compile("启动中|运行中|Starting|Running"))
            page.locator('button[data-tab="strategies"]').click()
            assert page.locator("#decision-content .dark-list-item").count() == 9
            assert page.evaluate("localStorage.length") == 0
            assert page.evaluate("sessionStorage.length") == 0
            _assert_no_horizontal_overflow(page, 1440)
            page.screenshot(path=str(tmp_path / "g032-desktop-1440x900.png"), full_page=True)

            page.locator("#stop-run").click()
            expect(page.locator("#runtime-label")).to_have_text(re.compile("已停止|Stopped"))

            page.set_viewport_size({"width": 390, "height": 844})
            page.reload(wait_until="networkidle")
            _assert_no_horizontal_overflow(page, 390)
            assert page.locator("#start-run").is_visible()
            assert page.locator("#trade-chart").is_visible()
            page.screenshot(path=str(tmp_path / "g032-mobile-390x844.png"), full_page=True)
            browser.close()
