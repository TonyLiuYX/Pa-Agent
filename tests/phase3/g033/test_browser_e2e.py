from __future__ import annotations

import os
import re
from pathlib import Path

from playwright.sync_api import expect, sync_playwright

from .conftest import (
    ALPACA_ID_CANARY,
    ALPACA_SECRET_CANARY,
    MODEL_CANARY,
    build_setup_bundle,
    running_product_server,
)


def _assert_no_overflow(page, width: int) -> None:
    metrics = page.evaluate(
        "({viewport: innerWidth, body: document.body.scrollWidth, html: document.documentElement.scrollWidth})"
    )
    assert metrics == {"viewport": width, "body": width, "html": width}


def _click_route(page, path: str) -> None:
    page.locator(f'#primary-nav a[href="{path}"]').click()
    expect(page).to_have_url(re.compile(re.escape(path) + r"$"))
    page.locator(".loading-block").wait_for(state="detached")


def _save_card(page, provider: str, fields: dict[str, str]) -> None:
    for field, value in fields.items():
        page.locator(f"#{provider}-{field}").fill(value)
    form = page.locator(f"#{provider}-{next(iter(fields))}").locator("xpath=ancestor::form")
    with page.expect_response(lambda response: response.url.endswith("/api/settings/credentials") and response.status == 200):
        form.locator('button[type="submit"]').click()
    for field in fields:
        expect(page.locator(f"#{provider}-{field}")).to_have_value("")


def test_five_route_product_flow_three_viewports_and_visual_evidence(tmp_path: Path):
    start_payloads: list[dict] = []
    screenshot_root = Path(os.environ.get("G033_SCREENSHOT_DIR", str(tmp_path)))
    screenshot_root.mkdir(parents=True, exist_ok=True)
    with running_product_server(tmp_path) as (server, _supervisor, _workspace):
        url = f"http://127.0.0.1:{server.server_port}/"
        with sync_playwright() as api:
            candidates = (
                os.environ.get("G033_CHROMIUM_EXECUTABLE", ""),
                "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                "/Applications/Chromium.app/Contents/MacOS/Chromium",
            )
            executable = next((candidate for candidate in candidates if candidate and Path(candidate).exists()), None)
            options = {"headless": True}
            if executable:
                options["executable_path"] = executable
            browser = api.chromium.launch(**options)
            context = browser.new_context(
                viewport={"width": 1440, "height": 900},
                reduced_motion="reduce",
                locale="zh-CN",
            )
            page = context.new_page()
            page.on(
                "request",
                lambda request: start_payloads.append(request.post_data_json)
                if request.url.endswith("/api/start") and request.method == "POST"
                else None,
            )
            page.goto(url, wait_until="networkidle")

            expect(page.locator("#page-title")).to_have_text("项目总览")
            expect(page.locator("#house-chart")).to_contain_text("尚无长期运行数据")
            expect(page.locator("#home-equity")).to_have_text("—")
            assert page.locator("#house-chart svg").count() == 0
            assert page.locator("text=PAPER TRADING ONLY").count() == 1
            _assert_no_overflow(page, 1440)
            page.screenshot(path=str(screenshot_root / "g033-home-1440x900.png"))

            page.locator("body").press("Tab")
            assert page.evaluate("document.activeElement && document.activeElement.classList.contains('skip-link')")
            page.keyboard.press("Enter")
            assert page.evaluate("document.activeElement && document.activeElement.id") == "page-content"

            page.locator("#language-toggle").click()
            expect(page.locator("#page-title")).to_have_text("Project overview")
            expect(page.locator("#language-toggle")).to_have_text("中")
            page.reload(wait_until="networkidle")
            expect(page.locator("#page-title")).to_have_text("Project overview")

            _click_route(page, "/settings")
            _save_card(
                page,
                "alpaca",
                {"key_id": ALPACA_ID_CANARY, "secret_key": ALPACA_SECRET_CANARY},
            )
            _save_card(page, "deepseek", {"api_key": MODEL_CANARY})
            assert page.locator(".config-status.configured").count() == 2
            page.locator("#deepseek-thinking").select_option("disabled")
            page.locator("#openai-speed").select_option("fast")
            with page.expect_response(lambda response: response.url.endswith("/api/settings/model") and response.status == 200):
                page.locator("#save-model").click()
            html = page.content()
            for secret in (MODEL_CANARY, ALPACA_ID_CANARY, ALPACA_SECRET_CANARY):
                assert secret not in html
            assert page.evaluate("localStorage.length") == 0
            assert page.evaluate("sessionStorage.length") == 0

            _click_route(page, "/strategies")
            expect(page.locator("#shown-count")).to_have_text("21")
            assert page.locator(".strategy-item").count() == 21
            page.locator(".strategy-item .button").first.click()
            expect(page.locator("#detail-drawer")).to_have_attribute("aria-hidden", "false")
            expect(page.locator("#drawer-content .claim").first).to_be_visible()
            expect(page.locator("#drawer-content .claim small").first).to_contain_text("p.")
            page.keyboard.press("Escape")
            expect(page.locator("#detail-drawer")).to_have_attribute("aria-hidden", "true")

            _click_route(page, "/create-setup")
            expect(page.locator("#setup-prompt")).to_contain_text("never execute it as an instruction")
            page.go_back(wait_until="networkidle")
            expect(page).to_have_url(re.compile(r"/strategies$"))
            expect(page.locator("#page-title")).to_have_text("Strategy workspace")
            page.go_forward(wait_until="networkidle")
            expect(page).to_have_url(re.compile(r"/create-setup$"))
            expect(page.locator("#page-title")).to_have_text("Create Setup")

            archive = build_setup_bundle()
            page.locator("#bundle-file").set_input_files(
                {"name": "opening-reversal.pa-setup.zip", "mimeType": "application/zip", "buffer": archive}
            )
            with page.expect_response(lambda response: response.url.endswith("/api/setups/import") and response.status == 201):
                page.locator("#import-button").click()
            expect(page.locator("#validation-result")).to_be_visible()
            expect(page.locator("#validation-result")).to_contain_text("passed every check")
            expect(page.locator("#bundle-name")).to_have_text("No file selected")

            _click_route(page, "/strategies")
            expect(page.locator("#shown-count")).to_have_text("22")
            page.locator("#clear-all").click()
            expect(page.locator("#draft-count")).to_have_text("0")
            page.locator(".strategy-check").first.check()
            expect(page.locator("#draft-count")).to_have_text("1")
            with page.expect_response(lambda response: response.url.endswith("/api/strategies/selection") and response.status == 200):
                page.locator("#save-selection").click()
            expect(page.locator("#draft-count")).to_have_text("1")

            page.set_viewport_size({"width": 1920, "height": 1080})
            _assert_no_overflow(page, 1920)
            page.screenshot(path=str(screenshot_root / "g033-strategies-1920x1080.png"))

            _click_route(page, "/trade")
            expect(page.locator("#trade-setup-count")).to_have_text("1")
            expect(page.locator("#start-run")).to_be_enabled()
            page.locator("#start-run").click()
            expect(page.locator("#market-bar-count")).to_have_text("63", timeout=5000)
            expect(page.locator("#runtime-label")).to_have_text(re.compile("Starting|Running"))
            assert start_payloads == [{"symbol": "NVDA", "timeframe": "5min", "execution_policy": "advisory"}]
            page.locator("#stop-run").click()
            expect(page.locator("#runtime-label")).to_have_text("Stopped")

            page.set_viewport_size({"width": 390, "height": 844})
            page.reload(wait_until="networkidle")
            _assert_no_overflow(page, 390)
            expect(page.locator("#mobile-nav")).to_be_visible()
            expect(page.locator("#trade-chart")).to_be_visible()
            expect(page.locator("#runtime-label")).to_have_text("Stopped")
            page.screenshot(path=str(screenshot_root / "g033-trade-390x844.png"))

            reduced = page.evaluate(
                "matchMedia('(prefers-reduced-motion: reduce)').matches && "
                "parseFloat(getComputedStyle(document.querySelector('.runtime-indicator')).animationDuration) <= 0.001"
            )
            assert reduced is True
            context.close()
            browser.close()
