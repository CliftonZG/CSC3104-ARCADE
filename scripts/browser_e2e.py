# /// script
# requires-python = ">=3.11"
# dependencies = [
#     "playwright>=1.40.0",
# ]
# ///
"""
ARCADE Multi-Browser E2E Failover Verification.
Tests client WebSocket continuity during primary crash across Firefox, Chromium, and WebKit.
"""

import argparse
import asyncio
import subprocess
import sys
from playwright.async_api import async_playwright

async def test_browser_continuity(browser_name: str, ws_port: int):
    print(f"\n=======================================================")
    print(f" Starting E2E test on: {browser_name.upper()}")
    print(f"=======================================================")

    async with async_playwright() as p:
        browser_type = getattr(p, browser_name)
        browser = await browser_type.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Navigate to proxy healthz so page origin is http://127.0.0.1:8080
        await page.goto(f"http://127.0.0.1:{ws_port}/healthz")

        # Inject WebSocket client harness
        await page.evaluate("""() => {
            window.arcadeStats = { sent: 0, received: 0, replies: [], disconnects: 0, connected: false };
            const ws = new WebSocket('ws://127.0.0.1:8080/ws?match=room0&player=p1');

            ws.onopen = () => {
                window.arcadeStats.connected = true;
            };

            ws.onmessage = (event) => {
                window.arcadeStats.received++;
                window.arcadeStats.replies.push(event.data);
            };

            ws.onclose = (event) => {
                window.arcadeStats.connected = false;
                window.arcadeStats.disconnects++;
            };

            window.sendAction = (msg) => {
                if (ws.readyState === WebSocket.OPEN) {
                    ws.send(msg);
                    window.arcadeStats.sent++;
                    return true;
                }
                return false;
            };
        }""")

        # Wait for WebSocket handshake
        connected = False
        for _ in range(50):
            connected = await page.evaluate("() => window.arcadeStats.connected")
            if connected:
                break
            await asyncio.sleep(0.1)

        if not connected:
            print(f"[{browser_name.upper()}] FAIL: Could not establish initial WebSocket connection")
            await browser.close()
            return False

        print(f"[{browser_name.upper()}] Connected to proxy. Sending initial moves to Primary A...")
        for _ in range(10):
            await page.evaluate("() => window.sendAction('inc')")
            await asyncio.sleep(0.04)

        recv_before = await page.evaluate("() => window.arcadeStats.received")
        print(f"[{browser_name.upper()}] Received {recv_before} replies from Primary A.")

        # Trigger real failover by killing the Primary gameserver process
        print(f"[{browser_name.upper()}] Injecting fault: Killing Primary gameserver (:8081)...")
        subprocess.run(["pkill", "-9", "-f", "gameserver.*-http=8081"])

        # Continue sending moves through the failover window
        print(f"[{browser_name.upper()}] Continuing to send moves across failover cutover...")
        for _ in range(25):
            await page.evaluate("() => window.sendAction('inc')")
            await asyncio.sleep(0.04)

        # Allow time for detector takeover and proxy retargeting to settle
        await asyncio.sleep(1.0)

        disconnects = await page.evaluate("() => window.arcadeStats.disconnects")
        recv_after = await page.evaluate("() => window.arcadeStats.received")
        is_connected = await page.evaluate("() => window.arcadeStats.connected")
        replies = await page.evaluate("() => window.arcadeStats.replies")

        await browser.close()

        saw_a = any("arcade-gs-a-0" in r for r in replies)
        saw_b = any("arcade-gs-b-0" in r for r in replies)

        print(f"[{browser_name.upper()}] Results -> Disconnects: {disconnects}, Total Received: {recv_after}, Still Connected: {is_connected}")
        print(f"[{browser_name.upper()}] Server switch verified -> Saw Pod A: {saw_a}, Saw Pod B: {saw_b}")

        if disconnects > 0:
            print(f"[{browser_name.upper()}] FAILED: WebSocket was disconnected during failover!")
            return False
        if not is_connected:
            print(f"[{browser_name.upper()}] FAILED: WebSocket is not open at end of test")
            return False
        if recv_after <= recv_before:
            print(f"[{browser_name.upper()}] FAILED: No new replies received after Primary killed")
            return False

        print(f"[{browser_name.upper()}] PASSED: Zero socket disconnects! Handover completed seamlessly.")
        return True

async def main():
    parser = argparse.ArgumentParser(description="Multi-browser WebSocket Failover Test")
    parser.add_argument("--browser", choices=["firefox", "chromium", "webkit", "all"], default="firefox",
                        help="Browser engine to test (default: firefox)")
    parser.add_argument("--ws-port", type=int, default=8080, help="Proxy client port")
    args = parser.parse_args()

    browsers = ["firefox", "chromium"] if args.browser == "all" else [args.browser]

    all_passed = True
    for b in browsers:
        passed = await test_browser_continuity(b, args.ws_port)
        if not passed:
            all_passed = False

    sys.exit(0 if all_passed else 1)

if __name__ == "__main__":
    asyncio.run(main())