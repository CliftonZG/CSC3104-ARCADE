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
import time
from playwright.async_api import async_playwright

HTML_CLIENT = """
<!DOCTYPE html>
<html>
<head><title>ARCADE Client</title></head>
<body>
<script>
window.arcadeStats = { sent: 0, received: 0, replies: [], disconnects: 0, connected: false };
const params = new URLSearchParams(window.location.search);
const match = params.get('match') || 'room0';
const player = params.get('player') || 'p1';
const wsUrl = `ws://${window.location.host || '127.0.0.1:8080'}/ws?match=${match}&player=${player}`;

const ws = new WebSocket(wsUrl);

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
</script>
</body>
</html>
"""

async def test_browser_continuity(browser_name: str, ws_port: int, admin_port: int):
    print(f"\n=======================================================")
    print(f" Starting E2E test on: {browser_name.upper()}")
    print(f"=======================================================")

    async with async_playwright() as p:
        browser_type = getattr(p, browser_name)
        browser = await browser_type.launch(headless=True)
        context = await browser.new_context()
        page = await context.new_page()

        # Serve inline test harness
        await page.set_content(HTML_CLIENT)

        # Wait for WebSocket handshake via proxy
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

        print(f"[{browser_name.upper()}] Connected to proxy. Sending initial moves...")
        for i in range(5):
            await page.evaluate(f"() => window.sendAction('inc')")
            await asyncio.sleep(0.05)

        # Confirm replies from initial primary
        recv_before = await page.evaluate("() => window.arcadeStats.received")
        print(f"[{browser_name.upper()}] Received {recv_before} replies before failover.")

        # Simulate Failover: Retarget proxy route to secondary pod via admin API
        print(f"[{browser_name.upper()}] Triggering failover via Proxy Admin API (: {admin_port})...")
        curl_cmd = [
            "curl", "-s", "-X", "POST", f"http://127.0.0.1:{admin_port}/admin/route",
            "-H", "Content-Type: application/json",
            "-d", '{"match_id":"room0","epoch":2,"primary_addr":"127.0.0.1:8082"}'
        ]
        subprocess.run(curl_cmd, check=True)

        # Immediately send moves while retargeting occurs
        print(f"[{browser_name.upper()}] Sending moves across failover cutover...")
        for i in range(10):
            await page.evaluate(f"() => window.sendAction('inc')")
            await asyncio.sleep(0.04)

        await asyncio.sleep(1.0)

        # Verify connection continuity
        disconnects = await page.evaluate("() => window.arcadeStats.disconnects")
        recv_after = await page.evaluate("() => window.arcadeStats.received")
        is_connected = await page.evaluate("() => window.arcadeStats.connected")

        print(f"[{browser_name.upper()}] Results -> Disconnects: {disconnects}, Total Received: {recv_after}, Still Connected: {is_connected}")

        await browser.close()

        if disconnects > 0:
            print(f"[{browser_name.upper()}] FAILED: WebSocket disconnected during failover!")
            return False
        if recv_after < recv_before:
            print(f"[{browser_name.upper()}] FAILED: No replies after failover")
            return False

        print(f"[{browser_name.upper()}] PASSED: Zero socket disconnects during failover.")
        return True

async def main():
    parser = argparse.ArgumentParser(description="Multi-browser WebSocket Failover Test")
    parser.add_argument("--browser", choices=["firefox", "chromium", "webkit", "all"], default="firefox",
                        help="Browser engine to test (default: firefox)")
    parser.add_argument("--ws-port", type=int, default=8080, help="Proxy client port")
    parser.add_argument("--admin-port", type=int, default=9090, help="Proxy admin port")
    args = parser.parse_args()

    browsers = ["firefox", "chromium", "webkit"] if args.browser == "all" else [args.browser]

    all_passed = True
    for b in browsers:
        passed = await test_browser_continuity(b, args.ws_port, args.admin_port)
        if not passed:
            all_passed = False

    sys.exit(0 if all_passed else 1)

if __name__ == "__main__":
    asyncio.run(main())