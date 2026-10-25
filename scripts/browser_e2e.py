#!/usr/bin/env python3
"""Real-browser failover test: Chromium's own WebSocket talks to the proxy while the
primary is kill -9'd and the takeover is announced.

    python3 scripts/browser_e2e.py

The page is served from http://127.0.0.1:18000, so the proxy's Origin check is
exercised for real (-origin 127.0.0.1:18000). A page from any other origin must be refused.
"""
import json, os, signal, subprocess, sys, tempfile, time, urllib.request, http.server, threading
from playwright.sync_api import sync_playwright

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
tmp = tempfile.mkdtemp()
procs = []

def start(*cmd):
    p = subprocess.Popen(cmd, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    procs.append(p)
    return p

def wait_http(url):
    for _ in range(100):
        try:
            urllib.request.urlopen(url, timeout=0.5).read(); return
        except Exception:
            time.sleep(0.1)
    raise SystemExit("not up: " + url)

def post_route(match, epoch, addr):
    req = urllib.request.Request("http://127.0.0.1:19090/admin/route", method="POST",
        data=json.dumps({"match_id": match, "epoch": epoch, "primary_addr": addr}).encode(),
        headers={"Authorization": "Bearer s3cret"})
    return json.load(urllib.request.urlopen(req))

PAGE = b"<!doctype html><title>arcade test</title><body>ok</body>"
class H(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200); self.send_header("Content-Type", "text/html"); self.end_headers(); self.wfile.write(PAGE)
    def log_message(self, *a): pass

try:
    subprocess.check_call(["go", "build", "-o", tmp + "/", "./cmd/proxy", "./cmd/fakegame", "./cmd/wsclient"], cwd=ROOT)
    state = tmp + "/state.json"
    A = start(tmp + "/fakegame", "-name", "A", "-listen", "127.0.0.1:19001", "-state", state)
    start(tmp + "/fakegame", "-name", "B", "-listen", "127.0.0.1:19002", "-state", state)
    start(tmp + "/proxy", "-listen", "127.0.0.1:18080", "-admin", "127.0.0.1:19090", "-admin-token", "s3cret",
          "-default-upstream", "127.0.0.1:19001", "-origin", "127.0.0.1:18000")
    for port in (19001, 19002, 18080, 19090):
        wait_http(f"http://127.0.0.1:{port}/healthz")

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 18000), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    JS = """
    (url) => new Promise((resolve) => {
      const log = {opened:false, closed:null, errors:0, msgs:[], sent:0};
      const ws = new WebSocket(url);
      window.__ws = ws; window.__log = log;
      ws.onopen = () => { log.opened = true; resolve(true); };
      ws.onmessage = (e) => log.msgs.push(e.data);
      ws.onclose = (e) => { log.closed = {code:e.code, reason:e.reason, clean:e.wasClean}; if (!log.opened) resolve(false); };
      ws.onerror = () => log.errors++;
    })
    """
    with sync_playwright() as pw:
        b = pw.chromium.launch(executable_path="/opt/pw-browsers/chromium", args=["--no-sandbox"]) \
            if os.path.exists("/opt/pw-browsers/chromium") and os.path.isfile("/opt/pw-browsers/chromium") \
            else pw.chromium.launch(args=["--no-sandbox"])

        # 1. an allowed origin works and survives the failover
        page = b.new_page()
        page.goto("http://127.0.0.1:18000/")
        assert page.evaluate(JS, "ws://127.0.0.1:18080/ws?match=room1&player=browser"), "allowed origin was refused"
        page.evaluate("() => { for (let i = 0; i < 5; i++) window.__ws.send('inc') }")
        page.wait_for_function("() => window.__log.msgs.length >= 5", timeout=5000)
        before = page.evaluate("() => window.__log.msgs.slice()")
        print("before failover:", before)
        assert all(m.startswith("A|room1|") for m in before), before

        # keep playing while the primary is killed and replaced
        page.evaluate("() => { window.__t = setInterval(() => window.__ws.send('inc'), 20) }")
        time.sleep(0.5)
        os.kill(A.pid, signal.SIGKILL)
        time.sleep(0.3)
        print("takeover announced:", post_route("room1", 2, "127.0.0.1:19002"))
        page.wait_for_function("() => window.__log.msgs.some(m => m.startsWith('B|'))", timeout=8000)
        time.sleep(0.5)
        page.evaluate("() => clearInterval(window.__t)")
        time.sleep(0.3)

        log = page.evaluate("() => window.__log")
        msgs = log["msgs"]
        counters = [int(m.split("|")[2]) for m in msgs]
        print(f"after failover: {len(msgs)} replies, first from B at index {next(i for i,m in enumerate(msgs) if m.startswith('B|'))}")
        print("socket state:", page.evaluate("() => window.__ws.readyState"), "close event:", log["closed"], "errors:", log["errors"])
        assert log["closed"] is None, f"browser socket was closed: {log['closed']}"
        assert log["errors"] == 0, "browser socket reported errors"
        assert page.evaluate("() => window.__ws.readyState") == 1, "socket not OPEN"
        assert counters == sorted(set(counters)), "counter went backwards or repeated"
        gaps = sum(b - a - 1 for a, b in zip(counters, counters[1:]))
        print(f"counter gaps (replies lost in flight at the crash): {gaps}")
        assert msgs[-1].startswith("B|"), msgs[-1]

        # 2. a page from a different origin is refused
        evil = b.new_page()
        srv2 = http.server.ThreadingHTTPServer(("127.0.0.1", 18001), H)
        threading.Thread(target=srv2.serve_forever, daemon=True).start()
        evil.goto("http://127.0.0.1:18001/")
        ok = evil.evaluate(JS, "ws://127.0.0.1:18080/ws?match=room2&player=evil")
        print("foreign origin accepted?", ok)
        assert ok is False, "a foreign origin was allowed to connect"
        srv2.shutdown()
        b.close()
    print("BROWSER E2E: PASS")
finally:
    for p in procs:
        try: p.kill()
        except Exception: pass
