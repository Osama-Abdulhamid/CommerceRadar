"""Internal, authenticated quality-check runner."""

import hmac
import json
import os
import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

lock = threading.Lock()


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, body):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path == "/health":
            self.reply(200, {"status": "ok"})
        else:
            self.reply(404, {"detail": "Not found"})

    def do_POST(self):
        expected = os.environ.get("INTERNAL_SERVICE_KEY", "")
        supplied = self.headers.get("X-Service-Key", "")
        if not expected or not hmac.compare_digest(supplied, expected):
            self.reply(401, {"detail": "Invalid service key"})
            return
        if self.path != "/run":
            self.reply(404, {"detail": "Not found"})
            return
        if not lock.acquire(blocking=False):
            self.reply(409, {"detail": "Quality check already running"})
            return
        try:
            result = subprocess.run(
                ["python", "/app/check_data.py"],
                capture_output=True, text=True, timeout=240,
            )
            print(result.stdout, flush=True)
            if result.returncode:
                print(result.stderr, flush=True)
                self.reply(500, {"success": False, "detail": "Quality check failed"})
            else:
                self.reply(200, {"success": True, "report": "/reports/latest.json"})
        except subprocess.TimeoutExpired:
            self.reply(504, {"success": False, "detail": "Quality check timed out"})
        finally:
            lock.release()


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
