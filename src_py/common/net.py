import json
import logging
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler
from urllib.error import URLError

logger = logging.getLogger(__name__)


def send_message(host, port, payload, retries=3, timeout=5):
    last_error = None
    for attempt in range(retries):
        try:
            conn = HTTPConnection(host, port, timeout=timeout)
            conn.request("POST", "/", json.dumps(payload), {"Content-Type": "application/json"})
            response = conn.getresponse()
            raw = response.read()
            result = json.loads(raw.decode()) if raw else {}
            return result
        except (OSError, URLError, ConnectionError) as e:
            last_error = e
        finally:
            conn.close()
    logger.error(f"All {retries} attempts failed for {host}:{port}")
    raise last_error


def make_handler(message_handler):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            pass  # Suppress default logging

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b""
            request = json.loads(raw.decode()) if length else {}
            response = message_handler(request)
            body = json.dumps(response)
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", len(body))
            self.end_headers()
            self.wfile.write(body.encode())

    return Handler
