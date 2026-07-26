#!/usr/bin/env python3
"""Development-only bridge from Sumi's Responses adapter to Codex OAuth.

The ChatGPT Codex backend intentionally differs from the public Responses API.
This bridge keeps those differences out of Sumi's production provider model:
it reads the local Codex login, injects the Codex headers, and removes request
fields that the subscription endpoint does not accept.

It binds to loopback only and never logs credentials or request bodies.
"""

from __future__ import annotations

import argparse
import json
import pathlib
import sys
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


UPSTREAM = "https://chatgpt.com/backend-api/codex/responses"


def load_codex_auth(path: pathlib.Path) -> tuple[str, str]:
    document = json.loads(path.read_text(encoding="utf-8"))
    tokens = document.get("tokens", document)
    access_token = tokens.get("access_token")
    account_id = tokens.get("account_id") or document.get("account_id")
    if not isinstance(access_token, str) or not access_token:
        raise RuntimeError(f"missing access_token in {path}")
    if not isinstance(account_id, str) or not account_id:
        raise RuntimeError(f"missing account_id in {path}")
    return access_token, account_id


class ResponsesBridge(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.0"
    auth_path: pathlib.Path
    capture_dir: pathlib.Path | None = None
    request_count = 0

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:
        if self.path not in ("/responses", "/v1/responses"):
            self.send_error(404)
            return

        try:
            length = int(self.headers.get("content-length", "0"))
            request_body = json.loads(self.rfile.read(length))
            if not isinstance(request_body, dict):
                raise ValueError("request body must be an object")

            # The subscription endpoint manages the output budget itself.
            request_body.pop("max_output_tokens", None)
            request_body["stream"] = True
            request_body["store"] = False

            access_token, account_id = load_codex_auth(self.auth_path)
            headers = {
                "Authorization": f"Bearer {access_token}",
                "ChatGPT-Account-Id": account_id,
                "originator": "codex_cli_rs",
                "Content-Type": "application/json",
                "Accept": "text/event-stream",
                "User-Agent": "codex-cli-rs/sumi-dev-bridge",
                "session-id": str(uuid.uuid4()),
                "thread-id": str(uuid.uuid4()),
            }
            upstream_request = urllib.request.Request(
                UPSTREAM,
                data=json.dumps(request_body, separators=(",", ":")).encode(),
                headers=headers,
                method="POST",
            )
            upstream = urllib.request.urlopen(upstream_request, timeout=300)
        except urllib.error.HTTPError as error:
            detail = error.read(4096)
            self.send_response(error.code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(detail)))
            self.end_headers()
            self.wfile.write(detail)
            return
        except Exception as error:
            detail = json.dumps({"error": type(error).__name__}).encode()
            self.send_response(502)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(detail)))
            self.end_headers()
            self.wfile.write(detail)
            return

        self.send_response(upstream.status)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        capture = None
        if self.capture_dir is not None:
            type(self).request_count += 1
            self.capture_dir.mkdir(parents=True, exist_ok=True)
            capture = (self.capture_dir / f"response-{self.request_count}.sse").open("wb")
        while chunk := upstream.read(16 * 1024):
            if capture is not None:
                capture.write(chunk)
            self.wfile.write(chunk)
            self.wfile.flush()
        if capture is not None:
            capture.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--auth-file",
        type=pathlib.Path,
        default=pathlib.Path.home() / ".codex" / "auth.json",
    )
    parser.add_argument("--capture-dir", type=pathlib.Path)
    args = parser.parse_args()
    ResponsesBridge.auth_path = args.auth_file
    ResponsesBridge.capture_dir = args.capture_dir
    server = ThreadingHTTPServer(("127.0.0.1", args.port), ResponsesBridge)
    print(f"LISTENING http://127.0.0.1:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
