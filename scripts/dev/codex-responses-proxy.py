#!/usr/bin/env python3
"""Development-only bridge from Sumi's Responses adapter to Codex OAuth.

The ChatGPT Codex backend intentionally differs from the public Responses API.
This bridge keeps those differences out of Sumi's production provider model:
it reads the local Codex login, injects the Codex headers, and removes request
fields that the subscription endpoint does not accept.

It binds to loopback only and never logs credentials or request bodies.

A single cryptographically strong shared secret is generated at startup and
required on every request before the request body is read. The Sumi adapter
sends the ordinary `Authorization: Bearer <proxy-secret>` header; the bridge
validates the bearer token in constant time before reading the body, then
replaces it with Codex OAuth credentials only for the upstream request.
"""

from __future__ import annotations

import argparse
import hmac
import json
import os
import pathlib
import secrets
import socket
import stat
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.client import HTTPConnection
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any


UPSTREAM = "https://chatgpt.com/backend-api/codex/responses"
AUTH_HEADER = "Authorization"
MAX_CAPTURE_NAME_ATTEMPTS = 8


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
    expected_secret: str = ""
    upstream_url: str = UPSTREAM

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _reject(self, status: int, message: str) -> None:
        detail = json.dumps({"error": message}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(detail)))
        self.end_headers()
        self.wfile.write(detail)

    def _bearer_token(self) -> str | None:
        header = self.headers.get(AUTH_HEADER, "")
        if not header:
            return None
        parts = header.split(None, 1)
        if len(parts) != 2:
            return None
        scheme, token = parts
        if len(scheme) != 6 or not hmac.compare_digest(scheme.lower(), "bearer"):
            return None
        return token

    def _open_capture(self):
        assert self.capture_dir is not None
        self.capture_dir.mkdir(parents=True, mode=0o700, exist_ok=True)
        if stat.S_IMODE(self.capture_dir.stat().st_mode) & 0o077:
            raise RuntimeError("capture directory must not be group/world accessible")
        for _ in range(MAX_CAPTURE_NAME_ATTEMPTS):
            capture_path = self.capture_dir / f"response-{uuid.uuid4()}.sse"
            try:
                fd = os.open(
                    capture_path,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                    0o600,
                )
            except FileExistsError:
                continue
            return os.fdopen(fd, "wb")
        raise RuntimeError("could not allocate an exclusive capture file")

    def do_POST(self) -> None:
        if self.path not in ("/responses", "/v1/responses"):
            self.send_error(404)
            return

        if not self.expected_secret:
            self._reject(401, "missing or invalid secret")
            return
        secret = self._bearer_token()
        if secret is None:
            self._reject(401, "missing or invalid secret")
            return
        if not hmac.compare_digest(secret, self.expected_secret):
            self._reject(401, "missing or invalid secret")
            return

        if self.headers.get("content-type") != "application/json":
            self._reject(415, "content-type must be application/json")
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
                self.upstream_url,
                data=json.dumps(request_body, separators=(",", ":")).encode(),
                headers=headers,
                method="POST",
            )
            upstream = urllib.request.urlopen(upstream_request, timeout=300)
        except urllib.error.HTTPError as error:
            detail = error.read(4096)
            error.close()
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

        capture = None
        try:
            if self.capture_dir is not None:
                capture = self._open_capture()
        except Exception as error:
            upstream.close()
            detail = json.dumps({"error": type(error).__name__}).encode()
            self.send_response(500)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(detail)))
            self.end_headers()
            self.wfile.write(detail)
            return

        self.send_response(upstream.status)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()

        try:
            while chunk := upstream.read(16 * 1024):
                if capture is not None:
                    capture.write(chunk)
                self.wfile.write(chunk)
                self.wfile.flush()
        except (ConnectionResetError, BrokenPipeError):
            pass
        finally:
            if capture is not None:
                capture.close()
            try:
                upstream.close()
            except Exception:
                pass


def _start_server(
    port: int,
    auth_path: pathlib.Path,
    capture_dir: pathlib.Path | None,
    secret: str,
    upstream_url: str,
) -> ThreadingHTTPServer:
    ResponsesBridge.auth_path = auth_path
    ResponsesBridge.capture_dir = capture_dir
    ResponsesBridge.expected_secret = secret
    ResponsesBridge.upstream_url = upstream_url
    server = ThreadingHTTPServer(("127.0.0.1", port), ResponsesBridge)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server


def _request(
    server: ThreadingHTTPServer,
    headers: dict[str, str] | None = None,
    body: bytes = b"{}",
    path: str = "/v1/responses",
    read_first_line: bool = False,
) -> tuple[int, bytes]:
    conn = HTTPConnection(*server.server_address)
    all_headers = headers or {}
    conn.request("POST", path, body=body, headers=all_headers)
    response = conn.getresponse()
    if read_first_line:
        first = response.readline()
        response.close()
        return response.status, first
    data = response.read()
    response.close()
    return response.status, data


def _self_test_auth(path: pathlib.Path) -> None:
    path.write_text(
        json.dumps({"access_token": "test-token", "account_id": "test-account"}),
        encoding="utf-8",
    )


class _SelfTestUpstream(BaseHTTPRequestHandler):
    requests: list[dict[str, str]] = []
    bodies: list[dict[str, Any]] = []

    def log_message(self, format: str, *args: Any) -> None:
        return

    def do_POST(self) -> None:
        length = int(self.headers.get("content-length", "0"))
        body = json.loads(self.rfile.read(length))
        self.__class__.requests.append(dict(self.headers.items()))
        self.__class__.bodies.append(body)
        body = b"data: first\n\ndata: second\n\n"
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        try:
            self.wfile.write(body)
            self.wfile.flush()
        except (ConnectionResetError, BrokenPipeError):
            pass


def _reject_without_body(server: ThreadingHTTPServer) -> bytes:
    """Prove unauthenticated requests are answered before their body is read."""
    with socket.create_connection(server.server_address, timeout=1) as connection:
        connection.settimeout(1)
        connection.sendall(
            b"POST /v1/responses HTTP/1.0\r\n"
            b"Host: 127.0.0.1\r\n"
            b"Content-Type: application/json\r\n"
            b"Content-Length: 65536\r\n"
            b"\r\n"
        )
        return connection.recv(4096)


def _run_self_test() -> int:
    failures: list[str] = []

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = pathlib.Path(tmpdir)
        auth_path = tmp / "auth.json"
        _self_test_auth(auth_path)
        capture_dir = tmp / "captures"
        secret = secrets.token_urlsafe(32)

        # Start a mock upstream Codex endpoint.
        _SelfTestUpstream.requests = []
        _SelfTestUpstream.bodies = []
        upstream_server = ThreadingHTTPServer(("127.0.0.1", 0), _SelfTestUpstream)
        upstream_thread = threading.Thread(target=upstream_server.serve_forever, daemon=True)
        upstream_thread.start()
        upstream_url = f"http://127.0.0.1:{upstream_server.server_address[1]}/v1/responses"

        proxy_server = _start_server(0, auth_path, capture_dir, secret, upstream_url)
        def check(name: str, ok: bool, detail: str = "") -> None:
            if not ok:
                failures.append(f"{name}: {detail}")
                print(f"FAIL {name}", flush=True)
            else:
                print(f"PASS {name}", flush=True)

        # Missing Authorization: must fail before reading the body.
        response = _reject_without_body(proxy_server)
        check("missing_authorization_rejected_before_body", response.startswith(b"HTTP/1.0 401"))
        status, _ = _request(proxy_server, {"content-type": "application/json"}, b"{}")
        check("missing_authorization_rejected", status == 401, f"status={status}")

        # Malformed Authorization scheme.
        status, _ = _request(
            proxy_server,
            {
                "Authorization": "Basic wrong",
                "content-type": "application/json",
            },
            b"{}",
        )
        check("malformed_authorization_scheme_rejected", status == 401, f"status={status}")

        # Wrong secret.
        status, _ = _request(
            proxy_server,
            {
                "Authorization": "Bearer wrong-secret",
                "content-type": "application/json",
            },
            b"{}",
        )
        check("wrong_secret_rejected", status == 401, f"status={status}")
        check(
            "rejected_requests_never_reach_upstream",
            not _SelfTestUpstream.requests,
            f"requests={len(_SelfTestUpstream.requests)}",
        )

        # Correct secret but wrong content-type: rejected before body is read.
        status, _ = _request(
            proxy_server,
            {
                "Authorization": f"Bearer {secret}",
                "content-type": "text/plain",
            },
            b"not json",
        )
        check("content_type_enforced", status == 415, f"status={status}")

        # Correct secret and content-type.
        status, body = _request(
            proxy_server,
            {
                "Authorization": f"Bearer {secret}",
                "content-type": "application/json",
            },
            b'{"max_output_tokens": 1}',
        )
        check("correct_secret_accepted", status == 200, f"status={status}")
        check("response_streamed", b"data: first" in body and b"data: second" in body)
        check(
            "proxy_secret_replaced_for_upstream_only",
            len(_SelfTestUpstream.requests) == 1
            and _SelfTestUpstream.requests[0].get("Authorization") == "Bearer test-token",
            f"requests={len(_SelfTestUpstream.requests)}",
        )
        check(
            "subscription_request_shape_is_bounded",
            len(_SelfTestUpstream.bodies) == 1
            and _SelfTestUpstream.bodies[0].get("store") is False
            and _SelfTestUpstream.bodies[0].get("stream") is True
            and "max_output_tokens" not in _SelfTestUpstream.bodies[0],
        )

        # Capture files are unique and exclusive.
        captures = sorted(capture_dir.iterdir())
        check("single_capture_written", len(captures) == 1, f"count={len(captures)}")
        check(
            "captures_are_private",
            stat.S_IMODE(capture_dir.stat().st_mode) == 0o700
            and len(captures) == 1
            and stat.S_IMODE(captures[0].stat().st_mode) == 0o600,
        )

        status, body = _request(
            proxy_server,
            {
                "Authorization": f"Bearer {secret}",
                "content-type": "application/json",
            },
            b'{"max_output_tokens": 2}',
        )
        check("second_request_accepted", status == 200, f"status={status}")
        captures = sorted(capture_dir.iterdir())
        check(
            "unique_exclusive_captures",
            len(captures) == 2 and captures[0] != captures[1],
            f"count={len(captures)}",
        )

        # Client disconnect before the full stream: cleanup must close the capture.
        status, first = _request(
            proxy_server,
            {
                "Authorization": f"Bearer {secret}",
                "content-type": "application/json",
            },
            b'{"max_output_tokens": 3}',
            read_first_line=True,
        )
        check("disconnect_partial_read", status == 200 and first.startswith(b"data:") and not first.startswith(b"data: second"))
        # Give the proxy thread time to run its finally block.
        time.sleep(0.2)
        captures_after = sorted(capture_dir.iterdir())
        check("disconnect_cleanup_file_closed", len(captures_after) == 3)

        proxy_server.shutdown()
        proxy_server.server_close()
        upstream_server.shutdown()
        upstream_server.server_close()

    if failures:
        print("\n".join(failures), file=sys.stderr, flush=True)
        return 1
    print("All proxy self-tests passed", flush=True)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument(
        "--auth-file",
        type=pathlib.Path,
        default=pathlib.Path.home() / ".codex" / "auth.json",
    )
    parser.add_argument("--capture-dir", type=pathlib.Path)
    parser.add_argument(
        "--secret",
        help="Shared secret required by every request. Generated if not provided.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Run an internal self-test and exit.",
    )
    args = parser.parse_args()

    if args.self_test:
        return _run_self_test()

    if args.secret:
        secret = args.secret
    else:
        secret = secrets.token_urlsafe(32)
        print(f"PROXY_SECRET={secret}", flush=True)

    ResponsesBridge.auth_path = args.auth_file
    ResponsesBridge.capture_dir = args.capture_dir
    ResponsesBridge.expected_secret = secret
    ResponsesBridge.upstream_url = UPSTREAM
    server = ThreadingHTTPServer(("127.0.0.1", args.port), ResponsesBridge)
    print(f"LISTENING http://127.0.0.1:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    sys.exit(main())
