#!/usr/bin/env python3
"""Open an ECS-hosted reverse tunnel for local provider webhook smoke tests."""

from __future__ import annotations

import argparse
import json
import select
import socket
import sys
import threading
from pathlib import Path
from typing import Any

import paramiko

DEFAULT_DEPLOYMENT_SECRET = Path("runtime/secrets/deployment.local.json")
DEFAULT_REMOTE_BIND_HOST = "127.0.0.1"
DEFAULT_REMOTE_PORT = 18765
DEFAULT_LOCAL_HOST = "127.0.0.1"
DEFAULT_LOCAL_PORT = 8765
DEFAULT_PUBLIC_WEBHOOK_PATH = "/local-dev/providers/apify/webhook"


def _load_ecs_config(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    ecs = payload.get("aliyun_ecs")
    if not isinstance(ecs, dict):
        raise SystemExit(f"{path} does not contain aliyun_ecs credentials")
    if not ecs.get("public_ip") or not ecs.get("login_user"):
        raise SystemExit(f"{path} is missing aliyun_ecs.public_ip/login_user")
    if not ecs.get("password"):
        raise SystemExit(
            f"{path} is missing aliyun_ecs.password; configure SSH keys or add a password"
        )
    return ecs


def _load_public_origin(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return "https://api.111874.xyz"
    cloudflare = payload.get("cloudflare_pages")
    if isinstance(cloudflare, dict):
        origin = str(cloudflare.get("backend_api_origin") or "").strip().rstrip("/")
        if origin:
            return origin
    return "https://api.111874.xyz"


def _forward_channel(channel: paramiko.Channel, local_host: str, local_port: int) -> None:
    try:
        local_sock = socket.create_connection((local_host, local_port), timeout=10)
    except OSError as exc:
        print(
            f"failed to connect local target {local_host}:{local_port}: {exc}",
            file=sys.stderr,
            flush=True,
        )
        channel.close()
        return

    with local_sock, channel:
        while True:
            readable, _, _ = select.select([channel, local_sock], [], [], 1.0)
            if channel in readable:
                data = channel.recv(16384)
                if not data:
                    break
                local_sock.sendall(data)
            if local_sock in readable:
                data = local_sock.recv(16384)
                if not data:
                    break
                channel.sendall(data)


def run_tunnel(args: argparse.Namespace) -> None:
    secret_path = Path(args.deployment_secret)
    ecs = _load_ecs_config(secret_path)
    public_origin = str(args.public_origin or _load_public_origin(secret_path)).rstrip("/")
    public_url = f"{public_origin}{args.public_path}"

    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(
        str(ecs["public_ip"]),
        username=str(ecs["login_user"]),
        password=str(ecs["password"]),
        port=int(ecs.get("port") or 22),
        timeout=20,
        banner_timeout=20,
        auth_timeout=20,
    )
    transport = client.get_transport()
    if transport is None:
        raise SystemExit("SSH transport was not established")
    transport.set_keepalive(30)
    transport.request_port_forward(args.remote_bind_host, args.remote_port)
    print(
        json.dumps(
            {
                "status": "listening",
                "public_webhook_url": public_url,
                "remote_forward": f"{args.remote_bind_host}:{args.remote_port}",
                "local_target": f"{args.local_host}:{args.local_port}",
            },
            ensure_ascii=False,
        ),
        flush=True,
    )

    try:
        while True:
            channel = transport.accept(timeout=1.0)
            if channel is None:
                continue
            thread = threading.Thread(
                target=_forward_channel,
                args=(channel, args.local_host, args.local_port),
                daemon=True,
            )
            thread.start()
    except KeyboardInterrupt:
        print("stopping ECS webhook reverse tunnel", flush=True)
    finally:
        try:
            transport.cancel_port_forward(args.remote_bind_host, args.remote_port)
        finally:
            client.close()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Forward ECS loopback traffic to the local backend so Apify can call a "
            "stable ECS URL during local webhook smoke tests."
        )
    )
    parser.add_argument(
        "--deployment-secret",
        default=str(DEFAULT_DEPLOYMENT_SECRET),
        help="Path to runtime/secrets/deployment.local.json",
    )
    parser.add_argument("--remote-bind-host", default=DEFAULT_REMOTE_BIND_HOST)
    parser.add_argument("--remote-port", type=int, default=DEFAULT_REMOTE_PORT)
    parser.add_argument("--local-host", default=DEFAULT_LOCAL_HOST)
    parser.add_argument("--local-port", type=int, default=DEFAULT_LOCAL_PORT)
    parser.add_argument("--public-origin", default="")
    parser.add_argument("--public-path", default=DEFAULT_PUBLIC_WEBHOOK_PATH)
    return parser


def main() -> None:
    run_tunnel(build_parser().parse_args())


if __name__ == "__main__":
    main()
