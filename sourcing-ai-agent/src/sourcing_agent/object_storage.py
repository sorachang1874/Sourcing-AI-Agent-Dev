from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
from pathlib import Path
from typing import Protocol
from urllib.parse import quote, urlsplit

import requests


class ObjectStorageError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class ObjectStorageConfig:
    enabled: bool = False
    provider: str = "filesystem"
    bucket: str = ""
    prefix: str = "sourcing-ai-agent-dev"
    endpoint_url: str = ""
    region: str = "us-east-1"
    access_key_id: str = ""
    secret_access_key: str = ""
    timeout_seconds: int = 60
    force_path_style: bool = True
    local_dir: str = ""


class ObjectStorageClient(Protocol):
    def upload_file(self, local_path: str | Path, object_key: str, *, content_type: str = "application/octet-stream") -> dict: ...

    def download_file(self, object_key: str, local_path: str | Path) -> dict: ...

    def object_url(self, object_key: str) -> str: ...


def build_object_storage_client(config: ObjectStorageConfig) -> ObjectStorageClient:
    provider = (config.provider or "").strip().lower()
    if provider in {"", "filesystem"}:
        return FilesystemObjectStorageClient(config)
    if provider in {"s3", "s3_compatible", "oss_s3"}:
        return S3CompatibleObjectStorageClient(config)
    raise ObjectStorageError(f"Unsupported object storage provider: {config.provider}")


class FilesystemObjectStorageClient:
    def __init__(self, config: ObjectStorageConfig) -> None:
        if not config.local_dir:
            raise ObjectStorageError("Filesystem object storage requires local_dir")
        self.config = config
        self.root = Path(config.local_dir).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def upload_file(self, local_path: str | Path, object_key: str, *, content_type: str = "application/octet-stream") -> dict:
        source = Path(local_path)
        if not source.exists():
            raise ObjectStorageError(f"Upload source not found: {source}")
        destination = self.root / _join_key(self.config.prefix, object_key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())
        return {
            "status": "uploaded",
            "provider": "filesystem",
            "object_key": object_key,
            "object_url": self.object_url(object_key),
            "content_type": content_type,
            "size_bytes": destination.stat().st_size,
        }

    def download_file(self, object_key: str, local_path: str | Path) -> dict:
        source = self.root / _join_key(self.config.prefix, object_key)
        if not source.exists():
            raise ObjectStorageError(f"Object not found: {object_key}")
        destination = Path(local_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(source.read_bytes())
        return {
            "status": "downloaded",
            "provider": "filesystem",
            "object_key": object_key,
            "object_url": self.object_url(object_key),
            "size_bytes": destination.stat().st_size,
        }

    def object_url(self, object_key: str) -> str:
        return f"file://{(self.root / _join_key(self.config.prefix, object_key)).as_posix()}"


class S3CompatibleObjectStorageClient:
    def __init__(self, config: ObjectStorageConfig) -> None:
        missing = [
            name
            for name, value in (
                ("bucket", config.bucket),
                ("endpoint_url", config.endpoint_url),
                ("access_key_id", config.access_key_id),
                ("secret_access_key", config.secret_access_key),
            )
            if not value
        ]
        if missing:
            raise ObjectStorageError(f"S3-compatible object storage missing config: {', '.join(missing)}")
        self.config = config
        self.session = requests.Session()

    def upload_file(self, local_path: str | Path, object_key: str, *, content_type: str = "application/octet-stream") -> dict:
        source = Path(local_path)
        if not source.exists():
            raise ObjectStorageError(f"Upload source not found: {source}")
        payload = source.read_bytes()
        url, signed_headers = self._signed_request(
            method="PUT",
            object_key=object_key,
            payload=payload,
            extra_headers={"content-type": content_type},
        )
        response = self.session.put(url, data=payload, headers=signed_headers, timeout=self.config.timeout_seconds)
        if response.status_code not in {200, 201, 204}:
            raise ObjectStorageError(f"Upload failed for {object_key}: {response.status_code} {response.text[:300]}")
        return {
            "status": "uploaded",
            "provider": "s3_compatible",
            "object_key": object_key,
            "object_url": self.object_url(object_key),
            "content_type": content_type,
            "size_bytes": len(payload),
        }

    def download_file(self, object_key: str, local_path: str | Path) -> dict:
        url, signed_headers = self._signed_request(method="GET", object_key=object_key, payload=b"")
        response = self.session.get(url, headers=signed_headers, timeout=self.config.timeout_seconds)
        if response.status_code != 200:
            raise ObjectStorageError(f"Download failed for {object_key}: {response.status_code} {response.text[:300]}")
        destination = Path(local_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(response.content)
        return {
            "status": "downloaded",
            "provider": "s3_compatible",
            "object_key": object_key,
            "object_url": self.object_url(object_key),
            "size_bytes": destination.stat().st_size,
        }

    def object_url(self, object_key: str) -> str:
        return self._build_url(object_key)

    def _build_url(self, object_key: str) -> str:
        endpoint = self.config.endpoint_url.rstrip("/")
        parsed = urlsplit(endpoint)
        key_path = quote(_join_key(self.config.prefix, object_key), safe="/-_.~")
        base_path = parsed.path.rstrip("/")
        if self.config.force_path_style:
            path = "/".join(part for part in [base_path, self.config.bucket, key_path] if part)
            return f"{parsed.scheme}://{parsed.netloc}/{path.lstrip('/')}"
        host = parsed.netloc
        if not host.startswith(f"{self.config.bucket}."):
            host = f"{self.config.bucket}.{host}"
        path = "/".join(part for part in [base_path, key_path] if part)
        return f"{parsed.scheme}://{host}/{path.lstrip('/')}"

    def _signed_request(self, *, method: str, object_key: str, payload: bytes, extra_headers: dict[str, str] | None = None) -> tuple[str, dict[str, str]]:
        url = self._build_url(object_key)
        parsed = urlsplit(url)
        timestamp = datetime.now(timezone.utc)
        amz_date = timestamp.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = timestamp.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(payload).hexdigest()
        headers = {
            "host": parsed.netloc,
            "x-amz-content-sha256": payload_hash,
            "x-amz-date": amz_date,
        }
        if extra_headers:
            for key, value in extra_headers.items():
                headers[key.lower()] = value
        signed_header_names = sorted(headers.keys())
        canonical_headers = "".join(f"{name}:{_normalize_header_value(headers[name])}\n" for name in signed_header_names)
        signed_headers = ";".join(signed_header_names)
        canonical_request = "\n".join(
            [
                method,
                parsed.path or "/",
                parsed.query or "",
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )
        credential_scope = f"{date_stamp}/{self.config.region}/s3/aws4_request"
        string_to_sign = "\n".join(
            [
                "AWS4-HMAC-SHA256",
                amz_date,
                credential_scope,
                hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
            ]
        )
        signing_key = _aws_v4_signing_key(self.config.secret_access_key, date_stamp, self.config.region, "s3")
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        authorization = (
            "AWS4-HMAC-SHA256 "
            f"Credential={self.config.access_key_id}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )
        final_headers = {name.title(): value for name, value in headers.items()}
        final_headers["Authorization"] = authorization
        return url, final_headers


def _normalize_header_value(value: str) -> str:
    return " ".join(value.strip().split())


def _aws_v4_signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    key_date = hmac.new(("AWS4" + secret_key).encode("utf-8"), date_stamp.encode("utf-8"), hashlib.sha256).digest()
    key_region = hmac.new(key_date, region.encode("utf-8"), hashlib.sha256).digest()
    key_service = hmac.new(key_region, service.encode("utf-8"), hashlib.sha256).digest()
    return hmac.new(key_service, b"aws4_request", hashlib.sha256).digest()


def _join_key(prefix: str, object_key: str) -> str:
    return "/".join(part.strip("/") for part in (prefix, object_key) if part and part.strip("/"))
