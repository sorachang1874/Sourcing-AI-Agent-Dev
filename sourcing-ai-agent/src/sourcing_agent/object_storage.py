from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import hmac
import json
import math
from pathlib import Path
import threading
from typing import Protocol
from urllib.parse import quote, urlsplit
from xml.etree import ElementTree

import requests


class ObjectStorageError(RuntimeError):
    pass


class ObjectStorageNotFoundError(ObjectStorageError):
    pass


class MultipartUploadMissingError(ObjectStorageError):
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
    max_workers: int = 8
    multipart_threshold_bytes: int = 64 * 1024 * 1024
    multipart_chunk_size_bytes: int = 64 * 1024 * 1024
    multipart_max_workers: int = 4


class ObjectStorageClient(Protocol):
    def upload_file(self, local_path: str | Path, object_key: str, *, content_type: str = "application/octet-stream") -> dict: ...

    def download_file(self, object_key: str, local_path: str | Path) -> dict: ...

    def upload_bytes(self, payload: bytes, object_key: str, *, content_type: str = "application/octet-stream") -> dict: ...

    def download_bytes(self, object_key: str) -> bytes: ...

    def has_object(self, object_key: str) -> bool: ...

    def delete_object(self, object_key: str) -> dict: ...

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
        return self.upload_bytes(source.read_bytes(), object_key, content_type=content_type)

    def upload_bytes(self, payload: bytes, object_key: str, *, content_type: str = "application/octet-stream") -> dict:
        destination = self.root / _join_key(self.config.prefix, object_key)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        return {
            "status": "uploaded",
            "provider": "filesystem",
            "object_key": object_key,
            "object_url": self.object_url(object_key),
            "content_type": content_type,
            "size_bytes": destination.stat().st_size,
        }

    def download_file(self, object_key: str, local_path: str | Path) -> dict:
        payload = self.download_bytes(object_key)
        destination = Path(local_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        return {
            "status": "downloaded",
            "provider": "filesystem",
            "object_key": object_key,
            "object_url": self.object_url(object_key),
            "size_bytes": destination.stat().st_size,
        }

    def download_bytes(self, object_key: str) -> bytes:
        source = self.root / _join_key(self.config.prefix, object_key)
        if not source.exists():
            raise ObjectStorageNotFoundError(f"Object not found: {object_key}")
        return source.read_bytes()

    def has_object(self, object_key: str) -> bool:
        return (self.root / _join_key(self.config.prefix, object_key)).exists()

    def delete_object(self, object_key: str) -> dict:
        target = self.root / _join_key(self.config.prefix, object_key)
        if not target.exists():
            return {
                "status": "missing",
                "provider": "filesystem",
                "object_key": object_key,
                "object_url": self.object_url(object_key),
            }
        target.unlink()
        return {
            "status": "deleted",
            "provider": "filesystem",
            "object_key": object_key,
            "object_url": self.object_url(object_key),
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
        self._thread_local = threading.local()
        self._multipart_chunk_size_bytes = max(5 * 1024 * 1024, int(config.multipart_chunk_size_bytes or 0))
        self._multipart_threshold_bytes = max(self._multipart_chunk_size_bytes, int(config.multipart_threshold_bytes or 0))
        self._multipart_max_workers = max(1, int(config.multipart_max_workers or 0))

    def upload_file(self, local_path: str | Path, object_key: str, *, content_type: str = "application/octet-stream") -> dict:
        source = Path(local_path)
        if not source.exists():
            raise ObjectStorageError(f"Upload source not found: {source}")
        size_bytes = source.stat().st_size
        if size_bytes >= self._multipart_threshold_bytes:
            return self._multipart_upload_file(source, object_key, content_type=content_type, size_bytes=size_bytes)
        return self.upload_bytes(source.read_bytes(), object_key, content_type=content_type)

    def upload_bytes(self, payload: bytes, object_key: str, *, content_type: str = "application/octet-stream") -> dict:
        url, signed_headers = self._signed_request(
            method="PUT",
            object_key=object_key,
            payload=payload,
            extra_headers={"content-type": content_type},
        )
        response = self._session().put(url, data=payload, headers=signed_headers, timeout=self.config.timeout_seconds)
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
        payload = self.download_bytes(object_key)
        destination = Path(local_path)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(payload)
        return {
            "status": "downloaded",
            "provider": "s3_compatible",
            "object_key": object_key,
            "object_url": self.object_url(object_key),
            "size_bytes": destination.stat().st_size,
        }

    def download_bytes(self, object_key: str) -> bytes:
        url, signed_headers = self._signed_request(method="GET", object_key=object_key, payload=b"")
        response = self._session().get(url, headers=signed_headers, timeout=self.config.timeout_seconds)
        if response.status_code == 404:
            raise ObjectStorageNotFoundError(f"Object not found: {object_key}")
        if response.status_code != 200:
            raise ObjectStorageError(f"Download failed for {object_key}: {response.status_code} {response.text[:300]}")
        return response.content

    def has_object(self, object_key: str) -> bool:
        url, signed_headers = self._signed_request(method="HEAD", object_key=object_key, payload=b"")
        response = self._session().head(url, headers=signed_headers, timeout=self.config.timeout_seconds)
        if response.status_code == 404:
            return False
        if response.status_code in {200, 204}:
            return True
        raise ObjectStorageError(f"HEAD failed for {object_key}: {response.status_code} {response.text[:300]}")

    def delete_object(self, object_key: str) -> dict:
        url, signed_headers = self._signed_request(method="DELETE", object_key=object_key, payload=b"")
        response = self._session().delete(url, headers=signed_headers, timeout=self.config.timeout_seconds)
        if response.status_code == 404:
            return {
                "status": "missing",
                "provider": "s3_compatible",
                "object_key": object_key,
                "object_url": self.object_url(object_key),
            }
        if response.status_code not in {200, 202, 204}:
            raise ObjectStorageError(f"Delete failed for {object_key}: {response.status_code} {response.text[:300]}")
        return {
            "status": "deleted",
            "provider": "s3_compatible",
            "object_key": object_key,
            "object_url": self.object_url(object_key),
        }

    def object_url(self, object_key: str) -> str:
        return self._build_url(object_key)

    def _session(self) -> requests.Session:
        session = getattr(self._thread_local, "session", None)
        if session is None:
            session = requests.Session()
            self._thread_local.session = session
        return session

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

    def _signed_request(
        self,
        *,
        method: str,
        object_key: str,
        payload: bytes,
        extra_headers: dict[str, str] | None = None,
        query_params: list[tuple[str, str]] | None = None,
        payload_hash_override: str = "",
    ) -> tuple[str, dict[str, str]]:
        base_url = self._build_url(object_key)
        canonical_query = _canonical_query_string(query_params or [])
        url = base_url if not canonical_query else f"{base_url}?{canonical_query}"
        parsed = urlsplit(url)
        timestamp = datetime.now(timezone.utc)
        amz_date = timestamp.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = timestamp.strftime("%Y%m%d")
        payload_hash = payload_hash_override or hashlib.sha256(payload).hexdigest()
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
                canonical_query,
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

    def _multipart_upload_file(
        self,
        source: Path,
        object_key: str,
        *,
        content_type: str,
        size_bytes: int,
    ) -> dict:
        checkpoint_path = self._multipart_checkpoint_path(source, object_key)
        upload_id = ""
        part_count = max(1, int(math.ceil(size_bytes / self._multipart_chunk_size_bytes)))
        checkpoint_payload = self._load_multipart_checkpoint(
            checkpoint_path,
            source=source,
            object_key=object_key,
            size_bytes=size_bytes,
        )
        uploaded_parts: dict[int, str] = {}
        try:
            if checkpoint_payload:
                upload_id = str(checkpoint_payload.get("upload_id") or "").strip()
                try:
                    uploaded_parts = self._list_multipart_parts(object_key, upload_id)
                except MultipartUploadMissingError:
                    upload_id = ""
                    self._clear_multipart_checkpoint(checkpoint_path)
            if not upload_id:
                upload_id = self._create_multipart_upload(object_key, content_type=content_type)
                self._save_multipart_checkpoint(
                    checkpoint_path,
                    {
                        "version": 1,
                        "bucket": self.config.bucket,
                        "prefix": self.config.prefix,
                        "object_key": object_key,
                        "source_path": str(source),
                        "source_size_bytes": size_bytes,
                        "source_mtime_ns": source.stat().st_mtime_ns,
                        "chunk_size_bytes": self._multipart_chunk_size_bytes,
                        "upload_id": upload_id,
                        "created_at": datetime.now(timezone.utc).isoformat(),
                    },
                )
            completed_parts: list[dict[str, str]] = [
                {"PartNumber": str(part_number), "ETag": etag}
                for part_number, etag in sorted(uploaded_parts.items())
            ]
            missing_part_numbers = [
                part_number
                for part_number in range(1, part_count + 1)
                if part_number not in uploaded_parts
            ]
            max_workers = min(self._multipart_max_workers, part_count)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        self._upload_multipart_part_with_retry,
                        source,
                        object_key,
                        upload_id,
                        part_number,
                        size_bytes,
                    ): part_number
                    for part_number in missing_part_numbers
                }
                for future in as_completed(futures):
                    completed_parts.append(future.result())
            completed_parts.sort(key=lambda item: int(item["PartNumber"]))
            self._complete_multipart_upload(object_key, upload_id, completed_parts)
            self._clear_multipart_checkpoint(checkpoint_path)
            return {
                "status": "uploaded",
                "provider": "s3_compatible",
                "object_key": object_key,
                "object_url": self.object_url(object_key),
                "content_type": content_type,
                "size_bytes": size_bytes,
                "multipart": True,
                "part_count": part_count,
                "resumed": bool(checkpoint_payload),
                "resumed_part_count": len(uploaded_parts),
            }
        except MultipartUploadMissingError:
            if upload_id:
                self._abort_multipart_upload(object_key, upload_id)
            self._clear_multipart_checkpoint(checkpoint_path)
            raise
        except Exception:
            raise

    def _create_multipart_upload(self, object_key: str, *, content_type: str) -> str:
        url, signed_headers = self._signed_request(
            method="POST",
            object_key=object_key,
            payload=b"",
            extra_headers={"content-type": content_type},
            query_params=[("uploads", "")],
        )
        response = self._session().post(url, data=b"", headers=signed_headers, timeout=self.config.timeout_seconds)
        if response.status_code not in {200, 201, 204}:
            raise ObjectStorageError(f"Multipart initiate failed for {object_key}: {response.status_code} {response.text[:300]}")
        upload_id = _find_xml_text(response.content, "UploadId")
        if not upload_id:
            raise ObjectStorageError(f"Multipart initiate missing upload id for {object_key}")
        return upload_id

    def _upload_multipart_part_with_retry(
        self,
        source: Path,
        object_key: str,
        upload_id: str,
        part_number: int,
        size_bytes: int,
    ) -> dict[str, str]:
        last_error: Exception | None = None
        for _attempt in range(1, 4):
            try:
                return self._upload_multipart_part(source, object_key, upload_id, part_number, size_bytes)
            except Exception as exc:  # pragma: no cover - retry branch depends on transport failure
                last_error = exc
        raise ObjectStorageError(
            f"Multipart upload failed for {object_key} part {part_number}: {last_error}"
        ) from last_error

    def _upload_multipart_part(
        self,
        source: Path,
        object_key: str,
        upload_id: str,
        part_number: int,
        size_bytes: int,
    ) -> dict[str, str]:
        offset = (part_number - 1) * self._multipart_chunk_size_bytes
        remaining = max(size_bytes - offset, 0)
        chunk_size = min(self._multipart_chunk_size_bytes, remaining)
        with source.open("rb") as handle:
            handle.seek(offset)
            payload = handle.read(chunk_size)
        if len(payload) != chunk_size:
            raise ObjectStorageError(
                f"Multipart read truncated for {object_key} part {part_number}: expected {chunk_size}, got {len(payload)}"
            )
        url, signed_headers = self._signed_request(
            method="PUT",
            object_key=object_key,
            payload=payload,
            query_params=[("partNumber", str(part_number)), ("uploadId", upload_id)],
        )
        response = self._session().put(url, data=payload, headers=signed_headers, timeout=self.config.timeout_seconds)
        if response.status_code not in {200, 201, 204}:
            raise ObjectStorageError(
                f"Multipart part upload failed for {object_key} part {part_number}: {response.status_code} {response.text[:300]}"
            )
        etag = str(response.headers.get("ETag") or response.headers.get("Etag") or "").strip()
        if not etag:
            raise ObjectStorageError(f"Multipart part upload missing ETag for {object_key} part {part_number}")
        return {"PartNumber": str(part_number), "ETag": etag}

    def _complete_multipart_upload(self, object_key: str, upload_id: str, completed_parts: list[dict[str, str]]) -> None:
        parts_xml = "".join(
            f"<Part><PartNumber>{item['PartNumber']}</PartNumber><ETag>{item['ETag']}</ETag></Part>"
            for item in completed_parts
        )
        payload = (
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
            f"<CompleteMultipartUpload>{parts_xml}</CompleteMultipartUpload>"
        ).encode("utf-8")
        url, signed_headers = self._signed_request(
            method="POST",
            object_key=object_key,
            payload=payload,
            extra_headers={"content-type": "application/xml"},
            query_params=[("uploadId", upload_id)],
        )
        response = self._session().post(url, data=payload, headers=signed_headers, timeout=self.config.timeout_seconds)
        if response.status_code not in {200, 201, 204}:
            raise ObjectStorageError(f"Multipart complete failed for {object_key}: {response.status_code} {response.text[:300]}")
        if _find_xml_text(response.content, "Code"):
            raise ObjectStorageError(f"Multipart complete returned error payload for {object_key}: {response.text[:300]}")

    def _abort_multipart_upload(self, object_key: str, upload_id: str) -> None:
        try:
            url, signed_headers = self._signed_request(
                method="DELETE",
                object_key=object_key,
                payload=b"",
                query_params=[("uploadId", upload_id)],
            )
            self._session().delete(url, headers=signed_headers, timeout=self.config.timeout_seconds)
        except Exception:
            return

    def _list_multipart_parts(self, object_key: str, upload_id: str) -> dict[int, str]:
        part_number_marker = ""
        uploaded_parts: dict[int, str] = {}
        while True:
            query_params = [("uploadId", upload_id), ("max-parts", "1000")]
            if part_number_marker:
                query_params.append(("part-number-marker", part_number_marker))
            url, signed_headers = self._signed_request(
                method="GET",
                object_key=object_key,
                payload=b"",
                query_params=query_params,
            )
            response = self._session().get(url, headers=signed_headers, timeout=self.config.timeout_seconds)
            if response.status_code == 404 or b"NoSuchUpload" in response.content:
                raise MultipartUploadMissingError(f"Multipart upload id missing for {object_key}")
            if response.status_code != 200:
                raise ObjectStorageError(f"Multipart list parts failed for {object_key}: {response.status_code} {response.text[:300]}")
            for part_number, etag in _parse_multipart_parts(response.content):
                uploaded_parts[part_number] = etag
            is_truncated = _find_xml_text(response.content, "IsTruncated").lower() == "true"
            part_number_marker = _find_xml_text(response.content, "NextPartNumberMarker")
            if not is_truncated or not part_number_marker:
                break
        return uploaded_parts

    def _multipart_checkpoint_path(self, source: Path, object_key: str) -> Path:
        digest = hashlib.sha1(
            f"{self.config.bucket}:{self.config.prefix}:{object_key}:{source.resolve()}".encode("utf-8")
        ).hexdigest()[:16]
        return source.parent / f".{source.name}.{digest}.multipart.json"

    def _load_multipart_checkpoint(
        self,
        checkpoint_path: Path,
        *,
        source: Path,
        object_key: str,
        size_bytes: int,
    ) -> dict[str, object]:
        if not checkpoint_path.exists():
            return {}
        try:
            payload = json.loads(checkpoint_path.read_text())
        except (OSError, ValueError, json.JSONDecodeError):
            self._clear_multipart_checkpoint(checkpoint_path)
            return {}
        if (
            str(payload.get("bucket") or "") != self.config.bucket
            or str(payload.get("prefix") or "") != self.config.prefix
            or str(payload.get("object_key") or "") != object_key
            or int(payload.get("source_size_bytes") or 0) != size_bytes
            or int(payload.get("chunk_size_bytes") or 0) != self._multipart_chunk_size_bytes
            or int(payload.get("source_mtime_ns") or 0) != int(source.stat().st_mtime_ns)
        ):
            self._clear_multipart_checkpoint(checkpoint_path)
            return {}
        upload_id = str(payload.get("upload_id") or "").strip()
        if not upload_id:
            self._clear_multipart_checkpoint(checkpoint_path)
            return {}
        return dict(payload)

    def _save_multipart_checkpoint(self, checkpoint_path: Path, payload: dict[str, object]) -> None:
        checkpoint_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n")

    def _clear_multipart_checkpoint(self, checkpoint_path: Path) -> None:
        try:
            checkpoint_path.unlink()
        except FileNotFoundError:
            return


def _normalize_header_value(value: str) -> str:
    return " ".join(value.strip().split())


def _aws_v4_signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    key_date = hmac.new(("AWS4" + secret_key).encode("utf-8"), date_stamp.encode("utf-8"), hashlib.sha256).digest()
    key_region = hmac.new(key_date, region.encode("utf-8"), hashlib.sha256).digest()
    key_service = hmac.new(key_region, service.encode("utf-8"), hashlib.sha256).digest()
    return hmac.new(key_service, b"aws4_request", hashlib.sha256).digest()


def _join_key(prefix: str, object_key: str) -> str:
    return "/".join(part.strip("/") for part in (prefix, object_key) if part and part.strip("/"))


def _canonical_query_string(params: list[tuple[str, str]]) -> str:
    normalized = [(str(key or ""), str(value or "")) for key, value in params if str(key or "")]
    normalized.sort(key=lambda item: (item[0], item[1]))
    return "&".join(
        f"{quote(key, safe='-_.~')}={quote(value, safe='-_.~')}"
        for key, value in normalized
    )


def _find_xml_text(payload: bytes, tag_name: str) -> str:
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError:
        return ""
    for element in root.iter():
        if str(element.tag).split("}")[-1] == tag_name:
            return str(element.text or "").strip()
    return ""


def _parse_multipart_parts(payload: bytes) -> list[tuple[int, str]]:
    try:
        root = ElementTree.fromstring(payload)
    except ElementTree.ParseError:
        return []
    results: list[tuple[int, str]] = []
    for element in root.iter():
        if str(element.tag).split("}")[-1] != "Part":
            continue
        part_number = ""
        etag = ""
        for child in list(element):
            tag_name = str(child.tag).split("}")[-1]
            if tag_name == "PartNumber":
                part_number = str(child.text or "").strip()
            elif tag_name == "ETag":
                etag = str(child.text or "").strip()
        if part_number and etag:
            try:
                results.append((int(part_number), etag))
            except ValueError:
                continue
    return results
