"""Durable append-only store for source-neutral mapping campaign waves.

The journal is the only authority.  Materialized campaign-head files are
repairable caches, and the global direct-proof registry is reconstructed by
replaying every journal entry.

Phase 1 deliberately does *not* validate an ``ExecutedWaveFacts`` graph or
derive its direct-proof inventory.  ``ValidatedWaveBundle`` is an explicit
trust boundary: the caller must supply a bundle and proof inventory that were
already validated by the source-neutral mapping contract.  This module still
freezes those values, content-addresses them, and rejects structural,
lineage, CAS, idempotency, and global proof-ownership violations.
"""

from __future__ import annotations

import errno
import fcntl
import hashlib
import json
import os
import re
import secrets
import stat
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

STORE_MANIFEST_SCHEMA_VERSION = "x.source_neutral.mapping.campaign_store_manifest.v1"
JOURNAL_ENTRY_SCHEMA_VERSION = "x.source_neutral.mapping.campaign_store_entry.v1"
MATERIALIZED_HEAD_SCHEMA_VERSION = "x.source_neutral.mapping.campaign_store_head.v1"

PHASE1_VALIDATION_BOUNDARY = "caller_supplied_validated_wave_bundle_and_direct_proof_inventory"

_ID_RE = re.compile(r"[a-z0-9][a-z0-9_.-]{0,127}")
_SHA_RE = re.compile(r"[0-9a-f]{64}")
_JOURNAL_FILE_RE = re.compile(r"([0-9]{20})\.([0-9a-f]{64})\.json")
_TEMP_FILE_RE = re.compile(r"\..+\.[0-9a-f]{24}\.tmp")
_FILE_MODE = 0o600
_DIR_MODE = 0o700


def _open_flags(*, directory: bool = False) -> int:
    flags = os.O_RDONLY if directory else os.O_RDWR
    if directory and hasattr(os, "O_DIRECTORY"):
        flags |= os.O_DIRECTORY
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    return flags


def _validate_private_directory_fd(fd: int, path: Path) -> os.stat_result:
    """Bind a private directory descriptor to its canonical pathname.

    POSIX directory link counts are topology-dependent (normally at least two),
    so the single-link invariant used for regular store files cannot apply to
    directories.  Directory substitution is instead rejected by exact
    descriptor/path device and inode binding plus owner and mode checks.
    """

    try:
        descriptor_info = os.fstat(fd)
        pathname_info = path.lstat()
    except OSError as exc:
        raise CampaignStoreCorruption("campaign_store_directory_path_binding_invalid") from exc
    if (descriptor_info.st_dev, descriptor_info.st_ino) != (pathname_info.st_dev, pathname_info.st_ino):
        raise CampaignStoreCorruption("campaign_store_directory_path_binding_invalid")
    if not stat.S_ISDIR(descriptor_info.st_mode) or not stat.S_ISDIR(pathname_info.st_mode):
        raise CampaignStoreCorruption("campaign_store_directory_invalid")
    if descriptor_info.st_uid != os.geteuid() or pathname_info.st_uid != os.geteuid():
        raise CampaignStoreCorruption("campaign_store_directory_owner_invalid")
    if stat.S_IMODE(descriptor_info.st_mode) != _DIR_MODE or stat.S_IMODE(pathname_info.st_mode) != _DIR_MODE:
        raise CampaignStoreCorruption("campaign_store_directory_permission_invalid")
    return descriptor_info


def _open_private_directory_fd(path: Path) -> int:
    try:
        fd = os.open(path, _open_flags(directory=True))
    except OSError as exc:
        raise CampaignStoreCorruption("campaign_store_directory_missing") from exc
    try:
        _validate_private_directory_fd(fd, path)
        return fd
    except BaseException:
        os.close(fd)
        raise


class CampaignStoreError(RuntimeError):
    """Base class carrying a stable machine-readable failure code."""

    def __init__(self, code: str) -> None:
        super().__init__(code)
        self.code = code


class CampaignStoreCorruption(CampaignStoreError):
    """The authoritative store cannot be replayed exactly."""


class CampaignStoreLockBusy(CampaignStoreError):
    """The one store-global lock was not acquired before the deadline."""


class CampaignHeadConflict(CampaignStoreError):
    """The supplied campaign head token lost a compare-and-swap race."""


class CampaignMutationConflict(CampaignStoreError):
    """A mutation id was reused with a different immutable intent."""


class DirectProofCollision(CampaignStoreError):
    """A direct proof is already owned by another committed wave."""


@dataclass(frozen=True)
class DirectProof:
    """One caller-validated direct execution proof."""

    proof_kind: str
    proof_sha256: str


@dataclass(frozen=True)
class ValidatedWaveBundle:
    """Phase-1 caller assertion, not a store-derived semantic validation.

    ``payload`` and ``direct_proofs`` are revalidated for canonical JSON and
    basic shape by the store, but their mapping-domain meaning must already
    have been validated by the caller.
    """

    payload: Mapping[str, Any]
    direct_proofs: tuple[DirectProof, ...]


@dataclass(frozen=True)
class StoredWave:
    """One replay-verified journal commitment."""

    sequence: int
    campaign_id: str
    wave_id: str
    mutation_id: str
    parent_head_token: str | None
    head_token: str
    bundle_sha256: str
    direct_proofs: tuple[DirectProof, ...]
    intent_sha256: str


@dataclass(frozen=True)
class CampaignHead:
    """Current replay-derived head for one campaign."""

    campaign_id: str
    head_token: str
    sequence: int
    wave_id: str
    bundle_sha256: str


@dataclass(frozen=True)
class DirectProofOwner:
    """Replay-derived global proof ownership."""

    proof_sha256: str
    proof_kind: str
    campaign_id: str
    wave_id: str
    head_token: str


@dataclass(frozen=True)
class CampaignStoreReplay:
    """Immutable replay result; no materialized cache is trusted as input."""

    store_id: str
    journal_head_token: str | None
    waves: tuple[StoredWave, ...]
    campaign_heads: tuple[CampaignHead, ...]
    direct_proof_registry: tuple[DirectProofOwner, ...]

    def campaign_head(self, campaign_id: str) -> CampaignHead | None:
        for head in self.campaign_heads:
            if head.campaign_id == campaign_id:
                return head
        return None

    def wave(self, campaign_id: str, wave_id: str) -> StoredWave | None:
        for item in self.waves:
            if item.campaign_id == campaign_id and item.wave_id == wave_id:
                return item
        return None


FaultInjector = Callable[[str], None]


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=True, allow_nan=False, separators=(",", ":"), sort_keys=True)
    except (TypeError, ValueError) as exc:
        raise CampaignStoreError("canonical_json_invalid") from exc


def _canonical_sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _hashed_object(value: Mapping[str, Any], field: str) -> dict[str, Any]:
    result = dict(value)
    result[field] = _canonical_sha256(result)
    return result


def _strict_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise CampaignStoreCorruption("duplicate_json_key")
        result[key] = value
    return result


def _reject_constant(value: str) -> None:
    raise CampaignStoreCorruption(f"non_finite_json_number:{value}")


def _strict_json_bytes(raw: bytes, error: str) -> dict[str, Any]:
    try:
        value = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=_strict_object,
            parse_constant=_reject_constant,
        )
    except (UnicodeError, json.JSONDecodeError) as exc:
        raise CampaignStoreCorruption(error) from exc
    if type(value) is not dict:
        raise CampaignStoreCorruption(error)
    return value


def _identifier(value: Any, error: str) -> str:
    if type(value) is not str or _ID_RE.fullmatch(value) is None:
        raise CampaignStoreError(error)
    return value


def _sha(value: Any, error: str) -> str:
    if type(value) is not str or _SHA_RE.fullmatch(value) is None:
        raise CampaignStoreError(error)
    return value


def _exact_keys(value: Any, keys: set[str], error: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise CampaignStoreCorruption(error)
    return value


def _freeze_payload(payload: Mapping[str, Any]) -> tuple[dict[str, Any], bytes, str]:
    if not isinstance(payload, Mapping):
        raise CampaignStoreError("validated_wave_payload_invalid")
    canonical = _canonical_json(payload)
    try:
        frozen = json.loads(canonical, object_pairs_hook=_strict_object, parse_constant=_reject_constant)
    except (json.JSONDecodeError, CampaignStoreCorruption) as exc:
        raise CampaignStoreError("validated_wave_payload_invalid") from exc
    if type(frozen) is not dict:
        raise CampaignStoreError("validated_wave_payload_invalid")
    raw = canonical.encode("utf-8") + b"\n"
    return frozen, raw, hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _normalize_direct_proofs(proofs: Sequence[DirectProof]) -> tuple[DirectProof, ...]:
    if isinstance(proofs, (str, bytes)) or not isinstance(proofs, Sequence) or not proofs:
        raise CampaignStoreError("direct_proof_inventory_invalid")
    normalized: list[DirectProof] = []
    seen: set[str] = set()
    for proof in proofs:
        if type(proof) is not DirectProof:
            raise CampaignStoreError("direct_proof_inventory_invalid")
        kind = _identifier(proof.proof_kind, "direct_proof_kind_invalid")
        digest = _sha(proof.proof_sha256, "direct_proof_sha256_invalid")
        if digest in seen:
            raise CampaignStoreError("direct_proof_inventory_duplicate")
        seen.add(digest)
        normalized.append(DirectProof(proof_kind=kind, proof_sha256=digest))
    return tuple(sorted(normalized, key=lambda item: (item.proof_sha256, item.proof_kind)))


def _proofs_json(proofs: Sequence[DirectProof]) -> list[dict[str, str]]:
    return [{"proof_kind": item.proof_kind, "proof_sha256": item.proof_sha256} for item in proofs]


def _parse_proofs(value: Any, error: str) -> tuple[DirectProof, ...]:
    if type(value) is not list or not value:
        raise CampaignStoreCorruption(error)
    result: list[DirectProof] = []
    for item in value:
        proof = _exact_keys(item, {"proof_kind", "proof_sha256"}, error)
        try:
            result.append(
                DirectProof(
                    proof_kind=_identifier(proof["proof_kind"], error),
                    proof_sha256=_sha(proof["proof_sha256"], error),
                )
            )
        except CampaignStoreError as exc:
            raise CampaignStoreCorruption(error) from exc
    try:
        normalized = _normalize_direct_proofs(result)
    except CampaignStoreError as exc:
        raise CampaignStoreCorruption(error) from exc
    if tuple(result) != normalized:
        raise CampaignStoreCorruption(error)
    return normalized


class _GlobalLock:
    """Two-level store-global lock with canonical-path binding.

    The private store-root inode is the serialization anchor.  The traditional
    ``.store.lock`` inode remains an independently validated lock/marker, but
    replacing that pathname cannot create a second writer lane because every
    conforming writer must first acquire the unchanged root-directory inode.
    """

    def __init__(self, root: Path, path: Path, timeout_seconds: float, *, allow_create: bool) -> None:
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, (int, float)) or timeout_seconds < 0:
            raise CampaignStoreError("lock_timeout_invalid")
        self._root = root
        self._path = path
        self._timeout_seconds = float(timeout_seconds)
        self._allow_create = allow_create
        self._root_fd: int | None = None
        self._fd: int | None = None

    @staticmethod
    def _acquire(fd: int, deadline: float) -> None:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                return
            except OSError as exc:
                if exc.errno not in (errno.EACCES, errno.EAGAIN):
                    raise CampaignStoreError("store_lock_acquire_failed") from exc
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise CampaignStoreLockBusy("campaign_store_lock_busy") from exc
                time.sleep(min(0.01, remaining))

    def _open_lock_file(self) -> tuple[int, bool]:
        flags = _open_flags()
        created = False
        if self._allow_create:
            try:
                fd = os.open(self._path, flags | os.O_CREAT | os.O_EXCL, _FILE_MODE)
                created = True
            except FileExistsError:
                try:
                    fd = os.open(self._path, flags)
                except OSError as exc:
                    raise CampaignStoreError("store_lock_open_failed") from exc
            except OSError as exc:
                raise CampaignStoreError("store_lock_open_failed") from exc
        else:
            try:
                fd = os.open(self._path, flags)
            except OSError as exc:
                raise CampaignStoreError("store_lock_open_failed") from exc
        if created:
            try:
                # Only a file proven to have been created by this O_EXCL call
                # receives umask normalization. Existing locks are immutable
                # inputs and wrong permissions fail closed below.
                os.fchmod(fd, _FILE_MODE)
            except BaseException:
                os.close(fd)
                raise
        return fd, created

    def _validate_lock_binding(self, fd: int) -> None:
        try:
            descriptor_info = os.fstat(fd)
            pathname_info = self._path.lstat()
        except OSError as exc:
            raise CampaignStoreCorruption("store_lock_path_binding_invalid") from exc
        if (descriptor_info.st_dev, descriptor_info.st_ino) != (pathname_info.st_dev, pathname_info.st_ino):
            raise CampaignStoreCorruption("store_lock_path_binding_invalid")
        if not stat.S_ISREG(descriptor_info.st_mode) or not stat.S_ISREG(pathname_info.st_mode):
            raise CampaignStoreCorruption("store_lock_not_regular")
        if descriptor_info.st_uid != os.geteuid() or pathname_info.st_uid != os.geteuid():
            raise CampaignStoreCorruption("store_lock_owner_invalid")
        if descriptor_info.st_nlink != 1 or pathname_info.st_nlink != 1:
            raise CampaignStoreCorruption("store_lock_link_count_invalid")
        if stat.S_IMODE(descriptor_info.st_mode) != _FILE_MODE or stat.S_IMODE(pathname_info.st_mode) != _FILE_MODE:
            raise CampaignStoreCorruption("store_lock_permission_invalid")

    def assert_canonical_binding(self) -> None:
        if self._root_fd is None or self._fd is None:
            raise CampaignStoreError("store_lock_not_held")
        _validate_private_directory_fd(self._root_fd, self._root)
        self._validate_lock_binding(self._fd)

    def __enter__(self) -> _GlobalLock:
        deadline = time.monotonic() + self._timeout_seconds
        root_fd = _open_private_directory_fd(self._root)
        root_locked = False
        fd: int | None = None
        fd_locked = False
        try:
            self._acquire(root_fd, deadline)
            root_locked = True
            fd, _ = self._open_lock_file()
            self._validate_lock_binding(fd)
            self._acquire(fd, deadline)
            fd_locked = True
            self._root_fd = root_fd
            self._fd = fd
            self.assert_canonical_binding()
            return self
        except BaseException:
            if fd is not None:
                if fd_locked:
                    fcntl.flock(fd, fcntl.LOCK_UN)
                os.close(fd)
            if root_locked:
                fcntl.flock(root_fd, fcntl.LOCK_UN)
            os.close(root_fd)
            raise

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self._fd is None or self._root_fd is None:
            return
        try:
            fcntl.flock(self._fd, fcntl.LOCK_UN)
        finally:
            os.close(self._fd)
            self._fd = None
            try:
                fcntl.flock(self._root_fd, fcntl.LOCK_UN)
            finally:
                os.close(self._root_fd)
                self._root_fd = None


class CampaignStore:
    """Private journal-backed store with one store-global mutation lock."""

    def __init__(
        self,
        root: Path,
        *,
        lock_timeout_seconds: float = 0.25,
        _fault_injector: FaultInjector | None = None,
    ) -> None:
        self.root = Path(root)
        self.lock_timeout_seconds = lock_timeout_seconds
        self._fault_injector = _fault_injector

    @property
    def manifest_path(self) -> Path:
        return self.root / "store_manifest.json"

    @property
    def lock_path(self) -> Path:
        return self.root / ".store.lock"

    @property
    def journal_dir(self) -> Path:
        return self.root / "journal"

    @property
    def objects_dir(self) -> Path:
        return self.root / "objects"

    @property
    def heads_dir(self) -> Path:
        return self.root / "heads"

    @property
    def temp_dir(self) -> Path:
        return self.root / "tmp"

    @classmethod
    def create(
        cls,
        root: Path,
        *,
        store_id: str,
        lock_timeout_seconds: float = 0.25,
        _fault_injector: FaultInjector | None = None,
    ) -> CampaignStore:
        store_id = _identifier(store_id, "store_id_invalid")
        instance = cls(
            root,
            lock_timeout_seconds=lock_timeout_seconds,
            _fault_injector=_fault_injector,
        )
        instance._prepare_layout()
        with instance._global_lock(allow_create=True) as lock:
            lock.assert_canonical_binding()
            manifest = _hashed_object(
                {
                    "schema_version": STORE_MANIFEST_SCHEMA_VERSION,
                    "store_id": store_id,
                    "layout_version": 1,
                    "canonicalization": "json.sort_keys.ascii.no_nan.compact.v1",
                    "hash_algorithm": "sha256",
                    "journal_authority": "global_append_only_replay",
                    "head_authority": "replay_derived_materialized_cache",
                    "direct_proof_registry_authority": "global_journal_replay",
                    "lock_scope": "store_global",
                    "phase1_validation_boundary": PHASE1_VALIDATION_BOUNDARY,
                },
                "manifest_sha256",
            )
            raw = _canonical_json(manifest).encode("utf-8") + b"\n"
            if instance.manifest_path.exists():
                current = instance._read_private_file(instance.manifest_path, "store_manifest_read_failed")
                if current != raw:
                    raise CampaignStoreCorruption("store_manifest_conflict")
            else:
                instance._publish_new_file(instance.manifest_path, raw, lock=lock)
            snapshot = instance._replay_unlocked(lock)
            instance._repair_heads_unlocked(snapshot, lock)
        return instance

    @classmethod
    def open(
        cls,
        root: Path,
        *,
        lock_timeout_seconds: float = 0.25,
        _fault_injector: FaultInjector | None = None,
    ) -> CampaignStore:
        instance = cls(
            root,
            lock_timeout_seconds=lock_timeout_seconds,
            _fault_injector=_fault_injector,
        )
        instance._validate_layout()
        instance.replay()
        return instance

    def replay(self) -> CampaignStoreReplay:
        """Replay journal authority and repair all materialized heads."""

        with self._global_lock() as lock:
            snapshot = self._replay_unlocked(lock)
            self._repair_heads_unlocked(snapshot, lock)
            return snapshot

    def append_validated_wave(
        self,
        *,
        campaign_id: str,
        wave_id: str,
        mutation_id: str,
        expected_head_token: str | None,
        validated_wave: ValidatedWaveBundle,
    ) -> StoredWave:
        """CAS-append one caller-validated wave.

        Exact retries of an already committed mutation return the original
        commitment even when a later wave has advanced the campaign head.
        Reusing the mutation id with a different intent fails closed.
        """

        campaign_id = _identifier(campaign_id, "campaign_id_invalid")
        wave_id = _identifier(wave_id, "wave_id_invalid")
        mutation_id = _identifier(mutation_id, "mutation_id_invalid")
        if expected_head_token is not None:
            expected_head_token = _sha(expected_head_token, "expected_head_token_invalid")
        if type(validated_wave) is not ValidatedWaveBundle:
            raise CampaignStoreError("validated_wave_bundle_invalid")
        _, bundle_raw, bundle_sha256 = _freeze_payload(validated_wave.payload)
        proofs = _normalize_direct_proofs(validated_wave.direct_proofs)
        intent = {
            "campaign_id": campaign_id,
            "wave_id": wave_id,
            "mutation_id": mutation_id,
            "expected_parent_head_token": expected_head_token,
            "bundle_sha256": bundle_sha256,
            "direct_proofs": _proofs_json(proofs),
        }
        intent_sha256 = _canonical_sha256(intent)

        with self._global_lock() as lock:
            snapshot = self._replay_unlocked(lock)
            self._repair_heads_unlocked(snapshot, lock)
            existing = next(
                (
                    item
                    for item in snapshot.waves
                    if item.campaign_id == campaign_id and item.mutation_id == mutation_id
                ),
                None,
            )
            if existing is not None:
                if existing.intent_sha256 != intent_sha256:
                    raise CampaignMutationConflict("campaign_mutation_conflict")
                self._repair_heads_unlocked(snapshot, lock)
                return existing

            if snapshot.wave(campaign_id, wave_id) is not None:
                raise CampaignMutationConflict("campaign_wave_id_already_committed")
            current = snapshot.campaign_head(campaign_id)
            current_token = None if current is None else current.head_token
            if current_token != expected_head_token:
                raise CampaignHeadConflict("campaign_head_conflict")

            proof_owners = {item.proof_sha256: item for item in snapshot.direct_proof_registry}
            for proof in proofs:
                if proof.proof_sha256 in proof_owners:
                    raise DirectProofCollision("direct_proof_collision")

            object_path = self.objects_dir / f"{bundle_sha256}.json"
            self._publish_content_addressed(object_path, bundle_raw, lock=lock)
            self._inject_fault("after_bundle_publish_before_journal")

            sequence = len(snapshot.waves) + 1
            entry_without_hash: dict[str, Any] = {
                "schema_version": JOURNAL_ENTRY_SCHEMA_VERSION,
                "sequence": sequence,
                "previous_entry_sha256": snapshot.journal_head_token,
                **intent,
                "intent_sha256": intent_sha256,
            }
            entry = _hashed_object(entry_without_hash, "entry_sha256")
            entry_sha256 = entry["entry_sha256"]
            entry_raw = _canonical_json(entry).encode("utf-8") + b"\n"
            journal_path = self.journal_dir / f"{sequence:020d}.{entry_sha256}.json"
            self._publish_new_file(journal_path, entry_raw, lock=lock)
            self._inject_fault("after_journal_publish_before_head")

            committed = self._replay_unlocked(lock)
            self._repair_heads_unlocked(committed, lock)
            result = next(
                item for item in committed.waves if item.campaign_id == campaign_id and item.mutation_id == mutation_id
            )
            return result

    def get_campaign_head(self, campaign_id: str) -> CampaignHead | None:
        campaign_id = _identifier(campaign_id, "campaign_id_invalid")
        return self.replay().campaign_head(campaign_id)

    def get_wave(self, campaign_id: str, wave_id: str) -> StoredWave | None:
        campaign_id = _identifier(campaign_id, "campaign_id_invalid")
        wave_id = _identifier(wave_id, "wave_id_invalid")
        return self.replay().wave(campaign_id, wave_id)

    def read_wave_bundle(self, campaign_id: str, wave_id: str) -> dict[str, Any]:
        """Return a fresh JSON value after replay verifies the owning entry."""

        campaign_id = _identifier(campaign_id, "campaign_id_invalid")
        wave_id = _identifier(wave_id, "wave_id_invalid")
        with self._global_lock() as lock:
            snapshot = self._replay_unlocked(lock)
            self._repair_heads_unlocked(snapshot, lock)
            item = snapshot.wave(campaign_id, wave_id)
            if item is None:
                raise CampaignStoreError("campaign_wave_not_found")
            lock.assert_canonical_binding()
            raw = self._read_private_file(
                self.objects_dir / f"{item.bundle_sha256}.json",
                "wave_bundle_read_failed",
            )
        value = _strict_json_bytes(raw, "wave_bundle_json_invalid")
        canonical = _canonical_json(value).encode("utf-8") + b"\n"
        if raw != canonical or _canonical_sha256(value) != item.bundle_sha256:
            raise CampaignStoreCorruption("wave_bundle_content_invalid")
        return value

    def _prepare_layout(self) -> None:
        self._ensure_private_directory(self.root, create=True)
        for path in (self.journal_dir, self.objects_dir, self.heads_dir, self.temp_dir):
            self._ensure_private_directory(path, create=True)
        self._fsync_directory(self.root)

    def _validate_layout(self) -> None:
        self._ensure_private_directory(self.root, create=False)
        for path in (self.journal_dir, self.objects_dir, self.heads_dir, self.temp_dir):
            self._ensure_private_directory(path, create=False)
        if not self.manifest_path.exists() or not self.lock_path.exists():
            raise CampaignStoreCorruption("campaign_store_layout_incomplete")

    def _ensure_private_directory(self, path: Path, *, create: bool) -> None:
        created = False
        if create:
            try:
                path.mkdir(mode=_DIR_MODE, parents=False, exist_ok=False)
                created = True
            except FileExistsError:
                pass
            except FileNotFoundError:
                if path == self.root:
                    try:
                        path.mkdir(mode=_DIR_MODE, parents=True, exist_ok=False)
                        created = True
                    except FileExistsError:
                        pass
                else:
                    raise CampaignStoreError("campaign_store_parent_missing") from None
            except OSError as exc:
                raise CampaignStoreError("campaign_store_directory_failed") from exc
        created_identity: tuple[int, int] | None = None
        if created:
            try:
                created_info = path.lstat()
                if not stat.S_ISDIR(created_info.st_mode) or created_info.st_uid != os.geteuid():
                    raise CampaignStoreCorruption("campaign_store_directory_invalid")
                created_identity = (created_info.st_dev, created_info.st_ino)
                os.chmod(path, _DIR_MODE, follow_symlinks=False)
            except OSError as exc:
                raise CampaignStoreError("campaign_store_directory_normalize_failed") from exc
        fd = _open_private_directory_fd(path)
        if created_identity is not None:
            opened_info = os.fstat(fd)
            if (opened_info.st_dev, opened_info.st_ino) != created_identity:
                os.close(fd)
                raise CampaignStoreCorruption("campaign_store_directory_path_binding_invalid")
        os.close(fd)

    def _global_lock(self, *, allow_create: bool = False) -> _GlobalLock:
        return _GlobalLock(
            self.root,
            self.lock_path,
            self.lock_timeout_seconds,
            allow_create=allow_create,
        )

    def _load_manifest_unlocked(self) -> dict[str, Any]:
        raw = self._read_private_file(self.manifest_path, "store_manifest_read_failed")
        value = _strict_json_bytes(raw, "store_manifest_json_invalid")
        expected_keys = {
            "schema_version",
            "store_id",
            "layout_version",
            "canonicalization",
            "hash_algorithm",
            "journal_authority",
            "head_authority",
            "direct_proof_registry_authority",
            "lock_scope",
            "phase1_validation_boundary",
            "manifest_sha256",
        }
        manifest = _exact_keys(value, expected_keys, "store_manifest_shape_invalid")
        if (
            manifest["schema_version"] != STORE_MANIFEST_SCHEMA_VERSION
            or manifest["layout_version"] != 1
            or manifest["canonicalization"] != "json.sort_keys.ascii.no_nan.compact.v1"
            or manifest["hash_algorithm"] != "sha256"
            or manifest["journal_authority"] != "global_append_only_replay"
            or manifest["head_authority"] != "replay_derived_materialized_cache"
            or manifest["direct_proof_registry_authority"] != "global_journal_replay"
            or manifest["lock_scope"] != "store_global"
            or manifest["phase1_validation_boundary"] != PHASE1_VALIDATION_BOUNDARY
        ):
            raise CampaignStoreCorruption("store_manifest_contract_invalid")
        try:
            _identifier(manifest["store_id"], "store_manifest_id_invalid")
            digest = _sha(manifest["manifest_sha256"], "store_manifest_sha_invalid")
        except CampaignStoreError as exc:
            raise CampaignStoreCorruption(exc.code) from exc
        if digest != _canonical_sha256({key: item for key, item in manifest.items() if key != "manifest_sha256"}):
            raise CampaignStoreCorruption("store_manifest_hash_invalid")
        if raw != _canonical_json(value).encode("utf-8") + b"\n":
            raise CampaignStoreCorruption("store_manifest_not_canonical")
        return dict(manifest)

    def _replay_unlocked(self, lock: _GlobalLock) -> CampaignStoreReplay:
        self._validate_layout()
        self._cleanup_temps_unlocked()
        lock.assert_canonical_binding()
        manifest = self._load_manifest_unlocked()
        journal_files: list[tuple[int, str, Path]] = []
        try:
            paths = list(self.journal_dir.iterdir())
        except OSError as exc:
            raise CampaignStoreCorruption("journal_list_failed") from exc
        for path in paths:
            match = _JOURNAL_FILE_RE.fullmatch(path.name)
            if match is None:
                raise CampaignStoreCorruption("journal_filename_invalid")
            journal_files.append((int(match.group(1)), match.group(2), path))
        journal_files.sort(key=lambda item: item[0])

        waves: list[StoredWave] = []
        heads: dict[str, CampaignHead] = {}
        proof_registry: dict[str, DirectProofOwner] = {}
        mutation_registry: set[tuple[str, str]] = set()
        wave_registry: set[tuple[str, str]] = set()
        previous_entry_sha256: str | None = None

        for expected_sequence, (sequence_from_name, sha_from_name, path) in enumerate(journal_files, 1):
            if sequence_from_name != expected_sequence:
                raise CampaignStoreCorruption("journal_sequence_gap")
            raw = self._read_private_file(path, "journal_entry_read_failed")
            value = _strict_json_bytes(raw, "journal_entry_json_invalid")
            entry = _exact_keys(
                value,
                {
                    "schema_version",
                    "sequence",
                    "previous_entry_sha256",
                    "campaign_id",
                    "wave_id",
                    "mutation_id",
                    "expected_parent_head_token",
                    "bundle_sha256",
                    "direct_proofs",
                    "intent_sha256",
                    "entry_sha256",
                },
                "journal_entry_shape_invalid",
            )
            if raw != _canonical_json(value).encode("utf-8") + b"\n":
                raise CampaignStoreCorruption("journal_entry_not_canonical")
            if entry["schema_version"] != JOURNAL_ENTRY_SCHEMA_VERSION:
                raise CampaignStoreCorruption("journal_entry_schema_invalid")
            if type(entry["sequence"]) is not int or entry["sequence"] != expected_sequence:
                raise CampaignStoreCorruption("journal_entry_sequence_invalid")
            if entry["previous_entry_sha256"] != previous_entry_sha256:
                raise CampaignStoreCorruption("journal_global_chain_invalid")
            try:
                campaign_id = _identifier(entry["campaign_id"], "journal_campaign_id_invalid")
                wave_id = _identifier(entry["wave_id"], "journal_wave_id_invalid")
                mutation_id = _identifier(entry["mutation_id"], "journal_mutation_id_invalid")
                bundle_sha256 = _sha(entry["bundle_sha256"], "journal_bundle_sha_invalid")
                intent_sha256 = _sha(entry["intent_sha256"], "journal_intent_sha_invalid")
                entry_sha256 = _sha(entry["entry_sha256"], "journal_entry_sha_invalid")
                parent = entry["expected_parent_head_token"]
                if parent is not None:
                    parent = _sha(parent, "journal_parent_head_invalid")
            except CampaignStoreError as exc:
                raise CampaignStoreCorruption(exc.code) from exc
            proofs = _parse_proofs(entry["direct_proofs"], "journal_direct_proofs_invalid")

            computed_entry_sha256 = _canonical_sha256(
                {key: item for key, item in entry.items() if key != "entry_sha256"}
            )
            if entry_sha256 != computed_entry_sha256 or entry_sha256 != sha_from_name:
                raise CampaignStoreCorruption("journal_entry_hash_invalid")
            intent = {
                "campaign_id": campaign_id,
                "wave_id": wave_id,
                "mutation_id": mutation_id,
                "expected_parent_head_token": parent,
                "bundle_sha256": bundle_sha256,
                "direct_proofs": _proofs_json(proofs),
            }
            if intent_sha256 != _canonical_sha256(intent):
                raise CampaignStoreCorruption("journal_intent_hash_invalid")
            if (campaign_id, mutation_id) in mutation_registry:
                raise CampaignStoreCorruption("journal_mutation_duplicate")
            if (campaign_id, wave_id) in wave_registry:
                raise CampaignStoreCorruption("journal_wave_duplicate")
            current_head = heads.get(campaign_id)
            expected_parent = None if current_head is None else current_head.head_token
            if parent != expected_parent:
                raise CampaignStoreCorruption("journal_campaign_chain_invalid")

            object_path = self.objects_dir / f"{bundle_sha256}.json"
            object_raw = self._read_private_file(object_path, "journal_bundle_missing")
            object_value = _strict_json_bytes(object_raw, "journal_bundle_json_invalid")
            if object_raw != _canonical_json(object_value).encode("utf-8") + b"\n":
                raise CampaignStoreCorruption("journal_bundle_not_canonical")
            if _canonical_sha256(object_value) != bundle_sha256:
                raise CampaignStoreCorruption("journal_bundle_hash_invalid")

            for proof in proofs:
                if proof.proof_sha256 in proof_registry:
                    raise CampaignStoreCorruption("journal_direct_proof_collision")
                proof_registry[proof.proof_sha256] = DirectProofOwner(
                    proof_sha256=proof.proof_sha256,
                    proof_kind=proof.proof_kind,
                    campaign_id=campaign_id,
                    wave_id=wave_id,
                    head_token=entry_sha256,
                )
            wave = StoredWave(
                sequence=expected_sequence,
                campaign_id=campaign_id,
                wave_id=wave_id,
                mutation_id=mutation_id,
                parent_head_token=parent,
                head_token=entry_sha256,
                bundle_sha256=bundle_sha256,
                direct_proofs=proofs,
                intent_sha256=intent_sha256,
            )
            waves.append(wave)
            heads[campaign_id] = CampaignHead(
                campaign_id=campaign_id,
                head_token=entry_sha256,
                sequence=expected_sequence,
                wave_id=wave_id,
                bundle_sha256=bundle_sha256,
            )
            mutation_registry.add((campaign_id, mutation_id))
            wave_registry.add((campaign_id, wave_id))
            previous_entry_sha256 = entry_sha256

        return CampaignStoreReplay(
            store_id=manifest["store_id"],
            journal_head_token=previous_entry_sha256,
            waves=tuple(waves),
            campaign_heads=tuple(heads[key] for key in sorted(heads)),
            direct_proof_registry=tuple(proof_registry[key] for key in sorted(proof_registry)),
        )

    def _repair_heads_unlocked(self, snapshot: CampaignStoreReplay, lock: _GlobalLock) -> None:
        lock.assert_canonical_binding()
        wave_by_token = {item.head_token: item for item in snapshot.waves}
        current_heads = {item.campaign_id: item for item in snapshot.campaign_heads}
        try:
            existing_paths = list(self.heads_dir.iterdir())
        except OSError as exc:
            raise CampaignStoreCorruption("materialized_head_list_failed") from exc

        for path in existing_paths:
            if path.suffix != ".json":
                raise CampaignStoreCorruption("materialized_head_filename_invalid")
            try:
                campaign_id_from_name = _identifier(path.stem, "materialized_head_filename_invalid")
            except CampaignStoreError as exc:
                raise CampaignStoreCorruption(exc.code) from exc
            raw = self._read_private_file(path, "materialized_head_read_failed")
            cached = self._parse_materialized_head(raw)
            if cached["campaign_id"] != campaign_id_from_name:
                raise CampaignStoreCorruption("materialized_head_filename_mismatch")

            cached_wave = wave_by_token.get(cached["head_token"])
            cached_global_wave = wave_by_token.get(cached["journal_head_token_at_materialization"])
            if cached_wave is None:
                raise CampaignStoreCorruption("materialized_campaign_frontier_unknown")
            if cached_global_wave is None:
                raise CampaignStoreCorruption("materialized_global_frontier_unknown")
            if (
                cached_wave.campaign_id != cached["campaign_id"]
                or cached_wave.sequence != cached["sequence"]
                or cached_wave.wave_id != cached["wave_id"]
                or cached_wave.bundle_sha256 != cached["bundle_sha256"]
            ):
                raise CampaignStoreCorruption("materialized_campaign_frontier_diverged")
            if cached_global_wave.sequence < cached_wave.sequence:
                raise CampaignStoreCorruption("materialized_global_frontier_regressed")
            current = current_heads.get(cached_wave.campaign_id)
            if current is None or cached_wave.sequence > current.sequence:
                raise CampaignStoreCorruption("materialized_campaign_frontier_ahead")

        expected_names: set[str] = set()
        for head in snapshot.campaign_heads:
            expected_names.add(f"{head.campaign_id}.json")
            value = _hashed_object(
                {
                    "schema_version": MATERIALIZED_HEAD_SCHEMA_VERSION,
                    "campaign_id": head.campaign_id,
                    "head_token": head.head_token,
                    "sequence": head.sequence,
                    "wave_id": head.wave_id,
                    "bundle_sha256": head.bundle_sha256,
                    "journal_head_token_at_materialization": snapshot.journal_head_token,
                },
                "materialized_head_sha256",
            )
            raw = _canonical_json(value).encode("utf-8") + b"\n"
            path = self.heads_dir / f"{head.campaign_id}.json"
            try:
                current = self._read_private_file(path, "materialized_head_read_failed")
            except CampaignStoreCorruption as exc:
                if exc.code != "private_file_missing":
                    raise
                current = None
            if current != raw:
                self._replace_file(path, raw, lock=lock)

        for path in self.heads_dir.iterdir():
            if path.name not in expected_names:
                raise CampaignStoreCorruption("materialized_head_without_replayed_campaign")

    @staticmethod
    def _parse_materialized_head(raw: bytes) -> dict[str, Any]:
        value = _strict_json_bytes(raw, "materialized_head_json_invalid")
        head = _exact_keys(
            value,
            {
                "schema_version",
                "campaign_id",
                "head_token",
                "sequence",
                "wave_id",
                "bundle_sha256",
                "journal_head_token_at_materialization",
                "materialized_head_sha256",
            },
            "materialized_head_shape_invalid",
        )
        if raw != _canonical_json(value).encode("utf-8") + b"\n":
            raise CampaignStoreCorruption("materialized_head_not_canonical")
        if head["schema_version"] != MATERIALIZED_HEAD_SCHEMA_VERSION:
            raise CampaignStoreCorruption("materialized_head_schema_invalid")
        if type(head["sequence"]) is not int or head["sequence"] <= 0:
            raise CampaignStoreCorruption("materialized_head_sequence_invalid")
        try:
            _identifier(head["campaign_id"], "materialized_head_campaign_id_invalid")
            _identifier(head["wave_id"], "materialized_head_wave_id_invalid")
            _sha(head["head_token"], "materialized_head_token_invalid")
            _sha(head["bundle_sha256"], "materialized_head_bundle_sha_invalid")
            _sha(
                head["journal_head_token_at_materialization"],
                "materialized_global_head_token_invalid",
            )
            digest = _sha(head["materialized_head_sha256"], "materialized_head_sha_invalid")
        except CampaignStoreError as exc:
            raise CampaignStoreCorruption(exc.code) from exc
        expected_digest = _canonical_sha256(
            {key: item for key, item in head.items() if key != "materialized_head_sha256"}
        )
        if digest != expected_digest:
            raise CampaignStoreCorruption("materialized_head_hash_invalid")
        return dict(head)

    def _read_private_file(self, path: Path, error: str) -> bytes:
        flags = os.O_RDONLY
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        try:
            fd = os.open(path, flags)
        except FileNotFoundError as exc:
            raise CampaignStoreCorruption("private_file_missing") from exc
        except OSError as exc:
            raise CampaignStoreCorruption(error) from exc
        try:
            self._validate_private_file_binding(fd, path)
            chunks: list[bytes] = []
            while True:
                chunk = os.read(fd, 1024 * 1024)
                if not chunk:
                    self._validate_private_file_binding(fd, path)
                    return b"".join(chunks)
                chunks.append(chunk)
        finally:
            os.close(fd)

    @staticmethod
    def _validate_private_file_binding(fd: int, path: Path) -> None:
        try:
            descriptor_info = os.fstat(fd)
            pathname_info = path.lstat()
        except OSError as exc:
            raise CampaignStoreCorruption("private_file_path_binding_invalid") from exc
        if (descriptor_info.st_dev, descriptor_info.st_ino) != (pathname_info.st_dev, pathname_info.st_ino):
            raise CampaignStoreCorruption("private_file_path_binding_invalid")
        if not stat.S_ISREG(descriptor_info.st_mode) or not stat.S_ISREG(pathname_info.st_mode):
            raise CampaignStoreCorruption("private_file_not_regular")
        if descriptor_info.st_uid != os.geteuid() or pathname_info.st_uid != os.geteuid():
            raise CampaignStoreCorruption("private_file_owner_invalid")
        if descriptor_info.st_nlink != 1 or pathname_info.st_nlink != 1:
            raise CampaignStoreCorruption("private_file_link_count_invalid")
        if stat.S_IMODE(descriptor_info.st_mode) != _FILE_MODE or stat.S_IMODE(pathname_info.st_mode) != _FILE_MODE:
            raise CampaignStoreCorruption("private_file_permission_invalid")

    def _write_temp(self, target: Path, content: bytes) -> Path:
        temp = self.temp_dir / f".{target.parent.name}-{target.name}.{secrets.token_hex(12)}.tmp"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        if hasattr(os, "O_CLOEXEC"):
            flags |= os.O_CLOEXEC
        try:
            fd = os.open(temp, flags, _FILE_MODE)
        except OSError as exc:
            raise CampaignStoreError("temporary_file_create_failed") from exc
        try:
            os.fchmod(fd, _FILE_MODE)
            self._validate_private_file_binding(fd, temp)
            view = memoryview(content)
            while view:
                written = os.write(fd, view)
                if written <= 0:
                    raise CampaignStoreError("temporary_file_write_failed")
                view = view[written:]
            os.fsync(fd)
            self._validate_private_file_binding(fd, temp)
        except BaseException:
            try:
                temp.unlink()
            except OSError:
                pass
            raise
        finally:
            os.close(fd)
        return temp

    def _publish_new_file(self, target: Path, content: bytes, *, lock: _GlobalLock) -> None:
        temp = self._write_temp(target, content)
        try:
            self._ensure_private_directory(target.parent, create=False)
            lock.assert_canonical_binding()
            os.link(temp, target)
        except FileExistsError as exc:
            raise CampaignStoreCorruption("append_only_target_exists") from exc
        except OSError as exc:
            raise CampaignStoreError("append_only_publish_failed") from exc
        finally:
            try:
                temp.unlink()
            except OSError:
                pass
        self._fsync_directory(target.parent)
        self._fsync_directory(self.temp_dir)

    def _publish_content_addressed(self, target: Path, content: bytes, *, lock: _GlobalLock) -> None:
        lock.assert_canonical_binding()
        if target.exists():
            current = self._read_private_file(target, "content_addressed_read_failed")
            if current != content:
                raise CampaignStoreCorruption("content_addressed_collision")
            return
        try:
            self._publish_new_file(target, content, lock=lock)
        except CampaignStoreCorruption as exc:
            if exc.code != "append_only_target_exists":
                raise
            current = self._read_private_file(target, "content_addressed_read_failed")
            if current != content:
                raise CampaignStoreCorruption("content_addressed_collision") from exc

    def _replace_file(self, target: Path, content: bytes, *, lock: _GlobalLock) -> None:
        temp = self._write_temp(target, content)
        try:
            self._ensure_private_directory(target.parent, create=False)
            lock.assert_canonical_binding()
            os.replace(temp, target)
        except OSError as exc:
            raise CampaignStoreError("materialized_head_replace_failed") from exc
        finally:
            try:
                temp.unlink()
            except OSError:
                pass
        self._fsync_directory(target.parent)
        self._fsync_directory(self.temp_dir)

    def _cleanup_temps_unlocked(self) -> None:
        removed = False
        try:
            paths = list(self.temp_dir.iterdir())
        except OSError as exc:
            raise CampaignStoreCorruption("temporary_directory_list_failed") from exc
        for path in paths:
            try:
                info = path.lstat()
            except OSError as exc:
                raise CampaignStoreCorruption("temporary_file_stat_failed") from exc
            if (
                _TEMP_FILE_RE.fullmatch(path.name) is None
                or not stat.S_ISREG(info.st_mode)
                or stat.S_ISLNK(info.st_mode)
                or info.st_uid != os.geteuid()
                or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) != _FILE_MODE
            ):
                raise CampaignStoreCorruption("temporary_file_invalid")
            try:
                path.unlink()
            except OSError as exc:
                raise CampaignStoreCorruption("temporary_file_cleanup_failed") from exc
            removed = True
        if removed:
            self._fsync_directory(self.temp_dir)

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        fd = _open_private_directory_fd(path)
        try:
            os.fsync(fd)
        except OSError as exc:
            raise CampaignStoreError("directory_fsync_failed") from exc
        finally:
            os.close(fd)

    def _inject_fault(self, point: str) -> None:
        if self._fault_injector is not None:
            self._fault_injector(point)


__all__ = [
    "CampaignHead",
    "CampaignHeadConflict",
    "CampaignMutationConflict",
    "CampaignStore",
    "CampaignStoreCorruption",
    "CampaignStoreError",
    "CampaignStoreLockBusy",
    "CampaignStoreReplay",
    "DirectProof",
    "DirectProofCollision",
    "DirectProofOwner",
    "JOURNAL_ENTRY_SCHEMA_VERSION",
    "MATERIALIZED_HEAD_SCHEMA_VERSION",
    "PHASE1_VALIDATION_BOUNDARY",
    "STORE_MANIFEST_SCHEMA_VERSION",
    "StoredWave",
    "ValidatedWaveBundle",
]
