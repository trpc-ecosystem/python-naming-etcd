# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""Test fixtures: an in-memory fake of the etcd3 client (KV, prefix watch, and leases)."""

from __future__ import annotations

import itertools
import os
import queue
import sys
import threading
import time
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import pytest

# Make the package and (in a monorepo checkout) the sibling trpc_sdk
# importable without installing anything.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_WORKSPACE_ROOT = os.path.dirname(_REPO_ROOT)
for _path in (_REPO_ROOT, _WORKSPACE_ROOT):
    if os.path.isdir(_path) and _path not in sys.path:
        sys.path.insert(0, _path)


@dataclass
class FakeKVMetadata:
    """Metadata stand-in for an etcd3 KV entry."""

    key: bytes
    create_revision: int = 1
    mod_revision: int = 1
    version: int = 1
    lease_id: int = 0


class FakeEvent:
    """Minimal stand-in for an etcd3 watch event."""

    def __init__(
        self,
        key: str | bytes,
        value: str | bytes | None,
        create_revision: int = 1,
        mod_revision: int = 1,
        prev_value: str | bytes | None = None,
        delete: bool = False,
    ):
        self.key = key.encode("utf-8") if isinstance(key, str) else key
        self.__class__ = FakeDeleteEvent if delete else FakePutEvent
        self.value = value.encode("utf-8") if isinstance(value, str) else value
        self.prev_value = prev_value.encode("utf-8") if isinstance(prev_value, str) else prev_value
        self.create_revision = create_revision
        self.mod_revision = mod_revision
        self.version_revision = mod_revision


class FakePutEvent(FakeEvent):
    pass


class FakeDeleteEvent(FakeEvent):
    pass


class FakeLease:
    """In-memory fake of an etcd3 Lease object."""

    _id_counter = itertools.count(1000)

    def __init__(self, ttl: int, lease_id: Optional[int] = None, client: Any = None):
        self.id = lease_id if lease_id is not None else next(self._id_counter)
        self.ttl = ttl
        self.client = client
        self.refresh_calls = 0
        self.fail_refresh = False
        self.revoked = False

    def refresh(self):
        self.refresh_calls += 1
        if self.fail_refresh or self.revoked:
            raise RuntimeError("keepAliveOnce fail")
        if self.client is not None and getattr(self.client, "fail_keep_alive_once", False):
            raise RuntimeError("keepAliveOnce fail")

        @dataclass
        class _RefreshRsp:
            ID: int
            TTL: int

        return [_RefreshRsp(ID=self.id, TTL=self.ttl)]

    def revoke(self):
        self.revoked = True


class _FakeWatch:

    def __init__(self, prefix: str, start_revision: Optional[int] = None):
        self.prefix = prefix
        self.start_revision = start_revision
        self.events: "queue.Queue[Optional[FakeEvent]]" = queue.Queue()
        self.cancelled = threading.Event()

    def iterator(self):
        while not self.cancelled.is_set():
            try:
                event = self.events.get(timeout=0.05)
            except queue.Empty:
                continue
            if event is None:
                return
            yield event

    def cancel(self):
        self.cancelled.set()
        self.events.put(None)


class FakeEtcdClient:
    """In-memory fake implementing the subset of etcd3 used by registry/discovery/selector."""

    def __init__(self, store: Optional[Dict[str, bytes]] = None):
        self._lock = threading.RLock()
        self.revision: int = 1
        self.last_revision: int = 1
        self.store: Dict[str, bytes] = {}
        self.meta: Dict[str, FakeKVMetadata] = {}
        self.leases: Dict[int, FakeLease] = {}
        self.watches: List[_FakeWatch] = []
        self.watch_calls: int = 0
        self.fail_grant: bool = False
        self.fail_keep_alive_once: bool = False
        self.fail_put: bool = False
        self.fail_get: bool = False
        self.closed: bool = False

        if store:
            for k, v in store.items():
                self.put(k, v)

    def status(self):
        return object()

    def lease(self, ttl: int, lease_id: Optional[int] = None) -> FakeLease:
        if self.fail_grant:
            raise RuntimeError("grant fail")
        with self._lock:
            lease_obj = FakeLease(ttl=ttl, lease_id=lease_id, client=self)
            self.leases[lease_obj.id] = lease_obj
            return lease_obj

    def put(self, key: str, value: str | bytes, lease: Any = None) -> None:
        if self.fail_put:
            raise RuntimeError("put fail")
        val_bytes = value.encode("utf-8") if isinstance(value, str) else bytes(value)
        lease_id = int(getattr(lease, "id", lease)) if lease is not None else 0
        with self._lock:
            self.revision += 1
            self.last_revision = self.revision
            prev = self.store.get(key)
            existing_meta = self.meta.get(key)
            create_rev = existing_meta.create_revision if existing_meta is not None else self.revision
            ver = (existing_meta.version + 1) if existing_meta is not None else 1
            self.store[key] = val_bytes
            self.meta[key] = FakeKVMetadata(
                key=key.encode("utf-8"),
                create_revision=create_rev,
                mod_revision=self.revision,
                version=ver,
                lease_id=lease_id,
            )
            ev = FakeEvent(
                key=key,
                value=val_bytes,
                create_revision=create_rev,
                mod_revision=self.revision,
                prev_value=prev,
                delete=False,
            )
            self._notify_watches_locked(key, ev)

    def add(self, key: str, value: str | bytes) -> bool:
        with self._lock:
            if key in self.store:
                return False
        self.put(key, value)
        return True

    def get(self, key: str):
        if self.fail_get:
            raise RuntimeError("get fail")
        with self._lock:
            val = self.store.get(key)
            meta = self.meta.get(key)
            return (val, meta) if val is not None else (None, None)

    def get_prefix(self, key_prefix: str):
        if self.fail_get:
            raise RuntimeError("get_prefix fail")
        with self._lock:
            self.last_revision = self.revision
            results = []
            for k in sorted(self.store.keys()):
                if k.startswith(key_prefix):
                    results.append((self.store[k], self.meta[k]))
            return results

    def delete(self, key: str) -> bool:
        with self._lock:
            prev = self.store.pop(key, None)
            meta = self.meta.pop(key, None)
            if prev is None:
                return False
            self.revision += 1
            self.last_revision = self.revision
            ev = FakeEvent(
                key=key,
                value=None,
                create_revision=meta.create_revision if meta else 1,
                mod_revision=self.revision,
                prev_value=prev,
                delete=True,
            )
            self._notify_watches_locked(key, ev)
            return True

    def watch_prefix(self, key_prefix: str, **kwargs):
        with self._lock:
            self.watch_calls += 1
            w = _FakeWatch(prefix=key_prefix, start_revision=kwargs.get("start_revision"))
            self.watches.append(w)
            return w.iterator(), w.cancel

    def _notify_watches_locked(self, key: str, event: FakeEvent) -> None:
        for w in list(self.watches):
            if not w.cancelled.is_set() and key.startswith(w.prefix):
                w.events.put(event)

    def close(self) -> None:
        self.closed = True
        for w in list(self.watches):
            w.cancel()


@pytest.fixture()
def fake_client() -> FakeEtcdClient:
    return FakeEtcdClient()


def wait_until(predicate, timeout: float = 5.0) -> bool:
    """Poll ``predicate`` until it returns True or ``timeout`` seconds elapse."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if predicate():
            return True
        time.sleep(0.02)
    return bool(predicate())
