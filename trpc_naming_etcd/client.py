# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""etcd client creation and lease management."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Union

from trpc_sdk.log import logger

from ._constants import DEFAULT_FORCE_KEEP_ALIVE_TIME
from .config import EtcdClientConfig
from .error import EtcdNamingError

# Minimum interval before a reused lease is refreshed again.
default_force_keep_alive_time: float = DEFAULT_FORCE_KEEP_ALIVE_TIME


def default_client_factory(config: EtcdClientConfig, endpoint: str) -> Any:
    """Create a real ``etcd3`` client for one endpoint (imported lazily)."""
    import etcd3  # pylint: disable=import-outside-toplevel

    return etcd3.client(**config.client_kwargs(endpoint))


def generate_etcd_client(
    cfg: Optional[Union[Dict[str, Any], EtcdClientConfig]] = None,
    client_factory: Optional[Callable[[EtcdClientConfig, str], Any]] = None,
) -> Any:
    """Create an etcd client with endpoint failover, TLS, and authentication.

    Tries each configured endpoint in order and probes ``status()`` when
    available so unreachable nodes fail over immediately.

    Args:
        cfg: Client configuration dict or :class:`EtcdClientConfig`.
        client_factory: Optional ``(config, endpoint) -> client`` callable.

    Returns:
        Connected etcd client instance.

    Raises:
        EtcdNamingError: If all endpoints fail to connect or TLS is invalid.
    """
    if isinstance(cfg, EtcdClientConfig):
        resolved_cfg = cfg
    elif isinstance(cfg, dict):
        resolved_cfg = EtcdClientConfig.from_dict(cfg)
    else:
        resolved_cfg = EtcdClientConfig()

    factory = client_factory or default_client_factory
    last_exc: Optional[Exception] = None
    for endpoint in resolved_cfg.endpoints:
        try:
            client = factory(resolved_cfg, endpoint)
            status = getattr(client, "status", None)
            if callable(status):
                status()
            return client
        except Exception as exc:  # pylint: disable=broad-except
            last_exc = exc
            logger.warning("connect etcd endpoint %s failed: %s", endpoint, exc)
    raise EtcdNamingError(f"connect etcd {resolved_cfg.endpoints} failed: {last_exc}") from last_exc


@dataclass
class _LeaseHolder:
    """Internal state for one active lease."""

    lease_id: int
    lease_obj: Any
    force_keep_alive_time: float
    exit_event: threading.Event = field(default_factory=threading.Event)
    ttl: float = 0.0
    thread: Optional[threading.Thread] = None


class LeaseManager:
    """Manage etcd leases keyed by TTL.

    Since creating a lease in etcd is relatively expensive, a single lease is
    shared across all services that use the same TTL. A background daemon
    thread keeps the lease alive; if keep-alive fails, the lease is removed
    from the map and ``exit_event`` is set so callers can re-register.
    """

    def __init__(self, client: Any, force_keep_alive_time: Optional[float] = None):
        self._client = client
        self._force_keep_alive_time = force_keep_alive_time
        self._lock = threading.Lock()
        self._lease_map: Dict[float, _LeaseHolder] = {}
        self._closed = False

    def _get_force_window(self) -> float:
        if self._force_keep_alive_time is not None:
            return self._force_keep_alive_time
        return default_force_keep_alive_time

    def get_lease(self, ttl: Union[int, float]) -> Tuple[Any, threading.Event]:
        """Acquire or reuse a lease for ``ttl`` seconds.

        Args:
            ttl: Lease time-to-live in seconds.

        Returns:
            Tuple of ``(lease_obj_or_id, expire_event)``. ``expire_event`` is
            a :class:`threading.Event` that is set when the lease expires or
            its keep-alive loop terminates.

        Raises:
            Exception: If granting a new lease fails.
        """
        ttl_sec = float(ttl)
        now = time.monotonic()
        force_window = self._get_force_window()

        with self._lock:
            holder = self._lease_map.get(ttl_sec)
            if holder is not None and not holder.exit_event.is_set():
                if now < holder.force_keep_alive_time:
                    return holder.lease_obj, holder.exit_event
                try:
                    self._keep_alive_once(holder)
                    holder.force_keep_alive_time = time.monotonic() + force_window
                    return holder.lease_obj, holder.exit_event
                except Exception:  # pylint: disable=broad-except
                    self._remove_lease_locked(holder)

            lease_id, lease_obj = self._grant_lease(int(max(1, round(ttl_sec))))
            holder = _LeaseHolder(
                lease_id=lease_id,
                lease_obj=lease_obj,
                force_keep_alive_time=time.monotonic() + force_window,
                exit_event=threading.Event(),
                ttl=ttl_sec,
            )
            self._lease_map[ttl_sec] = holder
            thread = threading.Thread(
                target=self._lease_keep_alive,
                args=(holder, ),
                name=f"etcd-lease-keepalive-{lease_id}",
                daemon=True,
            )
            holder.thread = thread
            thread.start()
            return holder.lease_obj, holder.exit_event

    def _grant_lease(self, ttl_seconds: int) -> Tuple[int, Any]:
        """Grant a lease via the underlying etcd client."""
        lease_fn = getattr(self._client, "lease", None)
        if callable(lease_fn):
            lease_obj = lease_fn(ttl_seconds)
            lease_id = int(getattr(lease_obj, "id", id(lease_obj)))
            return lease_id, lease_obj
        grant_fn = getattr(self._client, "grant_lease", None) or getattr(self._client, "grant", None)
        if callable(grant_fn):
            rsp = grant_fn(ttl_seconds)
            lease_id = int(getattr(rsp, "ID", getattr(rsp, "id", rsp)))
            return lease_id, lease_id
        raise EtcdNamingError("etcd client does not support lease() or grant()")

    def _keep_alive_once(self, holder: _LeaseHolder) -> None:
        """Refresh a lease once."""
        refresh_fn = getattr(holder.lease_obj, "refresh", None)
        if callable(refresh_fn):
            rsp = refresh_fn()
            if isinstance(rsp, list) and rsp:
                first = rsp[0]
                ttl_left = getattr(first, "TTL", getattr(first, "ttl", None))
                if ttl_left is not None and int(ttl_left) <= 0:
                    raise EtcdNamingError(f"lease {holder.lease_id} expired (TTL={ttl_left})")
            return
        client_refresh = getattr(self._client, "refresh_lease", None) or getattr(self._client, "keep_alive_once", None)
        if callable(client_refresh):
            rsp = list(client_refresh(holder.lease_id))
            if rsp:
                first = rsp[0]
                ttl_left = getattr(first, "TTL", getattr(first, "ttl", None))
                if ttl_left is not None and int(ttl_left) <= 0:
                    raise EtcdNamingError(f"lease {holder.lease_id} expired (TTL={ttl_left})")
            return
        raise EtcdNamingError("etcd lease does not support refresh")

    def _lease_keep_alive(self, holder: _LeaseHolder) -> None:
        """Background keep-alive loop."""
        keep_alive_fn = getattr(self._client, "keep_alive", None)
        if callable(keep_alive_fn):
            try:
                stream = keep_alive_fn(holder.lease_id)
                for _ in stream:
                    if self._closed or holder.exit_event.is_set():
                        break
            except Exception:  # pylint: disable=broad-except
                logger.debug("keep_alive stream for lease %s failed", holder.lease_id, exc_info=True)
            finally:
                with self._lock:
                    self._remove_lease_locked(holder)
            return

        # Standard python-etcd3 path: periodically call lease.refresh() at ttl / 3.
        interval = max(0.05, holder.ttl / 3.0)
        while not self._closed and not holder.exit_event.is_set():
            if holder.exit_event.wait(interval):
                break
            if self._closed or holder.exit_event.is_set():
                break
            try:
                self._keep_alive_once(holder)
                with self._lock:
                    holder.force_keep_alive_time = time.monotonic() + self._get_force_window()
            except Exception:  # pylint: disable=broad-except
                logger.debug("refresh lease %s failed", holder.lease_id, exc_info=True)
                with self._lock:
                    self._remove_lease_locked(holder)
                return

    def _remove_lease_locked(self, holder: _LeaseHolder) -> None:
        """Remove ``holder`` from the map and signal ``exit_event``."""
        existing = self._lease_map.get(holder.ttl)
        if existing is not None and existing.lease_id == holder.lease_id:
            del self._lease_map[holder.ttl]
        holder.exit_event.set()

    def close(self) -> None:
        """Stop all keep-alive loops and clear tracked leases."""
        self._closed = True
        with self._lock:
            holders = list(self._lease_map.values())
            self._lease_map.clear()
        for holder in holders:
            holder.exit_event.set()


def create_lease_manager(client: Any) -> LeaseManager:
    """Create a :class:`LeaseManager`."""
    return LeaseManager(client)
