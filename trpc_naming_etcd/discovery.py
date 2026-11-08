# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""etcd service discovery, local node cache, and prefix watcher.

Supports a shared prefix cache and per-service list configuration.
"""

from __future__ import annotations

import inspect
import queue
import threading
import time
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple
from typing import Union

from trpc_sdk.abc import BaseDiscovery
from trpc_sdk.abc import Node as TrpcNode
from trpc_sdk.abc import PluginType
from trpc_sdk.log import logger
from trpc_sdk.plugin import register_plugin

from ._constants import LIST_SELECTOR_CACHE_TIMEOUT
from ._constants import DEFAULT_CACHE_TTL
from ._constants import DEFAULT_ETCD_PREFIX
from ._constants import EventType
from ._constants import PLUGIN_NAME
from .client import generate_etcd_client
from .config import EtcdClientConfig
from .config import EtcdNamingConfig
from .config import EtcdSelectorConfig
from .config import EtcdServiceConfig
from .error import ServerNotAvailableError
from .error import StaleDataError
from .model import EtcdNode
from .model import convert_node
from .model import cpp_service_key
from .model import parse_cpp_endpoint_info
from .model import service_path
from .model import unmarshal
from .version import __version__


@dataclass
class DiscoveryConfig:
    """Configuration for :class:`EtcdDiscovery`."""

    prefix: str = DEFAULT_ETCD_PREFIX
    cache_ttl: float = DEFAULT_CACHE_TTL


@dataclass
class WatchResult:
    """One prefix-watch change notification."""

    version: int = 0
    event_type: EventType = EventType.UNKNOWN
    node: Optional[EtcdNode] = None


class _SingleFlight:
    """Coalesce concurrent calls for the same key."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._calls: Dict[str, "_InFlightCall"] = {}

    def do(self, key: str, fn: Callable[[], Any]) -> Any:
        """Execute ``fn`` once for all concurrent callers sharing ``key``."""
        with self._lock:
            call = self._calls.get(key)
            if call is not None:
                pass
            else:
                call = _InFlightCall()
                self._calls[key] = call
                is_leader = True
            if call is not None and not locals().get("is_leader", False):
                is_leader = False

        if not is_leader:
            call.event.wait()
            if call.exc is not None:
                raise call.exc
            return call.result

        try:
            call.result = fn()
            return call.result
        except Exception as exc:  # pylint: disable=broad-except
            call.exc = exc
            raise
        finally:
            with self._lock:
                self._calls.pop(key, None)
            call.event.set()


class _InFlightCall:
    """State of a single in-flight operation in :class:`_SingleFlight`."""

    def __init__(self) -> None:
        self.event = threading.Event()
        self.result: Any = None
        self.exc: Optional[Exception] = None


def _is_delete_event(event: Any) -> bool:
    """Return True if an ``etcd3`` watch event represents a key deletion."""
    event_type = getattr(event, "type", None)
    if event_type is not None:
        name = getattr(event_type, "name", str(event_type)).upper()
        if "DELETE" in name or "DEL" in name:
            return True
        if "PUT" in name:
            return False
    if type(event).__name__.endswith("DeleteEvent"):
        return True
    return getattr(event, "value", None) is None and getattr(event, "prev_value", None) is not None


def _classify_event(event: Any) -> Tuple[EventType, Optional[bytes], Optional[bytes], int]:
    """Extract ``(event_type, key, payload_bytes, revision)`` from an etcd watch event."""
    key = getattr(event, "key", None)
    mod_rev = int(getattr(event, "mod_revision", 0) or 0)
    create_rev = int(getattr(event, "create_revision", 0) or 0)
    header_rev = int(getattr(event, "version_revision", 0) or mod_rev or create_rev or 0)

    if _is_delete_event(event):
        raw_val = getattr(event, "prev_value", None)
        if raw_val is None:
            prev_kv = getattr(event, "prev_kv", None)
            if prev_kv is not None:
                raw_val = getattr(prev_kv, "value", None)
        if raw_val is None:
            raw_val = getattr(event, "value", None)
        return EventType.DELETE, key, _to_bytes(raw_val), header_rev

    raw_val = getattr(event, "value", None)
    if raw_val is None:
        kv = getattr(event, "kv", None)
        if kv is not None:
            raw_val = getattr(kv, "value", None)
            if not create_rev:
                create_rev = int(getattr(kv, "create_revision", 0) or 0)
            if not mod_rev:
                mod_rev = int(getattr(kv, "mod_revision", 0) or 0)
            if not header_rev:
                header_rev = mod_rev
    if create_rev and mod_rev and create_rev == mod_rev:
        ev_type = EventType.CREATE
    elif mod_rev > create_rev > 0:
        ev_type = EventType.UPDATE
    else:
        ev_type = EventType.CREATE
    return ev_type, key, _to_bytes(raw_val), header_rev


def _to_bytes(val: Any) -> Optional[bytes]:
    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        return bytes(val)
    return str(val).encode("utf-8")


class EtcdWatcher:
    """Watch an etcd key prefix and emit :class:`WatchResult` objects."""

    def __init__(self, etcd_client: Any, cfg: DiscoveryConfig):
        self.etcd_client = etcd_client
        self.cfg = cfg
        self.exit_event = threading.Event()
        self.watch_path = service_path(cfg.prefix or DEFAULT_ETCD_PREFIX, "")
        self._cancel_fn: Optional[Callable[[], Any]] = None
        self._lock = threading.Lock()
        self._last_revision: int = 0

    def stop(self) -> None:
        """Stop the prefix watch."""
        self.exit_event.set()
        with self._lock:
            cancel_fn, self._cancel_fn = self._cancel_fn, None
        if cancel_fn is not None:
            try:
                cancel_fn()
            except Exception:  # pylint: disable=broad-except
                logger.debug("cancel etcd prefix watch failed", exc_info=True)

    def _open_watch_prefix(self) -> Tuple[Iterator[Any], Optional[Callable[[], Any]]]:
        """Open a prefix watch iterator on ``self.etcd_client``."""
        watch_prefix_fn = getattr(self.etcd_client, "watch_prefix", None)
        if callable(watch_prefix_fn):
            kwargs: Dict[str, Any] = {}
            try:
                sig = inspect.signature(watch_prefix_fn)
                if "prev_kv" in sig.parameters or any(
                        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values()):
                    kwargs["prev_kv"] = True
                if self._last_revision and ("start_revision" in sig.parameters or any(
                        p.kind == inspect.Parameter.VAR_KEYWORD for p in sig.parameters.values())):
                    kwargs["start_revision"] = self._last_revision + 1
            except (TypeError, ValueError):
                pass
            try:
                return watch_prefix_fn(self.watch_path, **kwargs)
            except TypeError:
                return watch_prefix_fn(self.watch_path)

        watch_fn = getattr(self.etcd_client, "watch", None)
        if callable(watch_fn):
            return watch_fn(self.watch_path)
        raise RuntimeError("etcd client does not support watch_prefix() or watch()")

    def watch(self) -> "queue.Queue[Optional[WatchResult]]":
        """Start watching ``self.watch_path`` on a daemon thread and return a queue of results."""
        result_queue: "queue.Queue[Optional[WatchResult]]" = queue.Queue()

        def _run() -> None:
            try:
                while not self.exit_event.is_set():
                    try:
                        events, cancel = self._open_watch_prefix()
                        with self._lock:
                            self._cancel_fn = cancel
                        if self.exit_event.is_set():
                            if callable(cancel):
                                cancel()
                            break
                        for event in events:
                            if self.exit_event.is_set():
                                break
                            if isinstance(event, WatchResult):
                                result_queue.put(event)
                                continue
                            ev_type, key, val_bytes, rev = _classify_event(event)
                            if rev > 0:
                                self._last_revision = rev
                            if val_bytes is None:
                                continue
                            try:
                                node = unmarshal(val_bytes, key=key)
                            except Exception as exc:  # pylint: disable=broad-except
                                logger.error("unmarshal node fail, err: %s", exc)
                                continue
                            if node is None:
                                continue
                            result_queue.put(WatchResult(version=rev, event_type=ev_type, node=node))
                        # Watch stream ended normally; exit the watch loop.
                        break
                    except Exception:  # pylint: disable=broad-except
                        if self.exit_event.is_set():
                            break
                        logger.debug("etcd watcher interrupted", exc_info=True)
                        break
            finally:
                with self._lock:
                    self._cancel_fn = None
                result_queue.put(None)

        thread = threading.Thread(target=_run, name=f"etcd-watcher-{self.watch_path}", daemon=True)
        thread.start()
        return result_queue


class DiscoveryCache:
    """In-memory node cache updated by :class:`EtcdWatcher`."""

    def __init__(self, etcd_client: Any, cfg: Optional[DiscoveryConfig] = None, start_watcher: bool = True):
        self.cfg = cfg or DiscoveryConfig()
        if not self.cfg.prefix:
            self.cfg.prefix = DEFAULT_ETCD_PREFIX
        self._lock = threading.RLock()
        self.node_cache: Dict[str, List[TrpcNode]] = {}
        self.expires: Dict[str, float] = {}
        self.watched: Set[str] = set()
        self.version: int = 0
        self.exit_event = threading.Event()
        self.watcher = EtcdWatcher(etcd_client, self.cfg)
        self._watch_thread: Optional[threading.Thread] = None
        if start_watcher:
            self._watch_thread = threading.Thread(
                target=self.watch,
                name="etcd-discovery-cache-watch",
                daemon=True,
            )
            self._watch_thread.start()

    def _set_locked(self, service_name: str, nodes: List[TrpcNode]) -> None:
        self.node_cache[service_name] = nodes
        self.expires[service_name] = time.monotonic() + self.cfg.cache_ttl

    def invalid_cache(self, service_name: str) -> None:
        """Delete the cached node list for ``service_name``."""
        with self._lock:
            self._delete_locked(service_name)

    def _delete_locked(self, service_name: str) -> None:
        self.node_cache.pop(service_name, None)
        self.expires.pop(service_name, None)

    def cache(self, service_name: str, version: int, nodes: Optional[List[TrpcNode]]) -> None:
        """Store a full snapshot of ``nodes`` at ``version``.

        Raises:
            StaleDataError: If ``version < self.version``.
        """
        if nodes is None:
            return
        with self._lock:
            if version < self.version:
                raise StaleDataError()
            self.version = version
            if service_name not in self.watched:
                return
            self._set_locked(service_name, list(nodes))

    def update(self, result: Optional[WatchResult]) -> None:
        """Apply an incremental :class:`WatchResult` to the cache."""
        if result is None or result.node is None:
            return
        with self._lock:
            service_name = result.node.name
            if result.version < self.version:
                return
            if service_name not in self.watched:
                return
            if service_name not in self.node_cache:
                # Only apply incremental updates after a full snapshot has been cached.
                return
            self.version = result.version
            nodes = self.node_cache[service_name]

            matched_node: Optional[TrpcNode] = None
            matched_index: int = -1
            for i, existing in enumerate(nodes):
                if existing.address == result.node.address:
                    matched_node = existing
                    matched_index = i

            if result.event_type in (EventType.CREATE, EventType.UPDATE):
                converted = convert_node(result.node)
                if matched_node is None:
                    self._set_locked(service_name, list(nodes) + [converted])
                    return
                nodes[matched_index] = converted
            elif result.event_type == EventType.DELETE:
                if matched_node is None:
                    return
                new_nodes = [n for n in nodes if n.address != result.node.address]
                self._set_locked(service_name, new_nodes)

    @staticmethod
    def is_valid(nodes: Optional[List[TrpcNode]], expire: Optional[float]) -> bool:
        """Return True if ``nodes`` is cached and ``expire`` is in the future."""
        if nodes is None:
            return False
        if not expire or expire <= 0:
            return False
        if time.monotonic() > expire:
            return False
        return True

    def list(self, service_name: str) -> Optional[List[TrpcNode]]:
        """Look up ``service_name`` in the cache.

        Returns:
            A non-empty list of :class:`~trpc_sdk.abc.Node` on a valid cache
            hit, or ``None`` on a cache miss (after marking ``service_name``
            as watched).

        Raises:
            ServerNotAvailableError: If ``service_name`` is cached as an
                empty list and has not expired yet.
        """
        with self._lock:
            nodes = self.node_cache.get(service_name)
            expire = self.expires.get(service_name)
            if self.is_valid(nodes, expire):
                if len(nodes) == 0:  # type: ignore[arg-type]
                    raise ServerNotAvailableError()
                return list(nodes)  # type: ignore[arg-type]
            self.watched.add(service_name)
            return None

    def watch(self) -> None:
        """Consume events from :attr:`watcher` until stopped."""
        result_queue = self.watcher.watch()
        while not self.exit_event.is_set():
            try:
                result = result_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            if result is None or self.exit_event.is_set():
                return
            self.update(result)

    def stop(self) -> None:
        """Stop the cache watcher idempotently."""
        with self._lock:
            if self.exit_event.is_set():
                return
            self.exit_event.set()
            self.watcher.stop()


@dataclass
class _ListCacheEntry:
    """Per-key cache entry for list-form service discovery."""

    cache_time: float
    nodes: List[TrpcNode]


class EtcdDiscovery(BaseDiscovery):
    """Service discovery backed by etcd, local cache, and prefix watch.

    Registered as ``PluginType.NAMING_DISCOVERY`` under the name ``"etcd"``.
    """

    def __init__(
        self,
        etcd_client: Any = None,
        cfg: Optional[Union[DiscoveryConfig, EtcdSelectorConfig, Dict[str, Any]]] = None,
        client_factory: Optional[Callable[[EtcdClientConfig, str], Any]] = None,
    ):
        super().__init__()
        self._explicit_client = etcd_client
        self._explicit_cfg = cfg
        self._client_factory = client_factory
        self._lock = threading.RLock()
        self._sg = _SingleFlight()
        self._cache: Optional[DiscoveryCache] = None
        self._selector_cfg: Optional[EtcdSelectorConfig] = None
        self._etcd_client: Any = etcd_client
        self._list_cache: Dict[str, _ListCacheEntry] = {}
        self._endpoint_clients: Dict[str, Any] = {}

        if etcd_client is not None:
            disc_cfg = self._to_discovery_config(cfg)
            self._cache = DiscoveryCache(etcd_client, disc_cfg)

    @staticmethod
    def _to_discovery_config(
        cfg: Optional[Union[DiscoveryConfig, EtcdSelectorConfig, Dict[str, Any]]]
    ) -> DiscoveryConfig:
        if isinstance(cfg, DiscoveryConfig):
            return DiscoveryConfig(prefix=cfg.prefix or DEFAULT_ETCD_PREFIX, cache_ttl=cfg.cache_ttl)
        if isinstance(cfg, EtcdSelectorConfig):
            return DiscoveryConfig(prefix=cfg.prefix or DEFAULT_ETCD_PREFIX)
        if isinstance(cfg, dict):
            prefix = cfg.get("prefix") or cfg.get("Prefix") or DEFAULT_ETCD_PREFIX
            return DiscoveryConfig(prefix=str(prefix))
        return DiscoveryConfig(prefix=DEFAULT_ETCD_PREFIX)

    def set_plugin_config(self, config: Union[EtcdNamingConfig, EtcdSelectorConfig, Dict[str, Any]]) -> None:
        """Explicitly set the selector/discovery configuration."""
        with self._lock:
            if isinstance(config, EtcdNamingConfig):
                self._selector_cfg = config.selector_config
            elif isinstance(config, EtcdSelectorConfig):
                self._selector_cfg = config
            elif isinstance(config, dict):
                self._selector_cfg = EtcdSelectorConfig.from_raw(config)
            self._cache = None
            self._list_cache.clear()

    def _ensure_initialized(self) -> EtcdSelectorConfig:
        with self._lock:
            if self._selector_cfg is None:
                if isinstance(self._explicit_cfg, EtcdSelectorConfig):
                    self._selector_cfg = self._explicit_cfg
                elif isinstance(self._explicit_cfg, DiscoveryConfig):
                    self._selector_cfg = EtcdSelectorConfig(
                        client_config=EtcdClientConfig(prefix=self._explicit_cfg.prefix or DEFAULT_ETCD_PREFIX))
                elif isinstance(self._explicit_cfg, dict):
                    self._selector_cfg = EtcdSelectorConfig.from_raw(self._explicit_cfg)
                else:
                    self._selector_cfg = EtcdNamingConfig.from_global_config().selector_config

            if not self._selector_cfg.cpp_mode and self._cache is None:
                if self._etcd_client is None:
                    self._etcd_client = generate_etcd_client(
                        self._selector_cfg.client_config,
                        client_factory=self._client_factory,
                    )
                disc_cfg = DiscoveryConfig(prefix=self._selector_cfg.prefix or DEFAULT_ETCD_PREFIX)
                self._cache = DiscoveryCache(self._etcd_client, disc_cfg)

            return self._selector_cfg

    def _get_client_for_endpoint(self, base_cfg: EtcdClientConfig, endpoint: str) -> Any:
        if self._explicit_client is not None:
            return self._explicit_client
        with self._lock:
            if endpoint in self._endpoint_clients:
                return self._endpoint_clients[endpoint]
            cfg = base_cfg.merged({"endpoints": [endpoint]}) if endpoint else base_cfg
            client = generate_etcd_client(cfg, client_factory=self._client_factory)
            self._endpoint_clients[endpoint] = client
            return client

    def list_sync(self, service_name: str, namespace: str = "") -> List[TrpcNode]:
        """Synchronously list available nodes for ``service_name``."""
        del namespace  # Accepted for the tRPC discovery contract; etcd keys by prefix + service.
        selector_cfg = self._ensure_initialized()

        # Per-service path when configured as a list of service entries.
        if selector_cfg.cpp_mode or service_name in selector_cfg.services:
            svc_cfg = selector_cfg.services.get(service_name)
            if svc_cfg is not None and svc_cfg.cpp_mode:
                return self._list_configured_service(selector_cfg, svc_cfg)
            if selector_cfg.cpp_mode and svc_cfg is None:
                raise ServerNotAvailableError(f"Select failed, service_name:{service_name}, no service conf info")

        assert self._cache is not None
        cached = self._cache.list(service_name)
        if cached:
            return cached

        def _fetch() -> List[TrpcNode]:
            version, nodes = self._list_from_etcd(service_name)
            try:
                assert self._cache is not None
                self._cache.cache(service_name, version, nodes)
            except StaleDataError:
                assert self._cache is not None
                self._cache.invalid_cache(service_name)
            return nodes

        try:
            nodes = self._sg.do(service_name, _fetch)
        except Exception as exc:
            logger.error("get %s node from etcd fail, err = %s", service_name, exc)
            raise
        return list(nodes)

    async def list(self, service_name: str, namespace: str = "") -> List[TrpcNode]:
        """List available nodes for ``service_name`` (:class:`BaseDiscovery` contract)."""
        return self.list_sync(service_name, namespace)

    async def select_node(self, service_name: str, opt: Any = None) -> Optional[TrpcNode]:
        """Decline direct selection so :class:`TrpcSelector` runs router + load balancer."""
        del service_name, opt
        return None

    def _list_from_etcd(self, service_name: str) -> Tuple[int, List[TrpcNode]]:
        """Fetch all instances of ``service_name`` under the configured prefix."""
        selector_cfg = self._ensure_initialized()
        client = self._etcd_client
        prefix = service_path(selector_cfg.prefix or DEFAULT_ETCD_PREFIX, service_name)
        entries, revision = self._fetch_prefix(client, prefix)

        services: List[TrpcNode] = []
        for raw_val, raw_key, _meta in entries:
            if raw_val is None:
                continue
            node = unmarshal(raw_val, key=raw_key)
            if node is None:
                continue
            services.append(convert_node(node))
        return revision, services

    def _list_configured_service(self, selector_cfg: EtcdSelectorConfig, svc_cfg: EtcdServiceConfig) -> List[TrpcNode]:
        """Fetch nodes with the ``/{prefix}_{name}`` key layout and a 60s cache."""
        if not svc_cfg.endpoint and self._explicit_client is None:
            raise ServerNotAvailableError(
                f"Select failed, service_name:{svc_cfg.name}, no service conf endpoint info")
        str_key = cpp_service_key(svc_cfg.prefix, svc_cfg.name)
        now = time.monotonic()
        with self._lock:
            entry = self._list_cache.get(str_key)
            if entry is not None and entry.cache_time + LIST_SELECTOR_CACHE_TIMEOUT > now:
                return list(entry.nodes)

        client = self._get_client_for_endpoint(selector_cfg.client_config, svc_cfg.endpoint)
        entries, _ = self._fetch_prefix(client, str_key)
        if not entries:
            raise ServerNotAvailableError(f"Select failed, service_name:{str_key}, no service info")

        nodes: List[TrpcNode] = []
        for raw_val, raw_key, _meta in entries:
            key_str = raw_key.decode("utf-8", errors="replace") if isinstance(raw_key, (bytes, bytearray)) else str(
                raw_key or "")
            val_str = raw_val.decode("utf-8", errors="replace") if isinstance(raw_val, (bytes, bytearray)) else str(
                raw_val or "")
            ep = parse_cpp_endpoint_info(key_str, val_str)
            if ep is not None:
                nodes.append(
                    TrpcNode(
                        service_name=svc_cfg.name,
                        address=f"{ep.host}:{ep.port}",
                        ip=ep.host,
                        port=ep.port,
                        weight=1,
                        metadata=dict(ep.meta),
                    ))
            else:
                try:
                    etcd_node = unmarshal(val_str, key=key_str)
                    if etcd_node is not None:
                        nodes.append(convert_node(etcd_node))
                except Exception:  # pylint: disable=broad-except
                    continue

        if not nodes:
            raise ServerNotAvailableError(f"Select failed not node, service_name:{str_key}")

        with self._lock:
            self._list_cache[str_key] = _ListCacheEntry(cache_time=now, nodes=list(nodes))
        return nodes

    @staticmethod
    def _fetch_prefix(client: Any, prefix: str) -> Tuple[List[Tuple[Any, Any, Any]], int]:
        """Run a prefix query against ``client`` and normalize ``(value, key, meta)`` + revision."""
        revision = 0
        entries: List[Tuple[Any, Any, Any]] = []

        get_prefix_fn = getattr(client, "get_prefix", None)
        if callable(get_prefix_fn):
            result = list(get_prefix_fn(prefix))
            for item in result:
                if isinstance(item, tuple) and len(item) == 2:
                    val, meta = item
                    key = getattr(meta, "key", None)
                    rev = getattr(meta, "mod_revision", None) or getattr(
                        getattr(meta, "response_header", None), "revision", 0)
                    if rev and int(rev) > revision:
                        revision = int(rev)
                    entries.append((val, key, meta))
                else:
                    val = getattr(item, "value", item)
                    key = getattr(item, "key", None)
                    entries.append((val, key, None))
            last_rev = getattr(client, "last_revision", 0)
            if last_rev and not revision:
                revision = int(last_rev)
            return entries, revision

        get_fn = getattr(client, "get", None)
        if callable(get_fn):
            rsp = get_fn(prefix)
            kvs = getattr(rsp, "kvs", getattr(rsp, "Kvs", None))
            header = getattr(rsp, "header", getattr(rsp, "Header", None))
            if header is not None:
                revision = int(getattr(header, "revision", getattr(header, "Revision", 0)) or 0)
            if kvs is not None:
                for kv in kvs:
                    val = getattr(kv, "value", getattr(kv, "Value", None))
                    key = getattr(kv, "key", getattr(kv, "Key", None))
                    entries.append((val, key, kv))
                return entries, revision
            if isinstance(rsp, tuple) and len(rsp) == 2 and rsp[0] is not None:
                entries.append((rsp[0], prefix, rsp[1]))
                return entries, revision

        raise RuntimeError("etcd client does not support get_prefix() or get()")

    def close(self) -> None:
        """Stop the watcher and release cached resources."""
        with self._lock:
            cache, self._cache = self._cache, None
            self._list_cache.clear()
        if cache is not None:
            cache.stop()

    def stop(self) -> None:
        """Plugin lifecycle stop hook."""
        self.close()


def create_discovery(etcd_client: Any, cfg: Optional[DiscoveryConfig] = None) -> EtcdDiscovery:
    """Create a new :class:`EtcdDiscovery` instance."""
    return EtcdDiscovery(etcd_client=etcd_client, cfg=cfg or DiscoveryConfig())


register_plugin(PluginType.NAMING_DISCOVERY, PLUGIN_NAME, version=__version__, report=True)(EtcdDiscovery)
