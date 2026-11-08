# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""etcd service registration plugin for tRPC-Python.

Lease-backed registration, automatic keep-alive, exponential backoff
re-registration on lease expiry, and deregistration. Also accepts
:class:`~trpc_naming_etcd.model.RegistryInfo` through ``register_info``,
``unregister``, ``heartbeat``, and ``async_heartbeat``.
"""

from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Union

from trpc_sdk.abc import BaseServiceRegistry
from trpc_sdk.abc import PluginType
from trpc_sdk.log import logger
from trpc_sdk.plugin import register_plugin

from ._constants import DEFAULT_ETCD_PREFIX
from ._constants import DEFAULT_TTL
from ._constants import DEFAULT_WEIGHT
from ._constants import PLUGIN_NAME
from .client import LeaseManager
from .client import generate_etcd_client
from .config import EtcdClientConfig
from .config import EtcdNamingConfig
from .config import EtcdRegistryConfig
from .config import EtcdServiceConfig
from .model import EtcdNode
from .model import RegistryInfo
from .model import cpp_node_key
from .model import marshal
from .model import marshal_cpp_meta
from .model import node_path
from .model import service_id
from .model import split_host_port
from .version import __version__


@dataclass
class _RegistrationHandle:
    """Background keep-alive / re-registration loop state for one registered endpoint."""

    service_name: str
    node: EtcdNode
    key: str
    value: str
    ttl: int
    stop_event: threading.Event
    thread: Optional[threading.Thread] = None

    def stop(self) -> None:
        """Signal the background registration loop to stop."""
        self.stop_event.set()


def _normalize_address(address: Union[Tuple[str, Union[int, str]], str]) -> Tuple[str, int]:
    """Normalize a ``(host, port)`` tuple or ``"host:port"`` string."""
    if isinstance(address, tuple):
        if len(address) != 2:
            raise ValueError(f"invalid address tuple: {address!r}")
        host = str(address[0]).strip()
        port_raw = address[1]
        if not host or port_raw == "" or port_raw is None:
            raise ValueError(f"invalid address tuple: {address!r}")
        return host, int(port_raw)
    if isinstance(address, str):
        return split_host_port(address)
    raise ValueError(f"unsupported address type: {type(address).__name__}")


class EtcdRegistry(BaseServiceRegistry):
    """etcd service registry plugin (``PluginType.NAMING_REGISTRY``, name ``"etcd"``).

    Supports:
    - async ``register(service, (host, port))`` /
      ``deregister(service, (host, port))`` (called by ``ServiceRegister`` on
      ``RegistryThread``), including ``address="host:port"``, lease keep-alive,
      and exponential backoff re-registration.
    - ``register_info(RegistryInfo)`` / ``unregister(RegistryInfo)`` /
      ``heartbeat(RegistryInfo)`` / ``async_heartbeat(RegistryInfo)``.
    """

    def __init__(
        self,
        etcd_client: Any = None,
        cfg: Optional[Union[EtcdRegistryConfig, EtcdServiceConfig, Dict[str, Any]]] = None,
        lease_manager: Optional[LeaseManager] = None,
        client_factory: Optional[Callable[[EtcdClientConfig, str], Any]] = None,
        pid: Optional[Union[int, str]] = None,
    ):
        super().__init__()
        self._name = PLUGIN_NAME
        self._version = __version__
        self._type = PluginType.NAMING_REGISTRY
        self._explicit_client = etcd_client
        self._explicit_cfg = cfg
        self._client_factory = client_factory
        self._etcd_client: Any = etcd_client
        self._lease_manager: Optional[LeaseManager] = lease_manager or (
            LeaseManager(etcd_client) if etcd_client is not None else None)
        self._registry_cfg: Optional[EtcdRegistryConfig] = None
        self._pid: str = str(pid if pid is not None else os.getpid())
        self._lock = threading.RLock()
        self._handles: Dict[Tuple[str, str], _RegistrationHandle] = {}
        self._last_id_by_service: Dict[str, str] = {}
        self._endpoint_clients: Dict[str, Any] = {}
        self._registered_keys: set = set()
        self._inited: bool = etcd_client is not None or cfg is not None

    def init(self) -> int:
        """Initialize the registry plugin.

        Returns:
            ``0`` after configuration is loaded.
        """
        with self._lock:
            if self._inited and self._registry_cfg is not None:
                logger.debug("Already init")
                return 0
            self._ensure_config_locked()
            self._inited = True
            return 0

    def destroy(self) -> None:
        """Stop all background registration loops and clear state."""
        with self._lock:
            if not self._inited:
                logger.debug("No init yet")
                return
            handles = list(self._handles.values())
            self._handles.clear()
            self._last_id_by_service.clear()
            self._registered_keys.clear()
            lease_mgr, self._lease_manager = self._lease_manager, None
            self._registry_cfg = None
            self._inited = False
        for handle in handles:
            handle.stop()
        if lease_mgr is not None:
            lease_mgr.close()

    def set_plugin_config(self, config: Union[EtcdNamingConfig, EtcdRegistryConfig, Dict[str, Any], list]) -> None:
        """Set the plugin configuration explicitly."""
        with self._lock:
            if isinstance(config, EtcdNamingConfig):
                self._registry_cfg = config.registry_config
            elif isinstance(config, EtcdRegistryConfig):
                self._registry_cfg = config
            else:
                self._registry_cfg = EtcdRegistryConfig.from_raw(config)

    def _ensure_config_locked(self) -> EtcdRegistryConfig:
        if self._registry_cfg is not None:
            return self._registry_cfg
        if isinstance(self._explicit_cfg, EtcdRegistryConfig):
            self._registry_cfg = self._explicit_cfg
        elif isinstance(self._explicit_cfg, EtcdServiceConfig):
            svc = self._explicit_cfg
            if not svc.prefix and not svc.cpp_mode:
                svc.prefix = DEFAULT_ETCD_PREFIX
            if svc.weight == 0:
                svc.weight = DEFAULT_WEIGHT
            if svc.ttl == 0:
                svc.ttl = DEFAULT_TTL
            self._registry_cfg = EtcdRegistryConfig(
                client_config=EtcdClientConfig(prefix=svc.prefix or DEFAULT_ETCD_PREFIX),
                services={svc.name: svc} if svc.name else {},
                services_config=[svc],
                cpp_mode=svc.cpp_mode,
            )
        elif isinstance(self._explicit_cfg, (dict, list)):
            self._registry_cfg = EtcdRegistryConfig.from_raw(self._explicit_cfg)
        else:
            self._registry_cfg = EtcdNamingConfig.from_global_config().registry_config
        return self._registry_cfg

    def _ensure_client(self, endpoint: str = "") -> Any:
        with self._lock:
            cfg = self._ensure_config_locked()
            if self._explicit_client is not None:
                return self._explicit_client
            if endpoint:
                if endpoint in self._endpoint_clients:
                    return self._endpoint_clients[endpoint]
                ep_cfg = cfg.client_config.merged({"endpoints": [endpoint]})
                client = generate_etcd_client(ep_cfg, client_factory=self._client_factory)
                self._endpoint_clients[endpoint] = client
                return client
            if self._etcd_client is None:
                self._etcd_client = generate_etcd_client(cfg.client_config, client_factory=self._client_factory)
            return self._etcd_client

    def _ensure_lease_manager(self) -> LeaseManager:
        with self._lock:
            if self._lease_manager is None:
                client = self._ensure_client()
                self._lease_manager = LeaseManager(client)
            return self._lease_manager

    def _resolve_service_config(self, service_name: str) -> Optional[EtcdServiceConfig]:
        with self._lock:
            cfg = self._ensure_config_locked()
            if isinstance(self._explicit_cfg, EtcdServiceConfig):
                return self._explicit_cfg
            return cfg.get_service_config(service_name)

    @staticmethod
    def _put_key(client: Any, key: str, value: str, lease: Any = None) -> None:
        put_fn = getattr(client, "put", None)
        if not callable(put_fn):
            raise RuntimeError("etcd client does not support put()")
        if lease is not None:
            try:
                put_fn(key, value, lease=lease)
                return
            except TypeError:
                put_fn(key, value)
                return
        put_fn(key, value)

    @staticmethod
    def _delete_key(client: Any, key: str) -> bool:
        delete_fn = getattr(client, "delete", None) or getattr(client, "rm", None)
        if not callable(delete_fn):
            raise RuntimeError("etcd client does not support delete()")
        res = delete_fn(key)
        return res is not False

    def _etcd_register_once(self, node: EtcdNode, key: str, value: str, ttl: int) -> threading.Event:
        """Acquire a lease for ``ttl`` and write ``(key, value)`` into etcd."""
        lease_mgr = self._ensure_lease_manager()
        lease_obj, expire_event = lease_mgr.get_lease(ttl)
        client = self._ensure_client()
        self._put_key(client, key, value, lease=lease_obj)
        logger.debug("register %s success, key=%s", node.name, key)
        return expire_event

    def _etcd_register_loop(self, handle: _RegistrationHandle) -> None:
        """Background registration, keep-alive, and retry loop."""
        backoff = 0.2
        while not handle.stop_event.is_set():
            try:
                expire_event = self._etcd_register_once(
                    handle.node,
                    handle.key,
                    handle.value,
                    handle.ttl,
                )
                backoff = 0.2
                # Wait until either the lease expires or deregister stops the handle.
                while not handle.stop_event.is_set() and not expire_event.is_set():
                    if expire_event.wait(timeout=0.05):
                        break
            except Exception as exc:  # pylint: disable=broad-except
                if handle.stop_event.is_set():
                    break
                logger.warning("register %s fail, retry in %.2fs: %s", handle.service_name, backoff, exc)
                if handle.stop_event.wait(backoff):
                    break
                backoff = min(backoff * 2.0, 10.0)

    def register_sync(
        self,
        service: str,
        address: Union[Tuple[str, Union[int, str]], str],
    ) -> bool:
        """Synchronously register ``service`` at ``address`` and start keep-alive.

        Idempotent: if ``(service, node_id)`` is already registered and its
        keep-alive loop is active, refreshes registration without spawning a
        duplicate thread (safe for ``trpc_sdk``'s ``RegistryThread``, which
        calls ``register()`` every second).
        """
        host, port = _normalize_address(address)
        with self._lock:
            self._ensure_config_locked()
            self._inited = True

        svc_cfg = self._resolve_service_config(service)
        if svc_cfg is None:
            logger.error("Register failed, registry.etcd has no name:%s config", service)
            return False

        if svc_cfg.cpp_mode:
            info = RegistryInfo(name=service, host=host, port=port, meta=dict(svc_cfg.metadata))
            return self.register_info(info, allow_overwrite=True) == 0

        node_id = service_id(host, port, self._pid)
        prefix = svc_cfg.prefix or DEFAULT_ETCD_PREFIX
        weight = svc_cfg.weight if svc_cfg.weight > 0 else DEFAULT_WEIGHT
        ttl = svc_cfg.ttl if svc_cfg.ttl > 0 else DEFAULT_TTL
        node = EtcdNode(
            name=service,
            id=node_id,
            address=f"{host}:{port}",
            metadata=dict(svc_cfg.metadata),
            weight=weight,
        )
        key = node_path(prefix, node.name, node.id)
        value = marshal(node)

        handle_key = (service, node_id)
        with self._lock:
            self._last_id_by_service[service] = node_id
            existing = self._handles.get(handle_key)
            if existing is not None and not existing.stop_event.is_set():
                return True

        # Perform initial registration immediately so callers see the key right away,
        # and still start the background keep-alive/retry loop even if the first attempt fails.
        try:
            self._etcd_register_once(node, key, value, ttl)
        except Exception as exc:  # pylint: disable=broad-except
            logger.warning("initial etcd register for %s failed, background retry will continue: %s", service, exc)

        stop_event = threading.Event()
        handle = _RegistrationHandle(
            service_name=service,
            node=node,
            key=key,
            value=value,
            ttl=ttl,
            stop_event=stop_event,
        )
        thread = threading.Thread(
            target=self._etcd_register_loop,
            args=(handle, ),
            name=f"etcd-register-{service}-{node_id}",
            daemon=True,
        )
        handle.thread = thread
        with self._lock:
            old = self._handles.pop(handle_key, None)
            self._handles[handle_key] = handle
        if old is not None:
            old.stop()
        thread.start()
        return True

    async def register(self, service: str, address: Union[Tuple[str, Union[int, str]], str]) -> bool:
        """Publish one service endpoint to etcd (:class:`BaseServiceRegistry` contract)."""
        return self.register_sync(service, address)

    def deregister_sync(
        self,
        service: str,
        address: Optional[Union[Tuple[str, Union[int, str]], str]] = None,
    ) -> bool:
        """Synchronously withdraw ``service`` from etcd."""
        svc_cfg = self._resolve_service_config(service)
        if svc_cfg is not None and svc_cfg.cpp_mode and address is not None:
            host, port = _normalize_address(address)
            info = RegistryInfo(name=service, host=host, port=port)
            return self.unregister(info) == 0

        if address is not None:
            host, port = _normalize_address(address)
            node_id = service_id(host, port, self._pid)
        else:
            with self._lock:
                node_id = self._last_id_by_service.get(service, "")

        handle_key = (service, node_id)
        with self._lock:
            handle = self._handles.pop(handle_key, None)
            if handle is None and not node_id:
                # Stop all handles belonging to this service name.
                matching = [k for k in self._handles if k[0] == service]
                for k in matching:
                    h = self._handles.pop(k)
                    h.stop()
        if handle is not None:
            handle.stop()

        prefix = (svc_cfg.prefix if svc_cfg is not None else "") or DEFAULT_ETCD_PREFIX
        key = node_path(prefix, service, node_id)
        client = self._ensure_client()
        self._delete_key(client, key)
        return True

    async def deregister(
        self,
        service: str,
        address: Optional[Union[Tuple[str, Union[int, str]], str]] = None,
    ) -> bool:
        """Withdraw one service endpoint from etcd (:class:`BaseServiceRegistry` contract)."""
        return self.deregister_sync(service, address)

    def get_etcd_key(self, conf: EtcdServiceConfig, info: RegistryInfo) -> str:
        """Build the etcd key ``/{prefix}_{name}/{host}/{port}``."""
        return cpp_node_key(conf.prefix, conf.name, info.host, info.port)

    def register_info(self, info: Optional[RegistryInfo], allow_overwrite: bool = False) -> int:
        """Register a :class:`RegistryInfo` entry.

        Returns:
            ``0`` on success, ``-1`` on error.
        """
        if not self._inited:
            logger.error("No init yet")
            return -1
        if info is None or not info.name or not info.host or not info.port:
            logger.error("Input parameter is empty")
            return -1

        with self._lock:
            cfg = self._ensure_config_locked()
            conf = cfg.services.get(info.name)
        if conf is None:
            logger.error("Register failed, registry.etcd has no name:%s config", info.name)
            return -1
        if not conf.endpoint and self._explicit_client is None:
            logger.error("Register failed, registry.etcd has no endpoint:%s config", info.name)
            return -1

        str_key = self.get_etcd_key(conf, info)
        if not allow_overwrite and str_key in self._registered_keys:
            logger.error("Duplicate registration for key: %s", str_key)
            return -1

        merged_meta = dict(conf.metadata)
        if info.meta:
            merged_meta.update({str(k): str(v) for k, v in info.meta.items()})
        str_meta_json = marshal_cpp_meta(merged_meta)

        try:
            client = self._ensure_client(conf.endpoint)
            add_fn = getattr(client, "add", None)
            if callable(add_fn) and not allow_overwrite:
                ok = add_fn(str_key, str_meta_json)
                if ok is False:
                    return -1
            else:
                if not allow_overwrite:
                    get_fn = getattr(client, "get", None)
                    if callable(get_fn):
                        existing = get_fn(str_key)
                        val = existing[0] if isinstance(existing, tuple) else existing
                        if val is not None:
                            logger.error("Register failed, key %s already exists", str_key)
                            return -1
                self._put_key(client, str_key, str_meta_json)
            self._registered_keys.add(str_key)
            return 0
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Register failed, service_name:%s, err:%s", str_key, exc)
            return -1

    def unregister(self, info: Optional[RegistryInfo]) -> int:
        """Unregister a :class:`RegistryInfo` entry.

        Returns:
            ``0`` on success, ``-1`` on error.
        """
        if not self._inited:
            logger.error("No init yet")
            return -1
        if info is None or not info.name:
            logger.error("Input parameter is empty")
            return -1

        with self._lock:
            cfg = self._ensure_config_locked()
            conf = cfg.services.get(info.name)
        if conf is None:
            logger.error("Unregister failed, registry.etcd has no name:%s config", info.name)
            return -1
        if not conf.endpoint and self._explicit_client is None:
            logger.error("Unregister failed, registry.etcd has no endpoint:%s config", info.name)
            return -1

        str_key = self.get_etcd_key(conf, info)
        try:
            client = self._ensure_client(conf.endpoint)
            ok = self._delete_key(client, str_key)
            self._registered_keys.discard(str_key)
            return 0 if ok else -1
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Deregister failed, service_name:%s, err:%s", str_key, exc)
            return -1

    def heartbeat(self, info: Optional[RegistryInfo]) -> int:
        """Synchronous heartbeat check."""
        if not self._inited:
            logger.error("No init yet")
            return -1
        if info is None:
            logger.error("Input parameter is empty")
            return -1
        return 0

    async def async_heartbeat(self, info: Optional[RegistryInfo]) -> None:
        """Asynchronous heartbeat check."""
        if not self._inited:
            raise RuntimeError("No init yet")
        if info is None:
            raise ValueError("Input parameter is empty")

    def stop(self) -> None:
        """Plugin lifecycle stop hook."""
        self.destroy()


def create_registry(
    etcd_client: Any,
    cfg: Optional[Union[EtcdServiceConfig, EtcdRegistryConfig, Dict[str, Any]]] = None,
) -> EtcdRegistry:
    """Create a new :class:`EtcdRegistry`."""
    return EtcdRegistry(etcd_client=etcd_client, cfg=cfg or EtcdServiceConfig())


register_plugin(PluginType.NAMING_REGISTRY, PLUGIN_NAME, version=__version__, report=True)(EtcdRegistry)
