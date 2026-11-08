# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""etcd service selector plugin for tRPC-Python.

Selects a discovered node with a load balancer and reports call results.
Batch selection uses :class:`SelectorPolicy`.
"""

from __future__ import annotations

import inspect
import random
import threading
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from trpc_sdk.abc import BaseCircuitBreaker
from trpc_sdk.abc import BaseLoadBalance
from trpc_sdk.abc import BaseSelector
from trpc_sdk.abc import Node as TrpcNode
from trpc_sdk.abc import PluginType
from trpc_sdk.abc import SelectorOption
from trpc_sdk.log import logger
from trpc_sdk.plugin import get_plugin
from trpc_sdk.plugin import register_plugin

from ._constants import DEFAULT_LOAD_BALANCER
from ._constants import PLUGIN_NAME
from ._constants import SelectorPolicy
from .config import EtcdClientConfig
from .config import EtcdNamingConfig
from .config import EtcdSelectorConfig
from .discovery import EtcdDiscovery
from .error import BalancerNotExistError
from .error import ServerNotAvailableError
from .model import TrpcEndpointInfo
from .version import __version__


@dataclass
class SelectorConfig:
    """Configuration for :class:`EtcdSelector`."""

    load_balancer: str = DEFAULT_LOAD_BALANCER


@dataclass
class SelectorInfo:
    """Selector request: service name and how many endpoints to return."""

    name: str = ""
    policy: SelectorPolicy = SelectorPolicy.ONE


@dataclass
class InvokeResult:
    """Call outcome reported after an invocation."""

    name: str = ""
    cost: int = 0
    ret_code: int = 0


class _RoundRobinBalancer:
    """Thread-safe per-service round-robin load balancer."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._counters: Dict[str, int] = {}

    def select(self, service_name: str, nodes: List[TrpcNode]) -> TrpcNode:
        if not nodes:
            raise ServerNotAvailableError(f"no nodes available for service {service_name!r}")
        with self._lock:
            idx = self._counters.get(service_name, 0)
            self._counters[service_name] = idx + 1
        return nodes[idx % len(nodes)]


_ROUND_ROBIN = _RoundRobinBalancer()


def _select_by_balancer(
    balancer_name: str,
    service_name: str,
    nodes: List[TrpcNode],
    namespace: str = "",
) -> Any:
    """Resolve and run the load balancer named ``balancer_name``."""
    name = (balancer_name or DEFAULT_LOAD_BALANCER).strip()
    lower = name.lower()

    if lower in ("random", "randomlb", "random_load_balance"):
        if not nodes:
            raise ServerNotAvailableError(f"no nodes available for service {service_name!r}")
        return random.choice(nodes)

    if lower in ("round_robin", "roundrobin", "rr", "roundrobinlb"):
        return _ROUND_ROBIN.select(service_name, nodes)

    lb_plugin: Optional[BaseLoadBalance] = get_plugin(PluginType.NAMING_LOADBALANCE, name)
    if lb_plugin is None:
        raise BalancerNotExistError(f"load balancer {name!r} is not exist")

    if not nodes:
        raise ServerNotAvailableError(f"no nodes available for service {service_name!r}")

    return lb_plugin.select(service_name, nodes, namespace)


class EtcdSelector(BaseSelector):
    """etcd selector plugin (``PluginType.NAMING_SELECTOR``, name ``"etcd"``).

    Used when a client target is ``etcd://<service_name>`` or when accessed
    directly via ``get_selector("etcd")``.
    """

    def __init__(
        self,
        discovery: Optional[EtcdDiscovery] = None,
        cfg: Optional[Union[SelectorConfig, EtcdSelectorConfig, Dict[str, Any]]] = None,
        etcd_client: Any = None,
        client_factory: Optional[Callable[[EtcdClientConfig, str], Any]] = None,
    ):
        super().__init__()
        self._name = PLUGIN_NAME
        self._version = __version__
        self._type = PluginType.NAMING_SELECTOR
        self._explicit_discovery = discovery
        self._explicit_cfg = cfg
        self._explicit_client = etcd_client
        self._client_factory = client_factory
        self._discovery: Optional[EtcdDiscovery] = discovery
        self._selector_cfg: Optional[EtcdSelectorConfig] = None
        self._lock = threading.RLock()
        self._inited: bool = discovery is not None or cfg is not None or etcd_client is not None
        self._white_list: List[int] = []

    def init(self) -> int:
        """Initialize the selector plugin.

        Returns:
            ``0`` after discovery and configuration are ready.
        """
        with self._lock:
            if self._inited and self._discovery is not None:
                logger.debug("Already init")
                return 0
            self._ensure_initialized()
            self._inited = True
            return 0

    def destroy(self) -> None:
        """Release discovery and cache resources."""
        with self._lock:
            if not self._inited:
                logger.debug("No init yet")
                return
            disc = self._discovery
            if self._explicit_discovery is None:
                self._discovery = None
            self._selector_cfg = None
            self._inited = False
        if disc is not None and disc is not self._explicit_discovery:
            close_fn = getattr(disc, "close", None)
            if callable(close_fn):
                close_fn()

    def set_plugin_config(self, config: Union[EtcdNamingConfig, EtcdSelectorConfig, Dict[str, Any], list]) -> None:
        """Set the selector plugin configuration explicitly."""
        with self._lock:
            if isinstance(config, EtcdNamingConfig):
                self._selector_cfg = config.selector_config
            elif isinstance(config, EtcdSelectorConfig):
                self._selector_cfg = config
            else:
                self._selector_cfg = EtcdSelectorConfig.from_raw(config)
            if self._discovery is not None and hasattr(self._discovery, "set_plugin_config"):
                self._discovery.set_plugin_config(self._selector_cfg)

    def _ensure_initialized(self) -> Tuple[EtcdDiscovery, EtcdSelectorConfig]:
        with self._lock:
            if self._selector_cfg is None:
                if isinstance(self._explicit_cfg, EtcdSelectorConfig):
                    self._selector_cfg = self._explicit_cfg
                elif isinstance(self._explicit_cfg, SelectorConfig):
                    lb = self._explicit_cfg.load_balancer or DEFAULT_LOAD_BALANCER
                    self._selector_cfg = EtcdSelectorConfig.from_raw({"load_balance": {"name": lb}})
                elif isinstance(self._explicit_cfg, (dict, list)):
                    self._selector_cfg = EtcdSelectorConfig.from_raw(self._explicit_cfg)
                else:
                    self._selector_cfg = EtcdNamingConfig.from_global_config().selector_config

            if self._discovery is None:
                self._discovery = EtcdDiscovery(
                    etcd_client=self._explicit_client,
                    cfg=self._selector_cfg,
                    client_factory=self._client_factory,
                )
            self._inited = True
            return self._discovery, self._selector_cfg

    async def select(self, service_name: str, opt: Optional[SelectorOption] = None) -> TrpcNode:
        """Select one node for ``service_name`` (:class:`BaseSelector` contract).

        Args:
            service_name: Target service name.
            opt: Optional per-call :class:`~trpc_sdk.abc.SelectorOption`.

        Returns:
            Selected :class:`~trpc_sdk.abc.Node`.

        Raises:
            ValueError: If ``service_name`` is empty.
            BalancerNotExistError: If the requested load balancer is unknown.
            ServerNotAvailableError: If discovery returns no nodes.
        """
        if not service_name:
            raise ValueError("EtcdSelector.select received an empty service_name")

        disc, cfg = self._ensure_initialized()
        namespace = opt.namespace if opt is not None else ""
        balancer_name = (opt.loadbalance if (opt is not None and opt.loadbalance) else "") or cfg.load_balancer

        res = disc.list(service_name, namespace)
        nodes: List[TrpcNode] = await res if inspect.isawaitable(res) else res
        if not nodes:
            raise ServerNotAvailableError(f"no available node for service {service_name!r}")

        selected = _select_by_balancer(balancer_name, service_name, nodes, namespace)
        node: TrpcNode = await selected if inspect.isawaitable(selected) else selected
        if node is None:
            raise ServerNotAvailableError(f"load balancer returned no node for service {service_name!r}")

        if node.metadata is None:
            node.metadata = {}
        return node

    def select_sync(self, service_name: str, opt: Optional[SelectorOption] = None) -> TrpcNode:
        """Synchronously select one node for ``service_name``."""
        if not service_name:
            raise ValueError("EtcdSelector.select_sync received an empty service_name")

        disc, cfg = self._ensure_initialized()
        namespace = opt.namespace if opt is not None else ""
        balancer_name = (opt.loadbalance if (opt is not None and opt.loadbalance) else "") or cfg.load_balancer

        list_sync_fn = getattr(disc, "list_sync", None)
        if callable(list_sync_fn):
            nodes = list_sync_fn(service_name, namespace)
        else:
            res = disc.list(service_name, namespace)
            if inspect.isawaitable(res):
                import asyncio

                nodes = asyncio.run(res)
            else:
                nodes = res

        if not nodes:
            raise ServerNotAvailableError(f"no available node for service {service_name!r}")

        selected = _select_by_balancer(balancer_name, service_name, nodes, namespace)
        if inspect.isawaitable(selected):
            import asyncio

            node = asyncio.run(selected)
        else:
            node = selected
        if node is None:
            raise ServerNotAvailableError(f"load balancer returned no node for service {service_name!r}")
        return node

    async def report(self, node: Optional[TrpcNode], cost: int, code: int = 0) -> bool:
        """Report call outcome (:class:`BaseSelector` contract)."""
        if node is not None and isinstance(node.metadata, dict):
            breaker = node.metadata.get("circuitbreaker")
            if isinstance(breaker, BaseCircuitBreaker):
                await breaker.report(node, cost, code)
        return True

    def _select_impl(self, info: Optional[SelectorInfo]) -> Tuple[int, List[TrpcEndpointInfo]]:
        """Select endpoints for ``info`` and return ``(code, endpoints)``."""
        if not self._inited:
            logger.error("No init yet")
            return -1, []
        if info is None or not info.name:
            logger.error("Select failed, empty SelectorInfo")
            return -1, []

        if info.policy not in (SelectorPolicy.ONE, SelectorPolicy.ALL):
            logger.error("Select failed policy not support, service_name:%s", info.name)
            return -1, []

        try:
            disc, _ = self._ensure_initialized()
            nodes = disc.list_sync(info.name)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error("Select failed, service_name:%s, err:%s", info.name, exc)
            return -1, []

        if not nodes:
            logger.error("Select failed not node, service_name:%s", info.name)
            return -1, []

        endpoints: List[TrpcEndpointInfo] = []
        if info.policy == SelectorPolicy.ALL:
            for n in nodes:
                meta = {str(k): str(v) for k, v in (n.metadata or {}).items() if isinstance(v, (str, int, float))}
                endpoints.append(TrpcEndpointInfo(host=n.ip, port=n.port, meta=meta))
        else:
            chosen = random.choice(nodes)
            meta = {str(k): str(v) for k, v in (chosen.metadata or {}).items() if isinstance(v, (str, int, float))}
            endpoints.append(TrpcEndpointInfo(host=chosen.ip, port=chosen.port, meta=meta))

        return 0, endpoints

    def select_endpoint(self, info: Optional[SelectorInfo], endpoint: Optional[TrpcEndpointInfo] = None) -> int:
        """Select one endpoint into ``endpoint``.

        Returns:
            ``0`` on success, ``-1`` on error.
        """
        if not self._inited:
            logger.error("No inited yet")
            return -1
        ret, endpoints = self._select_impl(info)
        if ret == 0 and endpoints and endpoint is not None:
            endpoint.host = endpoints[0].host
            endpoint.port = endpoints[0].port
            endpoint.meta = dict(endpoints[0].meta)
        return ret

    async def async_select_endpoint(self, info: Optional[SelectorInfo]) -> TrpcEndpointInfo:
        """Asynchronously select one endpoint."""
        endpoint = TrpcEndpointInfo()
        ret = self.select_endpoint(info, endpoint)
        if ret != 0:
            raise RuntimeError("async select failed")
        return endpoint

    def select_batch(self, info: Optional[SelectorInfo], endpoints: Optional[List[TrpcEndpointInfo]] = None) -> int:
        """Select a batch of endpoints according to ``info.policy``."""
        if not self._inited:
            logger.error("No init yet")
            return -1
        ret, result = self._select_impl(info)
        if ret == 0 and endpoints is not None:
            endpoints.clear()
            endpoints.extend(result)
        return ret

    async def async_select_batch(self, info: Optional[SelectorInfo]) -> List[TrpcEndpointInfo]:
        """Asynchronously select a batch of endpoints."""
        endpoints: List[TrpcEndpointInfo] = []
        ret = self.select_batch(info, endpoints)
        if ret != 0:
            raise RuntimeError("async select batch failed")
        return endpoints

    def report_invoke_result(self, result: Optional[InvokeResult]) -> int:
        """Report an invocation result."""
        if not self._inited or result is None:
            logger.error("Invalid parameter: invoke result is invalid")
            return -1
        return 0

    def set_endpoints(self, info: Any = None) -> int:
        """No-op route endpoint setter."""
        del info
        return 0

    def set_circuit_break_white_list(self, framework_retcodes: List[int]) -> bool:
        """Record the circuit-breaker white list."""
        self._white_list = list(framework_retcodes or [])
        return True

    def stop(self) -> None:
        """Plugin lifecycle stop hook."""
        self.destroy()


def create_selector(
    discovery: EtcdDiscovery,
    cfg: Optional[Union[SelectorConfig, EtcdSelectorConfig, Dict[str, Any]]] = None,
) -> EtcdSelector:
    """Create a new :class:`EtcdSelector`."""
    return EtcdSelector(discovery=discovery, cfg=cfg or SelectorConfig())


register_plugin(PluginType.NAMING_SELECTOR, PLUGIN_NAME, version=__version__, report=True)(EtcdSelector)
