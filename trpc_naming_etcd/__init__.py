# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""etcd naming plugin (registry, discovery, selector) for the tRPC Python framework.

Importing this package registers :class:`EtcdRegistry` as the
``NAMING_REGISTRY`` plugin named ``etcd``, :class:`EtcdDiscovery` as the
``NAMING_DISCOVERY`` plugin named ``etcd``, and :class:`EtcdSelector` as the
``NAMING_SELECTOR`` plugin named ``etcd``::

    import trpc_naming_etcd  # noqa: F401  (registers registry, discovery, selector)

Public API:

    EtcdRegistry / create_registry: Lease-backed service registration and deregistration.
    EtcdDiscovery / create_discovery: Prefix-cached service discovery with watch updates.
    EtcdSelector / create_selector: Load-balanced service node selector.
    LeaseManager / create_lease_manager: TTL lease manager. ``generate_etcd_client`` builds the etcd client.
    EtcdNode / marshal / unmarshal / convert_node / node_path / service_path / service_id:
        Node model and key path helpers.
    EtcdClientConfig / EtcdServiceConfig / EtcdRegistryConfig / EtcdSelectorConfig / EtcdNamingConfig:
        Configuration models for dict and list YAML layouts.
"""

from __future__ import annotations

from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import DEFAULT_PLUGIN_MANAGER

from ._constants import DEFAULT_CACHE_TTL
from ._constants import DEFAULT_ENDPOINT
from ._constants import DEFAULT_ETCD_PREFIX
from ._constants import DEFAULT_LOAD_BALANCER
from ._constants import DEFAULT_PORT
from ._constants import DEFAULT_TIMEOUT
from ._constants import DEFAULT_TTL
from ._constants import DEFAULT_WEIGHT
from ._constants import EventType
from ._constants import PLUGIN_NAME
from ._constants import SelectorPolicy
from .client import LeaseManager
from .client import generate_etcd_client
from .client import create_lease_manager
from .config import EtcdClientConfig
from .config import EtcdNamingConfig
from .config import EtcdRegistryConfig
from .config import EtcdSelectorConfig
from .config import EtcdServiceConfig
from .config import LoadBalanceConfig
from .config import TLSConfig
from .config import parse_endpoint
from .discovery import DiscoveryCache
from .discovery import DiscoveryConfig
from .discovery import EtcdDiscovery
from .discovery import EtcdWatcher
from .discovery import WatchResult
from .discovery import create_discovery
from .error import BalancerNotExistError
from .error import EtcdNamingError
from .error import ServerNotAvailableError
from .error import StaleDataError
from .model import EtcdNode
from .model import RegistryInfo
from .model import TrpcEndpointInfo
from .model import convert_node
from .model import cpp_node_key
from .model import cpp_service_key
from .model import marshal
from .model import node_path
from .model import parse_cpp_endpoint_info
from .model import service_id
from .model import service_path
from .model import unmarshal
from .registry import EtcdRegistry
from .registry import create_registry
from .selector import EtcdSelector
from .selector import InvokeResult
from .selector import SelectorConfig
from .selector import SelectorInfo
from .selector import create_selector
from .version import __version__


def setup(config: dict | EtcdNamingConfig | None = None) -> None:
    """Initialize and register etcd naming instances in the global plugin manager."""
    naming_cfg = (
        config if isinstance(config, EtcdNamingConfig) else
        (EtcdNamingConfig.from_dict(config) if isinstance(config, dict) else EtcdNamingConfig.from_global_config()))

    reg = EtcdRegistry(cfg=naming_cfg.registry_config)
    reg.init()
    DEFAULT_PLUGIN_MANAGER.set_instance(PluginType.NAMING_REGISTRY, PLUGIN_NAME, reg)

    disc = EtcdDiscovery(cfg=naming_cfg.selector_config)
    DEFAULT_PLUGIN_MANAGER.set_instance(PluginType.NAMING_DISCOVERY, PLUGIN_NAME, disc)

    sel = EtcdSelector(discovery=disc, cfg=naming_cfg.selector_config)
    sel.init()
    DEFAULT_PLUGIN_MANAGER.set_instance(PluginType.NAMING_SELECTOR, PLUGIN_NAME, sel)


__all__ = [
    "PLUGIN_NAME",
    "DEFAULT_ENDPOINT",
    "DEFAULT_PORT",
    "DEFAULT_TIMEOUT",
    "DEFAULT_ETCD_PREFIX",
    "DEFAULT_TTL",
    "DEFAULT_WEIGHT",
    "DEFAULT_LOAD_BALANCER",
    "DEFAULT_CACHE_TTL",
    "EventType",
    "SelectorPolicy",
    "EtcdNamingError",
    "ServerNotAvailableError",
    "BalancerNotExistError",
    "StaleDataError",
    "TLSConfig",
    "LoadBalanceConfig",
    "EtcdServiceConfig",
    "EtcdClientConfig",
    "EtcdRegistryConfig",
    "EtcdSelectorConfig",
    "EtcdNamingConfig",
    "parse_endpoint",
    "EtcdNode",
    "RegistryInfo",
    "TrpcEndpointInfo",
    "marshal",
    "unmarshal",
    "convert_node",
    "node_path",
    "service_path",
    "service_id",
    "cpp_service_key",
    "cpp_node_key",
    "parse_cpp_endpoint_info",
    "LeaseManager",
    "generate_etcd_client",
    "create_lease_manager",
    "DiscoveryConfig",
    "WatchResult",
    "EtcdWatcher",
    "DiscoveryCache",
    "EtcdDiscovery",
    "create_discovery",
    "EtcdRegistry",
    "create_registry",
    "SelectorConfig",
    "SelectorInfo",
    "InvokeResult",
    "EtcdSelector",
    "create_selector",
    "setup",
    "__version__",
]
