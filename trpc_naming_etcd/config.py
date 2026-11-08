# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""Configuration models for the etcd naming plugin (registry, discovery, selector).

Supports a dict layout (``address``, ``timeout``, ``username``, ``password``,
``prefix``/``Prefix``, ``tls``, ``service``, ``load_balance``) and a list of
``{name, endpoint, prefix}`` entries under ``plugins.registry.etcd`` or
``plugins.selector.etcd``:

Dict form::

    plugins:
      registry:
        etcd:
          address: 127.0.0.1:2379,127.0.0.2:2379
          timeout: 5
          prefix: /registry
          tls:
            cafile: ./cert/etcd/ca.crt
            certfile: ./cert/etcd/tls.crt
            keyfile: ./cert/etcd/tls.key
          service:
            - name: trpc.test.helloworld.Greeter
              ttl: 10
              weight: 100
              metadata:
                tags: helloworld
      selector:
        etcd:
          address: 127.0.0.1:2379,127.0.0.2:2379
          timeout: 5
          prefix: /registry
          load_balance:
            name: round_robin

List form::

    plugins:
      registry:
        etcd:
          - name: trpc.test.helloworld.Greeter
            prefix: test_trpc
            endpoint: http://127.0.0.1:2379
      selector:
        etcd:
          - name: trpc.test.helloworld.Greeter
            prefix: test_trpc
            endpoint: http://127.0.0.1:2379
"""

from __future__ import annotations

from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

from trpc_sdk.log import logger

from ._constants import DEFAULT_ENDPOINT
from ._constants import DEFAULT_ETCD_PREFIX
from ._constants import DEFAULT_LOAD_BALANCER
from ._constants import DEFAULT_PORT
from ._constants import DEFAULT_TIMEOUT
from ._constants import DEFAULT_TTL
from ._constants import DEFAULT_WEIGHT
from ._constants import PLUGIN_NAME

_CLIENT_KEY_ALIASES = {
    "name": ("name", ),
    "endpoints": ("endpoints", "endpoint", "address"),
    "prefix": ("prefix", "Prefix"),
    "timeout": ("timeout", "dial_timeout", "dialtimeout", "dialTimeout"),
    "username": ("username", "user"),
    "password": ("password", ),
    "ca_cert": ("ca_cert", "caCert", "ca_file", "cacert", "cafile"),
    "cert_key": ("cert_key", "certKey", "key_file", "keyfile"),
    "cert_cert": ("cert_cert", "certCert", "cert_file", "certfile"),
}


def parse_endpoint(endpoint: str) -> Tuple[str, int]:
    """Parse ``[scheme://]host[:port]`` into ``(host, port)``.

    Args:
        endpoint: Endpoint such as ``http://127.0.0.1:2379`` or ``127.0.0.1``.

    Returns:
        Host and port, defaulting the port to 2379.
    """
    text = str(endpoint).strip()
    if "://" in text:
        text = text.split("://", 1)[1]
    text = text.rstrip("/")
    if not text:
        return "127.0.0.1", DEFAULT_PORT
    if text.startswith("["):  # IPv6 literal, e.g. [::1]:2379
        host, _, rest = text.partition("]")
        return host.strip("[]"), int(rest.lstrip(":")) if rest else DEFAULT_PORT
    host, sep, port = text.rpartition(":")
    if sep and port.isdigit():
        return host, int(port)
    return text, DEFAULT_PORT


def normalize_endpoints(value: Optional[Union[str, List[str]]]) -> List[str]:
    """Normalize a string or list of endpoints into a non-empty list."""
    if not value:
        return [DEFAULT_ENDPOINT]
    if isinstance(value, str):
        items = [item.strip() for item in value.split(",")]
    else:
        items = [str(item).strip() for item in value]
    return [item for item in items if item] or [DEFAULT_ENDPOINT]


@dataclass
class TLSConfig:
    """TLS certificate configuration."""

    cert_file: str = ""
    key_file: str = ""
    ca_file: str = ""

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]] = None) -> "TLSConfig":
        """Build a :class:`TLSConfig` from a ``tls`` dict."""
        if not isinstance(data, dict):
            return cls()
        cert_file = ""
        for k in ("certfile", "cert_file", "cert_cert", "certCert"):
            if data.get(k):
                cert_file = str(data[k])
                break
        key_file = ""
        for k in ("keyfile", "key_file", "cert_key", "certKey"):
            if data.get(k):
                key_file = str(data[k])
                break
        ca_file = ""
        for k in ("cafile", "ca_file", "ca_cert", "caCert", "cacert"):
            if data.get(k):
                ca_file = str(data[k])
                break
        return cls(cert_file=cert_file, key_file=key_file, ca_file=ca_file)


@dataclass
class LoadBalanceConfig:
    """Load balancer configuration."""

    name: str = DEFAULT_LOAD_BALANCER

    @classmethod
    def from_dict(cls, data: Optional[Union[str, Dict[str, Any]]] = None) -> "LoadBalanceConfig":
        """Build a :class:`LoadBalanceConfig` from a string or dict."""
        if not data:
            return cls()
        if isinstance(data, str):
            return cls(name=data)
        if isinstance(data, dict):
            return cls(name=str(data.get("name") or DEFAULT_LOAD_BALANCER))
        return cls()


@dataclass
class EtcdServiceConfig:
    """Per-service registration or selector configuration.

    Attributes:
        name: Logical service name (e.g. ``trpc.test.helloworld.Greeter``).
        prefix: Key prefix in etcd.
        weight: Node weight (default 1).
        ttl: Lease TTL in seconds (default 5).
        metadata: Arbitrary string key/value metadata attached to the node.
        endpoint: Per-service etcd endpoint. Empty means use the factory-level
            client endpoints.
        cpp_mode: True when parsed from a list entry (or explicitly requested).
            Uses the ``/{prefix}_{service}/{host}/{port}`` key layout and
            ``{"meta": {...}}`` values.
    """

    name: str = ""
    prefix: str = ""
    weight: int = DEFAULT_WEIGHT
    ttl: int = DEFAULT_TTL
    metadata: Dict[str, str] = field(default_factory=dict)
    endpoint: str = ""
    cpp_mode: bool = False

    @classmethod
    def from_dict(cls,
                  data: Optional[Dict[str, Any]] = None,
                  default_prefix: str = "",
                  cpp_mode: bool = False) -> "EtcdServiceConfig":
        """Build an :class:`EtcdServiceConfig` from a raw dict."""
        if not isinstance(data, dict):
            return cls(prefix=default_prefix, cpp_mode=cpp_mode)
        name = str(data.get("name") or data.get("service_name") or "")
        prefix = data.get("prefix")
        if prefix is None:
            prefix = data.get("Prefix")
        if prefix is None:
            prefix = default_prefix
        else:
            prefix = str(prefix)
        weight = int(data.get("weight") or DEFAULT_WEIGHT)
        ttl = int(data.get("ttl") or DEFAULT_TTL)
        raw_meta = data.get("metadata") or data.get("meta") or {}
        metadata = {str(k): str(v) for k, v in raw_meta.items()} if isinstance(raw_meta, dict) else {}
        endpoint = str(data.get("endpoint") or data.get("address") or "")
        is_cpp = bool(data.get("cpp_mode", cpp_mode))
        return cls(
            name=name,
            prefix=prefix,
            weight=weight,
            ttl=ttl,
            metadata=metadata,
            endpoint=endpoint,
            cpp_mode=is_cpp,
        )

    def display(self) -> None:
        """Log the node configuration."""
        logger.debug("--------------------------------")
        logger.debug("name:%s", self.name)
        logger.debug("endpoint:%s", self.endpoint)
        logger.debug("prefix:%s", self.prefix)
        logger.debug("--------------------------------")


@dataclass
class EtcdClientConfig:
    """Connection and authentication options for an etcd client."""

    name: str = PLUGIN_NAME
    endpoints: List[str] = field(default_factory=lambda: [DEFAULT_ENDPOINT])
    prefix: str = DEFAULT_ETCD_PREFIX
    timeout: float = DEFAULT_TIMEOUT
    username: str = ""
    password: str = ""
    ca_cert: str = ""
    cert_key: str = ""
    cert_cert: str = ""

    @property
    def address(self) -> str:
        """Comma-joined endpoints."""
        return ",".join(self.endpoints)

    @property
    def dial_timeout(self) -> float:
        """Alias for ``timeout``."""
        return self.timeout

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]] = None) -> "EtcdClientConfig":
        """Build an :class:`EtcdClientConfig` from a YAML/options dict."""
        if not isinstance(data, dict) or not data:
            return cls()
        picked: Dict[str, Any] = {}
        for field_name, aliases in _CLIENT_KEY_ALIASES.items():
            for alias in aliases:
                if alias in data and data[alias] is not None:
                    picked[field_name] = data[alias]
                    break
        if isinstance(data.get("tls"), dict):
            tls = TLSConfig.from_dict(data["tls"])
            if tls.ca_file and not picked.get("ca_cert"):
                picked["ca_cert"] = tls.ca_file
            if tls.key_file and not picked.get("cert_key"):
                picked["cert_key"] = tls.key_file
            if tls.cert_file and not picked.get("cert_cert"):
                picked["cert_cert"] = tls.cert_file
        if "endpoints" in picked:
            picked["endpoints"] = normalize_endpoints(picked["endpoints"])
        if "timeout" in picked:
            timeout_val = float(picked["timeout"])
            picked["timeout"] = timeout_val if timeout_val > 0 else DEFAULT_TIMEOUT
        if "prefix" in picked:
            prefix_val = str(picked["prefix"])
            picked["prefix"] = prefix_val if prefix_val != "" else DEFAULT_ETCD_PREFIX
        return cls(**picked)

    def merged(self, overrides: Optional[Union[Dict[str, Any], "EtcdClientConfig"]]) -> "EtcdClientConfig":
        """Return a copy with ``overrides`` applied on top of this config."""
        if overrides is None:
            return replace(self)
        if isinstance(overrides, EtcdClientConfig):
            return overrides
        other = EtcdClientConfig.from_dict(overrides)
        changes: Dict[str, Any] = {}
        for field_name, aliases in _CLIENT_KEY_ALIASES.items():
            if any(alias in overrides for alias in aliases):
                changes[field_name] = getattr(other, field_name)
        if isinstance(overrides.get("tls"), dict):
            for tls_field in ("ca_cert", "cert_key", "cert_cert"):
                val = getattr(other, tls_field)
                if val:
                    changes[tls_field] = val
        return replace(self, **changes)

    def client_kwargs(self, endpoint: str) -> Dict[str, Any]:
        """Build ``etcd3.client(...)`` keyword arguments for one endpoint."""
        host, port = parse_endpoint(endpoint)
        kwargs: Dict[str, Any] = {"host": host, "port": port, "timeout": self.timeout}
        if self.username:
            kwargs["user"] = self.username
            kwargs["password"] = self.password
        if self.ca_cert:
            kwargs["ca_cert"] = self.ca_cert
        if self.cert_key:
            kwargs["cert_key"] = self.cert_key
        if self.cert_cert:
            kwargs["cert_cert"] = self.cert_cert
        return kwargs


@dataclass
class EtcdRegistryConfig:
    """Registry plugin configuration."""

    client_config: EtcdClientConfig = field(default_factory=EtcdClientConfig)
    services: Dict[str, EtcdServiceConfig] = field(default_factory=dict)
    services_config: List[EtcdServiceConfig] = field(default_factory=list)
    cpp_mode: bool = False

    @property
    def prefix(self) -> str:
        """Default registration key prefix."""
        return self.client_config.prefix

    @classmethod
    def from_raw(cls, raw: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None) -> "EtcdRegistryConfig":
        """Parse a mapping or a list of per-service configs."""
        if raw is None:
            return cls()
        if isinstance(raw, list):
            # List form: plugins.registry.etcd is a list of {name, prefix, endpoint}.
            services_list: List[EtcdServiceConfig] = []
            services_map: Dict[str, EtcdServiceConfig] = {}
            first_endpoint = ""
            for item in raw:
                if isinstance(item, dict):
                    svc = EtcdServiceConfig.from_dict(item, default_prefix="", cpp_mode=True)
                    services_list.append(svc)
                    if svc.name:
                        services_map[svc.name] = svc
                    if svc.endpoint and not first_endpoint:
                        first_endpoint = svc.endpoint
            client_cfg = EtcdClientConfig(
                endpoints=normalize_endpoints(first_endpoint) if first_endpoint else [DEFAULT_ENDPOINT],
                prefix="",
            )
            return cls(
                client_config=client_cfg,
                services=services_map,
                services_config=services_list,
                cpp_mode=True,
            )
        if isinstance(raw, dict):
            client_cfg = EtcdClientConfig.from_dict(raw)
            services_list = []
            services_map = {}
            raw_services = raw.get("service") or raw.get("services") or raw.get("services_config") or []
            if isinstance(raw_services, list):
                for item in raw_services:
                    if isinstance(item, dict):
                        svc = EtcdServiceConfig.from_dict(
                            item,
                            default_prefix=client_cfg.prefix,
                            cpp_mode=bool(raw.get("cpp_mode", False)),
                        )
                        services_list.append(svc)
                        if svc.name:
                            services_map[svc.name] = svc
            return cls(
                client_config=client_cfg,
                services=services_map,
                services_config=services_list,
                cpp_mode=bool(raw.get("cpp_mode", False)),
            )
        return cls()

    def get_service_config(self, service_name: str) -> Optional[EtcdServiceConfig]:
        """Return the service configuration for ``service_name``.

        When ``cpp_mode`` is set, a service must be listed in ``services_config``.
        Otherwise, if a service is not listed under ``service:``, a default
        :class:`EtcdServiceConfig` inheriting the factory prefix is synthesized
        so ``server.service[].registry: etcd`` works without a per-service entry.
        """
        if service_name in self.services:
            return self.services[service_name]
        if self.cpp_mode:
            return None
        return EtcdServiceConfig(name=service_name, prefix=self.client_config.prefix)

    def display(self) -> None:
        """Log all service configurations."""
        logger.debug("--------------------------------")
        for svc in self.services_config:
            svc.display()
        logger.debug("--------------------------------")


@dataclass
class EtcdSelectorConfig:
    """Selector and discovery plugin configuration."""

    client_config: EtcdClientConfig = field(default_factory=EtcdClientConfig)
    load_balance: LoadBalanceConfig = field(default_factory=LoadBalanceConfig)
    services: Dict[str, EtcdServiceConfig] = field(default_factory=dict)
    services_config: List[EtcdServiceConfig] = field(default_factory=list)
    cpp_mode: bool = False

    @property
    def prefix(self) -> str:
        """Discovery key prefix."""
        return self.client_config.prefix

    @property
    def load_balancer(self) -> str:
        """Configured load balancer name."""
        return self.load_balance.name or DEFAULT_LOAD_BALANCER

    @classmethod
    def from_raw(cls, raw: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None) -> "EtcdSelectorConfig":
        """Parse a mapping or a list of per-service configs."""
        if raw is None:
            return cls()
        if isinstance(raw, list):
            services_list: List[EtcdServiceConfig] = []
            services_map: Dict[str, EtcdServiceConfig] = {}
            first_endpoint = ""
            for item in raw:
                if isinstance(item, dict):
                    svc = EtcdServiceConfig.from_dict(item, default_prefix="", cpp_mode=True)
                    services_list.append(svc)
                    if svc.name:
                        services_map[svc.name] = svc
                    if svc.endpoint and not first_endpoint:
                        first_endpoint = svc.endpoint
            client_cfg = EtcdClientConfig(
                endpoints=normalize_endpoints(first_endpoint) if first_endpoint else [DEFAULT_ENDPOINT],
                prefix="",
            )
            return cls(
                client_config=client_cfg,
                load_balance=LoadBalanceConfig(),
                services=services_map,
                services_config=services_list,
                cpp_mode=True,
            )
        if isinstance(raw, dict):
            client_cfg = EtcdClientConfig.from_dict(raw)
            lb_raw = raw.get("load_balance") or raw.get("loadbalance") or raw.get("load_balancer")
            lb_cfg = LoadBalanceConfig.from_dict(lb_raw)
            services_list = []
            services_map = {}
            raw_services = raw.get("service") or raw.get("services") or raw.get("services_config") or []
            if isinstance(raw_services, list):
                for item in raw_services:
                    if isinstance(item, dict):
                        svc = EtcdServiceConfig.from_dict(
                            item,
                            default_prefix=client_cfg.prefix,
                            cpp_mode=bool(raw.get("cpp_mode", False)),
                        )
                        services_list.append(svc)
                        if svc.name:
                            services_map[svc.name] = svc
            return cls(
                client_config=client_cfg,
                load_balance=lb_cfg,
                services=services_map,
                services_config=services_list,
                cpp_mode=bool(raw.get("cpp_mode", False)),
            )
        return cls()

    def display(self) -> None:
        """Log all service configurations."""
        logger.debug("--------------------------------")
        for svc in self.services_config:
            svc.display()
        logger.debug("--------------------------------")


@dataclass
class EtcdNamingConfig:
    """Combined registry and selector configuration."""

    name: str = PLUGIN_NAME
    registry_config: EtcdRegistryConfig = field(default_factory=EtcdRegistryConfig)
    selector_config: EtcdSelectorConfig = field(default_factory=EtcdSelectorConfig)

    @classmethod
    def from_dict(cls, data: Optional[Dict[str, Any]] = None, name: str = PLUGIN_NAME) -> "EtcdNamingConfig":
        """Decode ``plugins`` or root dict containing ``registry`` and ``selector`` sections."""
        if not isinstance(data, dict):
            return cls(name=name)
        plugins = data.get("plugins") if isinstance(data.get("plugins"), dict) else data
        reg_raw = None
        sel_raw = None
        if isinstance(plugins.get("registry"), dict):
            reg_raw = plugins["registry"].get(name)
        if isinstance(plugins.get("selector"), dict):
            sel_raw = plugins["selector"].get(name)
        if sel_raw is None and isinstance(plugins.get("discovery"), dict):
            sel_raw = plugins["discovery"].get(name)
        return cls(
            name=name,
            registry_config=EtcdRegistryConfig.from_raw(reg_raw),
            selector_config=EtcdSelectorConfig.from_raw(sel_raw),
        )

    @classmethod
    def from_global_config(cls, name: str = PLUGIN_NAME) -> "EtcdNamingConfig":
        """Read ``plugins.registry.etcd`` and ``plugins.selector.etcd`` from ``TrpcConfig``."""
        try:
            from trpc_sdk.config import TrpcConfig  # pylint: disable=import-outside-toplevel

            plugins = TrpcConfig.plugins_config() or {}
        except Exception:  # pylint: disable=broad-except
            plugins = {}
        return cls.from_dict(plugins, name=name)

    def display(self) -> None:
        """Log the naming configuration."""
        logger.debug("--------------------------------")
        logger.debug("name:%s", self.name)
        self.registry_config.display()
        self.selector_config.display()
        logger.debug("--------------------------------")
