# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""Node data models, key path helpers, and serialization for etcd naming.

Stores nodes as JSON, and also accepts ``{"meta": {...}}`` values whose
host and port are the last two segments of the etcd key
(``/{prefix}_{service}/{host}/{port}``).
"""

from __future__ import annotations

import json
import posixpath
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import Optional
from typing import Tuple
from typing import Union

from trpc_sdk.abc import Node as TrpcNode

from ._constants import DEFAULT_WEIGHT


@dataclass
class EtcdNode:
    """Service node stored in etcd.

    Attributes:
        name: Logical service name.
        id: Instance identifier (``host-port-pid``).
        address: Endpoint address in ``ip:port`` form.
        metadata: Key-value metadata map.
        weight: Selection weight.
    """

    name: str = ""
    id: str = ""
    address: str = ""
    metadata: Dict[str, str] = field(default_factory=dict)
    weight: int = DEFAULT_WEIGHT


@dataclass
class RegistryInfo:
    """Registration payload for one service endpoint."""

    name: str = ""
    host: str = ""
    port: int = 0
    meta: Dict[str, str] = field(default_factory=dict)


@dataclass
class TrpcEndpointInfo:
    """Selected endpoint returned to the caller."""

    host: str = ""
    port: int = 0
    id: int = 0
    meta: Dict[str, str] = field(default_factory=dict)


def marshal(node: EtcdNode) -> str:
    """Serialize an :class:`EtcdNode` to JSON.

    Args:
        node: Node to serialize.

    Returns:
        JSON string representation.
    """
    payload = {
        "name": node.name,
        "id": node.id,
        "address": node.address,
        "metadata": dict(node.metadata) if node.metadata is not None else {},
        "weight": int(node.weight),
    }
    return json.dumps(payload, separators=(",", ":"))


def unmarshal(raw: Union[bytes, str], key: Optional[Union[bytes, str]] = None) -> Optional[EtcdNode]:
    """Deserialize an :class:`EtcdNode` from JSON bytes or a string.

    Also accepts ``{"meta": {...}}`` payloads, taking ``host`` and ``port``
    from the last two segments of ``key``.

    Args:
        raw: JSON payload bytes or string.
        key: Optional etcd key, used to extract ``host`` and ``port``.

    Returns:
        The parsed :class:`EtcdNode`, or ``None`` if the JSON is ``null``.

    Raises:
        ValueError: If ``raw`` is not valid JSON or cannot be parsed.
    """
    if raw is None:
        return None
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, (bytes, bytearray)) else str(raw)
    if not text.strip():
        raise ValueError("empty node payload")
    data = json.loads(text)
    if data is None:
        return None
    if not isinstance(data, dict):
        raise ValueError(f"expected JSON object for node, got {type(data).__name__}")

    # Payload that already carries "address" or "name".
    if "address" in data or "name" in data:
        raw_meta = data.get("metadata") or {}
        metadata = {str(k): str(v) for k, v in raw_meta.items()} if isinstance(raw_meta, dict) else {}
        return EtcdNode(
            name=str(data.get("name") or ""),
            id=str(data.get("id") or ""),
            address=str(data.get("address") or ""),
            metadata=metadata,
            weight=int(data.get("weight") or 0),
        )

    # Metadata payload {"meta": {...}} with key "/{service}/{host}/{port}".
    if key is not None:
        key_str = key.decode("utf-8", errors="replace") if isinstance(key, (bytes, bytearray)) else str(key)
        endpoint = parse_cpp_endpoint_info(key_str, text)
        if endpoint is not None:
            parts = [p for p in key_str.split("/") if p]
            svc_name = parts[0] if parts else ""
            address = f"{endpoint.host}:{endpoint.port}"
            return EtcdNode(
                name=svc_name,
                id=f"{endpoint.host}-{endpoint.port}",
                address=address,
                metadata=endpoint.meta,
                weight=DEFAULT_WEIGHT,
            )

    raise ValueError(f"unrecognized node payload: {text}")


def split_host_port(address: str) -> Tuple[str, int]:
    """Split ``host:port`` or ``[ipv6]:port`` into ``(host, port)``.

    Args:
        address: Address string in ``host:port`` form.

    Returns:
        Tuple of ``(host, port)``.

    Raises:
        ValueError: If ``address`` has no valid host or port.
    """
    text = str(address).strip()
    if not text:
        raise ValueError("missing port in address")
    if text.startswith("["):
        if "]" not in text:
            raise ValueError(f"missing ']' in address {address!r}")
        host, _, rest = text.partition("]")
        if not rest.startswith(":") or not rest[1:].isdigit():
            raise ValueError(f"missing or invalid port in address {address!r}")
        return host[1:], int(rest[1:])
    if ":" not in text:
        raise ValueError(f"missing port in address {address!r}")
    host, _, port_str = text.rpartition(":")
    if not host or not port_str.isdigit():
        raise ValueError(f"invalid host:port address {address!r}")
    return host, int(port_str)


def convert_node(node: EtcdNode) -> TrpcNode:
    """Convert an :class:`EtcdNode` into a framework :class:`~trpc_sdk.abc.Node`.

    Args:
        node: Deserialized etcd node.

    Returns:
        A :class:`~trpc_sdk.abc.Node` ready for routing and load balancing.
    """
    ip = ""
    port = 0
    if node.address:
        try:
            ip, port = split_host_port(node.address)
        except ValueError:
            ip = node.address
    meta: Dict[str, Any] = {}
    if node.metadata:
        for k, v in node.metadata.items():
            meta[k] = v
    return TrpcNode(
        service_name=node.name,
        address=node.address,
        ip=ip,
        port=port,
        weight=node.weight,
        metadata=meta,
    )


def node_path(prefix: str, service: str, node_id: str) -> str:
    """Build the etcd key path for one node instance.

    Replaces ``/`` with ``-`` in both ``service`` and ``node_id``, then joins
    them under ``prefix`` and normalizes the result with :func:`posixpath.normpath`.
    """
    clean_service = service.replace("/", "-")
    clean_id = node_id.replace("/", "-")
    return posixpath.normpath(posixpath.join(prefix, clean_service, clean_id))


def service_path(prefix: str, service: str) -> str:
    """Build the etcd key prefix for a service.

    When ``service`` is empty, returns ``prefix`` unchanged. Otherwise joins
    ``prefix`` and ``service`` (with ``/`` replaced by ``-``) and normalizes
    the result, which strips a trailing slash.
    """
    if service == "":
        return prefix
    clean_service = service.replace("/", "-")
    return posixpath.normpath(posixpath.join(prefix, clean_service))


def service_id(host: str, port: Union[str, int], pid: Union[str, int]) -> str:
    """Construct the unique service instance ID."""
    return f"{host}-{port}-{pid}"


def cpp_service_key(prefix: str, name: str) -> str:
    """Build the selector key prefix ``/{prefix}_{name}``, or ``/{name}`` when ``prefix`` is empty."""
    if not prefix:
        return f"/{name}"
    return f"/{prefix}_{name}"


def cpp_node_key(prefix: str, name: str, host: str, port: Union[str, int]) -> str:
    """Build the registry key for one node under :func:`cpp_service_key`."""
    return f"{cpp_service_key(prefix, name)}/{host}/{port}"


def marshal_cpp_meta(meta: Optional[Dict[str, str]] = None) -> str:
    """Serialize node metadata as ``{"meta": {...}}``."""
    clean_meta = {str(k): str(v) for k, v in (meta or {}).items()}
    return json.dumps({"meta": clean_meta}, separators=(",", ":"))


def parse_cpp_endpoint_info(key: str, value: str) -> Optional[TrpcEndpointInfo]:
    """Parse an etcd key ``.../{host}/{port}`` and a ``{"meta": {...}}`` value into :class:`TrpcEndpointInfo`."""
    parts = [p for p in str(key).split("/") if p]
    if len(parts) < 3:
        return None
    host = parts[-2]
    try:
        port = int(parts[-1])
    except ValueError:
        return None
    meta: Dict[str, str] = {}
    if value:
        try:
            doc = json.loads(value)
            if isinstance(doc, dict) and isinstance(doc.get("meta"), dict):
                for k, v in doc["meta"].items():
                    if isinstance(v, str):
                        meta[str(k)] = v
        except Exception:  # pylint: disable=broad-except
            pass
    return TrpcEndpointInfo(host=host, port=port, meta=meta)
