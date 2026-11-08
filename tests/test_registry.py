# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""Tests for EtcdRegistry."""

import asyncio
import pytest

from trpc_sdk.abc import PluginType
from trpc_sdk.plugin import get_plugin

import trpc_naming_etcd
from trpc_naming_etcd import EtcdNamingConfig
from trpc_naming_etcd import EtcdRegistry
from trpc_naming_etcd import EtcdServiceConfig
from trpc_naming_etcd import RegistryInfo
from trpc_naming_etcd import create_registry
from trpc_naming_etcd import node_path
from trpc_naming_etcd import service_id
from trpc_naming_etcd import unmarshal

from conftest import wait_until


def test_plugin_registered_as_naming_registry():
    reg = get_plugin(PluginType.NAMING_REGISTRY, "etcd")
    assert isinstance(reg, EtcdRegistry)


def test_go_style_register_keepalive_and_deregister(fake_client):
    cfg = EtcdServiceConfig(
        name="testService",
        prefix="/registry",
        weight=100,
        ttl=5,
        metadata={"tags": "helloworld"},
    )
    reg = create_registry(fake_client, cfg)

    # Invalid address raises ValueError
    with pytest.raises(ValueError):
        reg.register_sync("testService", "invalid_address")

    ok = asyncio.run(reg.register("testService", ("8.8.8.8", 1000)))
    assert ok is True

    expected_key = node_path("/registry", "testService", service_id("8.8.8.8", 1000, reg._pid))
    assert wait_until(lambda: expected_key in fake_client.store)
    stored_node = unmarshal(fake_client.store[expected_key])
    assert stored_node is not None
    assert stored_node.name == "testService"
    assert stored_node.address == "8.8.8.8:1000"
    assert stored_node.weight == 100
    assert stored_node.metadata == {"tags": "helloworld"}

    # Calling register again for the same endpoint is idempotent (for RegistryThread cadence)
    assert asyncio.run(reg.register("testService", ("8.8.8.8", 1000))) is True

    # Simulate lease expiry: background loop should re-register automatically
    fake_client.store.pop(expected_key, None)
    for lease in list(fake_client.leases.values()):
        lease.fail_refresh = True
    assert wait_until(lambda: expected_key in fake_client.store, timeout=3.0)

    # Deregister removes the key from etcd
    assert asyncio.run(reg.deregister("testService", ("8.8.8.8", 1000))) is True
    assert expected_key not in fake_client.store
    reg.destroy()


def test_cpp_style_registry_without_instance_id(fake_client):
    raw_cfg = {
        "registry": {
            "etcd": [
                {"name": "test1.service", "prefix": "test1", "endpoint": "http://127.0.0.1:2379"},
                {"name": "test2.service", "prefix": "test2", "endpoint": ""},
            ]
        }
    }
    naming_cfg = EtcdNamingConfig.from_dict(raw_cfg)
    reg = EtcdRegistry(etcd_client=fake_client)
    reg.set_plugin_config(naming_cfg)
    reg._inited = False  # Simulate the state before init().

    info = RegistryInfo(host="127.0.0.1", port=10001, name="etcd")
    # Before init(), calls fail with -1.
    assert reg.register_info(info) == -1
    assert reg.unregister(info) == -1
    assert reg.heartbeat(info) == -1

    assert reg.init() == 0
    assert reg.init() == 0  # Idempotent
    assert reg.version != ""

    # Empty parameter fails with -1
    assert reg.register_info(None) == -1

    # Unknown service name fails with -1
    info.name = "test.service.no"
    assert reg.register_info(info) == -1

    # Registered service name succeeds with 0
    info.name = "test1.service"
    info.meta = {"instance_id": "return_instance"}
    assert reg.register_info(info) == 0
    assert "/test1_test1.service/127.0.0.1/10001" in fake_client.store

    # Duplicate registration fails with -1
    assert reg.register_info(info) == -1

    # Heartbeat checks succeed.
    assert reg.heartbeat(info) == 0
    asyncio.run(reg.async_heartbeat(info))

    # Unregister succeeds with 0
    assert reg.unregister(info) == 0
    assert "/test1_test1.service/127.0.0.1/10001" not in fake_client.store

    reg.destroy()
    reg.destroy()  # Idempotent
