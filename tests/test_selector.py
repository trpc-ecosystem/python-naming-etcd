# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""Tests for EtcdSelector."""

import asyncio
import pytest

from trpc_sdk.abc import PluginType
from trpc_sdk.abc import SelectorOption
from trpc_sdk.naming import TrpcSelector
from trpc_sdk.naming import get_selector
from trpc_sdk.plugin import DEFAULT_PLUGIN_MANAGER

import trpc_naming_etcd
from trpc_naming_etcd import BalancerNotExistError
from trpc_naming_etcd import DiscoveryConfig
from trpc_naming_etcd import EtcdDiscovery
from trpc_naming_etcd import EtcdNamingConfig
from trpc_naming_etcd import EtcdNode
from trpc_naming_etcd import EtcdRegistry
from trpc_naming_etcd import EtcdSelector
from trpc_naming_etcd import InvokeResult
from trpc_naming_etcd import RegistryInfo
from trpc_naming_etcd import SelectorConfig
from trpc_naming_etcd import SelectorInfo
from trpc_naming_etcd import SelectorPolicy
from trpc_naming_etcd import ServerNotAvailableError
from trpc_naming_etcd import TrpcEndpointInfo
from trpc_naming_etcd import create_selector
from trpc_naming_etcd import marshal
from trpc_naming_etcd import create_discovery
from trpc_naming_etcd import node_path
from trpc_naming_etcd import service_id


def test_plugins_registered_in_framework():
    assert isinstance(get_selector("etcd"), EtcdSelector)
    disc = DEFAULT_PLUGIN_MANAGER.get_plugin_spec(PluginType.NAMING_DISCOVERY, "etcd")
    assert disc is not None


def test_go_style_selector_random_and_round_robin(fake_client):
    prefix = "/registry"
    for port in (8080, 8081):
        node = EtcdNode(
            name="trpc.test.helloworld.Greeter",
            id=service_id("127.0.0.1", port, 1),
            address=f"127.0.0.1:{port}",
            metadata={"env": "dev"},
            weight=100,
        )
        fake_client.put(node_path(prefix, node.name, node.id), marshal(node))

    disc = create_discovery(fake_client, DiscoveryConfig(prefix=prefix))
    sel = create_selector(disc, SelectorConfig(load_balancer="round_robin"))

    n1 = asyncio.run(sel.select("trpc.test.helloworld.Greeter", SelectorOption()))
    n2 = asyncio.run(sel.select("trpc.test.helloworld.Greeter", SelectorOption()))
    assert {n1.address, n2.address} == {"127.0.0.1:8080", "127.0.0.1:8081"}
    assert asyncio.run(sel.report(n1, 10, 0)) is True

    # Unknown load balancer raises BalancerNotExistError
    bad_opt = SelectorOption()
    bad_opt.set_loadbalance("non_existent_lb")
    with pytest.raises(BalancerNotExistError):
        asyncio.run(sel.select("trpc.test.helloworld.Greeter", bad_opt))

    # Unknown service raises ServerNotAvailableError
    with pytest.raises(ServerNotAvailableError):
        asyncio.run(sel.select("unknown.service", SelectorOption()))

    disc.close()


def test_trpc_selector_with_etcd_discovery(fake_client):
    prefix = "/registry"
    node = EtcdNode(
        name="trpc.test.helloworld.Greeter",
        id=service_id("127.0.0.1", 8088, 1),
        address="127.0.0.1:8088",
        metadata={},
        weight=100,
    )
    fake_client.put(node_path(prefix, node.name, node.id), marshal(node))

    disc = EtcdDiscovery(etcd_client=fake_client, cfg=DiscoveryConfig(prefix=prefix))
    DEFAULT_PLUGIN_MANAGER.set_instance(PluginType.NAMING_DISCOVERY, "etcd", disc)
    try:
        trpc_sel = TrpcSelector()
        opt = SelectorOption()
        opt.set_discovery("etcd")
        selected = asyncio.run(trpc_sel.select("trpc.test.helloworld.Greeter", opt))
        assert selected.address == "127.0.0.1:8088"
        assert selected.ip == "127.0.0.1"
        assert selected.port == 8088
    finally:
        disc.close()


def test_cpp_style_selector_one_and_all(fake_client):
    raw_cfg = {
        "registry": {
            "etcd": [
                {"name": "test1.service", "prefix": "test1", "endpoint": "http://127.0.0.1:2379"},
            ]
        },
        "selector": {
            "etcd": [
                {"name": "test1.service", "prefix": "test1", "endpoint": "http://127.0.0.1:2379"},
            ]
        },
    }
    naming_cfg = EtcdNamingConfig.from_dict(raw_cfg)

    reg = EtcdRegistry(etcd_client=fake_client)
    reg.set_plugin_config(naming_cfg)
    assert reg.init() == 0

    sel = EtcdSelector(etcd_client=fake_client)
    sel.set_plugin_config(naming_cfg)
    assert sel.init() == 0

    # Register two nodes.
    info1 = RegistryInfo(
        name="test1.service",
        host="127.0.0.1",
        port=10001,
        meta={"instance_id": "return_instance1"},
    )
    info2 = RegistryInfo(
        name="test1.service",
        host="127.0.0.1",
        port=10002,
        meta={"instance_id": "return_instance2"},
    )
    assert reg.register_info(info1) == 0
    assert reg.register_info(info2) == 0

    # Select single endpoint (SelectorPolicy.ONE)
    select_info = SelectorInfo(name="test1.service", policy=SelectorPolicy.ONE)
    endpoint = TrpcEndpointInfo()
    assert sel.select_endpoint(select_info, endpoint) == 0
    assert endpoint.host == "127.0.0.1"
    assert endpoint.port in (10001, 10002)
    assert endpoint.meta.get("instance_id") in ("return_instance1", "return_instance2")

    # Report the invoke result.
    assert sel.report_invoke_result(InvokeResult(name="test1.service", cost=5, ret_code=0)) == 0

    # Select one endpoint asynchronously.
    async_ep = asyncio.run(sel.async_select_endpoint(select_info))
    assert async_ep.meta.get("instance_id") in ("return_instance1", "return_instance2")

    # Select every endpoint.
    select_info.policy = SelectorPolicy.ALL
    endpoints = []
    assert sel.select_batch(select_info, endpoints) == 0
    assert len(endpoints) == 2
    assert {ep.port for ep in endpoints} == {10001, 10002}

    # Select every endpoint asynchronously.
    batch_eps = asyncio.run(sel.async_select_batch(select_info))
    assert len(batch_eps) == 2

    # Clean up
    assert reg.unregister(info1) == 0
    assert reg.unregister(info2) == 0
    sel.destroy()
    reg.destroy()


@pytest.mark.integration
def test_real_etcd_register_and_select_roundtrip():
    """Requires a real etcd at 127.0.0.1:2379 (skipped otherwise)."""
    try:
        import etcd3
    except Exception:
        pytest.skip("etcd3 client not importable")
    try:
        raw = etcd3.client(host="127.0.0.1", port=2379, timeout=2)
        raw.status()
    except Exception:
        pytest.skip("no etcd server at 127.0.0.1:2379")

    prefix = "/trpc-naming-etcd-test"
    service_name = "trpc.test.helloworld.Greeter"
    reg = EtcdRegistry(
        etcd_client=raw,
        cfg={"prefix": prefix, "service": [{"name": service_name, "ttl": 5, "weight": 10}]},
    )
    disc = EtcdDiscovery(etcd_client=raw, cfg=DiscoveryConfig(prefix=prefix))
    sel = EtcdSelector(discovery=disc)
    try:
        assert asyncio.run(reg.register(service_name, ("127.0.0.1", 10001))) is True
        node = asyncio.run(sel.select(service_name, SelectorOption()))
        assert node.address == "127.0.0.1:10001"
    finally:
        asyncio.run(reg.deregister(service_name, ("127.0.0.1", 10001)))
        disc.close()
        reg.destroy()
        raw.close()
