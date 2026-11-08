# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""Tests for EtcdDiscovery, DiscoveryCache, and EtcdWatcher."""

import pytest

from trpc_naming_etcd import DiscoveryCache
from trpc_naming_etcd import DiscoveryConfig
from trpc_naming_etcd import EtcdDiscovery
from trpc_naming_etcd import EtcdNode
from trpc_naming_etcd import EventType
from trpc_naming_etcd import ServerNotAvailableError
from trpc_naming_etcd import StaleDataError
from trpc_naming_etcd import WatchResult
from trpc_naming_etcd import convert_node
from trpc_naming_etcd import marshal
from trpc_naming_etcd import node_path
from trpc_naming_etcd import service_id

from conftest import wait_until


def test_cache_list_and_incremental_updates(fake_client):
    c = DiscoveryCache(fake_client, DiscoveryConfig(prefix="/registry"), start_watcher=False)

    # Tolerates None
    c.update(None)

    first_node = EtcdNode(
        name="test",
        id=service_id("127.0.0.1", "8080", "123"),
        address="127.0.0.1:8080",
        metadata={"key": "value"},
        weight=100,
    )

    # Service "test" has not been watched yet -> update is ignored
    c.update(WatchResult(version=1, event_type=EventType.CREATE, node=first_node))

    # First list returns None (cache miss) and marks "test" as watched
    nodes = c.list("test")
    assert nodes is None
    assert "test" in c.watched

    # Cache initial empty snapshot at version 2
    c.cache("test", 2, [])

    # Create first node at version 2
    c.update(WatchResult(version=2, event_type=EventType.CREATE, node=first_node))
    nodes = c.list("test")
    assert nodes is not None and len(nodes) == 1
    assert nodes[0].weight == 100

    # Update first node weight to 50 at version 3
    updated_first = EtcdNode(
        name="test",
        id=first_node.id,
        address="127.0.0.1:8080",
        metadata={"key": "value"},
        weight=50,
    )
    c.update(WatchResult(version=3, event_type=EventType.UPDATE, node=updated_first))
    nodes = c.list("test")
    assert nodes is not None and len(nodes) == 1
    assert nodes[0].weight == 50

    # Stale version (< 3) is ignored
    stale_first = EtcdNode(
        name="test",
        id=first_node.id,
        address="127.0.0.1:8080",
        metadata={"key": "value"},
        weight=999,
    )
    c.update(WatchResult(version=2, event_type=EventType.UPDATE, node=stale_first))
    assert c.list("test")[0].weight == 50

    # Add second node at version 4
    second_node = EtcdNode(
        name="test",
        id=service_id("127.0.0.1", "8081", "123"),
        address="127.0.0.1:8081",
        metadata={"key": "value"},
        weight=100,
    )
    c.update(WatchResult(version=4, event_type=EventType.CREATE, node=second_node))
    assert len(c.list("test")) == 2

    # Delete first node at version 5
    c.update(WatchResult(version=5, event_type=EventType.DELETE, node=first_node))
    nodes = c.list("test")
    assert len(nodes) == 1
    assert nodes[0].address == "127.0.0.1:8081"

    # Delete second node at version 6 -> cache now holds empty list -> raises ServerNotAvailableError
    c.update(WatchResult(version=6, event_type=EventType.DELETE, node=second_node))
    with pytest.raises(ServerNotAvailableError):
        c.list("test")

    c.stop()
    c.stop()  # Idempotent


def test_cache_stale_version_and_invalid_cache(fake_client):
    c = DiscoveryCache(fake_client, DiscoveryConfig(prefix="/registry"), start_watcher=False)
    c.cache("test", 2, None)

    new_node = EtcdNode(
        name="test",
        id=service_id("127.0.0.1", "8080", "123"),
        address="127.0.0.1:8080",
        metadata={"key": "value"},
        weight=100,
    )
    # Not watched yet -> not cached
    c.cache("test", 2, [convert_node(new_node)])
    assert c.list("test") is None  # Now watched

    c.cache("test", 2, [convert_node(new_node)])
    assert len(c.list("test")) == 1

    # Older version raises StaleDataError
    with pytest.raises(StaleDataError):
        c.cache("test", 1, [convert_node(new_node)])

    c.invalid_cache("test")
    assert c.list("test") is None
    c.stop()


def test_discovery_list_and_live_watch(fake_client):
    prefix = "/registry"
    node1 = EtcdNode(
        name="test",
        id=service_id("127.0.0.1", "8080", "123"),
        address="127.0.0.1:8080",
        metadata={"key": "value"},
        weight=100,
    )
    key1 = node_path(prefix, "test", node1.id)
    fake_client.put(key1, marshal(node1))

    disc = EtcdDiscovery(etcd_client=fake_client, cfg=DiscoveryConfig(prefix=prefix))
    assert wait_until(lambda: len(fake_client.watches) == 1)

    nodes = disc.list_sync("test")
    assert len(nodes) == 1
    assert nodes[0].address == "127.0.0.1:8080"

    # Put a second node into etcd; watcher should incrementally update the cache
    node2 = EtcdNode(
        name="test",
        id=service_id("127.0.0.1", "8081", "123"),
        address="127.0.0.1:8081",
        metadata={"key": "value"},
        weight=80,
    )
    key2 = node_path(prefix, "test", node2.id)
    fake_client.put(key2, marshal(node2))
    assert wait_until(lambda: len(disc.list_sync("test")) == 2)

    # Delete first node; watcher should remove it from cache
    fake_client.delete(key1)
    assert wait_until(lambda: len(disc.list_sync("test")) == 1)
    assert disc.list_sync("test")[0].address == "127.0.0.1:8081"

    disc.close()
