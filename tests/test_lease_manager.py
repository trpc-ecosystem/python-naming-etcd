# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""Tests for LeaseManager and generate_etcd_client."""

import time
import pytest

import trpc_naming_etcd.client as client_mod
from trpc_naming_etcd import EtcdClientConfig
from trpc_naming_etcd import EtcdNamingError
from trpc_naming_etcd import LeaseManager
from trpc_naming_etcd import generate_etcd_client
from trpc_naming_etcd import create_lease_manager


def test_create_lease_manager(fake_client):
    lm = create_lease_manager(fake_client)
    assert isinstance(lm, LeaseManager)
    lm.close()


def test_lease_manager_get_lease_reuses_and_renews(fake_client, monkeypatch):
    monkeypatch.setattr(client_mod, "default_force_keep_alive_time", 0.08)
    ttl = 10
    lm = LeaseManager(fake_client)

    lease1, exit1 = lm.get_lease(ttl)
    lease2, exit2 = lm.get_lease(ttl)
    # Same TTL within force window reuses the exact same lease
    assert lease1.id == lease2.id
    assert exit1 is exit2

    # Sleep past the force window; a successful refresh keeps the same lease.
    time.sleep(0.1)
    lease3, _ = lm.get_lease(ttl)
    assert lease3.id == lease1.id
    assert lease1.refresh_calls >= 1

    # A failed refresh removes the old lease and grants a new one.
    fake_client.fail_keep_alive_once = True
    time.sleep(0.1)
    new_lease, _ = lm.get_lease(ttl)
    assert new_lease.id != lease1.id
    assert exit1.is_set()

    # Simulate Grant failure -> raises error
    fake_client.fail_grant = True
    time.sleep(0.1)
    with pytest.raises(RuntimeError):
        lm.get_lease(ttl)

    lm.close()


def test_lease_manager_keep_alive_failure_triggers_exit(fake_client):
    # Aligns with Test_leaseManager_leaseKeepAlive
    lm = LeaseManager(fake_client, force_keep_alive_time=0.05)
    lease_obj, exit_ev = lm.get_lease(0.15)
    lease_obj.fail_refresh = True
    # Background keep-alive loop should detect refresh failure and set exit_ev
    deadline = time.time() + 2.0
    while time.time() < deadline and not exit_ev.is_set():
        time.sleep(0.02)
    assert exit_ev.is_set()
    lm.close()


def test_generate_etcd_client_failover():
    tried = []

    class _DummyClient:

        def __init__(self, ep):
            self.ep = ep

        def status(self):
            if self.ep.startswith("bad"):
                raise RuntimeError("unreachable")
            return object()

    def factory(_cfg, ep):
        tried.append(ep)
        return _DummyClient(ep)

    client = generate_etcd_client(
        EtcdClientConfig(endpoints=["bad:2379", "good:2379"]),
        client_factory=factory,
    )
    assert client.ep == "good:2379"
    assert tried == ["bad:2379", "good:2379"]

    with pytest.raises(EtcdNamingError):
        generate_etcd_client(
            EtcdClientConfig(endpoints=["bad1:2379", "bad2:2379"]),
            client_factory=factory,
        )
