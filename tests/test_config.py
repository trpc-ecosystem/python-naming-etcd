# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""Tests for dict and list configuration models."""

from trpc_naming_etcd import DEFAULT_ETCD_PREFIX
from trpc_naming_etcd import EtcdClientConfig
from trpc_naming_etcd import EtcdNamingConfig
from trpc_naming_etcd import EtcdRegistryConfig
from trpc_naming_etcd import EtcdSelectorConfig
from trpc_naming_etcd import EventType
from trpc_naming_etcd import parse_endpoint


def test_event_type_stringer_matches_go():
    assert str(EventType.UNKNOWN) == "UnknownEventType"
    assert str(EventType.CREATE) == "Create"
    assert str(EventType.DELETE) == "Delete"
    assert str(EventType.UPDATE) == "Update"


def test_go_style_registry_and_selector_config():
    raw = {
        "plugins": {
            "registry": {
                "etcd": {
                    "address": "127.0.0.1:2379,127.0.0.2:2379",
                    "timeout": 5,
                    "prefix": "/registry",
                    "tls": {
                        "cafile": "./cert/ca.crt",
                        "certfile": "./cert/tls.crt",
                        "keyfile": "./cert/tls.key",
                    },
                    "service": [
                        {
                            "name": "trpc.test.helloworld.Greeter",
                            "ttl": 10,
                            "weight": 50,
                            "metadata": {"tags": "helloworld"},
                        }
                    ],
                }
            },
            "selector": {
                "etcd": {
                    "address": "127.0.0.1:2379",
                    "timeout": 5,
                    "prefix": "/registry",
                    "load_balance": {"name": "round_robin"},
                }
            },
        }
    }
    naming_cfg = EtcdNamingConfig.from_dict(raw)
    reg_cfg = naming_cfg.registry_config
    assert reg_cfg.client_config.endpoints == ["127.0.0.1:2379", "127.0.0.2:2379"]
    assert reg_cfg.client_config.timeout == 5.0
    assert reg_cfg.prefix == "/registry"
    assert reg_cfg.client_config.ca_cert == "./cert/ca.crt"
    assert reg_cfg.client_config.cert_cert == "./cert/tls.crt"
    assert reg_cfg.client_config.cert_key == "./cert/tls.key"

    svc = reg_cfg.get_service_config("trpc.test.helloworld.Greeter")
    assert svc is not None
    assert svc.ttl == 10
    assert svc.weight == 50
    assert svc.metadata == {"tags": "helloworld"}
    assert svc.prefix == "/registry"

    sel_cfg = naming_cfg.selector_config
    assert sel_cfg.load_balancer == "round_robin"
    assert sel_cfg.prefix == "/registry"


def test_cpp_style_naming_config():
    raw = {
        "registry": {
            "etcd": [
                {"name": "test1.service", "prefix": "test1", "endpoint": "http://127.0.0.1:2379"},
                {"name": "test2.service", "prefix": "test2", "endpoint": "test2"},
            ]
        },
        "selector": {
            "etcd": [
                {"name": "test1.service", "prefix": "test1", "endpoint": "http://127.0.0.1:2379"},
                {"name": "test2.service", "prefix": "test2", "endpoint": "test2"},
            ]
        },
    }
    naming_cfg = EtcdNamingConfig.from_dict(raw)
    naming_cfg.display()

    assert naming_cfg.registry_config.cpp_mode is True
    assert len(naming_cfg.registry_config.services_config) == 2
    assert naming_cfg.registry_config.services["test1.service"].endpoint == "http://127.0.0.1:2379"
    assert naming_cfg.registry_config.services["test1.service"].prefix == "test1"
    assert naming_cfg.registry_config.get_service_config("unknown.service") is None

    assert naming_cfg.selector_config.cpp_mode is True
    assert len(naming_cfg.selector_config.services_config) == 2


def test_client_config_defaults_and_kwargs():
    cfg = EtcdClientConfig.from_dict(None)
    assert cfg.endpoints == ["127.0.0.1:2379"]
    assert cfg.prefix == DEFAULT_ETCD_PREFIX
    assert cfg.timeout == 5.0
    assert parse_endpoint("http://127.0.0.1:2379") == ("127.0.0.1", 2379)
    assert parse_endpoint("[::1]:2380") == ("::1", 2380)

    cfg_auth = EtcdClientConfig.from_dict({
        "address": "10.0.0.1:2379",
        "username": "root",
        "password": "123",
    })
    kwargs = cfg_auth.client_kwargs("10.0.0.1:2379")
    assert kwargs == {"host": "10.0.0.1", "port": 2379, "timeout": 5.0, "user": "root", "password": "123"}
