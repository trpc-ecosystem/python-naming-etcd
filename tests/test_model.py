# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""Tests for model.py."""

import pytest

from trpc_naming_etcd.model import EtcdNode
from trpc_naming_etcd.model import convert_node
from trpc_naming_etcd.model import cpp_node_key
from trpc_naming_etcd.model import cpp_service_key
from trpc_naming_etcd.model import marshal
from trpc_naming_etcd.model import marshal_cpp_meta
from trpc_naming_etcd.model import node_path
from trpc_naming_etcd.model import parse_cpp_endpoint_info
from trpc_naming_etcd.model import service_id
from trpc_naming_etcd.model import service_path
from trpc_naming_etcd.model import split_host_port
from trpc_naming_etcd.model import unmarshal


def test_node_path():
    assert node_path("prefix", "service", "id") == "prefix/service/id"
    assert node_path("/registry", "trpc/app/svc", "127.0.0.1-8080-1") == "/registry/trpc-app-svc/127.0.0.1-8080-1"


def test_service_id():
    assert service_id("host", "port", "pid") == "host-port-pid"
    assert service_id("127.0.0.1", 8080, 123) == "127.0.0.1-8080-123"


def test_service_path():
    assert service_path("prefix", "service") == "prefix/service"
    assert service_path("prefix", "") == "prefix"
    assert service_path("/registry", "trpc/app/svc") == "/registry/trpc-app-svc"


def test_marshal_unmarshal_and_convert_node():
    node = EtcdNode(
        name="trpc.test.helloworld.Greeter",
        id=service_id("127.0.0.1", "8080", "123"),
        address="127.0.0.1:8080",
        metadata={"tags": "helloworld"},
        weight=100,
    )
    raw = marshal(node)
    parsed = unmarshal(raw)
    assert parsed == node

    trpc_node = convert_node(parsed)
    assert trpc_node.service_name == "trpc.test.helloworld.Greeter"
    assert trpc_node.address == "127.0.0.1:8080"
    assert trpc_node.ip == "127.0.0.1"
    assert trpc_node.port == 8080
    assert trpc_node.weight == 100
    assert trpc_node.metadata == {"tags": "helloworld"}


def test_cpp_key_and_endpoint_parsing():
    assert cpp_service_key("", "test1.service") == "/test1.service"
    assert cpp_service_key("test1", "test1.service") == "/test1_test1.service"
    key = cpp_node_key("test1", "test1.service", "127.0.0.1", 10001)
    assert key == "/test1_test1.service/127.0.0.1/10001"

    meta_json = marshal_cpp_meta({"instance_id": "return_instance1"})
    ep = parse_cpp_endpoint_info(key, meta_json)
    assert ep is not None
    assert ep.host == "127.0.0.1"
    assert ep.port == 10001
    assert ep.meta == {"instance_id": "return_instance1"}

    # unmarshal also accepts {"meta": ...} values whose host and port come from the key.
    node = unmarshal(meta_json, key=key)
    assert node is not None
    assert node.address == "127.0.0.1:10001"
    assert node.metadata == {"instance_id": "return_instance1"}


def test_split_host_port():
    assert split_host_port("127.0.0.1:8080") == ("127.0.0.1", 8080)
    assert split_host_port("[::1]:9090") == ("::1", 9090)
    with pytest.raises(ValueError):
        split_host_port("invalid-address")
    with pytest.raises(ValueError):
        split_host_port("")
