# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""Example: service registration, discovery, and selection with etcd.

Prerequisites (see examples/README.md): an etcd server at 127.0.0.1:2379.

Run:
    python3 examples/trpc_main.py
"""

from __future__ import annotations

import asyncio

import trpc_naming_etcd  # noqa: F401  (importing registers etcd registry, discovery, selector)
from trpc_sdk.abc import SelectorOption
from trpc_sdk.config import TrpcConfig
from trpc_sdk.naming import get_registry
from trpc_sdk.naming import get_selector


async def run_example() -> None:
    TrpcConfig.load("examples/trpc_python.yaml")
    service_name = "trpc.test.helloworld.Greeter"

    registry = get_registry("etcd")
    selector = get_selector("etcd")
    assert registry is not None and selector is not None

    # Register two instances of Greeter
    await registry.register(service_name, ("127.0.0.1", 10001))
    await registry.register(service_name, ("127.0.0.1", 10002))
    print(f"registered {service_name} -> 127.0.0.1:10001, 127.0.0.1:10002")

    # Select endpoints (configured with round_robin in examples/trpc_python.yaml)
    for i in range(4):
        node = await selector.select(service_name, SelectorOption())
        print(f"select #{i + 1} -> {node.address} (weight={node.weight}, metadata={node.metadata})")
        await selector.report(node, cost=5, code=0)

    # Deregister instances
    await registry.deregister(service_name, ("127.0.0.1", 10001))
    await registry.deregister(service_name, ("127.0.0.1", 10002))
    print(f"deregistered {service_name}")


def main() -> None:
    asyncio.run(run_example())


if __name__ == "__main__":
    main()
