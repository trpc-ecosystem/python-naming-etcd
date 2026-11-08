# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""Error types for the etcd naming plugin."""

from __future__ import annotations


class EtcdNamingError(Exception):
    """Base error raised by the etcd naming plugin."""


class ServerNotAvailableError(EtcdNamingError):
    """Raised when a service has no available instances in etcd."""

    def __init__(self, message: str = "server can not available"):
        super().__init__(message)


class BalancerNotExistError(EtcdNamingError):
    """Raised when the requested load balancer does not exist."""

    def __init__(self, message: str = "load balancer is not exist"):
        super().__init__(message)


class StaleDataError(EtcdNamingError):
    """Raised when the revision returned from etcd is older than the cache."""

    def __init__(self, message: str = "store data is stale"):
        super().__init__(message)
