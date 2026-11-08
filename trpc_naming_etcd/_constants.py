# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.
"""Constants and enums for the etcd naming plugin."""

from __future__ import annotations

import enum

PLUGIN_NAME = "etcd"
"""Registered plugin name for registry, selector, and discovery."""

DEFAULT_ENDPOINT = "127.0.0.1:2379"
DEFAULT_PORT = 2379

DEFAULT_TIMEOUT = 5.0
DEFAULT_ETCD_PREFIX = " trpc/registry/services/"
DEFAULT_TTL = 5
DEFAULT_WEIGHT = 1
DEFAULT_LOAD_BALANCER = "random"
DEFAULT_CACHE_TTL = 30.0

# Per-service list cache timeout, in seconds.
LIST_SELECTOR_CACHE_TIMEOUT = 60.0

DEFAULT_FORCE_KEEP_ALIVE_TIME = 1.0


class EventType(enum.IntEnum):
    """Watch event type."""

    UNKNOWN = 0
    CREATE = 1
    DELETE = 2
    UPDATE = 3

    def __str__(self) -> str:
        """Return the string name of this event type."""
        names = {
            EventType.UNKNOWN: "UnknownEventType",
            EventType.CREATE: "Create",
            EventType.DELETE: "Delete",
            EventType.UPDATE: "Update",
        }
        return names.get(self, f"EventType({int(self)})")


class SelectorPolicy(enum.IntEnum):
    """How many endpoints a batch select returns."""

    ONE = 0
    ALL = 1
