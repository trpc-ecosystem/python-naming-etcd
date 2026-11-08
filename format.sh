#!/bin/bash
#
# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.

set -e
cd "$(dirname "$0")"

python3 -m yapf -i -r trpc_naming_etcd tests examples
