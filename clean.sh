#!/bin/bash
#
# Tencent is pleased to support the open source community by making tRPC-Naming-Etcd available.
#
# Copyright (C) 2026 Tencent. All rights reserved.
#
# tRPC-Naming-Etcd is licensed under Apache-2.0.

set -e
cd "$(dirname "$0")"

rm -rf build dist *.egg-info trpc_naming_etcd.egg-info
find . -name __pycache__ -type d -not -path "./.venv/*" -exec rm -rf {} + 2>/dev/null || true
