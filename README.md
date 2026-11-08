English | [中文](README.zh_CN.md)

# tRPC-Python etcd naming plugin

[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

This plugin wraps the [etcd3](https://github.com/kragniz/python-etcd3)
client to provide etcd-based service registration, service discovery, and
routing selection for the tRPC-Python framework.

## Features

- Importing `trpc_naming_etcd` automatically registers three plugins named
  `etcd`:
  - `NAMING_REGISTRY` (`EtcdRegistry`): etcd lease-based service
    registration, reuse of leases with the same TTL and background
    keep-alive (`LeaseManager`), exponential-backoff re-registration when
    a lease expires, and graceful deregistration.
  - `NAMING_DISCOVERY` (`EtcdDiscovery`): singleflight to prevent cache
    stampedes, a local node cache (30s expiry by default, plus revision
    checks that reject stale write-backs), and a background prefix watch
    (`EtcdWatcher`) that incrementally handles `Create` / `Update` /
    `Delete` events.
  - `NAMING_SELECTOR` (`EtcdSelector`): supports
    `target: etcd://<service>` addressing, plus `random`, `round_robin`,
    and any `NAMING_LOADBALANCE` plugin registered with the framework.
- Dict and list configuration layouts, and both etcd key layouts those
  configs use (including `select_batch` with `SelectorPolicy.ONE` /
  `SelectorPolicy.ALL` and `RegistryInfo`).

## Quick start

### Step 1: install

```shell
pip install trpc-naming-etcd
```

### Step 2: configure

In `trpc_python.yaml`, configure `plugins.registry.etcd` and
`plugins.selector.etcd`:

```yaml
server:
  service:
    - name: trpc.test.helloworld.Greeter
      ip: 127.0.0.1
      port: 10001
      protocol: trpc
      network: tcp
      registry: etcd

plugins:
  registry:
    etcd:
      address: 127.0.0.1:2379,127.0.0.2:2379
      timeout: 5
      prefix: /registry
      # tls:
      #   cafile: ./cert/etcd/ca.crt
      #   certfile: ./cert/etcd/tls.crt
      #   keyfile: ./cert/etcd/tls.key
      service:
        - name: trpc.test.helloworld.Greeter
          ttl: 10
          weight: 100
          metadata:
            tags: helloworld
  selector:
    etcd:
      address: 127.0.0.1:2379,127.0.0.2:2379
      timeout: 5
      prefix: /registry
      load_balance:
        name: round_robin

client:
  service:
    - name: trpc.test.helloworld.Greeter
      target: etcd://trpc.test.helloworld.Greeter
      network: tcp
      protocol: trpc
      timeout: 1000
```

List-style configuration under `plugins.registry.etcd` and
`plugins.selector.etcd` is also supported:

```yaml
plugins:
  registry:
    etcd:
      - name: trpc.test.helloworld.Greeter
        prefix: test_trpc
        endpoint: http://127.0.0.1:2379
  selector:
    etcd:
      - name: trpc.test.helloworld.Greeter
        prefix: test_trpc
        endpoint: http://127.0.0.1:2379
```

### Step 3: use

```python
import trpc_naming_etcd  # noqa: F401  (import registers the plugins)
from trpc_sdk.abc import SelectorOption
from trpc_sdk.config import TrpcConfig
from trpc_sdk.naming import get_registry, get_selector

TrpcConfig.load("trpc_python.yaml")

registry = get_registry("etcd")
selector = get_selector("etcd")

await registry.register("trpc.test.helloworld.Greeter", ("127.0.0.1", 10001))
node = await selector.select("trpc.test.helloworld.Greeter", SelectorOption())
print(node.address, node.metadata)
await registry.deregister("trpc.test.helloworld.Greeter", ("127.0.0.1", 10001))
```

## Example

A complete runnable example is in [examples](examples/README.md).

## Development setup

```bash
# Install this package and the test dependencies
pip install -e .
pip install -r requirements-test.txt

# Run tests
pytest

# Format the code
./format.sh
```

## License

This project is licensed under Apache-2.0.
