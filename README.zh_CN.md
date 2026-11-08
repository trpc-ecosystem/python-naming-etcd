[English](README.md) | 中文

# tRPC-Python etcd 名字服务插件

[![LICENSE](https://img.shields.io/badge/license-Apache--2.0-green.svg)](LICENSE)

本插件封装了 [etcd3](https://github.com/kragniz/python-etcd3) 客户端，在
tRPC-Python 框架中提供基于 etcd 的服务注册、服务发现与路由选择能力。

## 功能特性

- 导入 `trpc_naming_etcd` 即自动向框架注册名为 `etcd` 的三类插件：
  - `NAMING_REGISTRY`（`EtcdRegistry`）：基于 etcd Lease 的服务注册、同 TTL 租约复用与后台自动续租（`LeaseManager`）、租约失效指数退避重注册、优雅反注册。
  - `NAMING_DISCOVERY`（`EtcdDiscovery`）：Singleflight 防缓存击穿、本地节点缓存（默认 30s 过期 + Revision 校验防旧数据回写）、后台前缀监听（`EtcdWatcher`）增量处理 `Create` / `Update` / `Delete` 事件。
  - `NAMING_SELECTOR`（`EtcdSelector`）：支持 `target: etcd://<service>` 寻址，支持 `random`、`round_robin` 及框架注册的 `NAMING_LOADBALANCE` 插件。
- 支持字典和列表两种 YAML 配置，以及对应的两种 etcd key 布局（含 `select_batch`、`SelectorPolicy.ONE` / `SelectorPolicy.ALL` 与 `RegistryInfo`）。

## 快速开始

### 第一步：安装

```shell
pip install trpc-naming-etcd
```

### 第二步：配置

在 `trpc_python.yaml` 中配置 `plugins.registry.etcd` 与 `plugins.selector.etcd`：

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

同时也支持列表配置：

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

### 第三步：使用

```python
import trpc_naming_etcd  # noqa: F401（import 即完成注册）
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

## 示例

可运行的完整示例见 [examples](examples/README.md)。

## 开发

```bash
# 安装本包和测试依赖
pip install -e .
pip install -r requirements-test.txt

# 运行测试
pytest

# 格式化代码
./format.sh
```

## 许可证

本项目采用 Apache-2.0 许可证。
