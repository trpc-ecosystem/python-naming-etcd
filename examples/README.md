# etcd naming example

This example demonstrates how to register, discover, and select service
endpoints stored in etcd from a tRPC-Python program.

## Prerequisites

An etcd cluster running at `127.0.0.1:2379`. For local testing a
single-node etcd is enough; follow the
[etcd quickstart](https://etcd.io/docs/v3.5/quickstart/) to start one.

## Run

```shell
pip install trpc-naming-etcd
python3 examples/trpc_main.py
```

Expected output:

```text
registered trpc.test.helloworld.Greeter -> 127.0.0.1:10001, 127.0.0.1:10002
select #1 -> 127.0.0.1:10001 (weight=100, metadata={'tags': 'helloworld'})
select #2 -> 127.0.0.1:10002 (weight=100, metadata={'tags': 'helloworld'})
select #3 -> 127.0.0.1:10001 (weight=100, metadata={'tags': 'helloworld'})
select #4 -> 127.0.0.1:10002 (weight=100, metadata={'tags': 'helloworld'})
deregistered trpc.test.helloworld.Greeter
```
