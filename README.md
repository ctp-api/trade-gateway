# alpha-trade-gateway（Open Trade CTP SE15 Go）

一个以 **WebSocket** 形式对外提供交易能力的 CTP 交易网关（Td），并内置可切换的行情源（天勤 TQ WebSocket / CTP MdApi），支持登录、查询、下单/撤单、结算通知，以及可选的**条件单**能力。

- 入口程序：[cmd/trader/main.go](cmd/trader/main.go)
- WebSocket 服务器：[`websocket.NewServer`](pkg/websocket/server.go)、[`websocket.Server.Start`](pkg/websocket/server.go)
- 配置加载：[`config.Load`](pkg/config/config.go)、[`config.LoadFromDir`](pkg/config/config.go)
- 行情统一接口：[`marketfeed.MarketClient`](pkg/marketfeed/interface.go)、[`marketfeed.NewMarketClientFromConfig`](pkg/marketfeed/interface.go)
- 条件单（可选）：[`condorder.Manager`](pkg/condorder/manager.go)、[`condorder.DefaultConfig`](pkg/condorder/types.go)

---

## 架构概览

启动顺序在入口中固定为：

1. 启动 MarketFeed（行情基础服务）：[`createMarketFeedClient`](cmd/trader/main.go) → [`marketfeed.NewMarketClientFromConfig`](pkg/marketfeed/interface.go)
2. 创建交易处理器（CTP Trader）：[`trader.New`](pkg/trader)（由 [cmd/trader/main.go](cmd/trader/main.go) 调用）
3. 启动 WebSocket 服务：[`websocket.NewServer`](pkg/websocket/server.go) → [`websocket.Server.Start`](pkg/websocket/server.go)

---

## 功能特性

- WebSocket 交易服务（默认 `0.0.0.0:7788`，见 [`config.setDefaults`](pkg/config/config.go)）
- 登录流程（含“次席”自定义 `broker_id/front` 支持）：[`TraderCTP.processReqLoginFull`](pkg/trader/login.go)
- 行情源可切换：
  - 天勤（`tq`）/ CTP 行情（`ctp`）：[`marketfeed.MarketClientType`](pkg/marketfeed/interface.go)
  - Quote 结构统一：[`marketfeed.Quote`](pkg/marketfeed/quote.go)
  - 行情回调：[`marketfeed.WithOnQuotes`](pkg/marketfeed/interface.go)
- 条件单（可配置启用）：
  - 初始化挂载：[`TraderCTP.initConditionOrderManager`](pkg/trader/trader.go)
  - 请求结构：[`condorder.ReqInsertConditionOrder`](pkg/condorder/types.go)、[`condorder.ReqCancelConditionOrder`](pkg/condorder/types.go)

---

## 目录结构

```text
cmd/trader/                 # 网关进程（启动行情 + Trader + WebSocket）
config/                     # 配置模板与 broker 列表
examples/                   # Go/Python 示例 + 协议说明
pkg/
  config/                   # 配置加载与 broker 列表
  websocket/                # WebSocket server/connection
  trader/                   # CTP Trader 实现（登录、下单、查询、持久化等）
  protocol/                 # JSON 编解码与协议结构体
  marketfeed/               # 行情接口 + TQ/CTP 实现
  condorder/                # 条件单（可选）
```

---

## tqsdk-python 对接（重点）

如果你希望在 Python 里继续使用天勤 `tqsdk` 的 API/生态，但把**交易通道**切到本项目的 WebSocket 网关，可以参考示例脚本：

- [examples/t41.py](examples/t41.py)

该示例的核心是把 `TqApi(..., _td_url="ws://127.0.0.1:7788")` 指向本项目网关地址。

### 运行步骤

1) 启动网关（确保 `host/port` 可访问；默认 `0.0.0.0:7788`）：

```sh
go run ./cmd/trader -config ./config/config.json
```

2) 安装 tqsdk（建议使用虚拟环境）：

```sh
python -m pip install tqsdk
```

3) 设置示例脚本所需环境变量并运行：

```sh
export SHINNYTECH_PW="<你的天勤密码>"
python examples/t41.py
```

### 关键参数说明

- `_td_url`：tqsdk 交易通道的地址，必须指向本网关，例如 `ws://127.0.0.1:7788`。

### 账号与凭证说明

示例脚本当前 `TqAccount("simnow", ...)` 账号写在代码里；脚本也预留了 `SIMNOW_USER_ID` / `SIMNOW_USER_PASSWD` 环境变量，但目前未使用。
如果你希望用环境变量驱动账号，请在 [examples/t41.py](examples/t41.py) 中把账号/密码替换为读取 `SIMNOW_USER_ID` / `SIMNOW_USER_PASSWD`。

---

## 快速开始

### 1) 准备配置

推荐从示例复制：

- 示例配置：[config/config.json.example](config/config.json.example)
- 示例 broker 列表：[config/broker_list.json.example](config/broker_list.json.example)

项目也提供一个可直接用于运行的配置样例（注意其中包含更偏开发/本地路径的配置项）：
- [cmd/trader/config.json](cmd/trader/config.json)
- [cmd/trader/broker_list.json](cmd/trader/broker_list.json)

> 配置读取逻辑：[`config.Load`](pkg/config/config.go)  
> broker 列表从 `broker_list_path` 加载：[`config.loadBrokerList`](pkg/config/config.go)

### 2) 启动网关

```sh
go run ./cmd/trader -config ./config/config.json
```

或构建后运行：

```sh
go build -o trader ./cmd/trader
./trader -config ./config/config.json
```

### 3) 测试连接（Python）

```sh
cd examples
pip install -r requirements.txt
python test_connection.py localhost 7788
```

脚本：[examples/test_connection.py](examples/test_connection.py)

---

## 配置说明（核心字段）

全局配置结构：[`config.Config`](pkg/config/config.go)

常用配置文件：

- JSON： [config/config.json](config/config.json)
- TOML 示例： [config/config.toml.example](config/config.toml.example)

### 行情源（marketfeed）

配置结构：[`config.MarketFeedConfig`](pkg/config/config.go)

- `marketfeed.type`: `"tq"` 或 `"ctp"`
- `marketfeed.symbols`: 默认订阅合约列表（例如：`"SHFE.ag2601"`）

创建逻辑位于入口：[`createMarketFeedClient`](cmd/trader/main.go)

---

## WebSocket 协议与示例

协议说明与请求示例集中在：

- [examples/README.md](examples/README.md)

常见请求 `aid`（以示例文档为准）：

- 登录：`req_login`（结构：[`protocol.ReqLogin`](pkg/protocol/types.go)）
- 下单：`insert_order`（结构：[`protocol.ActionInsertOrder`](pkg/protocol/types.go)）
- 撤单：`cancel_order`（结构：[`protocol.ActionCancelOrder`](pkg/protocol/types.go)）
- 主动拉取：`peek_message`

服务端 JSON 编解码：[`protocol.MarshalString`](pkg/protocol/json.go)、[`protocol.UnmarshalString`](pkg/protocol/json.go)  
通知类消息：[`protocol.BuildNotifyMsg`](pkg/protocol/json.go)、[`protocol.BuildSettlementNotifyMsg`](pkg/protocol/json.go)  
broker 列表推送：[`protocol.BuildBrokerListMsg`](pkg/protocol/json.go)

---

## 行情组件（MarketFeed）

行情库单独文档：

- [pkg/marketfeed/README.md](pkg/marketfeed/README.md)

示例：

- Go：CTP 行情订阅示例：[examples/ctpmarket/ctpmarket.go](examples/ctpmarket/ctpmarket.go)
- Go：统一接口切换行情源：[examples/marketinterface/marketinterface.go](examples/marketinterface/marketinterface.go)

---

## 条件单（Condition Order）

默认关闭（见 [`config.setDefaults`](pkg/config/config.go) 中 `condition_order.enabled`），启用后会在 Trader 初始化阶段加载/恢复并启动检测：

- 初始化：[`TraderCTP.initConditionOrderManager`](pkg/trader/trader.go)
- 配置结构：[`config.ConditionOrderConfig`](pkg/config/config.go)、默认值：[`condorder.DefaultConfig`](pkg/condorder/types.go)

---

## 备注

- 示例客户端：
  - 交互式 Python 客户端：[examples/python_client.py](examples/python_client.py)
  - 最简示例：[examples/simple_example.py](examples/simple_example.py)



- Implement `pyctp/gateway/protocol/` data models, enums, and JSON/message builders to match Go `pkg/protocol`
- Complete `pyctp/gateway/protocol/types.py` with full enums and core data models aligned to Go `pkg/protocol`
- Extend `pyctp/gateway/protocol/json.py` with builders/parsers for notify and rtn_data messages
- Implement unified config loading in `pyctp/gateway/config.py` matching Go `pkg/config`
- Implement structured logging wrapper in `pyctp/gateway/logger.py` matching Go `pkg/logger`
- Align `pyctp/gateway/market/` with Go `pkg/marketfeed` interface, cache, and reconnect behavior
- Verify and harden `pyctp/gateway/websocket/` conn_id, routing, and broadcast semantics
- Implement `pyctp/gateway/trader/` state machine, login flow, query flow, and order lifecycle
- Implement `pyctp/gateway/condorder/` data model, storage, validation, and trigger flow



- 实现 `pyctp/gateway/protocol/` 下的数据模型、枚举类型以及 JSON/消息构建器，使其与 Go 语言版本的 `pkg/protocol` 保持一致。
- 完善 `pyctp/gateway/protocol/types.py`，补充完整的枚举类型及核心数据模型，使其与 Go 语言版本的 `pkg/protocol` 对齐。
- 扩展 `pyctp/gateway/protocol/json.py`，增加用于处理通知（notify）及回报数据（rtn_data）消息的构建器与解析器。
- 在 `pyctp/gateway/config.py` 中实现统一的配置加载机制，使其与 Go 语言版本的 `pkg/config` 保持一致。
- 在 `pyctp/gateway/logger.py` 中实现结构化日志封装层，使其与 Go 语言版本的 `pkg/logger` 保持一致。
- 将 `pyctp/gateway/market/` 模块与 Go 语言版本的 `pkg/marketfeed` 在接口定义、缓存机制及重连行为上进行对齐。
- 验证并强化 `pyctp/gateway/websocket/` 模块中的连接 ID（conn_id）管理、消息路由及广播机制的逻辑与健壮性。
- 在 `pyctp/gateway/trader/` 模块中实现状态机、登录流程、查询流程以及订单生命周期管理。
- 在 `pyctp/gateway/condorder/` 模块中实现条件单的数据模型、存储机制、有效性校验及触发流程。



Implement `pyctp/gateway/protocol/` data models, enums, and JSON/message builders to match Go `pkg/protocol`

Add unified Python config and structured logger layers to match Go `pkg/config` and `pkg/logger`

Create a unified Python market client abstraction with cache/state APIs to match Go `pkg/marketfeed`

Implement trader state machine, login flow, settlement flow, and query scheduler core in Python to match Go `pkg/trader`

Stabilize Python WebSocket connection management, `conn_id`, and routing semantics to match Go `pkg/websocket`

Add order-key persistence and reconnect recovery for Python trader to match Go `pkg/trader/persistence.go`

Implement Python order/cancel/query/transfer/password-change handlers to match Go `pkg/trader/order.go`, `query.go`, `misc.go`, `transfer.go`

Build Python condition-order subsystem: models, validator, checker, storage, index, and manager to match Go `pkg/condorder`



实现 `pyctp/gateway/protocol/` 下的数据模型、枚举类型以及 JSON/消息构建器，以对标 Go 语言的 `pkg/protocol` 模块。

添加统一的 Python 配置层和结构化日志层，以对标 Go 语言的 `pkg/config` 和 `pkg/logger` 模块。

创建统一的 Python 行情客户端抽象层，并提供缓存与状态管理 API，以对标 Go 语言的 `pkg/marketfeed` 模块。

使用 Python 实现交易员状态机、登录流程、结算流程以及查询调度核心，以对标 Go 语言的 `pkg/trader` 模块。

稳定化 Python WebSocket 连接管理机制、`conn_id` 标识符以及路由语义，以对标 Go 语言的 `pkg/websocket` 模块。

为 Python 交易员模块添加订单键（order-key）持久化存储及重连恢复功能，以对标 Go 语言的 `pkg/trader/persistence.go` 文件。

实现 Python 端的报单、撤单、查询、资金划转及密码修改等业务处理器，以对标 Go 语言的 `pkg/trader/order.go`、`query.go`、`misc.go` 和 `transfer.go` 文件。

构建 Python 条件单子系统：包含数据模型、校验器、检查器、存储模块、索引模块及管理器，以对标 Go 语言的 `pkg/condorder` 模块。