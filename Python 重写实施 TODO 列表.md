# Python 重写实施 TODO 列表

> 说明：本文件用于跟踪 Python 重写与 Go 现有实现的对照进度。已完成项会标记为 `- [x]`，未完成项标记为 `- [ ]`。

## 总体实施顺序
1. `protocol`
2. `config` + `logger`
3. `websocket`
4. `marketfeed`
5. `trader` 主状态机与登录
6. `trader` 查询 / 订单 / 持久化
7. `condorder`
8. 集成测试与兼容性校验

---

## P1. 基础契约层先行

### 1) `pyctp/gateway/protocol/` — [x]
对应 Go：
- `pkg/protocol/types.go`
- `pkg/protocol/json.go`
- `pkg/protocol/enum_convert.go`

#### 已完成
- `pyctp/gateway/protocol/types.py`
  - [x] 已补齐核心枚举：`Direction` / `Offset` / `PriceType` / `VolumeCondition` / `TimeCondition` / `HedgeFlag` / `ContingentConditionType`
  - [x] 已补齐核心数据模型：`AccountData` / `PositionData` / `OrderData` / `TradeData` / `InstrumentData` / `UserData` / `NotifyData` / `RtnDataEnvelope` / `BrokerData`
  - [x] 已扩展 `InsertOrderRequest`
- `pyctp/gateway/protocol/json.py`
  - [x] 已支持 `request_id` 解析
  - [x] 已支持 `build_notify()`
  - [x] 已支持 `build_rtn_data()`
  - [x] 已支持 `build_brokers()`
  - [x] 已支持 `build_settlement_notify()`

#### 待补齐
- [ ] 枚举字符串/整数互转辅助函数
- [ ] 更完整的交易/条件单协议常量映射
- [ ] 与 Go 侧完全一致的 `rtn_data` / `notify` 嵌套格式（如后续发现兼容差异再补）
- [ ] 统一消息构建器的细节收敛

#### 验收标准
- [x] Python 可解析 Go 侧常见 `aid` 消息
- [x] Python 可构造与 Go 兼容的通知/数据包
- [x] 订单、持仓、账户、成交能用统一模型表达

---

### 2) `pyctp/gateway/config.py` / `pyctp/gateway/logger.py` — [x]
对应 Go：
- `pkg/config/config.go`
- `pkg/logger/logger.go`

#### 已完成
- `pyctp/gateway/config.py`
  - [x] 已补齐配置模型：`Config` / `LogConfig` / `CTPConfig` / `MarketFeedConfig` / `ConditionOrderConfig` / `BrokerConfig`
  - [x] 已补齐配置加载逻辑：默认值 / 文件加载 / broker 列表加载 / 全局访问
- `pyctp/gateway/logger.py`
  - [x] 已补齐基础日志封装：`info` / `warn` / `error` / `debug` / `exception`
  - [x] 已补齐默认 logger 配置入口

#### 待补齐
- [ ] 配置加载中的 broker_list 结构与 Go 侧完全对齐
- [ ] 日志的结构化上下文字段支持增强
- [ ] 文件日志输出与轮转策略（如需要）

#### 验收标准
- [x] 所有子系统统一从配置对象取值
- [ ] 日志输出格式统一、可定位 `conn_id` / `order_id` / `symbol`
- [x] 启动入口不再散落硬编码参数

---

## P1. 核心运行通道

### 3) `pyctp/gateway/market/` — [ ]
对应 Go：
- `pkg/marketfeed/interface.go`
- `pkg/marketfeed/ctpmarket.go`
- `pkg/marketfeed/ctpconn.go`
- `pkg/marketfeed/tqmarket.go`
- `pkg/marketfeed/tqconn.go`
- `pkg/marketfeed/quote.go`

#### 需要拆的任务
- 定义统一市场客户端抽象
  - `MarketClient`
- 实现缓存能力
  - `get_quote`
  - `get_all_quotes`
- 实现状态查询
  - `is_connected`
  - `is_logged_in`
  - `get_trading_day`
- 实现订阅/退订 API
- 实现自动重连与自动重订阅
- 实现 CTP 行情连接适配
- 实现行情数据标准化输出

#### 验收标准
- [ ] 可用统一接口替换具体行情源
- [ ] 断线后能恢复订阅
- [ ] 外部消费只依赖统一 `quote` 结构

---

### 4) `pyctp/gateway/websocket/` — [x]
对应 Go：
- `pkg/websocket/server.go`

#### 已完成
- [x] 连接注册与释放
- [x] `conn_id` 分配与维护
- [x] 单播/广播发送
- [x] 收消息转事件
- [x] 断开清理
- [x] 消息路由到 trader/market 引擎
- [x] 连接状态管理

#### 验收标准
- [x] 每个连接都有稳定 `conn_id`
- [x] 断开后不会残留订阅/会话状态
- [x] 交易和行情消息不会串线

---

## P1. 交易主链路

### 5) `pyctp/gateway/trader/` — [ ]
对应 Go：
- `pkg/trader/trader.go`
- `pkg/trader/login.go`
- `pkg/trader/message.go`

#### 需要拆的任务
- 实现交易状态机
  - `Init`
  - `Connecting`
  - `Connected`
  - `Authenticating`
  - `Authenticated`
  - `LoggingIn`
  - `LoggedIn`
  - `SettlementQuerying`
  - `SettlementConfirming`
  - `Ready`
  - `Stopping`
  - `Stopped`
- 实现连接管理
- 实现登录流程
- 实现结算单查询/确认流程
- 实现消息路由
  - `req_login`
  - `peek_message`
  - `insert_order`
  - `cancel_order`
  - `qry_account_info`
  - `qry_account_register`
  - `qry_transfer_serial`
  - `req_transfer`
  - `change_password`
- 实现用户数据模型与发送逻辑
- 实现交易日切换清理逻辑
- 实现行情注入到交易引擎

#### 验收标准
- [ ] 登录后能进入稳定 `ready`
- [ ] 能响应基础查询类消息
- [ ] 能向客户端发送用户数据和通知
- [ ] 状态切换可追踪、可恢复

---

## P2. 交易扩展能力

### 6) `pyctp/gateway/trader/persistence.py` — [ ]
对应 Go：
- `pkg/trader/persistence.go`

#### 需要拆的任务
- 订单 key 映射模型
- 本地/远端订单映射
- 持久化到文件
- 按用户、按交易日隔离
- 重连后恢复

#### 验收标准
- [ ] 重启或重连后能恢复订单映射
- [ ] 不同交易日不会串历史数据

---

### 7) `pyctp/gateway/trader/orders.py` — [ ]
对应 Go：
- `pkg/trader/order.go`

#### 需要拆的任务
- 下单请求解析
- 撤单请求解析
- 枚举兼容
- 订单引用管理
- 报单回报处理
- 成交回报处理
- 错误回报处理
- 订单状态更新
- 订单/成交本地索引维护

#### 验收标准
- [ ] 下单、撤单、回报链路闭环
- [ ] 订单状态能正确反映 CTP 回报
- [ ] 客户端能查到订单与成交数据

---

### 8) `pyctp/gateway/trader/query.py` — [ ]
对应 Go：
- `pkg/trader/query.go`
- `pkg/trader/scheduler.go`

#### 需要拆的任务
- 资金账户查询
- 持仓查询
- 委托查询
- 成交查询
- 查询完成后的用户数据组装
- 查询调度器
- 查询节流
- 查询完成标志
- 持仓初始化
- 成交重放

#### 验收标准
- [ ] 登录后能完整拉起账户/持仓/委托/成交
- [ ] 查询流程不会重复刷爆
- [ ] ready 状态建立在数据初始化完成之后

---

### 9) `pyctp/gateway/trader/misc.py` / `transfer.py` — [ ]
对应 Go：
- `pkg/trader/misc.go`
- `pkg/trader/transfer.go`

#### 需要拆的任务
- 修改密码
- 修改资金密码
- 银期转账
- 结算单通知
- 交易通知
- 合约状态通知
- 银行/签约关系查询

#### 验收标准
- [ ] 边缘账户能力可用
- [ ] 通知消息格式统一
- [ ] 银期相关请求与响应能走通

---

## P2. 条件单子系统

### 10) `pyctp/gateway/condorder/` — [ ]
对应 Go：
- `pkg/condorder/types.go`
- `pkg/condorder/manager.go`
- `pkg/condorder/validator.go`
- `pkg/condorder/checker.go`
- `pkg/condorder/storage.go`
- `pkg/condorder/index.go`
- `pkg/condorder/errors.go`

#### 需要拆的任务
- 条件单数据模型
- 历史条件单模型
- 条件单存储
- 条件单索引
- 条件单验证器
- 条件检测器
- 条件触发与执行管理
- 暂停/恢复/撤销/查询
- 与 trader 的回调联动
- 与行情数据联动

#### 验收标准
- [ ] 能创建/暂停/恢复/取消条件单
- [ ] 能根据行情变化触发条件单
- [ ] 能保存和查询历史条件单
- [ ] 条件单不影响主交易链路稳定性

---

## P3. 稳定性和集成

### 11) 集成测试与兼容性检查 — [ ]
#### 需要拆的任务
- WebSocket 消息兼容测试
- `aid` 路由测试
- 交易登录流程测试
- 行情订阅/退订测试
- 下单/撤单回报测试
- 条件单触发测试
- 重连恢复测试
- 消息 schema 回归测试

#### 验收标准
- [ ] 各模块互通
- [ ] Python 侧输出与 Go 侧预期结构一致
- [ ] 关键流程有可重复的验证脚本

---

## 当前状态摘要

### 已完成
- [x] `pyctp/gateway/protocol/types.py`
- [x] `pyctp/gateway/protocol/json.py`
- [x] `pyctp/gateway/config.py`
- [x] `pyctp/gateway/logger.py`
- [x] `pyctp/gateway/websocket/`
- [x] 本 TODO 文档已更新为可执行版本

### 进行中
- [ ] 协议层补强的边角兼容项（如后续发现差异再补）

### 下一步建议
- [ ] 进入 `marketfeed`
- [ ] 然后进入 `trader` 主链路
- [ ] 再推进 `condorder`
