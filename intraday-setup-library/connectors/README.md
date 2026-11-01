# 行情连接器

三个 provider 共用 `describe() -> dict`、`events(stop_event=None) -> Iterator[MarketEvent]` 接口。来源按实际接收顺序提供事件，运行器顺序处理与持久化。

- `DemoProvider(symbols=..., minutes=390, sessions=1, pace_seconds=0)`：五品种默认名单，明确标为合成资料，提供历史校准样例、分钟柱和后续报价；多日模式跳过周末，不冒充真实交易日历。
- `JsonlProvider(path, pace_seconds=0)`：原输入顺序回放；启动与结束验证流式文件哈希。能力依据每个 session 的声明，不能从文件存在推断能力完整。
- `AlpacaIEXProvider(symbols=...)`：免费账号的显式 IEX 行情；read-only calendar/history GET 与 WebSocket，无券商订单接口。

默认观察名单 `SPY QQQ AAPL MSFT NVDA`，允许替换为 1–30 个唯一大写 ticker。Demo、JSONL及 REST 使用 Python 标准库。仅实时 WebSocket 需要 Python 3.11 或更新版本及固定依赖 `websockets==17.1`：

```sh
python3 -m pip install -r connectors/requirements.txt
```

项目根目录的 `requirements-runtime.txt` 与此处安装同一个实时客户端，均固定为17.1；没有按运行平台和连接器分别安装不同版本的需求。安装、诊断和运行应使用同一个Python环境。2026-10-03核对时，项目本地 `.venv` 的包元数据与实际导入版本均为17.1，安装器为pip；系统Python未安装该可选包，因此已有虚拟环境时应使用 `.venv/bin/python -m pip` 和 `.venv/bin/python -m runtime doctor`。官方 [PyPI 17.1发行记录](https://pypi.org/project/websockets/17.1/) 列明2026-08-26发布、Python最低版本3.11及 `websockets.sync.client.connect` 接口；本地元数据中的项目与文档地址对应官方项目。版本核对不代表真实Alpaca连接验收。

Alpaca 密钥从 `ALPACA_API_KEY_ID`、`ALPACA_API_SECRET_KEY` 读取，也支持 `APCA_API_KEY_ID`、`APCA_API_SECRET_KEY` 别名。输出只含 configured 布尔值，不保存密钥。未配置密钥时实时源返回明确错误，演示和 JSONL 可正常使用。连接器不会调用模型或收费 feed。

IEX 是单一交易所资料，成交量、Profile、VWAP都保留 `IEX_only` 范围；不称全美市场。买卖分类、深度、新闻与完整市场成交量不可用。交易以 `side=unknown` 保存；没有 notional 时无法冒充精确柱成交额。分钟柱起始时间加一分钟作为现有契约的区间终点；`vendor_vwap` 只保留供应商观测，不计算 `vw × volume`。

交易日历由 Alpaca `/v2/calendar` 提供，纽约时区转换支持夏令时和半日市。启动默认请求 15 个已完成会话的 RTH 一分钟历史（可配置 0–30）；只有每分钟齐全、现金开盘和收盘边界明确、且早于当前 session 的资料进入日/同刻校准。历史不足或请求失败在 `history_status` 中保存原因，资料保持未知。中途启动以实际 arrival 保存日历初始化，coverage 明示不完整，不伪造已错过行情。同日 WebSocket 重连不重拉历史，也不自动回补当日缺口；历史初始化在新交易日再次执行。

CLI使用`--history-sessions 30`，配置使用`alpaca_history_sessions`；0明确关闭历史请求。session的`history_calendar`保存已完成交易日的明确日期、开收盘及日历出处。前日参考必须匹配最近日历交易日的完整summary；最近日缺一分钟也不能用更早日替代。ATR、均量和同刻特征按各自所需连续完整日数检查。显式`history=[]`清除旧历史；省略history的旧事件接口仍可保留引擎已因果收集的历史。

日历初始化与流连接共用 1–60 秒指数退避。网络超时、限流、HTTP 408/425/429 和 5xx 会继续重试；认证、权限、其他不可恢复 HTTP 错误或非法日历停止来源并交由运行器保存安全错误状态。日历不可用期间不会猜测时段或生成市场 session。`set_status_callback(callback)` 可接运行器的 `source_status(state, **details)`：重试保存 `DISCONNECTED`、阶段、分类与下一次等待秒数，初始化成功为 `INITIALIZED`，实际订阅核验后才为 `CONNECTED`。这些管理通知独立于行情，不推进识别时钟；终止错误由运行器统一记录一次。独立使用时 `describe().initialization_status` 可读取初始化阶段。

WebSocket 包含认证、订阅核验、15 秒认证/订阅超时、ping、5 秒可中断接收检查、1–60 秒指数重连等待及显式 gap 状态。外部异常只保存类型；协议错误只保存代码，避免请求头或密钥进入日志。断线后不补造报价。

当前只订阅 bars、trades、quotes 与 updatedBars，未订阅或验收真实 `statuses`。停牌/恢复消息转换有协议单测，真实停牌观测能力保持未知；不能用连接恢复代替交易所恢复。供应商状态订阅及生产权限须另行验证。同一接收帧的行情与派生状态共用接收时间，保持原行顺序。

原始 bar ID 按来源、品种及分钟固定；trade ID 按来源、品种、交易日、交易所及供应商 trade ID 固定。同 ID 异内容由运行存储隔离。无独立供应商 ID 的 quote 按内容哈希。重传的 arrival 可能不同，运行去重比较排除 arrival 并保留首次接收证据。更新柱、成交更正和撤销作为新的 status 证据及资料缺口，绝不重写先前判断或成交。供应商纳秒时间原文保留在 payload，契约 datetime 精度为微秒。

API参考（2026-10-03 核对）：[实时消息](https://docs.alpaca.markets/us/docs/real-time-stock-pricing-data)、[数据来源范围](https://docs.alpaca.markets/us/docs/historical-stock-data-1)、[行情口径](https://docs.alpaca.markets/us/docs/market-data-faq)、[日历](https://docs.alpaca.markets/us/v1.1/reference/getcalendar-1)、[WebSocket同步客户端](https://websockets.readthedocs.io/en/17.1/reference/sync/client.html)。

```sh
python3 -m unittest connectors.tests
```

测试包含记录协议帧、fake socket、日历瞬时故障重试恢复、不可恢复初始化分类、递增接收时钟下的混合帧、半日市/夏令时、缺历史分钟、分页和循环、身份碰撞、更正、未知方向、非法报价、密钥导出与中断。没有密钥及实际开市接收时，真实连接验收保持 `not_run`。
