# azure-proxy

本地 OpenAI 协议代理。持有 Azure 凭据，对外暴露**无需鉴权**的
`/v1/chat/completions`、`/v1/responses`、`/v1/images/generations` 和
`/v1/images/edits`。调用端只给模型名和参数，代理负责挑
endpoint、附加凭据、把模型名换成该 endpoint 上的部署名，按每个部署各自的配额
分摊流量，并在 429/5xx/流内限流时切到另一个部署重试。

几个面**分开路由**，因为它们本来就是不同的几组部署。Azure 把 Responses API 挡在
**独立的数据操作**
（`Microsoft.CognitiveServices/accounts/OpenAI/responses/write`）后面，老的
api-version 也根本不提供这个接口——所以一个 chat 能用的模型完全可能没有 responses
路由。图像模型（`gpt-image-*`）又是另外一组：它们没有任何文本面，而且只存在于个别
资源上（swc 有，scus 没有）。谁支持哪个面由探测决定。

调用端有三类，行为都是实测的：

| | |
|---|---|
| [Harbor](https://github.com/harbor-framework/harbor)（Terminal-Bench 2.0 harness） | `litellm.acompletion`，chat 面，非流式 |
| codex 0.149.0 | responses 面，**只走流式**；`store: false`，每轮重发完整 `input`；`include: ["reasoning.encrypted_content"]` |
| mini-swe-agent | `litellm.responses()`，responses 面，**默认非流式** |

流式和非流式均受支持。携带加密内容、请求加密推理输出、使用
`previous_response_id` 或显式 `store:true` 的请求会固定绑定 endpoint。

但 `store: false` 的意思是「服务端别存」，**不等于「这个请求是自包含的」**。codex 拿回
去又发回来的加密 reasoning 就是跨轮状态，只不过由客户端背着走——而且**绑定在产出它的
那个 endpoint 上**。所以有一件事必须做：**会话亲和**。见下面「加密 reasoning 与会话亲和」。


---

## 部署

**一台机器只部署一份。** 负载均衡是靠一本账本实现的——代理记下自己往每个部署发了
多少，据此决定下一个请求去哪。两份代理各记各的账、花同一份配额，还都以为自己是唯一的，
那个账本就成了废纸。所以这不是「建议」，是它能工作的前提；`start.sh` 撞上已在运行的
实例会直接报错退出，就是为了守住这一条。

推论是:**装在一个所有人都能访问的路径下**，别装在某个人的家目录里。
`/srv/azure-proxy`、`/opt/azure-proxy` 或者一块共享盘上的目录都行——重点是所有需要
起停它的人都能进得去、写得动。整棵树是自足的（代码 + 虚拟环境 + 凭据），`mv` 走整个
目录就是一次完整的搬迁，没有任何指回原处的绝对路径。

```bash
git clone <repo> /srv/azure-proxy && cd /srv/azure-proxy

python3 -m venv .venv                          # 1. 环境
.venv/bin/pip install -r requirements.txt

./import-identity.sh sc-1234567@microsoft.com   # 2. 身份，见下
.venv/bin/python probe/probe.py                 # 3. 探测有哪些部署可用
./start.sh                                      # 4. 起
```

### 1. 环境

| | |
|---|---|
| Python | ≥ 3.9（3.9 和 3.11 上都跑过全量测试） |
| 位置 | `./.venv`，`.gitignore` 挡掉 |
| 依赖 | 见 `requirements.txt`，六个包，全都被代码 import |

还需要**系统装了 Azure CLI**（`az`）。它不是 pip 依赖：`azure-identity` 的
`AzureCliCredential` 是 shell 出去调 `az account get-access-token`。

不需要 `conda activate` 之类的东西——脚本都先 `cd` 到仓库根再用 `./.venv/bin/python`。
想用别的解释器就设 `AZURE_PROXY_PYTHON`。

### 2. 身份

代理不是用你的 Azure 账号跑的，而是用**部署被授权给的那个 principal**。它需要那个身份
的凭据放在自己的目录里（`.az-identity/`），不能用 `~/.azure`——你自己的 `az login`
不能把代理的身份换掉，反过来也一样。

如果这台机器上已经登录过那个账号：

```bash
./import-identity.sh sc-1234567@microsoft.com
```

它从 `~/.azure`（或 `$AZURE_CONFIG_DIR`）里**只把这一个账号的凭据**挑出来写进
`.az-identity/`，然后实测一次能不能签出 token。

**别用 `cp ~/.azure/*.json`。** 一份 `~/.azure` 装着你登录过的每一个账号的
refresh token——这台机器上的那份就同时有两个——整份拷过去等于把代理用不到的那个身份
也交了出去，而这个目录是所有运维者都能读的。`import-identity.sh` 存在的理由就是这个。

没登录过就先登录，注意走 `./az.sh` 而不是裸 `az`：

```bash
./az.sh login --use-device-code
```

细节和「到底最少需要哪两个文件」见 `.az-identity/README.md`。

### 3. 探测

`runtime/` 是探测的产物，不进版本库，所以新 clone 在跑过探测之前是起不来的——这是有意的:
那两个文件描述的是「某个订阅在某一刻有哪些部署」，是环境状态不是代码。

---

## 目录

```
azure-proxy/              # 自足：代码 + 虚拟环境 + 凭据，整个目录可以直接搬走
├── start.sh / tui.sh / stop.sh / restart.sh
├── preflight.sh         # 看板启动检查，供 tui.sh 使用
├── az.sh                # 代理专属的 az —— 运维一律走它，别用裸 az
├── import-identity.sh   # 从已有登录里挑出一个账号的凭据装进来
├── requirements.txt
├── .venv/               # 虚拟环境，gitignore
├── .az-identity/        # 运行身份的 az 配置目录，含 refresh token。gitignore
│   └── README.md            要放哪两个文件、为什么只需要两个
├── tools/
│   └── import_identity.py
├── settings/            # 人写
│   ├── endpoints.yaml       候选 endpoint（含订阅坐标）+ 探不到时的后备名单
│   └── policy.yaml          端口、超时、重试策略、凭据目录
├── runtime/             # 探测生成，勿手改
│   ├── sources.json         每个 endpoint 的状态与存活部署
│   ├── models.json          模型名 → 路由（按故障切换顺序），带每个部署的配额
│   ├── capacity.json        routing 导出的最大安全 RPM
│   ├── control.sqlite3      映射、原始观测与统计检查点（含 WAL 附属文件）
│   └── affinity.sqlite3     独立 48 小时 endpoint 绑定（含 WAL 附属文件）
├── probe/
│   └── probe.py
├── proxy/
│   ├── __main__.py          serving 前台入口
│   ├── supervisor.py / worker.py 监听所有权、预热、接流和排空
│   ├── affinity.py          原子绑定、持久化、续期和恢复
│   ├── server.py            转发和连接
│   ├── bridge.py            Target 协议、内存映射与后台观测同步
│   ├── config.py / state.py 共享配置和状态交换
│   ├── manage.py / process.py 进程管理
│   └── events.py            结构化事件定义
├── routing/
│   ├── __main__.py          routing 前台入口
│   ├── engine.py            重放、检查点、映射发布
│   └── quota.py             配额计算与选路
├── tui/                 # 看板。独立进程，只读，不管服务死活
│   ├── app.py               Live 循环、鼠标交互、看板切换
│   ├── boards.py            源 / 模型 / 事件流 三个看板
│   ├── bars.py              分段容量条
│   ├── layout.py            宽度感知的列分配与格子分配
│   ├── snapshot.py          把 JSON 归一成「按源」「按模型」两个视图
│   ├── client.py            轮询
│   └── theme.py             低饱和调色板 + 字符表
└── test/
    ├── fake_azure.py        可编程的假上游
    ├── run_tests.py         协议与配额回归
    ├── test_split.py        独立重启与恢复验收
    └── test_event_levels.py 等级筛选与超时分类
```

`settings/` 使用 YAML 保存人工配置。`runtime/` 使用 JSON 保存探测结果，
使用 SQLite 保存进程间交换状态和统计检查点。

---

## 运行

serving 由持有监听 socket 的 supervisor 和 worker 组成；routing 独立运行，看板保持独立只读。
两个服务进程使用同一系统账号和同一份本地运行目录；SQLite WAL 需要本地文件系统。

| 命令 | 行为 |
|---|---|
| `./start.sh` | 先启动 routing 并生成映射，再启动 serving |
| `./start.sh serving` / `./start.sh routing` | 启动指定进程 |
| `./restart.sh` | 默认只重启 routing，等待 serving 接受新映射 |
| `./restart.sh serving` / `./restart.sh all` | 预热新 worker 后滚动接流；all 先升级 routing |
| `./stop.sh routing` | 停止 routing，serving 继续使用缓存映射 |
| `./stop.sh` | 先排空并停止 serving，再停止 routing |
| `./tui.sh` | 打开看板 |

serving supervisor 使用 `.proxy.pid`，所有 worker 日志汇入 `proxy.log`；routing 使用 `.routing.pid` 和
`routing.log`。每个角色有独立的单实例锁。重复启动返回错误。
停止默认等待 30 秒；当请求仍在排空时，命令返回错误并保留进程。
可以通过 `--timeout 120` 延长等待；`--force` 明确允许超时后强制结束。

前台入口分别是 `./.venv/bin/python -m proxy` 和
`./.venv/bin/python -m routing`。serving 启动需要一份有效映射快照。

### 职责与更新

serving 持有 HTTP/SSE 连接、Azure token 和会话绑定，按内存中的映射执行转发。
routing 根据配额计算各接口、各模型的当前目标和备用顺序，生成完整上游 URL，
负责 RPM、容量学习和看板统计。serving 启动时只解析自身所需设置。

- 修改 routing 代码、`routing.balance`、`routing.balancing` 或重新探测部署后，执行
  `./restart.sh routing`。新映射由后台同步，已有连接继续使用原快照，会话保持原 endpoint。
- 修改 serving 的转发代码、监听地址、认证、请求兼容处理、超时重试或
  `routing.session_affinity` 设置后，需要安排 serving 重启。
- 当部署从新映射移除时，新会话停止分配到该部署；已有会话保存完整部署信息，并按原 TTL 排空。
  上游本身的故障仍按既有重试和亲和策略处理。
- 同一请求固定使用一代映射。routing 停止期间，已有会话和新请求都使用 serving 的最后有效映射。
- 当同名 endpoint 的资源地址或认证 scope 改变时，旧绑定返回身份冲突。绑定保持不变。

### 稳定的目标协议

目标记录字段：

| 字段 | serving 中的用途 |
|---|---|
| `endpoint`、`deployment` | endpoint 绑定、独立部署身份和请求 model 改写 |
| `selection_weight` | endpoint 内 deployment 的选择权重；serving 每请求抽样 |
| `scope` | 选择 Azure token；空值使用默认 scope |
| `targets` | 客户端接口路径到完整上游 URL 的映射 |
| `routing_data` | 原样保存的 JSON 对象，随观测的 `route` 字段回传给 routing |

四张模型路由表决定接口可用性与候选顺序。serving 校验目标身份、URL、scope 和
协议版本；routing 负责解释 `routing_data` 的内部结构。新增可选元数据和统计字段
可以随快照发布。报告通过 `/routes` 提供，现有健康字段从报告中取值；
当可选展示值缺失时，`/healthz` 对应字段返回 `null`。

映射快照版本为 2，数据库和统计 checkpoint 版本保持 1。配额观测携带
`retry-after` 和 `x-ratelimit-*` 响应头。访问凭据及请求、响应正文均不进入交换数据库。
重试、超时和会话生命周期参数继续在 serving 启动时生效。

### 状态交换与恢复

`runtime/control.sqlite3` 使用 SQLite WAL，保存原始观测、统计检查点、消费游标和版本化快照。
进程默认每 100ms 同步一次；serving 使用内存路由，每个有会话标识的请求同时核对独立绑定数据库。
空闲时 routing 每秒更新心跳，事件历史仅在发生变化时写入。
当 SQLite 暂时被写锁占用时，routing 保留消费进度并重试，锁释放后继续发布；
启动阶段也会重试锁冲突。其他数据库错误会明确报错退出。
原始观测包含时间、部署、状态、配额头和 token 用量，请求与响应正文不进入交换数据库。

routing 按事件发生时间恢复统计，完成追平后发布新映射。消费游标和检查点事务提交，
重复重放不会重复记账；每次发布最多清理 2,000 条已完成检查点覆盖的原始记录，
限制积压恢复期间的写锁占用。已学习容量继续导出到
`runtime/capacity.json`。当需要复制运行中的数据库时，应使用 SQLite backup API；
WAL 模式的数据库包含尚未合并到主文件的记录。

`/healthz.routing` 提供 routing 的 PID、实例 ID、心跳、已接受映射版本、消费水位和积压，
`/routes.stats_stale` 标明统计是否陈旧。当 routing 停止、快照不兼容或交换存储异常时，
serving 保持旧映射并显示告警。内存观测队列上限为 100,000 条；当存储故障使队列溢出时，
`telemetry_dropped` 记录统计缺口，转发继续运行。
内部协议采用版本检查，不兼容的进程或数据库升级需要维护窗口。

### 首次迁移与滚动重启

首次迁移安排在旧会话结束后。旧版内存绑定无法完整导出，首次切换后仍携带旧状态的
会话会收到绑定缺失错误。先执行 `./restart.sh routing` 升级权重和遥测协议，再排空
旧 serving 并 `./start.sh serving`。旧版服务运行时，`restart.sh serving` 会明确提示首次迁移要求。

新版 `restart.sh serving` 先恢复绑定和快照、并行预热各 scope 认证，通过独立就绪通道
确认后开启新接流并排空旧 worker。旧 SSE 持续到结束。预热失败时保留旧 worker，
命令返回失败；已有 worker 排空时拒绝第二次切换。修改监听地址或端口需要 stop/start。

`/healthz` 返回 supervisor PID、active/starting/draining worker、预热与切换耗时及绑定存储健康。
遥测启动和退出只清理对应 producer；会话数由共享数据库统计。日志诊断、清理和压缩
在启动关键路径之外运行。管理脚本启动的日志按小时轮换并保留 24 小时，遥测和事件
历史按 24 小时清理；独立的 `affinity.sqlite3` 保留最近活动 48 小时的绑定。
备份数据库请使用 SQLite backup API。回退版本必须继续保留固定 endpoint 和完整加密状态。

### 北京时间 01:00 定时重启

```bash
./.venv/bin/python tools/scheduled_restart.py            # 下一次北京时间 01:00，执行一次
./.venv/bin/python tools/scheduled_restart.py --dry-run  # 查看时刻
./.venv/bin/python tools/scheduled_restart.py --daily    # 每日执行
```

脚本先重启 routing，再滚动切换 serving，并验证健康接口。首次旧版迁移需在旧会话
结束后显式使用 `--legacy-migration`，等待在途请求排空后 stop/start；超时会报错。
脚本需由 tmux、at 或系统调度器持有。外部调度器到点调用时使用 `--now`。

测试全部使用临时配置和本地假上游：

```bash
./.venv/bin/python test/run_tests.py
./.venv/bin/python -m unittest discover -s test -p 'test_*.py'
```

监听 `127.0.0.1:8811`（在 `settings/policy.yaml` 改）。共享机器上这个端口段常有别人的
服务，换端口前先 `ss -lnt` 看一眼。

七个接口：

| | |
|---|---|
| `POST /v1/chat/completions` | OpenAI 协议，无需鉴权 |
| `POST /v1/responses` | Responses API，流式与非流式都支持 |
| `POST /v1/images/generations` | 生图，JSON。字段就是 OpenAI 那套 `model/prompt/n/size/quality` |
| `POST /v1/images/edits` | 改图，multipart。body 原样转发，只读出 `model` 用来选路由 |
| `GET /v1/models` | 可用模型 + 每个模型的路由链和支持的面 |
| `GET /healthz` | 存活、每个面各有几个模型、凭据目录、上次探测时间、当前 `balance` 模式和溢出阈值、监听地址、已运行时长 |
| `GET /routes` | 每条路由的当前 RPM、持久化的最大安全 RPM、限流时观测到的 `others` RPM、按五个「面」拆开的 RPM、当前权重、429 次数、降权状态、钉住的会话 |
| `GET /events` | 最近的结构化事件环，看板的数据源。`?since=<游标>&limit=&kind=`，`kind=problems` 只要出问题的那几类 |

**不实现** `GET/DELETE /v1/responses/{id}`、`/cancel`、`/input_items`。没有调用方
需要有状态会话，而且这些请求 body 里没有 `model`，无法按模型路由。

响应头带 `x-azure-proxy-route`，写明这次实际走的是哪个 endpoint 的哪个部署。
排查时先看它。

日志在 `proxy.log`：启动摘要（当前 az 账号、token 寿命、每个 endpoint 各个面的状态、
每个面各有几个模型）、请求完成记录（模型、是否流式、走的哪条路由、耗时）、
每次故障切换、每次 token 刷新。**请求体和响应体在任何级别都不记**——prompt 是用户
数据，`/events` 守同一条规矩。`settings/policy.yaml` 的 `server.log_level` 调级别，
`debug` 会增加请求进入和每次上游尝试的记录。

调用方式见 `../USING-AZURE-PROXY.md`。


---

## 看板（TUI）

```bash
./tui.sh                          # 看本机这个代理
./tui.sh --url http://host:8811   # 看别的机器上的
./tui.sh --scroll-lines 4         # 每次滚轮事件移动 4 行，默认 2 行
python -m tui --url ...                 # 同上，少了下面那几项检查
```

需要 `rich`，`requirements.txt` 里已经有了。

左上角显示 `azure-proxy`、访问地址，以及 `serving`、`routing`，用绿色、黄色、红色和灰色表示正常、
异常或未确认、离线和连接中。「proxy 状态」页集中显示监听地址、凭据与探测信息、
进程 PID、运行时间、routing 心跳、统计积压、管理进程、持久化状态和历史提示。
serving 状态来自 `/healthz` 是否可达；routing 心跳超过 3 秒为 `no heartbeat`，
正常停止为 `stopped`。当 serving 不可达时，routing 显示 `unknown`，PID 标为上次记录。
右上角显示接口取数年龄（`fetch`）与统计快照年龄（`stats`），统计陈旧时标记 `stale`。

**看板只读，不管服务的死活。** 它是独立进程，通过代理自己的 HTTP 面
（`/healthz` `/routes` `/events`）取数，所以随开随关都不碰服务；关掉它所在的终端
带走的只有看板。执行 `./restart.sh` 时 serving 持续可达；当 routing 重启造成
统计暂时陈旧时，看板显示进程状态与 `stats … stale`，恢复后自动刷新。
serving 离线时也可启动 `./tui.sh`。观察本机配置的地址时，看板通过只读数据库和
supervisor 状态文件独立读取 routing 的 PID、心跳和积压；未知值显示 `?`。
观察远程地址时，HTTP 不可达期间保留最后数据，并将 routing 状态显示为未知。

点击顶部的「源」「模型」「事件流」「proxy 状态」标签切换看板，鼠标滚轮逐行滚动。
顶部状态区与选项卡之间留一行空白。
输入触发即时刷新，连续滚动合并为最高每秒 60 帧；数据按轮询间隔自动更新。
事件行按内容和宽度缓存，滚动时只排版新进入视野的行。
滚动灵敏度可在 `settings/policy.yaml` 中设置，命令行 `--scroll-lines` 优先：

```yaml
tui:
  scroll_lines: 2
```

步长须为正整数，修改后重新打开 TUI 生效。

**源** —— 一个 endpoint 一张卡。这个资源上哪些部署在被用、当前 RPM 和最大安全 RPM。
一个 endpoint 是一份钱、一个爆炸半径，所以按它分组。
源卡片根据实际高度动态分栏，长卡片优先独占一列，短卡片在同一列上下组合。
模型页使用与源页一致的列数和卡片宽度。
卡片目标宽度为原布局的 1.5 倍，根据终端宽度减少列数并均分剩余空间。
摘要与首条明细之间、相邻明细之间、末条明细与下边框之间各留一行空白。
框内每行与左右边框各留两格空白。

**模型**：一个模型一张卡，按源展示当前用量。卡片摘要统一显示 `MAX RPM: n`，
数值单独着色并位于卡片右上角；源/deployment 数量、请求方式列表及版本日期隐藏。
卡片标题居中显示名称；标题下同一行显示 pinned 总数、隐藏旧模型数及右对齐的 `MAX RPM`。
条目的 pinned 数量使用独立的右对齐列，颜色比 RPM 更浅。
数量为零或缺失时，摘要和每条 deployment 明细均显示 `·` 占位。
完整的 `n pinned` 只在标题下第一行显示，明细列只显示数字或 `·`。
用量与条形同一行显示为 `n ───── n`：左侧为 ours，右侧为 others；最大 RPM 仍在最右列。
不另占说明行，也不显示“尚无完成样本”提示。
源卡片显示各模型的数量，模型卡片显示各源的数量。
同一源、模型的多个 deployment 共享 pinned 数量，每条明细均显示该数量；
卡片总数使用去重统计，按 deployment 重复展示的数字不相加。
明细按未过期 family × 模型去重，同一 family 使用多个模型时会分别计入。
明细需要升级 serving，已有会话随后续请求补齐；「proxy 状态」页提示缺少明细的情况。

### 模型卡片的顺序，以及什么被藏起来了

默认顺序是**先活跃度，再推出时间，再强弱**：有流量的置顶（这是个看板，在动的才是要看的），
其余按 `model_version` 从新到旧 —— 那是探测从 ARM 拿到的真实发布日期，不是从名字猜的。
同一天发布的（`gpt-5.6-luna` / `sol` / `terra` 都是 2026-07-09）再比强弱：
pro > codex-max > codex > 裸模型 > mini > nano。

「测到过上限」和「从没测过」**不参与排序**。两者的区别是真的、条形也画得出来，但那是
「代理被告知过什么」而非「有没有流量」，让它参与排序的结果是一个闲着的 `gpt-5.4` 
莫名其妙压在三个更新的模型上面。真有负载的仍然排在两者之前。

**旧模型闲着时默认不显示。** 保留最新的 3 个版本号（当前是 5.6 / 5.5 / 5.4），
`gpt-5.3` 及以前、`gpt-4` 系列、`o` 系列都收起来。**有流量就一定显示**，不管多老。

按**版本数量**而不是固定下限，是因为固定下限会烂：钉死在 5.4 就意味着 5.4 永远在屏幕上，
每出一个新模型都得回来改一次。数数是自己滑动的 —— 5.7 一上来，5.4 就从底下掉出去。

按**版本号**而不是发布日期，尽管日期又准又现成：日期挨得太近，切不开。
`gpt-5.3-codex` 是 2026-02-24，`gpt-5.4` 是 2026-03-05，隔九天 —— 要这么精确的一个窗口，
下一次发布就会让它失效。**日期负责排序，版本号负责判断新旧**，两个问题两个信号。

没有 `gpt-<数字>` 版本号的家族（`o3`、`o4-mini`）一律算旧。这是唯一诚实的答案 ——
名字里没有任何东西能说明 `o3` 相对 `gpt-5.4` 站在哪。代价是将来一个陌生家族闲着时也会被
收起来，所以**隐藏了几个永远写在页脚**，点击「显示旧模型」随时展开，标签页也显示 `9/20` 而不是 `20`。
看板可以少显示东西，但不能不吭声。

事件流按时间倒序展示，默认显示 INFO 及以上事件。点击「等级」循环切换最低等级：
DEBUG → INFO → WARNING → ERROR。点击「类型」独立切换：全部 → 问题 → 他人流量。
标签显示匹配数／总数；切换过滤时保留已收到的完整事件历史。
旧服务返回的等级会按同一规则归一化，打开更新后的 TUI 即可使用。

等级保留 `DEBUG / INFO / WARN / ERROR`。新版事件类型和说明显示为中文，
重试按目标区分为“原地重试”“换部署重试”“换源重试”；限流信号与降低权重分别显示。
“数据流结束，HTTP 200”表示传输结束，模型执行结果仍以完成或失败事件为准。

首次使用 `/events?initial=true&limit=400` 取最新历史；历史截断仅显示“已载入最近 400 条历史”。
之后使用 `initial=false&since=...` 增量读取，首次空流也算完成首次加载。
`stream_id` 在正常重启中保持稳定；流重建时更换 ID。旧快照不回退游标。
缓冲覆盖、返回限额和保留期清理造成的未读缺口分别计数。
TUI 显示“轮询期间漏读 N 条事件：原因”，30 秒后隐藏。在事件流页面有待确认提示时，
左下角显示「确认提示」按钮，点击可提前确认；累计漏读数保留。旧协议无法确定数量时标为未知。
本地过滤、滚动和已读缓存淘汰不计漏读；serving 遥测丢失单独展示。

| 等级 | 事件 |
|---|---|
| DEBUG | 请求进入 |
| INFO | 请求完成、容量学习、正常会话绑定、token 刷新 |
| WARNING | endpoint 限流、降权、故障切换 |
| ERROR | I/O 超时、流中断、上游失败、重试耗尽、凭据不可用 |

限流与超时分别统计。限流由 HTTP 429、限流响应头或流内 `rate_limit_exceeded` 表示；
超时由连接、读、写或连接池等待超时表示。连接超时为 15 秒，其余 I/O 超时采用
`routing.request_timeout_seconds`，该值按 I/O 阶段生效。
`stream_probe.hold_seconds` 到期只结束预读等待，后续继续转发。
超时事件记录异常类型、实际耗时与该阶段的超时配置，并以 ERROR 展示。

事件类型后方的宽列统一显示请求方向与状态信息，例如 `→ endpoint-b/gpt-5.4`、
当前路由及外部用量、降权时间。路由已包含部署信息；当尚无路由时显示请求的模型。
右侧保留事件说明。请求方向列最多占 52 格，额外宽度留给说明；滚动时列宽保持稳定。

### RPM 条怎么读

RPM（requests per minute）表示每分钟请求数。`/routes` 使用 `current_rpm`、
`capacity_rpm`、`other_rpm` 和 `rpm_by_face` 字段；统计窗口由
`routing.balancing.rpm_window_seconds` 配置。旧配置与历史容量文件可继续读取，
容量文件下次保存时使用新字段，原有每分钟数值保持不变。

```
gpt-5.4        ███▓▓▒▒▚····················░░░░   100.0 RPM
               current     available      others
```

条形的分母是这条路由历史上成功承载过的最大 RPM：

- **current**（鼠尾草绿）：当前一分钟滑动窗口内的请求数。
  五类请求统一使用原来的绿色，通过块状纹理区分：`█` chat、`▓` chat 流式、
  `▒` responses、`▚` responses 流式、`▞` 图像。每个字符占一格，图例采用相同画法。
- **░ others**（灰褐）：当当前 RPM 低于历史最大值却触发限流时，两者的差值。
  固定贴右侧；用量下降时左边界向右收缩，为 current 留出更多可用空间。
- **· available**（暗）：最大安全 RPM 中尚未被 current 和 others 占用的部分。

卡片摘要显示 `MAX RPM: n`，每行最右侧只显示最大 RPM 数值；尚无完成样本时显示 `—`。

**空条 `────` 加 `—` 表示还没有成功请求可用于建立安全 RPM。** 每个成功请求都会用它发出
时的一分钟窗口 RPM 更新最大值；最大值只增不减，由 routing 提交检查点后原子导出到 `runtime/capacity.json`。
对于 Responses 流式请求，收到完整的 `response.completed` 事件时立即记账。
当请求仍在生成、提前断开或被限流时，该请求尚不能提供成功样本，因此当前 RPM 可能暂时高于历史最大安全 RPM。

细到看不见的量会强行占一格。四次请求对 300k token 的上限是 1e-5，50 格的条上五个面
全部舍成 0、空白吃满整条 —— 那条条会说「这里什么都没发生」，而它恰恰是正在扛流量的
那条。这一格是从最大的那段（一般是 free）借的。

### 操作

| | |
|---|---|
| 点击标签 | 切看板 |
| 鼠标滚轮 | 按配置步长滚动，默认每次 2 行 |
| 左下角「显示／收起旧模型」 | 展开/收起闲置的旧模型 |
| 左下角「等级」 | 切换事件最低等级，默认 INFO+ |
| 左下角「类型」 | 切换事件类型：全部／问题／他人流量 |
| 左下角「确认提示」 | 仅在事件流有待确认提示时可用 |

界面操作通过点击标签、按钮和滚轮完成。数据自动刷新，Ctrl+C 退出看板。
底部页面按钮与图例共用一行，按钮靠左、图例靠右，下面留一行空白。图例省略 `current` 前缀。
其余键盘输入不触发切换、滚动或其他界面操作。

宽度感知：按最小卡宽算列数，余数逐列摊掉，所以无论几列右边缘都是齐的。终端窄到
放不下一张卡时降级成单列，先砍掉数字副行 —— 条和百分比是卡本身，副行是对它的注解。

数据 1 秒拉一次，画面 4 Hz 重绘。代理不可达时不清屏，继续画最后一份好数据并在页脚
标出它多旧了 —— 冻住的屏幕对「它还活着吗」是个比过期屏幕更糟的回答。

---

## 测试

```bash
.venv/bin/python test/run_tests.py         # 全部
.venv/bin/python test/run_tests.py failover  # 名字匹配的
```

测的是**代理自己的行为**，不是 Azure 的行为——故障切换、优先级、三种均衡模式、
按阈值溢出、参数透传、凭据注入、SSE 增量转发、流开始之后不再切换。全部跑在
`test/fake_azure.py` 提供的假上游上，所以不需要 Azure 登录、不花配额，而且能精确制造
真实服务不会按需产生的失败：429 风暴、5xx、挂死、主机不存在、流吐到一半断掉，
以及**「HTTP 200 + 流内限流事件」**——真 Azure 只在自己过载时才产生它。

每个用例起一个真实的代理进程，配一份临时的 `settings/` + `runtime/`，然后同时断言
**调用端看到了什么**和**上游收到了什么**。代理认两个环境变量来支持这件事：
`AZURE_PROXY_HOME` 换配置根目录，`AZURE_PROXY_STATIC_TOKEN` 绕过 `az`。

responses 那组用例发的是**实测抓到的 codex 0.148.0 请求体**，逐字段比对上游收到的
内容，所以任何一个"看起来无害"的字段过滤都会让测试红掉。


---

## 探测

**新 clone 的第一步。** `runtime/` 不进版本库（那是环境状态不是代码，签进去只会
慢慢和现实脱节），而代理启动时要读 `runtime/models.json`——所以没跑过探测之前
`./start.sh` 起不来。

```bash
P=.venv/bin/python
$P probe/probe.py                    # 全部
$P probe/probe.py --only endpoint-a   # 单个 endpoint
$P probe/probe.py --no-responses     # 跳过 responses 那一轮
$P probe/probe.py --no-images        # 跳过图像那一轮
$P probe/probe.py --no-arm           # 不查 ARM，退回猜名单
```

前置：用**代理自己的凭据**登录过（见下一节的 `./az.sh`）。取 token 直接调
`az account get-access-token`，不依赖 SDK；要两份，一份给数据面
（`cognitiveservices`），一份给 ARM（`management.azure.com`）。

**部署清单从 ARM 来，不是猜的。** 数据面确实没有发现接口，但管理面有，而且它回答了
数据面回答不了的三件事：

| | |
|---|---|
| 有哪些部署 | 不用再拿名字去撞 |
| 每个部署**实际服务哪个模型** | 这不等于部署名——`endpoint-b` 上叫 `gpt-4o-mini` 的部署跑的是 gpt-4.1-mini |
| 每个部署的配额 | `properties.rateLimits`，**和 `x-ratelimit-limit-*` 同单位**，所以能直接当冷启动先验 |

要查一个 endpoint 的订阅坐标（填进 `endpoints.yaml` 的 `subscription` /
`resource_group`）：

```bash
./az.sh graph query -q "resources
  | where type =~ 'microsoft.cognitiveservices/accounts' and name == 'endpoint-a'
  | project resourceGroup, subscriptionId"
```

**ARM 说存在，数据面说能不能用。** 两件事分开：principal 完全可能有 ARM 读权限而没有
任何 data action。ARM 列出的每个部署照样一个个探。ARM 查不到时（没填坐标、没权限、
网络不通）退回 `endpoints.yaml` 的 `deployments` 名单去猜，`sources.json` 里每个
endpoint 的 `discovery` 字段记录了这次走的是哪条路（`arm` / `list`）。

ARM 那一步就地排掉三类部署，省下请求：**Batch SKU**
（`gpt-4.1-batch`、`gpt-4o-data` 这类只走 batch API）、**还没建好或已停用的**
（`provisioningState != Succeeded`，swc 上那个退役的 `Dalle3` 就是这样被挡掉的，
它现在回 410）、以及**一个面都不占的**（embedding 部署）。

留下来的每个部署带着**该探哪些面**，来自 ARM 的 capability 标记。这个门只朝一个方向
关：图像部署不去探 chat，因为它对 `chat/completions` 的拒绝会被算进那个 **endpoint**
的 chat 状态里——一个资源上挂了一个看起来是死的部署，不等于这个资源死了。反过来，
带 chat 或 responses capability 的部署仍然两个面都探，和以前一样：只有 responses 面
的那批模型对 chat 回一个光秃秃的 400，capability 标记不足以信任。

`models.json` 按**真实模型名**归并。一个 endpoint 上同一模型有多个部署是常态——
换个 SKU 再买一份就是**第二份独立配额**——它们是同一个名字下的多条路由，不是冲突。
`endpoint-a` 的 gpt-5.5 就有两条：DataZoneStandard 5000 和 GlobalStandard 15000。

顺带确定每个部署要 `max_completion_tokens` 还是 `max_tokens`（GPT-5.x 与 o 系列要
前者，GPT-4.x 要后者）。判活时本来就要试，所以不额外花请求。探测请求的 token 上限给
到 256 而不是一两个：推理模型要先花掉预算做推理才吐第一个可见 token，上限太低会
报一个看起来像「部署不存在」的错。

然后是 **responses 面的第二轮**：每个 endpoint 先试 URL 形状
（先 `openai/v1/responses`，再 `openai/responses?api-version=…`），命中的那个记进
`sources.json` 的 `responses_path`；再对**每一个**部署各发一次。

这一轮**不以 chat 判活为前提**，因为有些模型只有 responses 面：`gpt-5-pro`、
`gpt-5.4-pro`、`gpt-5.1-codex`、`gpt-5.1-codex-max`、`gpt-5.3-codex` 对
`chat/completions` 一律回 `400 The requested operation is unsupported.`，同一个部署
在 `/responses` 上回 200（实测 2026-08-21）。一个部署**任意一个面能用就算能用**，
`faces` 记录它到底有哪些面。

最后是**图像那一轮**，只探 ARM 说带 `imageGenerations` 的那些部署。它故意发一个
**注定失败的请求**：`{"prompt": ""}`。生成一张 1024×1024 要花钱、要十几到二十几秒，
而空 prompt 会被同一个部署、出于同样的原因、在一秒内拒掉，而且是在产出任何一个像素
之前。回来的那个拒绝把该分的都分开了（2026-08-29 实测于 gpt4v-swc）：

| 回应 | 结论 |
|---|---|
| `400 empty_string` / `missing_required_parameter` | 部署在、有权限、确实服务 imageGenerations |
| `429 RateLimitReached` | **也算活的**，而且是更硬的证据：Azure 把这次调用记到了这个部署的账上。`gpt-image-2` 只有 2 RPM，多数时候就是这个回应 |
| `400 OperationNotSupported` | 这是个 chat 部署 |
| `404 DeploymentNotFound` | 不在 |
| `410` | 模型已退役（swc 上的 `Dalle3`） |
| `401` / `403` | 没有 data action |

`imageEdits` **不探**——改图请求必须带一张真实的图片。它取自 ARM 的 capability 标记，
记在路由的 `image_edits` 上，用来在调用端拿一个只会生成的模型去请求
`/v1/images/edits` 时给出 404 `no_image_edits_route`，而不是把 Azure 的困惑转发回去。

endpoint 级失败会被归类，而不是笼统的"不可用"：`dns_nxdomain`（资源已删除）、
`public_access_disabled`（需私有终结点）、`auth_denied`（principal 缺 data action）、
`no_known_deployments`（endpoint 正常但没有部署应答）。responses 面单独报
`responses_status`：`auth_denied`（缺 `responses/write` 这个**单独的**权限）、
`unsupported`（api-version 太老或该资源不提供）。

Azure 增删部署后重跑。`runtime/*.json` 头部有 `_generated_at`。


---

## 参数处理

**全部原样透传。** 代理只改写 `model` 字段，其余不动。

图像面连 `model` 都不改：Azure 从 URL 路径读部署名，body 里那个字段它不看。所以
`/v1/images/edits` 的 multipart 是**逐字节**转发的，见「图像面」一节。

如果某个部署拒绝某个参数，它的 400 原样回给调用端。代理不代为丢弃参数——那会让
`--temperature 0.7` 表面成功、实际按默认值运行，benchmark 数字失去可比性，而且
下游无从察觉。上游的 400 更难受但更诚实。

responses 面同理，而且更要紧：codex 发的 `include: ["reasoning.encrypted_content"]`
`reasoning.context` `prompt_cache_key` `client_metadata` `text.verbosity` 都是较新的
字段，白名单式过滤会看起来无害地把它们吃掉。它的工具还**不在顶层 `tools` 数组**里，
而是塞在 `input` 里一条 `type: "additional_tools"` 的 developer 消息中——按
chat/completions 那套结构去理解请求会踩空。回程也一样：SSE 是**字节级转发**，
代理不解析事件，所以加密的 reasoning 原样回吐，codex 靠它在无状态前提下跨轮保留
推理链。

已知会撞的一条：Harbor 同时发 `reasoning_effort` 和 `temperature`，而 GPT-5 系列
拒绝这个组合。用 GPT-5 系列时二选一。

另一条不是代理造成的，但会像 bug 一样表现：**推理模型的思考 token 计入
`max_completion_tokens`**。给 `gpt-5.5` 设 16，16 个全被推理吃掉，`content` 是空串、
`finish_reason` 是 `length`。给几百以上才有可见输出。

### body 的唯一例外：responses 面的两处改写

`request.responses_compat`（`settings/policy.yaml`，默认 `true`）。只作用于
`/v1/responses`，chat 面不受影响：

| # | 改写 | 起因 |
| --- | --- | --- |
| 1 | `input[*].tools[*].description` 和顶层 `tools[*].description` 的**空串**填成 `(no description)`，递归到嵌套的 `tools` | codex 把真工具包在一个 `type: "namespace"`、`name: "functions"` 的壳里，壳自己的 description 是空串。Azure 报 `empty_string`，真 OpenAI 接受 |
| 2 | 删掉 `input[*].internal_chat_message_metadata_passthrough` | codex 给每条 input 消息挂 `{turn_id, create_time}`。Azure 报 `unknown_parameter: ...create_time`。这是 codex 给有状态会话存储用的簿记，本代理**没有会话状态**，它没有可以 passthrough 的去处 |

两处都只动结构，不碰 prompt、采样参数和工具行为，所以跑分仍可比。启动时日志里会写
`responses compat rewrites: on`，`/healthz` 里也有 `responses_compat` 字段——真出了
解释不了的结果，这行是提醒你「代理确实碰过 body」的唯一线索。关掉就能看到 Azure 的
原话，当初这两条就是这么定位出来的。

**2026-08-20 复测：这两个 400 在 `endpoint-a` / `2025-04-01-preview` 上已经复现不出来了。**
用空 description（顶层 `tools`、嵌套子工具、`input[*].tools` 三种位置）和带
`create_time` 的 `internal_chat_message_metadata_passthrough` 各发一遍，
`responses_compat` 关着也全部 200。原始那次是 2026-08-19 用抓包录下的 codex 真实请求
逐字段定位的，那份 capture 没有留存，所以无法逐字重放。可能是 Azure 改了校验，也
可能触发条件还依赖请求里别的部分。

结论：**改写留着，但别把它当成 codex 已经跑得通的证据。** 它已通过单元测试、且对常规
流量零影响（全量回归全过）；codex 到底还卡不卡，只有真跑一次 codex 才知道。真跑之后
如果发现根本不需要，把 `responses_compat` 设 `false` 就退回纯透传。

---

## 图像面（`/v1/images/*`）

两个接口，OpenAI SDK 直接能用：

```bash
curl -s http://127.0.0.1:8811/v1/images/generations \
  -H 'Content-Type: application/json' \
  -d '{"model":"gpt-image-1.5","prompt":"a red circle","size":"1024x1024","quality":"low"}'

curl -s http://127.0.0.1:8811/v1/images/edits \
  -F model=gpt-image-1.5 -F 'prompt=turn it blue' -F image=@in.png
```

swc 上有 `gpt-image-1.5`（30 RPM）和 `gpt-image-2`（2 RPM），`gpt-image-2` 在
`yifanyang-foundry-img-polandcentral` 上还有一个部署（同样 2 RPM，合计 4 RPM），
scus 和 eastus2 上一个都没有。所以图像面的路由表和 chat 的不是一套，**不能假定同样的
endpoint 集合**。`GET /v1/models` 的 `faces` 里能看到 `image` 和 `image_edits`。

`yifanyang-foundry-img-polandcentral` 只有图像部署，没有 chat 部署，所以探测把它的
整体 `status` 记成 `unreachable`（chat 面一个都没答）而 `image_status` 是 `ok`。
路由表按面建，图像路由照常注册。

### 部署名在 URL 路径里，所以 multipart 原样转发

Azure 的 `images/{generations,edits}` 从 **URL 路径**读部署名，body 里的 `model`
和它不一致时以路径为准（2026-08-29 实测：向 `gpt-image-1.5` 的路径发一个写着
`gpt-image-2` 的 body，返回 200，出图的是 `gpt-image-1.5`）。

这件事决定了 `/v1/images/edits` 怎么写。改图的 body 是几 MB 的 multipart，代理需要
从里面拿到的只有一个东西：`model` 的值，用来决定发去哪个部署。所以 body 被**扫描**
而不是解析——按 RFC 7578 固定的分隔结构定位那一个小文本字段，读完就停，然后整个
body **逐字节**转发，连客户端自己选的 boundary 一起。这样既不用为读一个字符串引入
`python-multipart`（`requirements.txt` 至今只有六个包，每个都真的被 import），也
不用把几 MB 的 PNG 解出来再编回去，去改一个 Azure 反正会忽略的字段。

### 429 是常态，所以重试规则不一样

图像配额按**每分钟请求数**给，而且给得很紧：`gpt-image-2` 每个部署 2 RPM。单次调用要
10-25 秒。这两件事凑在一起，429 就是一个忙碌分钟的正常形状，不是出事的信号。

其余的面每条路由最多试一次，试过就走，因为在旁边还有三个 endpoint 待命时回头再敲
刚拒绝过你的那个是浪费。图像模型的部署很少，那条规则会把第一个 429 直接
交给调用端——而调用端往往不重试（OpenAI SDK 调图像接口时 `maxRetries: 0` 很常见），
一个 429 就等于一页幻灯片失败。

所以图像面的尝试列表是**在路由里循环**的，等待用 Azure 自己的 `Retry-After` 而不是
盲目指数退避——每分钟的额度什么时候回来，Azure 知道，代理不知道。两个旋钮在
`settings/policy.yaml` 的 `routing.image` 下：

```yaml
routing:
  image:
    max_attempts: 3        # 尝试次数，不是路由数
    max_wait_seconds: 60   # Retry-After 的上限，防止永不恢复的部署把调用端挂住
```

`gpt-image-2` 现在有两条路由，循环会在 swc 和 polandcentral 之间来回：

```
!! 429 from gpt4v-swc/gpt-image-2, waiting for yifanyang-foundry-img-polandcentral/gpt-image-2 in 4.0s
!! 429 from yifanyang-foundry-img-polandcentral/gpt-image-2, waiting for gpt4v-swc/gpt-image-2 in 19.0s
```

2026-08-30 实测（4 并发，1024x1024，quality=low）：`max_wait_seconds` 是 30 时 1/4
成功，抬到 60 之后 3/4 成功。原因在 `Retry-After` 的量级：两条路由都满的时候 Azure
给的是 19-34 秒，30 的上限会把等待截断，把一个再等一会儿就能成的请求变成 429。
剩下那个失败的是三次尝试用尽（62.5 秒，第三次拿到 60 秒的 `Retry-After`，正好顶到
上限且没有尝试次数了）。2 并发稳定 200，**这是 `gpt-image-2` 的实用并发上限**。

同期日志里还有 `throttled at our load 0.50: estimating 50% foreign load`——swc 上的
`gpt-image-2` 有代理之外的人在用，实际能拿到的比账面 2 RPM 少。

`load_window_seconds` 默认 60 秒对图像是**对的**，不需要按面另配：账本记的是
**派发时刻**，天花板是每分钟请求数，两边同单位。单次调用耗时长不影响这个比值——
要紧的是派发速率，不是每个请求在飞多久。

**代理不会在 `gpt-image-1.5` 和 `gpt-image-2` 之间轮转**：路由表按模型名建，这是
两个不同的模型，不是同一个模型的两条路由。要在两者之间溢出，得由调用端自己决定
先要哪个。

### Responses 面自己画图（`tools: [{"type": "image_generation"}]`）

这条路以前能走通但要调用端自己填一个 header：Azure 不会替你挑图像部署，缺了就报

```
imagegen deployment must be provided through header:
x-ms-oai-image-generation-deployment
```

而部署名只在**它所在的那个 endpoint** 上有效——调用端既不知道均衡器把这一轮发去了
哪个资源，也不该知道那上面部署了什么。现在代理自己填：带 `image_generation` 工具的
一轮会先被**收窄到有图像部署的 endpoint**，再逐次尝试地补上 header。调用端已经自己
设了这个 header 的，不覆盖。

哪个 endpoint 用哪个部署，`GET /routes` 的 `image_deployments` 里写着；同一资源上有
多个时取配额最大的那个。

---

## 路由与故障切换

`settings/endpoints.yaml` 里 **candidates 的先后顺序就是优先级**——排在最前的
endpoint 最先被尝试。探测把这个顺序固化成每条路由的 `priority` 字段，服务端按它
排序，不依赖 JSON 数组顺序在读写间不变。要改优先级，调整 `endpoints.yaml` 的条目
顺序再重跑探测。

其余规则见 `settings/policy.yaml`：

- 切换只在 **429 / 5xx / 连接错误 / 流内限流** 时触发，退避后换下一个 endpoint，同一个不重试
- **超时不触发切换**。推理模型的慢请求和挂死在客户端看起来一样，超时重试会为同一个
  prompt 付两次钱，而且第一个请求可能还在跑。超时返回 504，由调用端决定
- **4xx 直接透传**。参数原样转发，所以一个关于不支持参数的 400 就是真实且有用的结果
- **流式请求一旦给调用方发出过字节就不再切换**。中途断流只能让它断，`proxy.log` 里记
  一行说明为什么没重试

### 200 里面藏着的 429

Azure 限流一个**流式** Responses 请求时，不给 429，给 200。实测抓包（2026-08-20，
把 endpoint-a 压过 TPM 上限，8 个并发里 5 个被拒）：

```
HTTP/1.1 200 OK
retry-after: 4
x-ratelimit-remaining-tokens: -27689        <- 负数

event: response.created   {"status":"in_progress", ...}
event: error              {"code":"rate_limit_exceeded","message":"Your requests to
                           gpt-5.6-sol for gpt-5.6-sol in swedencentral have exceeded
                           rate limit."}
event: response.failed    {"status":"failed", ...}
```

**判据在 header 里。** `retry-after` 在 5 个被拒的响应上全都有、在 3 个成功的响应上
一个都没有。所以代理就按它判——和读 429 状态行是同一个时刻，**在任何 body 字节之前，
零延迟，而且和流有多大完全无关。**

最后半句是这件事的全部教训。第一版是去**扫 body 的前 16KB**，单元测试全绿，上线一个
都没抓到：codex 形状的 `response.created` 会把整个请求（`instructions` + 工具定义）
原样回显，它自己就比扫描窗口大——实测限流响应恒定 61566 字节，`rate_limit_exceeded`
这个字符串**落在第 30667 字节**，是 16KB 窗口的 1.9 倍开外。于是一次跑分里
**184 个限流被记成了 63464 字节的「成功」**，codex 那边每一个都是
`stream disconnected before completion`。字符串从来没错，错的是「限流一定出现在开头」
这个假设。

**负的 `x-ratelimit-remaining-tokens` 故意不作为判据**：同一次抓包里有一个成功跑完的
响应报的是 -27978。这个计数器只要窗口超订就会变负，`retry-after` 才是 Azure 真的在
拒绝这一个请求。

body 扫描作为**兜底**保留（万一哪天 Azure 不带 header 就发限流），但现在扫**整条流**
而不是一个窗口——限制它正是当初失效的原因。每个 chunk 一次 `bytes.find`，相对它本来
就在做的 HTTP 而言不值一提。

`routing.stream_probe` 仍然在：上游回 200 之后先把流的开头读进内存，一个字节都不往下
发，读到可重试的错误就换 endpoint 重来，读到第一个非前奏事件就原样放行。现在它是兜底
而不是主判据。首字节延迟实测无代价：对着真 Azure A/B 各 8 次，开着是中位数 0.89s、
关掉是 1.18s，差异在 Azure 自己的抖动里。

**窗口之后到达的流内限流仍然只计数、不重试。** 这时调用方已经在解析这条流了，拼第二
条流进去是这个代理绝对不能做的事。它照原样转发，但那条路由会被降权。

字节级转发没有被破坏：检测只 `bytes.find`，转发的是**原封不动的同一批 chunk**。
codex 的 `include: ["reasoning.encrypted_content"]` 照常工作。

### 诊断开关：`server.capture_dir`

**默认关闭，也应该一直关着**——它把上游原始流写到磁盘，里面有 prompt。

它存在是因为上面那个 bug 从日志里被误判了两次，而把真实字节落到磁盘上五分钟就定了案。
用法：打开 → 复现 → 读 → 关掉 → 删文件。`capture_mode: suspicious` 只留没跑到
`response.completed` 的流（既是有意思的那批、也是小的那批），`all` 用于「healthy 的
响应到底长什么样」这类问题，`capture_limit` 防止忘了关。

---

## 主动负载均衡（`routing.balance`）

三种模式，默认 `priority_threshold`。routing 计算目标顺序，serving 执行该顺序。
同一个同步周期内的新请求可能使用同一目标；分流结果随 routing 的观测和发布更新。
已有绑定的会话继续使用其部署。

| 模式 | 行为 |
| --- | --- |
| `strict_priority` | 严格优先级。优先级 1 吃下 100% 流量，只有它出错才往下走。这是 kill switch |
| `priority_threshold` | **默认。** 按优先级走，但一条路由用掉自己配额的 `spill_threshold`（默认 70%）之后就跳过它，发给下一条 |
| `capacity` | routing 按实测配额抽样计算当前目标和备用顺序 |

`priority` / `weighted` 是前两个和最后一个的旧名字，仍然有效。

**为什么默认是 `priority_threshold` 而不是 `capacity`：** 三个 endpoint 不是等价的。
`endpoint-c` 的 api_version 是 `2024-12-01-preview`，另外两个是
`2025-04-01-preview`；三个还在不同 region。优先级顺序是一个「谁更好」的判断，在不花钱
的时候值得保留。它唯一不该做的事是**攥着自己送不出去的流量**——这正是阈值修掉的。

### 「70% 负载」是怎么算的

- **相对该路由自己的配额**，不是绝对值。gpt-5.6-sol 三条路由的实测上限是
  333k / 1M / 499k TPM，共用一个绝对数就等于「按最小的那个来」
- **RPM 和 TPM 都算，取大的那个**——先撞到的那个才是会产生 429 的那个。实测通常是 TPM：
  跑分时 TPM 用到 ~27% 而 RPM 只有 ~8%，TPM 早约 3 倍撞墙
- 30% 的余量留给两件看不见的事：Azure 实际是按远短于一分钟的窗口准入的，突发可以
  超过分钟均值而均值看不出来；账本只数代理自己的流量，别人也在用同一份配额时它是瞎的

### 「当前负载」用的是代理自己的账本，不是 `x-ratelimit-remaining-*`

这是实测之后的选择，不是偏好。**先验证过 `remaining` 的语义，结论是它不能用**
（2026-08-20，对着闲置的 endpoint-b/gpt-4.1-mini，2000 RPM / 2M TPM）：

- 16 个并发请求 = 瞬时约 960 RPM，是上限的 **48%**，而
  `x-ratelimit-remaining-requests` 只从 2000 掉到 **1989**——按 60 秒窗口算应该掉到
  1984 并且待在那儿
- **3 秒后它就回到 1999** 并保持了后续 35 秒的全部采样。没有任何东西按分钟衰减
- `remaining-tokens` 一样，而且扣的是**实际用量而不是 `max_completion_tokens` 预留**：
  16 个请求每个预留 4000 token，总共只让它动了 27
- 同一个突发内部各个响应互相矛盾（1999、1996、1995、1990、1989…），连快照都算不上
- `x-ratelimit-reset-*` 每一次采样都是 `0`，没有信息

也就是说，尽管 `renewalperiod` 写着 60，**`remaining` 是一个亚秒级的桶，除以的却是每分钟
的上限**：真实分钟负载 ~48% 的时刻它报告消耗了 0.55%，差了将近两个数量级。任何 70%
的阈值挂在它上面都永远不会触发。

所以负载来自**代理自己的账本**：每发出一个请求就按 (endpoint, deployment) 记一条
`[时间戳, token 数]`，负载 = 窗口内还没过期的部分 ÷ 实测上限。取舍是明确的：

- 对自己的流量**精确**，不需要等响应回来就能更新——这很重要，并发 30、推理一轮一分钟
  的时候，一个只在完成时更新的信号会滞后整整一个请求
- **没有流量 = 0 负载**，不是「未知」。安静的路由会被优先选中而不是被饿死
- **看不见别人**。这一点没法从这里修；兜底的是反应式的那一半——429（含流内 429）降权

token 数在**发出时**按估计计入（Azure 也是在准入时计费的），估计值 = 请求体字节数 ÷
`assumed_chars_per_token`；任何一个报了 `usage.total_tokens` 的响应都会回头修正这条
账目、并更新该路由的「每字节多少 token」比率。这是推理模型那些**根本没出现在请求里**
的思考 token 唯一能被算进来的途径。流式响应也一样——`usage` 是从流过去的字节里
`bytes.find` 出来的，不解析、不重新序列化。

窗口 60 秒，因为这里每个 deployment 报的 `renewalperiod` 都是 60，账本和被除的上限
必须是同一个周期。

### 怎么看

```bash
curl -s http://127.0.0.1:8811/routes | python -m json.tool
```

`sent_requests_in_window` / `sent_tokens_in_window`（代理的账本）就摆在
`remaining_requests` / `remaining_tokens`（Azure 的账本）旁边——**两者对不上的时候，
就是有别人在花同一个 deployment 的配额。**

配额上限不用单独去测：`limit_tokens` / `limit_requests` 就是代理从正在跑的流量里读到的
实测值。

### 为什么 n=4 就能把 swc 打限流

不是并发数的问题，是**每个请求太大**。codex 每一轮都把整份 transcript 重发一遍，所以
TPM ≈ 单请求 token 数 × 轮次频率 × 并发，和 n 只是线性关系里的一项。

实测（2026-08-20，`/routes` 账本，codex 形状的请求）：

| | |
| --- | --- |
| endpoint-a 的 gpt-5.6-sol TPM 上限 | 333,000 |
| 单个 codex 请求的实测 token 数 | ~50,000（账本学到 0.2275 token/字节） |
| **一个请求就占上限的** | **~15%** |
| 5 个请求之后账本读数 | 263,316 = **79%**，已越过 70% 阈值并开始溢出 |

也就是说 **swc 的整个配额只装得下 6~7 个 codex 请求**。阈值判断发生在发出之前，而 4~8
个请求会在任何一个返回之前全部发出去，所以账本可能从 56% 一跳到 79%——**颗粒度就是
15%，想卡准 70% 是不可能的。**

账本本身是准的：限流那一刻 Azure 报 `remaining-tokens: -27689`（即它数到约 360k），
账本数到 263k / 5 个请求，量级和单价都对得上，误差大约 15%。所以 70% 这个阈值有意义，
**但对 gpt-5.6-sol + codex 这种「单请求占 15% 上限」的负载，它只能减少限流、不能消灭
限流**——限流照样会发生，只是现在会被透明地切走，调用方看不到。

**建议：gpt-5.6-sol 跑 codex 且 n≥4 时用 `balance: capacity`。** 三条路由的容量是
333k / 1M / 499k，`capacity` 从第一个请求就按 18% / 55% / 27% 分，谁都不会贴着自己的
天花板跑；`priority_threshold` 则会让最小的那条（swc，只占总量 18%）长期停在 79% 并
反复触发限流——虽然每次都被透明处理掉，但每次都要多付一个往返加一次退避。

`gpt-5.5` 有 5 条路由（swc 和 scus 各有两个部署）；`gpt-5.4` `gpt-5.4-mini`
`gpt-5.4-nano` `gpt-5.6-sol` `gpt-5.6-luna` `gpt-5.6-terra` `o3` 有 3 条；
`gpt-4.1-mini` `gpt-4o` 有 3 条、`gpt-4.1` `gpt-5.1` `o4-mini` `gpt-5.4-pro` 有 2 条；
`gpt-5.2` `gpt-5-mini` `gpt-5-pro` `gpt-5.1-codex` `gpt-5.1-codex-max`
`gpt-5.3-codex` 只有 1 条，**没有切换余地**，429 时只能把错误回给调用端。
长时间 benchmark 优先选前两组。

**`gpt-5-pro` `gpt-5.4-pro` `gpt-5.1-codex` `gpt-5.1-codex-max` `gpt-5.3-codex`
只有 responses 面**——它们对 `chat/completions` 一律回
`400 The requested operation is unsupported.`，走 `/v1/chat/completions` 会拿到
404 `no_chat_route`（错误信息里会说清它在哪个面上）。

**`gpt-image-1.5` `gpt-image-2` 只有图像面**，两个都支持 generations 和 edits。
`gpt-image-1.5` 1 条路由在 swc；`gpt-image-2` 2 条，swc 和
`yifanyang-foundry-img-polandcentral`。它们没有任何文本面，走文本接口同样拿
404 `no_chat_route` /
`no_responses_route`；反过来拿一个文本模型去 `/v1/images/*` 拿 404 `no_image_route`。

以 `GET /v1/models` 为准，上面这份是 2026-08-29 探测的快照。

一个模型在各个面上的路由数**可能不同，也可能只有一个面**。`GET /v1/models` 的
`faces` 字段是准的，`runtime/models.json` 里每条路由的 `faces` 是它的来源。

### 权重是怎么来的（`capacity` 模式，以及后备链的排序）

权重来自 Azure 每个响应都带的 `x-ratelimit-limit-tokens`（没有就退回
`-limit-requests`）。没被观测过的路由用先验，而**先验现在几乎不是猜的**：探测已经从
ARM 拿到了每个部署的 `rateLimits`，和响应头同单位，写在 `runtime/models.json` 的
`capacity_requests` / `capacity_tokens` 里——进程刚起来就知道每条路由的真实上限，
第一个响应只是确认它。`GET /routes` 把两者并排列出：`capacity_*` 是 ARM 声明的，
`limit_*` 是流量实测的，对不上就说明配额在上次探测之后动过。

`policy.yaml` 的 `static_weights` 退成**最后的兜底**，只在 ARM 给不出容量时才用到
（endpoint 没填订阅坐标、没有 ARM 读权限、或者 `runtime/` 是旧的）。它天生更差，
不是因为数字旧：**配额是按部署给的**，同一个 endpoint 的不同部署根本不共享一个
天花板——`endpoint-b` 对 gpt-5.6-sol 是 1000 RPM，对 gpt-4.1-mini 是 2000。
一个 endpoint 一个数字，不可能对两者都成立。

走到兜底那条路时，**先验会先被换算成实测的单位**再参与比较：Azure 报的是几十万的
TPM，配置里写的是几百的 RPM，直接放在一起比，第一个应答的路由会拿到 100000 的权重、
其余的还停在 1，从此再也抽不到——那就是「优先级路由多绕一圈」而已。换算的汇率取自
那些两个数都已知的路由。ARM 来的先验本来就同单位，这个汇率对它们退化成 1。

后备链的排序也用同一份容量：**endpoint 优先级在前，容量只在同一个 endpoint 内部
打破平手**。`endpoint-a` 上 gpt-5.5 有 5000 和 15000 两个部署，没有理由先去够小的
那个。

另外两个信号：

- `x-ratelimit-remaining-*` 只在**掉到上限的一半以下**时才降权，线性降到地板。按上面
  实测的语义，这一条基本永远不会触发；留着是因为它不花钱，而且真触发的那一次它报的
  是一个真实的瞬时突发。**别把它当成负载信号**，负载信号是账本
- **429 会临时降权**（乘 0.25 并按 `Retry-After` 停发），不只是这一次请求绕开它。
  停发窗口结束后按半衰期指数恢复。5xx、连接错误和流内限流走同一条路径。停发中的路由
  在 `priority_threshold` 里也不会被选作链头——账本只知道自己的流量，`Retry-After`
  说的是所有人的

抽样而不是「谁剩得多发给谁」：后者在并发下所有在飞的请求会算出同一个答案、一起压向
同一个 endpoint，而修正要等响应回来才发生。抽样没有这个反馈延迟，也不需要记在途请求。

---

## 别人也在用同一份配额（`balancing.foreign_load`）

上面那本账只数**自己**发出去的量。但配额是按 deployment 给的，跟所有拿得到凭据的人共享
——光这台机器上，另一个账号底下就有 8 个代理指着同一批 endpoint，其中 2 个的部署列表
读不到（不是同一个账号，没权限），无法确认是不是在抢同一个部署。

**只看自己的账本，就会一直撞一堵从这里看不见的墙。**

### 唯一能看见别人的时刻

就是 Azure 拒绝我们的那一刻。限流意味着**总量**顶到了天花板，而我们自己那一份是已知的：

```
foreign = clamp(1 - our_load_at_throttle, 0, 1)
```

我们负载 0.9 时被限流 → 基本是自己撑满的。我们负载 0.05 时被限流 → **别人占了 95%**，
我们只是最后到的那个请求。后者正是重点。

两个模式都改成看 `our_load + foreign_load`：`priority_threshold` 按合计值溢出，
`capacity` 按 `limit × (1 - total_load)`（**实际剩下的**）抽样，而不是按名义上限。

实测验证（2026-08-21，直连 Azure 打满 swc 扮演「别人」，同时让代理只发几个请求）：

```
endpoint-a/gpt-5.6-sol                  our=0.003  foreign=0.997  total=1.0   n=1
endpoint-b/gpt-5.6-sol                 our=0.002  foreign=0.0    total=0.002
endpoint-c/gpt-5.6-sol  our=0.008  foreign=0.0    total=0.008
```

代理自己只用了 0.3%，却正确判定这个 deployment 是满的，而且没有污染另外两条路由。
`/routes` 里长期观测 `foreign_load` 也就是**验证「到底有没有人在抢 gpt-5.6-sol」的手段**。

### 合成探测没有用

「主动发个测试请求看看」是个自然的想法，但它不成立：限流是按**聚合量**触发的，别人占
90% 还是 0%，你单发一个小请求都会成功。要能分辨就得发到足以撞墙的量——那既花配额又
干扰被测对象。**唯一有信息量的探针就是真实流量。** 这跟 TCP 是同一个问题：可用容量只能
靠用它来发现。

### 控制律是 AIMD

- **乘性减**（我们的份额）：被限流时估计值**直接跳到观测值**，而且只升不降
  （跟当前值取 `max`）。因为我们自己的负载会波动——某次限流恰好发生在我们负载很高的
  时候，不该抹掉之前那次「别人占了 0.8」的发现，否则接下来几分钟都在重新学同一件事
- **加性增**（我们的份额）：两次限流之间，`foreign` 按 `reclaim_per_minute: 0.1`
  **线性**回收，10 分钟忘干净一个 100% 的估计

**为什么线性而不是指数半衰期**：指数在刚被限流后的那一分钟里还得最快——正是最该谨慎的
时候；之后又拖尾很久——那时估计已经陈旧、最不值钱。两头都是反的。线性是恒定的温和收回。

而且 AIMD 是唯一能让**互相看不见的多个控制器**收敛到公平分配的组合（AIAD、MIMD 都不
行）。这台机器上另外几个代理很可能也在做自适应，所以这条不是理论洁癖。

**回收要等 `Retry-After` 过去再开始**——Azure 直接说了这次拥塞还要多久，没必要自己猜。

实测回收曲线（`/routes` 每分钟采样一次）：

```
foreign=0.989 → 0.889 → 0.789 → 0.689 → 0.589      每分钟正好 -0.100
```

### 闭环靠 `weight_floor` 收敛

一条被判定为满载的路由**仍然保底拿到 `weight_floor`（5%）的流量**。那些请求就是探针
——是真实流量，零额外成本，别人真走了它们就会成功，估计值随之回收、容量拿回来。

**注意 floor 是对所有削减因子的乘积生效，不是对每一项分别生效。** 之前是后者：一条
又满载、又停发中、又低 headroom 的路由会被 floor 三次，落到 `0.05³` ≈ 万分之一，
那不叫探针那叫饿死。

### 两个取舍

**闲置也照样回收**（不冻结）。估计描述的是**外面的世界**，而世界不会因为我们没在看就
停住。冻结还有个陷阱：一条我们不再使用的路由会**正因为我们不再使用它**而永远被压着。
代价是恢复流量的第一批请求可能撞一次墙——那是一次透明的故障切换，而且它本身就是信息，
撞完估计值立刻回到正确值。

**震荡周期**：环路是「撞墙 → 估计跳高 → 少发 → 不再撞 → 线性回收 → 多发 → 撞墙」，
周期 =（估计值要降多少我们才会重新顶到 1.0）/ 回收速率。稳定外部负载 0.8 时，我们大约
多要回 0.2 就会重新撞上，**周期约 2 分钟，代价是一个被透明切走的请求**；最坏情况
（估计 1.0 全部回收完）是 10 分钟。两者都比 `demote_seconds`（30s）和 `Retry-After`
（实测 1–8s）高一个数量级——快控制器吸收突发、慢控制器定基线，这个分离是它们不打架的
前提。`reclaim_per_minute` 调到 0.2 以上就会开始破坏这个分离。

### 和 demote 的关系：叠加，不是替代

两者时间尺度差一个数量级，管的也不是同一件事：

| | 管什么 | 尺度 | 对 5xx/连接错误 |
| --- | --- | --- | --- |
| `demote` + parking | 「现在别打这里」的躲避动作 | 秒 | **也生效** |
| `foreign_load` | 「这条路本来就没那么大」的稳态估计 | 分钟 | 不生效 |

一条被限流的路由**既该马上躲开（parking）、又该从此被当成更小（foreign）**，所以是相乘
叠加。只有 429 会更新 `foreign_load`——500 或连接重置说明 endpoint 病了，跟配额被谁占
没有关系，拿它去推断外部负载会让一条只是短暂故障的路由被永久缩小。

---

## 加密 reasoning 与 endpoint 绑定（`routing.session_affinity`）

需要状态的请求在首次派发前持久化 family→endpoint 绑定。触发条件包括携带
`encrypted_content`、请求 `reasoning.encrypted_content` 输出、引用
`previous_response_id`、显式设置 `store:true`。已有绑定时，后续请求均继承它。

会话标识按 `session-id` header、`prompt_cache_key`、
`client_metadata.session_id`、`x-session-id` header 的优先级选取，去除首尾空白后取摘要。
不同载体中相同的值表示同一 family；线程、模型和 deployment 均不参与绑定键。
主会话与 subagent 共享 endpoint，可选择其中不同模型和 deployment。
routing 发布 deployment 权重，serving 逐请求抽样并在该 endpoint 内重试。

Codex 的 `/btw`、`/side` 和 `/fork` 会生成新的会话标识。serving 从
`x-codex-turn-metadata` header，或请求体
`client_metadata["x-codex-turn-metadata"]` 的 JSON 字符串中读取
`forked_from_thread_id`，原子地复制父会话的有效 endpoint 绑定和路由描述。
每个分支独立续期，后续请求和重启均可复用新绑定。该逻辑适用于所有模型别名和
endpoint 名称；目标模型需要在父会话绑定的 endpoint 上可用。
如果分支尚无绑定且父绑定缺失或到期，携带旧状态的请求仍返回 `affinity_missing`。
通过 copilot-api 等中间网关调用时，请保留上述 header 或请求体中的元数据副本。

```yaml
routing:
  session_affinity:
    ttl_seconds: 172800
    active_window_seconds: 300
    wait_attempts: 4
    max_wait_seconds: 30
```

`active_window_seconds` 控制看板统计的活跃窗口，默认 300 秒，须为正有限数。
窗口内有请求的绑定会话、以及仍有请求在途的会话计入总数、源和模型统计。
`ttl_seconds` 独立控制持久化绑定的保留时间。超出活跃窗口后绑定继续保留，
恢复请求时会重新出现在看板中。修改活跃窗口后执行 `./restart.sh serving` 滚动生效。

`runtime/affinity.sqlite3` 使用 WAL、FULL 同步、短事务和 0600 文件权限。
唯一 family 键保证并发首请求和多个 worker 只能提交一个绑定；提交成功后才允许派发。
最近活动续期 48 小时，主会话、子 agent 和在途请求均计入活动。普通活动按秒合并刷盘，
临近过期时同步确认续期，正常退出前刷新。崩溃可能丢失最后一秒尚未合并的活动。
描述缓存淘汰不删除有效绑定；过期绑定与失去引用的描述后台分批清理。

数据库保存标识摘要、endpoint 稳定身份、活动时间和去重后的最小路由描述。
凭据、请求正文及密文均不进入该数据库。已移除部署的描述可供原有绑定继续使用；
同名 endpoint 指向另一资源时返回身份冲突。

| 情况 | 错误 |
|---|---|
| 状态请求缺少会话标识 | `session_id_required`（400） |
| 携带密文或 previous_response_id，但绑定缺失或到期 | `affinity_missing`（409） |
| 分支首次继承时，并发创建的绑定与父会话 endpoint 不一致 | `affinity_parent_conflict`（409） |
| 原 endpoint 无法提供目标模型 | `bound_model_unavailable`（404） |
| 同名 endpoint 资源身份改变 | `endpoint_identity_conflict`（409） |
| 新绑定或必要续期无法提交 | `affinity_store_unavailable`（503） |

绑定一经确定，限流、超时和兼容性错误均不改变 endpoint。所有重试保留加密字段与内容顺序；
上游兼容性错误原样返回。旧配置中的模式开关不再改变绑定行为。
`live_sessions`、`sessions_per_endpoint` 和 `sessions_per_model` 均按活跃窗口筛选。
模型统计提供 `total` 和 `endpoints` 明细；同一活跃会话可以出现在其使用过的多个模型中。
`retained_sessions` 表示全部未过期绑定数，`active_window_seconds` 返回当前统计窗口。
统计记录保存在绑定数据库内，随绑定过期清理。

完整历史重放语义参考 [OpenAI reasoning guide](https://developers.openai.com/api/docs/guides/reasoning)。
同 endpoint 跨 deployment 兼容性以 [脱敏实验结果](test/results/) 为依据，不能据此保证
所有模型版本均兼容；上游拒绝会原样报告。

---

## 凭据与 token 过期

代理用 `AzureCliCredential`，也就是 `az account get-access-token` 返回的东西。

**关键事实一：代理跑的不是你的 Azure 账号。** 它借用 `svc-account@example.com`
——`settings/endpoints.yaml` 里那几个 endpoint 的 data action 是授给这个 principal 的。
你自己的账号（`your-own@example.com`）登进去，token 签得出来，但每个 deployment 都会
401/403。实测撞过一次：`az` 被换成另一个 principal，代理的下一次 token 刷新就全线 401。

所以代理有**自己的凭据目录**（`settings/policy.yaml` 的 `auth.az_config_dir`，
默认是仓库内的 `.az-identity/`）。`AzureCliCredential` 是拿 `os.environ` 的副本去
spawn `az` 的，所以进程里设一个 `AZURE_CONFIG_DIR` 就够了。**双向隔离**：你 `az login`
不影响代理，代理的登录也不影响你。

路径写成**相对**的，按仓库根解析，不按调用者的 cwd。以前写的是
`~/<某人>/.azure-proxy`——那是代理还跑在一个共享账号下时写的。
后来换成独立账号，同一个 `~` 指向了一个不存在的地方，代理照常启动，
然后每个请求都是 503 `credentials_unavailable`。后来改成绝对路径填了这个坑，但绝对路径
换来的是另一个坑：整棵树一搬家就得回来改这一行。相对路径两个都躲开了。

代理相关的 az 操作一律走 `./az.sh`，它替你设好那个目录：

```bash
./az.sh login --use-device-code
./az.sh account set --subscription "Advanced Machine Learning"
./az.sh account show --query user.name -o tsv
```

裸 `az` 操作的是你自己的 `~/.azure`，对代理没有任何作用。

**关键事实二：一份 `~/.azure` 里通常不止一个身份。** 如果你没法用 device code 登进
那个 principal，但别处有一份它还登录着的配置目录，用：

```bash
./import-identity.sh sc-1234567@microsoft.com --from <那份目录>
```

**不要 `cp -a <那份目录>/. .az-identity/`**，尽管这份代理最初就是那么来的。
`msal_token_cache.json` 的每个分区都按 `home_account_id` 索引，一份 `~/.azure` 里有几个
登录过的账号，里面就有几个 refresh token——这台机器上那份就同时装着 `other-account` 和
`svc-account`，而代理只用得到后者。整份拷过去，等于把一个用不到的活凭据也放进了一个
所有运维者都能读的目录，而且两个文件看起来跟单个登录毫无区别，没有任何东西会提示你。

`import-identity.sh` 做的是**过滤**而不是拷贝：按 `home_account_id` 精确挑出一个账号的
`Account` / `RefreshToken` / `IdToken`，裁掉 `azureProfile.json` 里不属于它的订阅并保证
留一个 default，然后实测签一次 token 证明确实可用。它还会告诉你把谁留在了原处。

顺手两个手工容易做错的地方：`azureProfile.json` 是带 UTF-8 BOM 写出来的（当 utf-8 读会
直接抛异常）；access token 故意不带过来——它有效期约一小时，带过去的话第一次
`az account get-access-token` 会直接命中缓存、根本没验证 refresh token，于是一个坏掉的
refresh token 要等一小时后才暴露。

**同一份 refresh token 别留两份在用。** AAD 对 public client 的 refresh token 是轮换的，
两个进程各拿一份去刷，早晚互相把对方作废。所以拷完就该把原来那份停掉。

serving 启动时会比较当前账号和 `policy.yaml` 的 `expected_account`。当两者不一致时，
`proxy.log` 会记录警告和修复提示。routing 的启动和重启使用本地部署信息。

**关键事实二：`az account get-access-token` 给的是 `az` 自己缓存里的 token，剩余寿命
不可预测。** 实测拿到过 6 分钟和 9.7 分钟的，也可能拿到接近一小时的——取决于你取的
时候它已经活了多久。所以不能假设"token 有一小时"。

处理规则三条：

- **token 一直用到真正过期前 30 秒**。`refresh_margin_seconds` 只决定什么时候**开始尝试**
  续，不决定什么时候**停止信任**手上这个
- **margin 会被 clamp 到 token 实际寿命的一半**。不然一个寿命短于 margin 的 token 一到手
  就算"该刷新了"，于是每个请求都去 shell 一次 `az`（实测 0.7 秒），并发下全堵在锁上
- **刷新尝试有频率下限**（10 秒）。`az` 彻底坏掉时不会退化成每请求一次子进程

后台有个刷新协程，所以正常情况下**没有请求需要为取 token 付时间**。

刷新失败但手上的 token 还能用时，**继续用**，错误记在 `/healthz` 的 `token.last_error`
里，`proxy.log` 里记一行（只在状态变化时记，不会每 30 秒刷屏）；只有真的没有可用
token 时才返回 503：

```json
{"error": {"message": "Azure credentials unavailable (...). on the proxy host run: ./az.sh login --use-device-code",
           "code": "credentials_unavailable"}}
```

看 token 还剩多久：

```bash
curl -s http://127.0.0.1:8811/healthz | python -m json.tool
```

`az` 登录彻底失效时（refresh token 到期、条件访问策略要求重新认证），在**代理所在的机器**上：

```bash
./az.sh login --use-device-code
./az.sh account set --subscription "Advanced Machine Learning"
```

不需要重启代理——后台协程下一轮就会取到新 token。


---

## 安全

- 只绑 `127.0.0.1`。调用端无需凭据，意味着能连上这个端口的任何东西都能消耗团队
  的 Azure 配额。改绑 `0.0.0.0` 之前必须先加鉴权
- 所有 endpoint 都走 AD token，仓库里不存任何密钥
- `auth.az_config_dir` 指向仓库**外面**，因为那个目录里有 refresh token。别把它挪
  进仓库，`.gitignore` 也不该被指望来兜这个底
- 日志不记录请求体和响应体。prompt 是用户数据，一个悄悄归档它们的代理会比它帮忙
  排查的任何问题都严重
