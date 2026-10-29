# CCV Realtime Collection

Twitch 和 YouTube 直播实时并发观看人数（CCV）采集工具。项目包含官方 API 轮询、网页前端采集、双通道采集、暂停/恢复/停止控制、断点续采和报告生成。

## 项目审查结论

这套项目适合在本机或受控工作机上做低频、定向的直播人数监测，核心链路可以实际运行：

- Twitch 使用 Helix API，支持按游戏分类或指定频道采集。
- YouTube 使用 Data API v3，支持指定频道或指定直播视频采集。
- 每轮采样会写入原始 JSONL、单场 CSV、汇总 CSV 和会话目录，进程中断后可以续采。
- `server.py` 提供本机控制台，可添加任务、暂停、恢复和停止任务。
- 指定频道/视频可以使用 `dual: true` 同时保存 API 和网页两路数据，便于比较平台 API 与页面显示差异。

使用时需要注意：API 返回的 `viewer_count` 可能存在平台缓存，不应把 15～30 秒轮询误解为严格的秒级实时；网页模式更接近页面显示，但依赖页面结构、浏览器和登录/同意页面，稳定性较低，也应遵守 Twitch/YouTube 的服务条款。网页采集目前只建议低频、少量频道使用。

## 目录

```text
获取数据/
├── monitor.py              # Twitch API 采集器
├── server.py               # 多任务控制台（推荐入口）
├── twitch_web_pilot.js     # Twitch 网页采集
├── yt_web_pilot.js         # YouTube 网页采集
├── report.py               # API/Web 数据报告入口
├── _build_web_xlsx.py      # 网页采集数据报告
├── build_report.mjs        # xlsx 构建器
├── charts.py               # PNG 图表
├── config.json             # 默认配置
├── .env.example            # 凭据模板
├── requirements.txt        # Python 依赖
└── docs/                   # 工作流图
```

`data/`、`charts/`、`输出/`、`报告_*/` 和日志都是运行时产物，默认不会提交到 Git。`.env` 和 Twitch token 也不会提交。

## 快速开始

### 1. 准备 Python

需要 Python 3.10 或更高版本。Windows 可以使用项目内虚拟环境，也可以直接使用系统 Python：

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

如果已经有可用的 Python，直接执行：

```powershell
python -m pip install -r requirements.txt
```

### 2. 配置凭据

复制模板并填写凭据：

```powershell
Copy-Item .env.example .env
```

`.env` 中的字段：

```text
TWITCH_CLIENT_ID=...
TWITCH_CLIENT_SECRET=...
YOUTUBE_API_KEY=...
```

- Twitch 任务需要前两个字段，申请地址：<https://dev.twitch.tv/console/apps>
- YouTube API 任务需要 `YOUTUBE_API_KEY`，申请地址：<https://console.cloud.google.com/apis/credentials>
- 仅使用 YouTube 网页试点时可以不配置 YouTube API key。

也可以使用进程环境变量；环境变量优先于 `.env`：

```powershell
$env:TWITCH_CLIENT_ID = "..."
$env:TWITCH_CLIENT_SECRET = "..."
$env:YOUTUBE_API_KEY = "..."
```

### 3. 先做一次配置检查

只采一轮，不会长时间运行：

```powershell
python monitor.py --once
```

缺少 Twitch 凭据时，命令会直接提示并退出。没有凭据只想查看图表流程，可以运行演示数据：

```powershell
python demo_data.py
python charts.py
```

### 4. 运行控制台

```powershell
python server.py
```

然后打开 <http://127.0.0.1:8088/>。Windows 双击 `启动看板.bat` 也可以启动。批处理脚本会优先使用 `.venv\Scripts\python.exe`，否则使用 PATH 中的 `python`。

控制台支持：

- Twitch 游戏分类：输入 `Wuthering Waves` 等游戏名，自动保留 CCV 最高的若干场直播。
- Twitch 指定频道：输入 `https://www.twitch.tv/<login>`。
- YouTube 指定频道：输入 `@handle`、`https://www.youtube.com/@handle` 或 `/channel/UC...`。
- YouTube 指定视频：输入 `https://www.youtube.com/watch?v=...` 或视频 ID。
- 任务的暂停、恢复和停止。

控制台只绑定 `127.0.0.1`，没有登录认证，不要把端口暴露到局域网或公网。

## 直接运行 API 采集器

### Twitch 游戏分类

```powershell
python monitor.py --duration 20 --interval 30 --auto-charts
```

### Twitch 指定频道

指定频道建议通过控制台创建；也可以调用控制 API：

```powershell
$body = @{
  platform = "twitch"
  mode = "channel"
  target = "https://www.twitch.tv/tenz"
  name = "Tenz"
  poll_interval_seconds = 30
  monitor_duration_minutes = 20
} | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8088/api/tasks -Method Post -ContentType "application/json" -Body $body
```

### YouTube 视频或频道

```powershell
$body = @{
  platform = "youtube"
  mode = "video"
  target = "https://www.youtube.com/watch?v=VIDEO_ID"
  name = "YouTube直播"
  poll_interval_seconds = 30
  monitor_duration_minutes = 20
} | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8088/api/tasks -Method Post -ContentType "application/json" -Body $body
```

频道模式会定期搜索当前直播视频；视频模式直接轮询指定视频。频道搜索会消耗更多 YouTube API quota。

### 双通道

双通道只接受指定频道/视频中的具体视频目标，不能对 YouTube 频道目标直接启动网页通道：

```powershell
$body = @{
  platform = "twitch"
  mode = "channel"
  target = "https://www.twitch.tv/caedrel"
  name = "Caedrel双通道"
  dual = $true
  poll_interval_seconds = 15
} | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8088/api/tasks -Method Post -ContentType "application/json" -Body $body
```

双通道会把 API 数据写到 `data/<task_id>/`，网页数据写到 `data/<task_id>/web/`。

## 网页采集模式

网页模式用于 API 延迟较大或需要观察页面实际显示值的情况。需要 Node.js、Playwright 和可用浏览器环境。推荐在项目目录安装 Playwright：

```powershell
npm install playwright
npx playwright install chromium
```

如果使用项目外的 Playwright 安装，可以设置：

```powershell
$env:CODEX_NODE_EXE = "C:\Program Files\nodejs\node.exe"
$env:CODEX_NODE_MODULES = "D:\path\to\node_modules"
```

单独运行 Twitch 网页采集：

```powershell
node twitch_web_pilot.js caedrel 10 10 data/twitch_web_caedrel
```

单独运行 YouTube 网页采集：

```powershell
node yt_web_pilot.js "https://www.youtube.com/watch?v=VIDEO_ID" 10 10 data/yt_web_VIDEO_ID
```

网页脚本会保留 `streams.csv`、`raw.jsonl` 和 `meta.json`。网页结构变化、Cookie 同意页、地区限制或浏览器启动失败都会导致采样为空或任务失败；请先用 1～2 分钟短任务验证页面能读到人数。

## 停止、暂停和恢复

控制台操作优先。API 也可以直接控制：

```powershell
Invoke-RestMethod http://127.0.0.1:8088/api/tasks/<task_id>/pause -Method Post
Invoke-RestMethod http://127.0.0.1:8088/api/tasks/<task_id>/resume -Method Post
Invoke-RestMethod http://127.0.0.1:8088/api/tasks/<task_id>/stop -Method Post
```

直接运行 `monitor.py` 时，可以使用项目目录下的 `stop.flag`；任务模式下使用 `data/<task_id>/stop.flag` 和 `pause.flag`。程序会在当前轮询结束后优雅停止，并把正在采集的场次标记为 `interrupted`。

## 输出数据

每个任务使用独立目录：

```text
data/<task_id>/
├── raw/                         # API 原始 JSONL（按日期）
├── streams/                     # 每场直播的 CCV 时序 CSV
├── sessions/<session_id>/       # 本次运行的原始快照和 meta
├── streams_summary.csv          # 峰值、均值、最低值、时长、采样数
├── state.json                   # 断点续采状态
└── heartbeat.json               # 最近一次成功轮询
```

报告生成：

```powershell
python report.py --task <task_id>
python report.py --all
```

报告默认写入 `输出/`。仓库中的 `examples/` 保留了几份历史任务报告，仅用于展示输出格式。PNG 图表可以单独生成：

```powershell
python charts.py
```

### 报告运行时说明

`report.py` 的 xlsx 构建步骤使用 `build_report.mjs` 和 `@oai/artifact-tool`。采集和 CSV/JSON 保存不依赖这个 Node 包；如果当前环境没有兼容的 `@oai/artifact-tool`，API/Web 采集仍可运行，但 xlsx 报告需要先准备该运行时，并通过 `CODEX_NODE_EXE`、`CODEX_NODE_MODULES` 指向它。

## 配置说明

主要配置在 `config.json`：

| 配置 | 作用 |
|---|---|
| `poll_interval_seconds` | API 轮询间隔，建议 Twitch 至少 30 秒 |
| `max_streams` | 游戏分类模式最多跟踪场数 |
| `monitor_duration_minutes` | 监测时长，0 表示不限时 |
| `max_stream_pages` | Twitch 游戏分类 API 最多翻页数 |
| `dashboard_port` | 本机控制台端口 |
| `timezone` | 图表显示时区 |
| `auto_charts_on_exit` | 直接运行采集器结束后是否自动生成 PNG |

指定频道模式会优先跟踪目标频道；游戏分类模式按当前 CCV 选择前 `max_streams` 场，并尽量保持已跟踪场次不被频繁替换。

## 故障排查

- **缺少 Twitch 凭据**：复制 `.env.example` 为 `.env`，填写 `TWITCH_CLIENT_ID` 和 `TWITCH_CLIENT_SECRET`。
- **YouTube 任务无法创建**：检查 `YOUTUBE_API_KEY`、API quota 和视频是否仍在直播。
- **网页采集没有人数**：确认 Node/Playwright 已安装，先用短任务运行；页面结构变化时需要调整选择器。
- **看板显示任务停止**：查看 `server.log`、任务目录中的 `heartbeat.json` 和会话 `meta.json`。
- **没有 xlsx 报告**：先确认采集目录有数据，再检查 `CODEX_NODE_EXE`/`CODEX_NODE_MODULES` 是否指向包含 `@oai/artifact-tool` 的 Node 运行时。
- **Windows 双击脚本无反应**：在 PowerShell 中直接执行对应的 `python ...` 命令，确认 Python 已加入 PATH 或已创建 `.venv`。

## 数据与合规边界

凭据、token 和采集数据可能包含敏感信息，发布或提交前请确认 `.env`、`data/`、日志和报告没有被强制加入 Git。网页采集应保持低频、少量、定向，并遵守 Twitch 和 YouTube 的服务条款、访问限制及所在组织的合规要求。
