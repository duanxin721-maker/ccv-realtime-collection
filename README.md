# CCV Realtime Collection

Twitch 直播实时并发观看人数（CCV）采集工具。项目包含官方 API 轮询、Twitch 网页双通道、暂停/恢复/停止、断点续采、趋势看板和报告导出。

## 功能

- 按 Twitch 游戏分类自动跟踪 CCV 最高的直播。
- 按 Twitch 频道或直播间链接定向采集。
- 频道未开播时持续守望，开播后自动采集 CCV，下播后继续等待下一次开播。
- 可选 API + Twitch 网页双通道对照。
- 每轮采样写入 JSONL、单场 CSV、汇总 CSV 和会话目录。
- 本机看板支持任务控制、CCV 趋势、CSV 与轻量 Excel 导出。
- 进程中断后可根据状态文件续采。

本项目不再内置 YouTube 采集器。看板仅保留一个可配置的外部工具跳转按钮，两套工具之间不交换任务、状态或数据。

## 快速开始

需要 Python 3.10 或更高版本：

```powershell
py -3 -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
Copy-Item .env.example .env
```

在 `.env` 中填写 Twitch 开发者应用凭据：

```text
TWITCH_CLIENT_ID=...
TWITCH_CLIENT_SECRET=...
```

凭据申请地址：<https://dev.twitch.tv/console/apps>

启动看板：

```powershell
python server.py
```

然后打开 <http://127.0.0.1:8088/>。也可以双击 `启动看板.bat`。控制台只绑定 `127.0.0.1`，没有登录认证，请勿将端口暴露到局域网或公网。

## 外部 YouTube 工具按钮

在 `config.json` 中填写未来独立工具的地址：

```json
{
  "youtube_tool_url": "https://example.internal/youtube-tool"
}
```

仅接受绝对 `http://` 或 `https://` 地址，不接受相对路径、带用户名密码的地址或 `javascript:`、`file:` 等协议。配置为空或无效时，看板显示“YouTube 采集工具（待配置）”且不可点击。

该按钮只执行浏览器跳转；本项目不会启动、探测或监控外部工具。

## 直接运行 Twitch API 采集器

游戏分类：

```powershell
python monitor.py --duration 20 --interval 30 --auto-charts
```

只采一轮检查配置：

```powershell
python monitor.py --once
```

指定频道建议通过看板创建，也可以调用本机 API：

```powershell
$body = @{
  platform = "twitch"
  mode = "channel"
  target = "https://www.twitch.tv/tenz"
  name = "Tenz"
  watch_interval_seconds = 30
  live_interval_seconds = 30
} | ConvertTo-Json
Invoke-RestMethod http://127.0.0.1:8088/api/tasks -Method Post -ContentType "application/json" -Body $body
```

## Twitch 网页双通道

网页通道需要 Node.js、Playwright 和 Chromium：

```powershell
npm install
npx playwright install chromium
```

单独运行网页采集：

```powershell
node twitch_web_pilot.js caedrel 10 10 data/twitch_web_caedrel
```

看板任务设置 `dual: true` 时，API 数据写到 `data/<task_id>/`，网页数据写到 `data/<task_id>/web/`。网页模式依赖页面结构、浏览器和地区页面，建议先用短任务验证。

## 任务控制

```powershell
Invoke-RestMethod http://127.0.0.1:8088/api/tasks/<task_id>/pause -Method Post
Invoke-RestMethod http://127.0.0.1:8088/api/tasks/<task_id>/resume -Method Post
Invoke-RestMethod http://127.0.0.1:8088/api/tasks/<task_id>/stop -Method Post
```

每个任务使用独立目录：

```text
data/<task_id>/
├── raw/
├── streams/
├── sessions/<session_id>/
├── streams_summary.csv
├── state.json
└── heartbeat.json
```

升级前保存的非 Twitch 任务会标记为 `unsupported`，不会自动启动；项目不会自动删除其历史数据。

## 报告

```powershell
python report.py --task <task_id>
python report.py --all
python charts.py
```

看板的 Excel 导出使用 `openpyxl`，下载文件包含两个工作表：

- `采样数据`：逐次记录采样时间（北京时间）、可点击直播链接、主播名称、直播标题、CCV 和直播场次 ID。
- `CCV趋势图`：按直播场次生成 Excel 原生折线图，图表直接引用采样数据，可在 Excel 中编辑标题、坐标轴、样式和数据范围。

导出优先读取完整单场 CSV，不受看板状态文件仅保留最近采样点的限制。完整报告使用 `build_report.mjs` 和 `@oai/artifact-tool`；若该可选运行时不存在，采集、CSV/JSON 保存和看板 Excel 导出仍可使用。

## 主要配置

| 配置 | 作用 |
|---|---|
| `poll_interval_seconds` | API 轮询间隔，建议至少 30 秒 |
| `watch_interval_seconds` | 频道未开播时的检查间隔，默认 30 秒 |
| `live_interval_seconds` | 频道开播后的 CCV 采样间隔，默认 30 秒 |
| `max_streams` | 游戏分类最多跟踪场数 |
| `monitor_duration_minutes` | 监测时长，0 表示不限时 |
| `max_stream_pages` | Twitch 游戏分类 API 最多翻页数 |
| `dashboard_port` | 本机看板端口 |
| `timezone` | 图表显示时区 |
| `youtube_tool_url` | 外部 YouTube 工具跳转地址，可留空 |

频道任务会在等待开播和直播采集之间持续循环，仅暂停、停止、删除或关闭服务会中断任务。旧任务只有 `poll_interval_seconds` 时，该值会同时作为守望和采样间隔。

## 数据与合规

`.env`、token、采集数据、日志和报告默认不会提交到 Git。Twitch API 数据可能存在平台缓存，轮询间隔不代表平台提供严格秒级实时数据。使用 API 和网页采集时应遵守 Twitch 服务条款及所在组织的合规要求。
