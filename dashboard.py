#!/usr/bin/env python3
"""本地可视化看板：查看采集进程状态、会话信息和直播 CCV 进度。

用法:
  python dashboard.py           # 默认 http://127.0.0.1:8088/
  python dashboard.py --port 9000
"""

from __future__ import annotations

import argparse
import csv
import json
import io
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

BASE_DIR = Path(__file__).resolve().parent


def load_config() -> dict:
    cfg_path = BASE_DIR / "config.json"
    return json.loads(cfg_path.read_text(encoding="utf-8")) if cfg_path.exists() else {}


def read_heartbeat() -> dict:
    path = BASE_DIR / "data" / "heartbeat.json"
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def heartbeat_age_seconds(hb: dict) -> float | None:
    try:
        last = datetime.fromisoformat(hb["last_tick_utc"])
        return (datetime.now(timezone.utc) - last).total_seconds()
    except Exception:
        return None


def process_alive(hb: dict) -> bool:
    pid = hb.get("pid")
    if not pid:
        return False
    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, int(pid))
        if not handle:
            return False
        try:
            exit_code = ctypes.c_ulong()
            ok = kernel32.GetExitCodeProcess(handle, ctypes.byref(exit_code))
            kernel32.CloseHandle(handle)
            if not ok:
                return False
            return exit_code.value == 259  # STILL_ACTIVE
        except Exception:
            kernel32.CloseHandle(handle)
            return False
    except Exception:
        # 非 Windows 或 API 不可用时退回心跳新鲜度判断
        age = heartbeat_age_seconds(hb)
        return age is not None and age < 90


def read_summary() -> list[dict]:
    path = BASE_DIR / "data" / "streams_summary.csv"
    if not path.exists():
        return []
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as fh:
            return list(csv.DictReader(fh))
    except Exception:
        return []


def read_stream_tail(stream_id: str, max_rows: int = 240) -> list[dict]:
    stream_dir = BASE_DIR / "data" / "streams"
    match = list(stream_dir.glob(f"*_{stream_id}.csv"))
    if not match:
        return []
    try:
        with match[0].open("r", encoding="utf-8-sig", newline="") as fh:
            rows = list(csv.DictReader(fh))
        out = []
        for row in rows[-max_rows:]:
            try:
                out.append(
                    {
                        "t": row.get("ts_utc", ""),
                        "ccv": float(row.get("viewer_count", 0) or 0),
                    }
                )
            except Exception:
                continue
        return out
    except Exception:
        return []


def list_sessions() -> list[dict]:
    sessions_dir = BASE_DIR / "data" / "sessions"
    if not sessions_dir.exists():
        return []
    result = []
    for meta_path in sorted(sessions_dir.glob("*/meta.json"), reverse=True):
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            raw_path = meta_path.parent / "raw.jsonl"
            meta["raw_rows"] = sum(1 for _ in raw_path.open("r", encoding="utf-8")) if raw_path.exists() else 0
            result.append(meta)
        except Exception:
            continue
    return result[:20]


PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>鸣潮 CCV 采集看板</title>
<style>
  :root { --bg:#0f172a; --card:#1e293b; --line:#334155; --ok:#22c55e; --bad:#ef4444; --text:#e2e8f0; --dim:#94a3b8; }
  * { box-sizing: border-box; }
  body { margin:0; background:var(--bg); color:var(--text); font-family:"Microsoft YaHei",system-ui,sans-serif; }
  .wrap { max-width:1200px; margin:0 auto; padding:20px; }
  h1 { font-size:20px; margin:0 0 4px; }
  .sub { color:var(--dim); font-size:13px; margin-bottom:16px; }
  .grid { display:grid; grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); gap:12px; margin-bottom:16px; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px; }
  .card h2 { font-size:13px; color:var(--dim); margin:0 0 8px; font-weight:500; }
  .card .big { font-size:24px; font-weight:700; }
  .pill { display:inline-block; padding:2px 10px; border-radius:999px; font-size:12px; }
  .pill.ok { background:rgba(34,197,94,.15); color:var(--ok); }
  .pill.bad { background:rgba(239,68,68,.15); color:var(--bad); }
  .mono { font-family:Consolas,monospace; font-size:12px; color:var(--dim); }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th,td { text-align:left; padding:8px 6px; border-bottom:1px solid var(--line); }
  th { color:var(--dim); font-weight:500; }
  .spark { width:180px; height:44px; display:block; }
  .bar { height:8px; background:var(--line); border-radius:4px; overflow:hidden; margin-top:8px; }
  .bar > div { height:100%; background:var(--ok); }
  .section { margin-bottom:24px; }
  .section h2 { font-size:15px; margin:0 0 10px; }
  .muted { color:var(--dim); }
  #refreshAt { color:var(--dim); font-size:12px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>鸣潮直播实时 CCV 采集看板</h1>
  <div class="sub">自动刷新（10 秒） · 数据来自 monitor.py 的 data/ 目录 · <span id="refreshAt"></span></div>

  <div class="grid" id="statusGrid"></div>

  <div class="section">
    <h2>当前跟踪的直播</h2>
    <div class="card" style="overflow-x:auto;">
      <table id="streamTable">
        <thead><tr>
          <th>主播</th><th>当前 CCV</th><th>峰值</th><th>均值</th><th>采样数</th><th>已采时长(分)</th><th>CCV 曲线</th>
        </tr></thead>
        <tbody id="streamBody"><tr><td colspan="7" class="muted">加载中...</td></tr></tbody>
      </table>
    </div>
  </div>

  <div class="section">
    <h2>历史会话</h2>
    <div class="card" style="overflow-x:auto;">
      <table>
        <thead><tr><th>会话 ID</th><th>状态</th><th>开始(本地)</th><th>结束(本地)</th><th>原始行数</th><th>结束时在播</th></tr></thead>
        <tbody id="sessionBody"></tbody>
      </table>
    </div>
  </div>
</div>

<script>
const TZ = "Asia/Singapore";
function fmtLocal(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  return d.toLocaleString("zh-CN", { timeZone: TZ, hour12: false });
}
function fmtAgo(seconds) {
  if (seconds == null) return "-";
  if (seconds < 60) return Math.round(seconds) + " 秒前";
  if (seconds < 3600) return Math.round(seconds / 60) + " 分钟前";
  return Math.round(seconds / 3600) + " 小时前";
}
function sparkline(series, peak) {
  if (!series || series.length < 2) return '<span class="muted">数据不足</span>';
  const w = 180, h = 44, pad = 2;
  const vals = series.map(p => p.ccv);
  const max = Math.max(...vals, 1);
  const step = (w - pad * 2) / (vals.length - 1);
  const pts = vals.map((v, i) => `${(pad + i * step).toFixed(1)},${(h - pad - (v / max) * (h - pad * 2)).toFixed(1)}`).join(" ");
  return `<svg class="spark" viewBox="0 0 ${w} ${h}" preserveAspectRatio="none">
    <polyline points="${pts}" fill="none" stroke="#38bdf8" stroke-width="1.5"/></svg>`;
}
function statusPill(running) {
  return running
    ? '<span class="pill ok">● 运行中</span>'
    : '<span class="pill bad">● 未运行</span>';
}
async function load() {
  try {
    const [status, streams] = await Promise.all([
      fetch("/api/status").then(r => r.json()),
      fetch("/api/streams").then(r => r.json()),
    ]);
    document.getElementById("refreshAt").textContent = "上次刷新 " + new Date().toLocaleTimeString("zh-CN", { hour12: false });

    const hb = status.heartbeat || {};
    const grid = document.getElementById("statusGrid");
    const running = status.running;
    const age = status.heartbeat_age_seconds;
    grid.innerHTML = `
      <div class="card"><h2>采集进程</h2><div class="big">${statusPill(running)}</div>
        <div class="mono" style="margin-top:6px">PID ${hb.pid || "-"} · 上次查看 ${fmtAgo(age)}</div></div>
      <div class="card"><h2>当前会话</h2><div class="big mono">${hb.session_id || "-"}</div>
        <div class="mono" style="margin-top:6px">开始 ${fmtLocal(hb.session_started_at_utc)}</div></div>
      <div class="card"><h2>跟踪中的直播</h2><div class="big">${hb.live_count ?? "-"} <span style="font-size:13px;color:var(--dim)">/ ${hb.tracked_total ?? "-"} 累计</span></div></div>
      <div class="card"><h2>监测时长</h2>
        ${hb.duration_minutes > 0
          ? `<div class="big">${hb.duration_minutes} 分钟</div>
             <div class="mono">剩余约 ${hb.remaining_seconds != null ? Math.ceil(hb.remaining_seconds / 60) : "-"} 分钟</div>
             <div class="bar"><div style="width:${hb.duration_minutes > 0 ? Math.min(100, 100 - (hb.remaining_seconds ?? 0) / (hb.duration_minutes * 60) * 100) : 0}%"></div></div>`
          : '<div class="big" style="font-size:18px">不限时</div>'}
      </div>`;

    const body = document.getElementById("streamBody");
    if (!streams.streams || streams.streams.length === 0) {
      body.innerHTML = '<tr><td colspan="7" class="muted">当前没有跟踪中的直播</td></tr>';
    } else {
      body.innerHTML = streams.streams.map(s => `
        <tr>
          <td><b>${escapeHtml(s.channel)}</b><div class="mono">${escapeHtml(s.display_name || "")}</div></td>
          <td><b>${s.current_ccv ?? "-"}</b></td>
          <td>${s.peak_ccv ?? "-"}</td>
          <td>${s.avg_ccv ?? "-"}</td>
          <td>${s.sample_count ?? 0}</td>
          <td>${s.duration_min ?? "-"}</td>
          <td>${sparkline(s.series, s.peak_ccv)}</td>
        </tr>`).join("");
    }

    const sessBody = document.getElementById("sessionBody");
    const sessions = await fetch("/api/sessions").then(r => r.json());
    sessBody.innerHTML = (sessions.sessions || []).map(s => `
      <tr>
        <td class="mono">${escapeHtml(s.session_id)}</td>
        <td>${statusPill(s.status === "running")} ${escapeHtml(s.status || "")}</td>
        <td>${fmtLocal(s.started_at_utc)}</td>
        <td>${fmtLocal(s.ended_at_utc)}</td>
        <td>${s.raw_rows ?? 0}</td>
        <td>${s.live_count ?? "-"}</td>
      </tr>`).join("") || '<tr><td colspan="6" class="muted">暂无会话</td></tr>';
  } catch (e) {
    document.getElementById("streamBody").innerHTML = '<tr><td colspan="7" class="muted">加载失败: ' + e.message + '</td></tr>';
  }
}
function escapeHtml(s) {
  return String(s ?? "").replace(/[&<>"']/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
load();
setInterval(load, 10000);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def _json(self, payload: dict, code: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/":
            data = PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
            return
        if path == "/api/status":
            hb = read_heartbeat()
            alive = process_alive(hb)
            age = heartbeat_age_seconds(hb)
            running = alive and (age is None or age < 90)
            self._json(
                {
                    "running": running,
                    "process_alive": alive,
                    "heartbeat_age_seconds": age,
                    "heartbeat": hb,
                }
            )
            return
        if path == "/api/streams":
            summary = read_summary()
            streams = []
            for row in summary:
                if row.get("status") != "live":
                    continue
                stream_id = row.get("stream_id", "")
                series = read_stream_tail(stream_id)
                streams.append(
                    {
                        "stream_id": stream_id,
                        "channel": row.get("channel", ""),
                        "display_name": row.get("display_name", ""),
                        "current_ccv": series[-1]["ccv"] if series else None,
                        "peak_ccv": row.get("peak_ccv", ""),
                        "avg_ccv": row.get("avg_ccv", ""),
                        "sample_count": row.get("sample_count", ""),
                        "duration_min": row.get("duration_min", ""),
                        "series": series,
                    }
                )
            self._json({"streams": streams})
            return
        if path == "/api/sessions":
            sessions = list_sessions()
            hb = read_heartbeat()
            if hb.get("session_id") and hb["session_id"] not in [s.get("session_id") for s in sessions]:
                raw_path = BASE_DIR / "data" / "sessions" / hb["session_id"] / "raw.jsonl"
                raw_rows = 0
                if raw_path.exists():
                    try:
                        raw_rows = sum(1 for _ in raw_path.open("r", encoding="utf-8"))
                    except Exception:
                        raw_rows = 0
                sessions.insert(
                    0,
                    {
                        "session_id": hb["session_id"],
                        "started_at_utc": hb.get("session_started_at_utc", ""),
                        "ended_at_utc": "",
                        "status": "running",
                        "live_count": hb.get("live_count"),
                        "tracked_total": hb.get("tracked_total"),
                        "raw_rows": raw_rows,
                    },
                )
            self._json({"sessions": sessions})
            return
        if path.startswith("/charts/"):
            name = Path(path.removeprefix("/charts/")).name
            chart = BASE_DIR / "charts" / name
            if chart.exists():
                data = chart.read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "image/png")
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
                return
            self._json({"error": "not found"}, 404)
            return
        self._json({"error": "not found"}, 404)


def main() -> None:
    parser = argparse.ArgumentParser(description="鸣潮 CCV 采集看板")
    parser.add_argument("--port", type=int, default=int(load_config().get("dashboard_port", 8088)))
    args = parser.parse_args()
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    print(f"看板已启动: http://127.0.0.1:{args.port}/")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
