#!/usr/bin/env python3
"""鸣潮 CCV 采集守护 + 可视化看板（支持 Twitch / YouTube 多任务）。

功能:
  - 任务管理：从看板添加（Twitch 游戏/频道、YouTube 频道/视频）、暂停、恢复、停止
  - Twitch 与 YouTube 任务各自独立线程、独立数据目录 data/<task_id>/
  - 可视化看板 http://127.0.0.1:8088/

用法:
  python server.py
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests

import monitor as monitor_mod
from monitor import (
    BASE_DIR,
    Monitor,
    STREAM_CSV_COLUMNS,
    SUMMARY_COLUMNS,
    TwitchClient,
    utcnow,
)

REPORT_THREADS: list[threading.Thread] = []

WEB_NODE_EXE = os.environ.get("CODEX_NODE_EXE") or shutil.which("node") or "node"
WEB_NODE_MODULES = os.environ.get("CODEX_NODE_MODULES", "")


def load_env() -> dict[str, str]:
    env: dict[str, str] = {}
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                env[key.strip()] = value.strip().strip('"').strip("'")
    # 进程环境优先于 .env，方便 CI、计划任务和容器化运行。
    for key in ("TWITCH_CLIENT_ID", "TWITCH_CLIENT_SECRET", "YOUTUBE_API_KEY"):
        if os.environ.get(key):
            env[key] = os.environ[key]
    return env


def node_env() -> dict[str, str]:
    """返回网页采集进程环境；优先使用显式 NODE_PATH，否则使用项目本地模块。"""
    env = dict(os.environ)
    env["PYTHONIOENCODING"] = "utf-8"
    if WEB_NODE_MODULES:
        env["NODE_PATH"] = WEB_NODE_MODULES
    elif not env.get("NODE_PATH"):
        local_modules = BASE_DIR / "node_modules"
        if local_modules.exists():
            env["NODE_PATH"] = str(local_modules)
    return env


def load_config() -> dict[str, Any]:
    cfg_path = BASE_DIR / "config.json"
    return json.loads(cfg_path.read_text(encoding="utf-8")) if cfg_path.exists() else {}


def slugify(value: str) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")
    return text[:48]


def parse_twitch_channel(target: str) -> str:
    value = str(target or "").strip().lower()
    if "twitch.tv/" in value:
        value = value.split("twitch.tv/", 1)[1]
    value = value.split("/")[0].split("?")[0].split("#")[0].strip("@")
    if not re.fullmatch(r"[a-z0-9_]{1,64}", value):
        raise ValueError(f"无效的 Twitch 频道: {target}")
    return value


def parse_youtube_target(target: str) -> tuple[str, str, str | None]:
    """返回 (mode, channel_id_or_video_id, handle)。"""
    value = str(target or "").strip()
    if "watch?v=" in value:
        video_id = value.split("watch?v=", 1)[1].split("&")[0]
        return "video", video_id, None
    m = re.search(r"youtu\.be/([A-Za-z0-9_-]+)", value, re.I)
    if m:
        return "video", m.group(1), None
    if "/channel/" in value:
        return "channel", value.split("/channel/", 1)[1].split("/")[0].split("?")[0], None
    m = re.search(r"(?:youtube\.com|youtu\.be)/@([A-Za-z0-9_.-]+)", value)
    if m:
        return "channel", None, m.group(1)
    if value.startswith("@"):
        return "channel", None, value[1:]
    if value.startswith("UC") and len(value) == 24:
        return "channel", value, None
    raise ValueError("无效的 YouTube 目标，支持 @handle、/channel/UC...、watch?v=...")


def youtube_author_name(video_id: str) -> str | None:
    """通过 YouTube oEmbed（无需 API key）获取视频所属频道名。"""
    try:
        resp = requests.get(
            "https://www.youtube.com/oembed",
            params={"url": f"https://www.youtube.com/watch?v={video_id}", "format": "json"},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json().get("author_name")
    except Exception:
        return None


# ============================================================ YouTube 采集


class YoutubeMonitor:
    """单个 YouTube 任务：频道/视频的实时 CCV 采集。"""

    def __init__(self, cfg: dict[str, Any], api_key: str):
        self.cfg = cfg
        self.api_key = api_key
        self.data_dir = BASE_DIR / cfg["data_dir"]
        self.streams_dir = self.data_dir / "streams"
        self.sessions_dir = self.data_dir / "sessions"
        for d in (self.data_dir, self.streams_dir, self.sessions_dir):
            d.mkdir(parents=True, exist_ok=True)
        self.state_path = self.data_dir / "state.json"
        self.summary_path = self.data_dir / "streams_summary.csv"
        self.raw_path = self.data_dir / "raw.jsonl"
        self.heartbeat_path = self.data_dir / "heartbeat.json"
        self.stop_flag = self.data_dir / "stop.flag"
        self.pause_flag = self.data_dir / "pause.flag"
        self.state: dict[str, Any] = {"streams": {}}
        self.channel_id = cfg.get("channel_id")
        self.channel_handle = cfg.get("channel_handle")
        self.video_id = cfg.get("video_id")
        self.mode = cfg.get("mode", "channel")
        self._search_next = 0.0
        self._stopping = False
        self._normal_exit = False
        self._session_status = "running"
        self.session_id = ""
        self.session_dir: Path | None = None
        self.session_raw_path: Path | None = None
        self.session_started_at: datetime | None = None
        self.last_live_count = 0
        self.load_state()
        self.begin_session()

    # ---------- 基础 ----------
    def yt_get(self, path: str, params: dict[str, Any]) -> dict[str, Any]:
        resp = requests.get(
            f"https://www.googleapis.com/youtube/v3{path}",
            params={**params, "key": self.api_key},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def load_state(self) -> None:
        if self.state_path.exists():
            try:
                self.state = json.loads(self.state_path.read_text(encoding="utf-8"))
            except Exception:
                self.state = {"streams": {}}

    def save_state(self) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.state_path)

    def begin_session(self) -> None:
        self.session_id = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
        self.session_dir = self.sessions_dir / self.session_id
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.session_raw_path = self.session_dir / "raw.jsonl"
        self.session_raw_path.touch(exist_ok=True)
        self.session_started_at = utcnow()
        logging.info("[%s] 会话开始 %s", self.cfg["task_id"], self.session_id)

    def end_session(self) -> None:
        if not self.session_dir:
            return
        meta = {
            "session_id": self.session_id,
            "started_at_utc": (self.session_started_at or utcnow()).isoformat(),
            "ended_at_utc": utcnow().isoformat(),
            "status": self._session_status,
            "live_count": self.last_live_count,
            "tracked_total": len(self.state["streams"]),
            "streams": [
                {
                    "stream_id": s.get("stream_id", ""),
                    "channel": s.get("channel", ""),
                    "status": s.get("status", ""),
                    "peak_ccv": s.get("peak_ccv", ""),
                    "avg_ccv": s.get("avg_ccv", ""),
                    "sample_count": s.get("sample_count", ""),
                }
                for s in self.state["streams"].values()
            ],
        }
        (self.session_dir / "meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        if self.summary_path.exists():
            shutil.copy2(self.summary_path, self.session_dir / "streams_summary.csv")
        logging.info("[%s] 会话结束 %s", self.cfg["task_id"], self.session_id)

    # ---------- 存储 ----------
    def append_raw(self, row: dict[str, Any]) -> None:
        row = dict(row)
        row["session_id"] = self.session_id
        row["platform"] = "youtube"
        line = json.dumps(row, ensure_ascii=False) + "\n"
        with self.raw_path.open("a", encoding="utf-8") as fh:
            fh.write(line)
        if self.session_raw_path:
            with self.session_raw_path.open("a", encoding="utf-8") as fh:
                fh.write(line)

    def stream_csv_path(self, channel: str, stream_id: str) -> Path:
        return self.streams_dir / f"{channel}_{stream_id}.csv"

    def write_summary(self) -> None:
        with self.summary_path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=SUMMARY_COLUMNS)
            writer.writeheader()
            for stream in self.state["streams"].values():
                if stream.get("status") == "live":
                    self.compute_summary(stream)
                writer.writerow({c: stream.get(c, "") for c in SUMMARY_COLUMNS})

    def compute_summary(self, stream: dict[str, Any]) -> dict[str, Any]:
        rows = stream.get("samples", [])
        viewers = [r["viewer_count"] for r in rows if r.get("viewer_count") is not None]
        duration_end = stream.get("ended_at_utc") or stream.get("last_seen_utc") or utcnow().isoformat()
        duration_min = 0.0
        try:
            duration_min = (
                datetime.fromisoformat(duration_end) - datetime.fromisoformat(stream.get("first_seen_utc", duration_end))
            ).total_seconds() / 60.0
        except Exception:
            duration_min = 0.0
        stream.update(
            {
                "peak_ccv": max(viewers) if viewers else "",
                "avg_ccv": round(sum(viewers) / len(viewers), 1) if viewers else "",
                "min_ccv": min(viewers) if viewers else "",
                "sample_count": len(viewers),
                "duration_min": round(duration_min, 1),
                "last_seen_utc": rows[-1]["ts_utc"] if rows else stream.get("last_seen_utc", ""),
            }
        )
        return stream

    def write_heartbeat(self, now: datetime, live_count: int) -> None:
        duration_minutes = int(self.cfg.get("monitor_duration_minutes", 0) or 0)
        remaining = None
        if duration_minutes > 0 and self.session_started_at:
            remaining = max(0, int(duration_minutes * 60 - (now - self.session_started_at).total_seconds()))
        heartbeat = {
            "task_id": self.cfg["task_id"],
            "platform": "youtube",
            "session_id": self.session_id,
            "pid": os.getpid(),
            "last_tick_utc": now.isoformat(),
            "session_started_at_utc": (self.session_started_at or now).isoformat(),
            "duration_minutes": duration_minutes,
            "remaining_seconds": remaining,
            "live_count": live_count,
            "tracked_total": len(self.state["streams"]),
            "poll_interval_seconds": self.cfg.get("poll_interval_seconds", 30),
            "video_id": self.video_id or "",
        }
        tmp = self.heartbeat_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(heartbeat, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.heartbeat_path)

    def finalize_stream(self, stream_id: str, status: str) -> None:
        stream = self.state["streams"].get(stream_id)
        if not stream:
            return
        stream["status"] = status
        if status != "live":
            stream["ended_at_utc"] = utcnow().isoformat()
        self.compute_summary(stream)
        self.write_summary()
        logging.info(
            "[%s][下播] %s 峰值=%s 均值=%s",
            self.cfg["task_id"],
            stream.get("channel"),
            stream.get("peak_ccv"),
            stream.get("avg_ccv"),
        )

    # ---------- 采集 ----------
    def resolve_channel(self) -> None:
        if self.channel_id:
            return
        if self.channel_handle:
            data = self.yt_get("/channels", {"part": "id,snippet", "forHandle": self.channel_handle})
        else:
            raise ValueError("YouTube 频道任务缺少 channel_id/handle")
        items = data.get("items", [])
        if not items:
            raise ValueError(f"找不到 YouTube 频道: {self.channel_handle}")
        self.channel_id = items[0]["id"]
        self.cfg["channel_id"] = self.channel_id
        logging.info("[%s] YouTube 频道解析成功: %s -> %s", self.cfg["task_id"], self.channel_handle, self.channel_id)

    def find_live_video(self) -> str | None:
        data = self.yt_get(
            "/search",
            {
                "part": "id",
                "channelId": self.channel_id,
                "eventType": "live",
                "type": "video",
                "maxResults": 1,
            },
        )
        items = data.get("items", [])
        return items[0]["id"]["videoId"] if items else None

    def video_details(self, video_id: str) -> dict[str, Any] | None:
        data = self.yt_get("/videos", {"part": "snippet,liveStreamingDetails", "id": video_id})
        items = data.get("items", [])
        return items[0] if items else None

    def tick(self) -> None:
        now = utcnow()
        if self.mode == "channel":
            self.resolve_channel()
            if not self.video_id or time.monotonic() >= self._search_next:
                try:
                    found = self.find_live_video()
                    if found:
                        self.video_id = found
                        logging.info("[%s] 发现直播视频 %s", self.cfg["task_id"], found)
                    self._search_next = time.monotonic() + 300
                except Exception as exc:  # noqa: BLE001
                    logging.warning("[%s] 搜索直播视频失败: %s", self.cfg["task_id"], exc)
                    self._search_next = time.monotonic() + 60
        elif not self.video_id:
            self.video_id = self.cfg.get("video_id")

        if not self.video_id:
            for sid in list(self.state["streams"].keys()):
                if self.state["streams"][sid].get("status") == "live":
                    self.finalize_stream(sid, "ended")
            self.last_live_count = 0
            self.write_heartbeat(now, 0)
            self.save_state()
            self.write_summary()
            logging.info("[%s] 当前无直播视频", self.cfg["task_id"])
            return

        details = self.video_details(self.video_id)
        lsd = (details or {}).get("liveStreamingDetails") or {}
        viewers_raw = lsd.get("concurrentViewers")
        if not details or viewers_raw is None:
            if self.state["streams"].get(self.video_id, {}).get("status") == "live":
                self.finalize_stream(self.video_id, "ended")
            self.video_id = None
            self._search_next = time.monotonic() + 60
            self.last_live_count = 0
            self.write_heartbeat(now, 0)
            self.save_state()
            self.write_summary()
            logging.info("[%s] 直播已结束/离线", self.cfg["task_id"])
            return

        viewer = int(viewers_raw)
        channel = str((details or {}).get("snippet", {}).get("channelTitle", "") or self.cfg.get("name", "youtube"))
        channel_slug = slugify(channel) or "youtube"
        title = str((details or {}).get("snippet", {}).get("title", ""))
        stream_id = self.video_id
        raw_row = {
            "ts_utc": now.isoformat(),
            "stream_id": stream_id,
            "channel": channel_slug,
            "display_name": channel,
            "title": title,
            "viewer_count": viewer,
            "started_at_utc": lsd.get("actualStartTime", ""),
            "language": "",
            "game_id": "",
            "game_name": self.cfg.get("game_name", ""),
            "tags": "[]",
        }
        self.append_raw(raw_row)

        stream = self.state["streams"].get(stream_id)
        if stream is None:
            stream = {
                "stream_id": stream_id,
                "channel": channel_slug,
                "display_name": channel,
                "title": title,
                "language": "",
                "started_at_utc": lsd.get("actualStartTime", ""),
                "first_seen_utc": now.isoformat(),
                "status": "live",
                "samples": [],
            }
            self.state["streams"][stream_id] = stream
            path = self.stream_csv_path(channel_slug, stream_id)
            with path.open("w", encoding="utf-8-sig", newline="") as fh:
                csv.writer(fh).writerow(STREAM_CSV_COLUMNS)
            logging.info("[%s][开播] %s CCV=%s", self.cfg["task_id"], channel, viewer)
        if stream.get("status") != "live":
            stream["status"] = "live"
            stream["ended_at_utc"] = ""
            logging.info("[%s][续采] %s", self.cfg["task_id"], channel)
        stream["last_seen_utc"] = now.isoformat()
        stream["title"] = title
        stream["samples"].append(
            {"ts_utc": now.isoformat(), "viewer_count": viewer, "title": title, "language": "", "game_name": "", "tags": "[]"}
        )
        if len(stream["samples"]) > 2000:
            stream["samples"] = stream["samples"][-2000:]
        with self.stream_csv_path(channel_slug, stream_id).open("a", encoding="utf-8", newline="") as fh:
            csv.writer(fh).writerow([now.isoformat(), str(viewer), title.replace("\n", " "), "", "", "[]"])

        self.last_live_count = 1
        self.save_state()
        self.write_summary()
        self.write_heartbeat(now, 1)

    def stop(self, status: str = "interrupted") -> None:
        if self._stopping:
            return
        self._stopping = True
        self._session_status = status
        for sid in list(self.state["streams"].keys()):
            if self.state["streams"][sid].get("status") == "live":
                self.finalize_stream(sid, "interrupted")
        self.save_state()

    def run(self) -> None:
        interval = max(5, int(self.cfg.get("poll_interval_seconds", 30)))
        duration_minutes = int(self.cfg.get("monitor_duration_minutes", 0) or 0)
        duration_secs = duration_minutes * 60 if duration_minutes > 0 else 0
        session_started = time.monotonic()
        for flag in (self.stop_flag, self.pause_flag):
            if flag.exists():
                try:
                    flag.unlink()
                except OSError:
                    pass
        while not self._stopping:
            if self.stop_flag.exists():
                logging.info("[%s] 收到停止标记", self.cfg["task_id"])
                self.stop(status="stopped")
                try:
                    self.stop_flag.unlink()
                except OSError:
                    pass
                break
            if self.pause_flag.exists():
                time.sleep(2)
                continue
            started = time.monotonic()
            try:
                self.tick()
            except Exception as exc:  # noqa: BLE001
                logging.exception("[%s] 本轮异常: %s", self.cfg["task_id"], exc)
            elapsed_total = time.monotonic() - session_started
            if duration_secs > 0 and elapsed_total >= duration_secs:
                logging.info("[%s] 达到监测时长 %s 分钟", self.cfg["task_id"], duration_minutes)
                self._normal_exit = True
                self.stop(status="completed")
                break
            if duration_secs > 0 and self.last_live_count == 0:
                logging.info("[%s] 无直播，提前结束", self.cfg["task_id"])
                self._normal_exit = True
                self.stop(status="completed")
                break
            elapsed = time.monotonic() - started
            if self._stopping:
                break
            time.sleep(max(1, interval - elapsed))
        self.end_session()


# ============================================================ 任务管理


class TaskManager:
    def __init__(self, twitch_creds: dict[str, str], youtube_key: str):
        self.tasks_path = BASE_DIR / "data" / "tasks.json"
        self.tasks: dict[str, dict[str, Any]] = {}
        self.threads: dict[str, threading.Thread] = {}
        self.lock = threading.Lock()
        self.twitch_creds = twitch_creds
        self.youtube_key = youtube_key
        self.load()

    def load(self) -> None:
        if self.tasks_path.exists():
            try:
                data = json.loads(self.tasks_path.read_text(encoding="utf-8"))
                self.tasks = {t["id"]: t for t in data.get("tasks", [])}
            except Exception:
                self.tasks = {}
        if not self.tasks:
            default_id = "twitch_ww_default"
            self.tasks[default_id] = {
                "id": default_id,
                "name": "鸣潮Twitch(默认)",
                "platform": "twitch",
                "mode": "game",
                "game_name": "Wuthering Waves",
                "game_id": 209738946,
                "channels": [],
                "max_streams": 3,
                "poll_interval_seconds": 30,
                "monitor_duration_minutes": 0,
                "status": "running",
                "created_at": utcnow().isoformat(),
            }
            self.save()
        for task in list(self.tasks.values()):
            if task.get("status") == "running":
                self.start_thread(task["id"])

    def save(self) -> None:
        self.tasks_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.tasks_path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps({"tasks": list(self.tasks.values())}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        os.replace(tmp, self.tasks_path)

    def task_dir(self, task_id: str) -> Path:
        return BASE_DIR / "data" / task_id

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        platform = str(payload.get("platform", "twitch")).lower()
        mode = str(payload.get("mode", "")).lower()
        target = str(payload.get("target", "")).strip()
        name = str(payload.get("name", "")).strip() or target
        if platform not in ("twitch", "youtube"):
            raise ValueError("platform 只支持 twitch/youtube")
        task: dict[str, Any] = {
            "platform": platform,
            "mode": mode,
            "name": name,
            "max_streams": max(1, int(payload.get("max_streams", 3) or 3)),
            "poll_interval_seconds": max(5, int(payload.get("poll_interval_seconds", 30) or 30)),
            "monitor_duration_minutes": max(0, int(payload.get("monitor_duration_minutes", 0) or 0)),
            "status": "running",
            "created_at": utcnow().isoformat(),
        }
        if platform == "twitch":
            if mode == "channel":
                task["channels"] = [parse_twitch_channel(target)]
                task["target"] = target
                # 规则: twitch_<主播login>_<HHMMSS>
                task_id = f"twitch_{slugify(task['channels'][0])}_{datetime.now().strftime('%H%M%S')}"
            else:
                task["mode"] = "game"
                task["game_name"] = target or "Wuthering Waves"
                task["target"] = task["game_name"]
                # 规则: twitch_game_<游戏slug>_<HHMMSS>
                task_id = f"twitch_game_{slugify(task['game_name'])}_{datetime.now().strftime('%H%M%S')}"
        else:
            if not self.youtube_key and not payload.get("dual"):
                raise ValueError("缺少 YOUTUBE_API_KEY，请在 .env 中配置")
            yt_mode, channel_id, handle = parse_youtube_target(target)
            if payload.get("dual") and (mode == "channel" or yt_mode == "channel"):
                raise ValueError("YouTube 双通道目前仅支持具体直播视频，不支持频道目标")
            if mode == "video" or yt_mode == "video":
                task["mode"] = "video"
                task["video_id"] = channel_id
                author = youtube_author_name(channel_id)
                if author:
                    task["channel_name"] = author
                if author and slugify(author):
                    # 规则: yt_<频道名slug>_<HHMMSS>
                    task_id = f"yt_{slugify(author)}_{datetime.now().strftime('%H%M%S')}"
                else:
                    # oEmbed 失败兜底: yt_video_<videoId>_<HHMMSS>
                    task_id = f"yt_video_{slugify(channel_id)[:12]}_{datetime.now().strftime('%H%M%S')}"
            else:
                task["mode"] = "channel"
                task["channel_id"] = channel_id
                task["channel_handle"] = handle
                task["target"] = target
                # 规则: yt_<handle或channelId slug>_<HHMMSS>
                task_id = f"yt_{slugify(handle or channel_id) or 'channel'}_{datetime.now().strftime('%H%M%S')}"
        task["id"] = task_id
        if payload.get("dual"):
            if task.get("mode") == "game":
                raise ValueError("dual 模式仅支持指定频道/视频，请使用直播间链接")
            task["dual"] = True
        with self.lock:
            self.tasks[task_id] = task
            self.save()
            self.start_thread(task_id)
        return task

    def start_thread(self, task_id: str) -> None:
        thread = threading.Thread(target=self._worker, args=(task_id,), daemon=True, name=f"task-{task_id}")
        self.threads[task_id] = thread
        thread.start()

    def _worker(self, task_id: str) -> None:
        task = self.tasks.get(task_id)
        if not task:
            return
        data_dir = f"data/{task_id}"
        try:
            if task.get("dual"):
                self._run_dual(task, data_dir)
            elif task["platform"] == "twitch":
                Monitor(self._api_cfg(task, data_dir), self._api_client(task, data_dir)).run()
            else:
                YoutubeMonitor(self._api_cfg(task, data_dir), self.youtube_key).run()
        except Exception as exc:  # noqa: BLE001
            logging.exception("[%s] 任务异常退出: %s", task_id, exc)
        finally:
            with self.lock:
                task["status"] = "stopped"
                task["stopped_at"] = utcnow().isoformat()
                self.save()
                self.threads.pop(task_id, None)
            logging.info("[%s] 任务线程结束", task_id)
            if load_config().get("auto_report_on_stop", True) and self.task_has_data(task_id):
                thread = threading.Thread(
                    target=self._run_report,
                    args=(task_id,),
                    daemon=True,
                    name=f"report-{task_id}",
                )
                REPORT_THREADS.append(thread)
                thread.start()

    def _api_client(self, task: dict[str, Any], data_dir: str) -> TwitchClient:
        return TwitchClient(
            self.twitch_creds["client_id"],
            self.twitch_creds["client_secret"],
            BASE_DIR / data_dir / "twitch_token.json",
        )

    def _api_cfg(self, task: dict[str, Any], data_dir: str) -> dict[str, Any]:
        if task["platform"] == "twitch":
            return {
                "task_id": task["id"],
                "data_dir": data_dir,
                "charts_dir": f"charts/{task['id']}",
                "game_name": task.get("game_name", "Wuthering Waves"),
                "game_id": task.get("game_id", 209738946),
                "languages": [],
                "channels": task.get("channels", []),
                "max_streams": task.get("max_streams", 3),
                "poll_interval_seconds": task.get("poll_interval_seconds", 30),
                "monitor_duration_minutes": task.get("monitor_duration_minutes", 0),
                "auto_charts_on_exit": False,
            }
        return {
            "task_id": task["id"],
            "data_dir": data_dir,
            "mode": task.get("mode", "channel"),
            "channel_id": task.get("channel_id"),
            "channel_handle": task.get("channel_handle"),
            "video_id": task.get("video_id"),
            "name": task.get("name", ""),
            "game_name": task.get("game_name", ""),
            "poll_interval_seconds": task.get("poll_interval_seconds", 30),
            "monitor_duration_minutes": task.get("monitor_duration_minutes", 0),
        }

    def _api_worker(self, task: dict[str, Any], cfg: dict[str, Any]) -> None:
        if task["platform"] == "twitch":
            Monitor(cfg, self._api_client(task, cfg["data_dir"])).run()
        else:
            YoutubeMonitor(cfg, self.youtube_key).run()

    def _run_dual(self, task: dict[str, Any], data_dir: str) -> None:
        task_dir = BASE_DIR / data_dir
        task_dir.mkdir(parents=True, exist_ok=True)
        interval = int(task.get("poll_interval_seconds", 30) or 30)
        stop_file = str(task_dir / "stop.flag")
        pause_file = str(task_dir / "pause.flag")
        cfg = self._api_cfg(task, data_dir)
        api_thread = None
        if task["platform"] == "youtube" and not self.youtube_key:
            logging.warning("[%s] 缺少 YOUTUBE_API_KEY，YouTube 双通道仅运行 Web 采集", task["id"])
        else:
            api_thread = threading.Thread(
                target=self._api_worker,
                args=(task, cfg),
                daemon=True,
                name=f"api-{task['id']}",
            )
            api_thread.start()
        if task["platform"] == "twitch":
            script = str(BASE_DIR / "twitch_web_pilot.js")
            target = (task.get("channels") or [""])[0] or task.get("target", "")
        else:
            script = str(BASE_DIR / "yt_web_pilot.js")
            target = task.get("video_id") or task.get("channel_id") or task.get("target", "")
        web_out = str(task_dir / "web")
        env = node_env()
        logging.info("[%s] dual 启动: API 线程 + Web(%s)", task["id"], script)
        proc = subprocess.Popen(
            [
                WEB_NODE_EXE,
                script,
                target,
                str(interval),
                "0",
                web_out,
                "--stop-file",
                stop_file,
                "--pause-file",
                pause_file,
            ],
            cwd=BASE_DIR,
            env=env,
        )
        if api_thread:
            api_thread.join()
            # API 结束后（stop 已触发），等 web 进程读到 stop 文件退出
            try:
                proc.wait(timeout=60)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass
        else:
            # 仅 Web 通道（如 YouTube 缺 API key）：等待 stop 文件出现
            stop_flag_path = task_dir / "stop.flag"
            while not stop_flag_path.exists():
                time.sleep(2)
            try:
                proc.wait(timeout=60)
            except Exception:
                try:
                    proc.kill()
                except Exception:
                    pass

    def task_has_data(self, task_id: str) -> bool:
        task_dir = self.task_dir(task_id)
        summary = task_dir / "streams_summary.csv"
        if summary.exists():
            try:
                with summary.open("r", encoding="utf-8-sig", newline="") as fh:
                    if sum(1 for _ in csv.DictReader(fh)) > 0:
                        return True
            except Exception:
                pass
        web_csv = task_dir / "web" / "streams.csv"
        if web_csv.exists():
            try:
                with web_csv.open("r", encoding="utf-8") as fh:
                    if sum(1 for _ in fh) > 1:
                        return True
            except Exception:
                pass
        return False

    def _run_report(self, task_id: str) -> None:
        try:
            import subprocess

            env = dict(os.environ)
            env["PYTHONIOENCODING"] = "utf-8"
            result = subprocess.run(
                [sys.executable, str(BASE_DIR / "report.py"), "--task", task_id],
                cwd=BASE_DIR,
                env=env,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=600,
            )
            if result.stdout:
                logging.info("[报告] %s", result.stdout.strip())
            if result.returncode != 0 and result.stderr:
                logging.warning("[报告失败] %s", result.stderr.strip()[-1000:])
        except Exception as exc:  # noqa: BLE001
            logging.exception("[报告失败] %s", exc)

    def control(self, task_id: str, action: str) -> dict[str, Any]:
        task = self.tasks.get(task_id)
        if not task:
            raise KeyError(f"任务不存在: {task_id}")
        task_dir = self.task_dir(task_id)
        task_dir.mkdir(parents=True, exist_ok=True)
        if action == "pause":
            (task_dir / "pause.flag").write_text("", encoding="utf-8")
            task["status"] = "paused"
        elif action == "resume":
            flag = task_dir / "pause.flag"
            if flag.exists():
                flag.unlink()
            stop_flag = task_dir / "stop.flag"
            if stop_flag.exists():
                stop_flag.unlink()
            task["status"] = "running"
            if task_id not in self.threads or not self.threads[task_id].is_alive():
                self.start_thread(task_id)
        elif action == "stop":
            (task_dir / "stop.flag").write_text("", encoding="utf-8")
            task["status"] = "stopping"
        else:
            raise ValueError(f"未知操作: {action}")
        self.save()
        return task

    def snapshot(self) -> list[dict[str, Any]]:
        result = []
        for task in self.tasks.values():
            item = dict(task)
            item["thread_alive"] = self.threads.get(task["id"]).is_alive() if self.threads.get(task["id"]) else False
            item["heartbeat"] = self.read_heartbeat(task["id"])
            item["live_streams"] = self.read_live_streams(task["id"])
            result.append(item)
        return result

    def read_heartbeat(self, task_id: str) -> dict[str, Any]:
        path = self.task_dir(task_id) / "heartbeat.json"
        if not path.exists():
            return {}
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def read_live_streams(self, task_id: str) -> list[dict[str, Any]]:
        summary_path = self.task_dir(task_id) / "streams_summary.csv"
        if not summary_path.exists():
            return []
        try:
            with summary_path.open("r", encoding="utf-8-sig", newline="") as fh:
                rows = list(csv.DictReader(fh))
        except Exception:
            return []
        streams = []
        for row in rows:
            if row.get("status") != "live":
                continue
            stream_id = row.get("stream_id", "")
            series = self.read_stream_tail(task_id, stream_id)
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
        return streams

    def read_stream_tail(self, task_id: str, stream_id: str, max_rows: int = 240) -> list[dict[str, Any]]:
        stream_dir = self.task_dir(task_id) / "streams"
        match = list(stream_dir.glob(f"*_{stream_id}.csv")) if stream_dir.exists() else []
        if not match:
            return []
        try:
            with match[0].open("r", encoding="utf-8-sig", newline="") as fh:
                rows = list(csv.DictReader(fh))
            out = []
            for row in rows[-max_rows:]:
                try:
                    out.append({"t": row.get("ts_utc", ""), "ccv": float(row.get("viewer_count", 0) or 0)})
                except Exception:
                    continue
            return out
        except Exception:
            return []

    def shutdown(self) -> None:
        for task in self.tasks.values():
            if task.get("status") in ("running", "paused", "stopping"):
                (self.task_dir(task["id"]) / "stop.flag").write_text("", encoding="utf-8")
        for thread in list(self.threads.values()):
            thread.join(timeout=30)
        self.save()


# ============================================================ 看板页面

PAGE = """<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>鸣潮 CCV 采集控制台</title>
<style>
  :root { --bg:#0f172a; --card:#1e293b; --line:#334155; --ok:#22c55e; --bad:#ef4444; --warn:#f59e0b; --text:#e2e8f0; --dim:#94a3b8; }
  * { box-sizing:border-box; }
  body { margin:0; background:var(--bg); color:var(--text); font-family:"Microsoft YaHei",system-ui,sans-serif; }
  .wrap { max-width:1200px; margin:0 auto; padding:20px; }
  h1 { font-size:20px; margin:0 0 4px; }
  .sub { color:var(--dim); font-size:13px; margin-bottom:16px; }
  .card { background:var(--card); border:1px solid var(--line); border-radius:10px; padding:14px; margin-bottom:14px; }
  .card h2 { font-size:14px; margin:0 0 8px; }
  .pill { display:inline-block; padding:2px 10px; border-radius:999px; font-size:12px; margin-right:6px; }
  .pill.ok { background:rgba(34,197,94,.15); color:var(--ok); }
  .pill.bad { background:rgba(239,68,68,.15); color:var(--bad); }
  .pill.warn { background:rgba(245,158,11,.15); color:var(--warn); }
  .pill.info { background:rgba(148,163,184,.15); color:var(--dim); }
  .mono { font-family:Consolas,monospace; font-size:12px; color:var(--dim); }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th,td { text-align:left; padding:7px 6px; border-bottom:1px solid var(--line); }
  th { color:var(--dim); font-weight:500; }
  .spark { width:160px; height:40px; display:block; }
  button { background:#2563eb; color:#fff; border:0; border-radius:6px; padding:5px 12px; cursor:pointer; margin-right:6px; font-size:13px; }
  button.warn2 { background:#d97706; }
  button.danger { background:#dc2626; }
  button:disabled { opacity:.45; cursor:not-allowed; }
  input,select { background:#0f172a; color:var(--text); border:1px solid var(--line); border-radius:6px; padding:7px 10px; font-size:13px; margin:2px 6px 2px 0; }
  label { font-size:12px; color:var(--dim); }
  .row { display:flex; flex-wrap:wrap; align-items:center; gap:6px; }
  #refreshAt { color:var(--dim); font-size:12px; }
</style>
</head>
<body>
<div class="wrap">
  <h1>鸣潮直播实时 CCV 控制台</h1>
  <div class="sub">Twitch + YouTube 多任务 · 自动刷新（10 秒） · <span id="refreshAt"></span></div>

  <div class="card">
    <h2>添加采集任务</h2>
    <div class="row">
      <select id="platform">
        <option value="twitch">Twitch</option>
        <option value="youtube">YouTube</option>
      </select>
      <select id="mode">
        <option value="game">游戏分类（自动选最高CCV）</option>
        <option value="channel">指定频道/直播间链接</option>
      </select>
      <input id="target" size="34" placeholder="游戏名 / 频道或直播间链接" />
      <input id="name" size="14" placeholder="任务名(可选)" />
      <label>最多跟踪 <input id="maxStreams" type="number" value="3" min="1" max="20" style="width:64px" /></label>
      <label>时长(分) <input id="duration" type="number" value="0" min="0" style="width:64px" /></label>
      <label>间隔(秒) <input id="interval" type="number" value="30" min="5" style="width:64px" /></label>
      <button onclick="addTask()">开始任务</button>
    </div>
    <div class="mono" style="margin-top:6px">示例：Twitch 频道 https://www.twitch.tv/tenz ；YouTube 频道 https://www.youtube.com/@handle 或视频 https://www.youtube.com/watch?v=ID</div>
  </div>

  <div id="taskList"></div>
</div>

<script>
const TZ = "Asia/Singapore";
function esc(s){ return String(s ?? "").replace(/[&<>"']/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c])); }
function fmt(iso){ if(!iso) return "-"; return new Date(iso).toLocaleString("zh-CN",{timeZone:TZ,hour12:false}); }
function age(sec){ if(sec==null) return "-"; if(sec<60) return Math.round(sec)+"秒前"; if(sec<3600) return Math.round(sec/60)+"分钟前"; return Math.round(sec/3600)+"小时前"; }
function pill(status){
  if(status==="running") return '<span class="pill ok">运行中</span>';
  if(status==="paused") return '<span class="pill warn">已暂停</span>';
  if(status==="stopping") return '<span class="pill warn">停止中</span>';
  return '<span class="pill bad">已停止</span>';
}
function spark(series){
  if(!series || series.length < 2) return '<span class="mono">数据不足</span>';
  const w=160,h=40,pad=2,vals=series.map(p=>p.ccv),max=Math.max(...vals,1),step=(w-pad*2)/(vals.length-1);
  const pts=vals.map((v,i)=>(pad+i*step).toFixed(1)+","+(h-pad-(v/max)*(h-pad*2)).toFixed(1)).join(" ");
  return '<svg class="spark" viewBox="0 0 '+w+' '+h+'" preserveAspectRatio="none"><polyline points="'+pts+'" fill="none" stroke="#38bdf8" stroke-width="1.5"/></svg>';
}
async function api(path, opts){
  const r = await fetch(path, opts);
  const data = await r.json().catch(()=>({}));
  if(!r.ok) throw new Error(data.error || ("HTTP "+r.status));
  return data;
}
async function load(){
  try{
    const data = await api("/api/tasks");
    document.getElementById("refreshAt").textContent = "上次刷新 " + new Date().toLocaleTimeString("zh-CN",{hour12:false});
    const list = document.getElementById("taskList");
    list.innerHTML = "";
    for(const t of data.tasks){
      const hb = t.heartbeat || {};
      const streams = t.live_streams || [];
      const div = document.createElement("div");
      div.className = "card";
      div.innerHTML = `
        <h2>${pill(t.status)} ${esc(t.name)} <span class="pill info">${t.platform}/${t.mode}</span> <span class="mono">${t.id}</span></h2>
        <div class="row" style="margin-bottom:8px">
          <span class="mono">PID ${hb.pid || "-"} · 上次查看 ${age(data.now_ms ? Math.max(0,(Date.now()-new Date(hb.last_tick_utc).getTime())/1000) : null)}</span>
          <span class="mono">会话 ${hb.session_id || "-"}</span>
          <span class="mono">在播 ${hb.live_count ?? 0} / 累计 ${hb.tracked_total ?? 0}</span>
          ${hb.remaining_seconds!=null ? `<span class="mono">剩余 ${Math.ceil(hb.remaining_seconds/60)} 分钟</span>` : ""}
        </div>
        <div class="row" style="margin-bottom:8px">
          <button ${t.status==="running"?"":"disabled"} onclick="ctrl('${t.id}','pause')">暂停</button>
          <button class="warn2" ${t.status==="paused"?"":"disabled"} onclick="ctrl('${t.id}','resume')">继续</button>
          <button class="danger" ${t.status==="stopping"?"disabled":""} onclick="ctrl('${t.id}','stop')">停止</button>
        </div>
        <table><thead><tr><th>主播</th><th>当前CCV</th><th>峰值</th><th>均值</th><th>采样</th><th>时长(分)</th><th>曲线</th></tr></thead>
        <tbody>${streams.length===0 ? '<tr><td colspan="7" class="mono">暂无跟踪中的直播</td></tr>' : streams.map(s=>`
          <tr><td><b>${esc(s.channel)}</b></td><td>${s.current_ccv ?? "-"}</td><td>${s.peak_ccv ?? "-"}</td><td>${s.avg_ccv ?? "-"}</td><td>${s.sample_count ?? 0}</td><td>${s.duration_min ?? "-"}</td><td>${spark(s.series)}</td></tr>`).join("")}
        </tbody></table>`;
      list.appendChild(div);
    }
  }catch(e){
    document.getElementById("taskList").innerHTML = '<div class="card"><span class="mono">加载失败: '+esc(e.message)+'</span></div>';
  }
}
async function ctrl(id, action){
  try{ await api("/api/tasks/"+encodeURIComponent(id)+"/"+action, {method:"POST"}); load(); }
  catch(e){ alert(e.message); }
}
async function addTask(){
  const payload = {
    platform: document.getElementById("platform").value,
    mode: document.getElementById("mode").value,
    target: document.getElementById("target").value.trim(),
    name: document.getElementById("name").value.trim(),
    max_streams: Number(document.getElementById("maxStreams").value),
    monitor_duration_minutes: Number(document.getElementById("duration").value),
    poll_interval_seconds: Number(document.getElementById("interval").value),
  };
  if(!payload.target){ alert("请填写目标"); return; }
  try{
    await api("/api/tasks", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(payload)});
    document.getElementById("target").value=""; document.getElementById("name").value="";
    load();
  }catch(e){ alert(e.message); }
}
document.getElementById("platform").addEventListener("change", ()=>{
  const p = document.getElementById("platform").value;
  const m = document.getElementById("mode");
  m.innerHTML = p==="twitch"
    ? '<option value="game">游戏分类（自动选最高CCV）</option><option value="channel">指定频道/直播间链接</option>'
    : '<option value="channel">指定频道</option><option value="video">指定直播视频</option>';
  document.getElementById("target").placeholder = p==="twitch" ? "游戏名 / 频道或直播间链接" : "YouTube 频道或视频链接";
});
load();
setInterval(load, 10000);
</script>
</body>
</html>
"""


class Handler(BaseHTTPRequestHandler):
    manager: TaskManager

    def log_message(self, *args):
        pass

    def _json(self, payload: Any, code: int = 200) -> None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

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
        if path == "/api/tasks":
            self._json({"tasks": self.manager.snapshot(), "now_ms": int(time.time() * 1000)})
            return
        self._json({"error": "not found"}, 404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        if path == "/api/tasks":
            try:
                task = self.manager.create(self._body())
                self._json(task, 201)
            except Exception as exc:  # noqa: BLE001
                self._json({"error": str(exc)}, 400)
            return
        match = re.fullmatch(r"/api/tasks/([^/]+)/(pause|resume|stop)", path)
        if match:
            try:
                task = self.manager.control(match.group(1), match.group(2))
                self._json(task)
            except KeyError as exc:
                self._json({"error": str(exc)}, 404)
            except Exception as exc:  # noqa: BLE001
                self._json({"error": str(exc)}, 400)
            return
        self._json({"error": "not found"}, 404)


def main() -> None:
    parser = argparse.ArgumentParser(description="鸣潮 CCV 采集守护 + 看板")
    parser.add_argument("--port", type=int, default=int(load_config().get("dashboard_port", 8088)))
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            TimedRotatingFileHandler(BASE_DIR / "server.log", when="midnight", backupCount=30, encoding="utf-8"),
        ],
    )

    env = load_env()
    twitch_creds = {
        "client_id": env.get("TWITCH_CLIENT_ID", ""),
        "client_secret": env.get("TWITCH_CLIENT_SECRET", ""),
    }
    if not twitch_creds["client_id"] or not twitch_creds["client_secret"]:
        logging.warning("未配置 Twitch 凭据，Twitch 任务将无法启动")
    youtube_key = env.get("YOUTUBE_API_KEY", "")
    if not youtube_key:
        logging.warning("未配置 YOUTUBE_API_KEY，YouTube 任务将无法创建（请在 .env 添加）")

    manager = TaskManager(twitch_creds, youtube_key)
    Handler.manager = manager
    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    logging.info("控制台已启动: http://127.0.0.1:%s/", args.port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        manager.shutdown()
        for thread in REPORT_THREADS:
            thread.join(timeout=300)


if __name__ == "__main__":
    main()
