#!/usr/bin/env python3
"""鸣潮直播实时 CCV 采集器（Twitch 官方 API）。

功能:
  - 按游戏分类轮询 Twitch /helix/streams，默认鸣潮 (Wuthering Waves)
  - 每轮把每个直播间的 viewer_count(CCV) 快照保存:
      data/raw/YYYY-MM-DD.jsonl        全字段原始快照（尽可能多的数据）
      data/streams/<channel>_<id>.csv  每场直播的 CCV 时序
      data/streams_summary.csv         每场直播汇总（峰值/均值/时长等）
      data/state.json                  直播注册表（重启续采不丢场次）
  - 自动识别开播/下播，下播后写入汇总
  - Ctrl+C 停止时把当前直播标记为“中断”(status=interrupted)

用法:
  python monitor.py            # 使用 config.json + .env
  python monitor.py --once     # 只采集一轮后退出（测试用）
  python monitor.py --interval 60
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
from typing import Any

import requests

BASE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BASE_DIR / "config.json"
ENV_PATH = BASE_DIR / ".env"

RAW_COLUMNS = [
    "ts_utc",
    "stream_id",
    "channel",
    "display_name",
    "title",
    "viewer_count",
    "started_at_utc",
    "language",
    "game_id",
    "game_name",
    "tags",
]

STREAM_CSV_COLUMNS = ["ts_utc", "viewer_count", "title", "language", "game_name", "tags"]

SUMMARY_COLUMNS = [
    "stream_id",
    "channel",
    "display_name",
    "title",
    "language",
    "started_at_utc",
    "first_seen_utc",
    "last_seen_utc",
    "ended_at_utc",
    "duration_min",
    "peak_ccv",
    "avg_ccv",
    "min_ccv",
    "sample_count",
    "status",
]


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def parse_env(path: Path) -> dict[str, str]:
    env: dict[str, str] = {}
    if not path.exists():
        return env
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        env[key.strip()] = value.strip().strip('"').strip("'")
    return env


def load_config() -> dict[str, Any]:
    cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    env = parse_env(ENV_PATH)
    cfg["client_id"] = os.environ.get("TWITCH_CLIENT_ID") or env.get("TWITCH_CLIENT_ID", "")
    cfg["client_secret"] = os.environ.get("TWITCH_CLIENT_SECRET") or env.get("TWITCH_CLIENT_SECRET", "")
    return cfg


class TwitchClient:
    """Twitch Helix 客户端：应用级 token + 自动刷新 + 重试。"""

    BASE = "https://api.twitch.tv/helix"
    TOKEN_URL = "https://id.twitch.tv/oauth2/token"

    def __init__(self, client_id: str, client_secret: str, token_path: Path):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_path = token_path
        self._token: str | None = None
        self._token_expires: float = 0.0

    def _read_cached_token(self) -> tuple[str, float] | None:
        if not self.token_path.exists():
            return None
        try:
            data = json.loads(self.token_path.read_text(encoding="utf-8"))
            return data["access_token"], float(data["expires_at"])
        except Exception:
            return None

    def _write_cached_token(self, token: str, expires_at: float) -> None:
        self.token_path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.token_path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"access_token": token, "expires_at": expires_at}), encoding="utf-8")
        os.replace(tmp, self.token_path)

    def access_token(self) -> str:
        now = time.time()
        if self._token and now < self._token_expires:
            return self._token
        cached = self._read_cached_token()
        if cached and now < cached[1]:
            self._token, self._token_expires = cached
            return self._token
        resp = requests.post(
            self.TOKEN_URL,
            params={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_expires = now + int(data.get("expires_in", 3600)) - 300
        self._write_cached_token(self._token, self._token_expires)
        return self._token

    def get(self, path: str, params: dict[str, Any] | None = None, retries: int = 3) -> dict[str, Any]:
        last_error: Exception | None = None
        for attempt in range(1, retries + 1):
            try:
                resp = requests.get(
                    f"{self.BASE}{path}",
                    params=params,
                    headers={
                        "Client-ID": self.client_id,
                        "Authorization": f"Bearer {self.access_token()}",
                    },
                    timeout=30,
                )
                if resp.status_code == 401:
                    # token 过期，清掉缓存重试一次
                    self._token = None
                    self._token_expires = 0.0
                    if self.token_path.exists():
                        self.token_path.unlink(missing_ok=True)
                    if attempt == retries:
                        resp.raise_for_status()
                    continue
                resp.raise_for_status()
                return resp.json()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt < retries:
                    time.sleep(attempt * 2)
        raise last_error  # type: ignore[misc]

    def resolve_game_id(self, game_name: str, fallback_id: int | None = None) -> int:
        try:
            data = self.get("/games", {"name": game_name})
            games = data.get("data", [])
            if games:
                return int(games[0]["id"])
        except Exception as exc:  # noqa: BLE001
            logging.warning("解析游戏 ID 失败: %s，使用配置中的 game_id", exc)
        if fallback_id:
            return int(fallback_id)
        raise RuntimeError(f"无法解析游戏 {game_name}，请在 config.json 填写 game_id")

    def get_live_streams(self, game_id: int, max_pages: int = 10) -> list[dict[str, Any]]:
        streams: list[dict[str, Any]] = []
        cursor: str | None = None
        for _ in range(max_pages):
            params: dict[str, Any] = {"game_id": str(game_id), "first": 100}
            if cursor:
                params["after"] = cursor
            data = self.get("/streams", params)
            streams.extend(data.get("data", []))
            cursor = (data.get("pagination") or {}).get("cursor")
            if not cursor or len(data.get("data", [])) < 100:
                break
        return streams


class Monitor:
    def __init__(self, cfg: dict[str, Any], client: TwitchClient):
        self.cfg = cfg
        self.client = client
        self.data_dir = BASE_DIR / cfg["data_dir"]
        self.charts_dir = BASE_DIR / cfg["charts_dir"]
        self.raw_dir = self.data_dir / "raw"
        self.streams_dir = self.data_dir / "streams"
        self.state_path = self.data_dir / "state.json"
        self.summary_path = self.data_dir / "streams_summary.csv"
        self.token_path = self.data_dir / "twitch_token.json"
        self.state: dict[str, Any] = {"streams": {}}
        self._game_id: int | None = None
        self._stopping = False
        self._normal_exit = False
        self._session_status = "running"
        self.session_id = ""
        self.session_dir: Path | None = None
        self.session_raw_path: Path | None = None
        self.session_started_at: datetime | None = None
        self.last_live_count = 0
        for d in (self.data_dir, self.raw_dir, self.streams_dir, self.charts_dir):
            d.mkdir(parents=True, exist_ok=True)

    # ---------- 会话 ----------
    def begin_session(self) -> None:
        self.session_id = datetime.now().astimezone().strftime("%Y%m%d_%H%M%S")
        self.session_dir = self.data_dir / "sessions" / self.session_id
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self.session_raw_path = self.session_dir / "raw.jsonl"
        self.session_raw_path.touch(exist_ok=True)
        self.session_started_at = utcnow()
        self._normal_exit = False
        self._session_status = "running"
        logging.info("采集会话开始: %s", self.session_id)

    def end_session(self) -> None:
        if not self.session_dir or not self.session_id:
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
        logging.info("会话 %s 结束: %s", self.session_id, self._session_status)

    # ---------- 状态 ----------
    def load_state(self) -> None:
        if self.state_path.exists():
            try:
                self.state = json.loads(self.state_path.read_text(encoding="utf-8"))
            except Exception as exc:  # noqa: BLE001
                logging.warning("读取 state.json 失败，重置状态: %s", exc)
                self.state = {"streams": {}}

    def save_state(self) -> None:
        tmp = self.state_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.state, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.state_path)

    # ---------- 存储 ----------
    def append_raw(self, row: dict[str, Any]) -> None:
        row = dict(row)
        row["session_id"] = self.session_id
        line = json.dumps(row, ensure_ascii=False) + "\n"
        day = utcnow().strftime("%Y-%m-%d")
        with (self.raw_dir / f"{day}.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(line)
        if self.session_raw_path:
            with self.session_raw_path.open("a", encoding="utf-8") as fh:
                fh.write(line)

    def stream_csv_path(self, channel: str, stream_id: str) -> Path:
        return self.streams_dir / f"{channel}_{stream_id}.csv"

    def start_stream_csv(self, channel: str, stream_id: str) -> None:
        path = self.stream_csv_path(channel, stream_id)
        if not path.exists():
            with path.open("w", encoding="utf-8-sig", newline="") as fh:
                csv.writer(fh).writerow(STREAM_CSV_COLUMNS)

    def append_stream_row(self, channel: str, stream_id: str, row: list[str]) -> None:
        path = self.stream_csv_path(channel, stream_id)
        with path.open("a", encoding="utf-8", newline="") as fh:
            csv.writer(fh).writerow(row)

    def write_summary(self) -> None:
        with self.summary_path.open("w", encoding="utf-8-sig", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=SUMMARY_COLUMNS)
            writer.writeheader()
            for stream in self.state["streams"].values():
                if stream.get("status") == "live":
                    self._compute_summary(stream, utcnow())
                writer.writerow({c: stream.get(c, "") for c in SUMMARY_COLUMNS})

    def write_heartbeat(self, now: datetime, live_count: int) -> None:
        duration_minutes = int(self.cfg.get("monitor_duration_minutes", 0) or 0)
        remaining_seconds = None
        if duration_minutes > 0 and self.session_started_at:
            elapsed = (now - self.session_started_at).total_seconds()
            remaining_seconds = max(0, int(duration_minutes * 60 - elapsed))
        heartbeat = {
            "session_id": self.session_id,
            "pid": os.getpid(),
            "last_tick_utc": now.isoformat(),
            "session_started_at_utc": (self.session_started_at or now).isoformat(),
            "duration_minutes": duration_minutes,
            "remaining_seconds": remaining_seconds,
            "live_count": live_count,
            "tracked_total": len(self.state["streams"]),
            "poll_interval_seconds": self.cfg.get("poll_interval_seconds", 30),
        }
        tmp = self.data_dir / "heartbeat.json.tmp"
        tmp.write_text(json.dumps(heartbeat, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.data_dir / "heartbeat.json")

    # ---------- 生命周期 ----------
    def _compute_summary(self, stream: dict[str, Any], now: datetime) -> dict[str, Any]:
        rows = stream.get("samples", [])
        viewers = [r["viewer_count"] for r in rows if r.get("viewer_count") is not None]
        duration_min = 0.0
        first_seen = stream.get("first_seen_utc", "")
        duration_end = (
            stream.get("ended_at_utc")
            or stream.get("last_seen_utc")
            or now.isoformat()
        )
        if first_seen:
            try:
                duration_min = (
                    datetime.fromisoformat(duration_end) - datetime.fromisoformat(first_seen)
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

    def finalize_stream(self, stream_id: str, status: str, now: datetime) -> None:
        stream = self.state["streams"].get(stream_id)
        if not stream:
            return
        stream["status"] = status
        if status != "live":
            stream["ended_at_utc"] = now.isoformat()
        self._compute_summary(stream, now)
        self.write_summary()
        logging.info(
            "[下播] %s (%s) 峰值=%s 均值=%s 时长=%s分",
            stream.get("channel"),
            stream_id,
            stream.get("peak_ccv"),
            stream.get("avg_ccv"),
            stream.get("duration_min"),
        )

    # ---------- 主循环 ----------
    def tick(self) -> None:
        now = utcnow()
        if self._game_id is None:
            self._game_id = self.client.resolve_game_id(self.cfg["game_name"], self.cfg.get("game_id"))
            logging.info("游戏解析成功: %s (id=%s)", self.cfg["game_name"], self._game_id)
        game_id = self._game_id
        languages = self.cfg.get("languages") or []
        channel_whitelist = [c.strip().lower() for c in (self.cfg.get("channels") or []) if c.strip()]
        max_streams = int(self.cfg.get("max_streams", 0) or 0)
        streams = self.client.get_live_streams(game_id, self.cfg.get("max_stream_pages", 10))
        if channel_whitelist:
            # 频道任务：直接按主播查询，不限于鸣潮分类
            all_streams: list[dict[str, Any]] = []
            for login in channel_whitelist:
                try:
                    data = self.client.get("/streams", {"user_login": login})
                    all_streams.extend(data.get("data", []))
                except Exception as exc:  # noqa: BLE001
                    logging.warning("查询频道 %s 失败: %s", login, exc)
            streams = all_streams

        # 语言过滤
        streams = [
            s
            for s in streams
            if not languages or not s.get("language") or s.get("language") in languages
        ]

        # 指定主播优先；否则只保留 CCV 最高的 max_streams 场
        if channel_whitelist:
            streams = [s for s in streams if s.get("user_login", "").lower() in channel_whitelist]
            selected = streams
        elif max_streams > 0:
            streams.sort(key=lambda s: s.get("viewer_count") or 0, reverse=True)
            candidates = {str(s["id"]): s for s in streams}
            keep: dict[str, Any] = {}
            for sid, stream in self.state["streams"].items():
                if stream.get("status") == "live" and sid in candidates:
                    keep[sid] = candidates.pop(sid)
            slots = max_streams - len(keep)
            if slots > 0:
                for sid in list(candidates.keys())[:slots]:
                    keep[sid] = candidates[sid]
            selected = list(keep.values())
        else:
            selected = streams

        seen_ids: set[str] = set()
        for s in selected:
            stream_id = str(s["id"])
            channel = s["user_login"]
            seen_ids.add(stream_id)
            lang = s.get("language", "")
            tags = json.dumps(s.get("tags", []), ensure_ascii=False)
            raw_row = {
                "ts_utc": now.isoformat(),
                "stream_id": stream_id,
                "channel": channel,
                "display_name": s.get("user_name", channel),
                "title": s.get("title", ""),
                "viewer_count": s.get("viewer_count"),
                "started_at_utc": s.get("started_at", ""),
                "language": lang,
                "game_id": s.get("game_id", ""),
                "game_name": s.get("game_name", ""),
                "tags": tags,
            }
            self.append_raw(raw_row)

            stream = self.state["streams"].get(stream_id)
            if stream is None:
                stream = {
                    "stream_id": stream_id,
                    "channel": channel,
                    "display_name": s.get("user_name", channel),
                    "title": s.get("title", ""),
                    "language": lang,
                    "started_at_utc": s.get("started_at", ""),
                    "first_seen_utc": now.isoformat(),
                    "status": "live",
                    "samples": [],
                }
                self.state["streams"][stream_id] = stream
                self.start_stream_csv(channel, stream_id)
                logging.info("[开播] %s (%s) CCV=%s", channel, stream_id, s.get("viewer_count"))
            if stream.get("status") != "live":
                stream["status"] = "live"
                stream["ended_at_utc"] = ""
                logging.info("[续采] %s (%s) 恢复直播中", channel, stream_id)
            stream["last_seen_utc"] = now.isoformat()
            stream["title"] = s.get("title", stream.get("title", ""))
            stream["language"] = lang
            stream["samples"].append(
                {
                    "ts_utc": now.isoformat(),
                    "viewer_count": s.get("viewer_count"),
                    "title": s.get("title", ""),
                    "language": lang,
                    "game_name": s.get("game_name", ""),
                    "tags": tags,
                }
            )
            # 状态文件只保留最近 2000 个采样点，完整数据都在每场 CSV 里
            if len(stream["samples"]) > 2000:
                stream["samples"] = stream["samples"][-2000:]
            self.append_stream_row(
                channel,
                stream_id,
                [
                    now.isoformat(),
                    str(s.get("viewer_count", "")),
                    s.get("title", "").replace("\n", " "),
                    lang,
                    s.get("game_name", ""),
                    tags,
                ],
            )

        # 下播检测
        for stream_id in list(self.state["streams"].keys()):
            if stream_id not in seen_ids and self.state["streams"][stream_id].get("status") == "live":
                self.finalize_stream(stream_id, "ended", now)

        self.save_state()
        self.write_summary()
        live_count = sum(1 for s in self.state["streams"].values() if s.get("status") == "live")
        self.last_live_count = live_count
        self.write_heartbeat(now, live_count)
        logging.info("本轮完成: 在播 %s 场，累计记录 %s 场", live_count, len(self.state["streams"]))

    def stop(self, status: str = "interrupted", *_args: Any) -> None:
        if self._stopping:
            return
        self._stopping = True
        self._session_status = status
        now = utcnow()
        for stream_id in list(self.state["streams"].keys()):
            if self.state["streams"][stream_id].get("status") == "live":
                self.finalize_stream(stream_id, "interrupted", now)
        self.save_state()
        logging.info("已停止，当前直播标记为 interrupted")

    def run(self, once: bool = False) -> None:
        self.load_state()
        self.begin_session()
        stop_flag = BASE_DIR / "stop.flag"
        task_stop_flag = self.data_dir / "stop.flag"
        task_pause_flag = self.data_dir / "pause.flag"
        if stop_flag.exists():
            try:
                stop_flag.unlink()
                logging.info("清理了残留的 stop.flag")
            except OSError:
                pass
        for flag in (task_stop_flag, task_pause_flag):
            if flag.exists():
                try:
                    flag.unlink()
                    logging.info("清理了残留的任务控制标记: %s", flag.name)
                except OSError:
                    pass
        interval = max(5, int(self.cfg.get("poll_interval_seconds", 30)))
        duration_minutes = int(self.cfg.get("monitor_duration_minutes", 0) or 0)
        duration_secs = duration_minutes * 60 if duration_minutes > 0 else 0
        session_started = time.monotonic()
        seen_live = False
        offline_streak = 0
        while not self._stopping:
            if stop_flag.exists() or task_stop_flag.exists():
                logging.info("检测到 stop.flag，正在优雅停止...")
                self.stop(status="stopped")
                try:
                    stop_flag.unlink()
                except OSError:
                    pass
                try:
                    task_stop_flag.unlink()
                except OSError:
                    pass
                break
            if task_pause_flag.exists():
                time.sleep(2)
                continue
            started = time.monotonic()
            try:
                self.tick()
            except Exception as exc:  # noqa: BLE001
                logging.exception("本轮采集异常: %s", exc)
            # 开播守望: 开播自动采集（tick 已覆盖），连续 2 轮无人观看后自动停止
            if self.cfg.get("auto_stop_on_offline"):
                if self.last_live_count > 0:
                    seen_live = True
                    offline_streak = 0
                elif seen_live:
                    offline_streak += 1
                    if offline_streak >= 2:
                        logging.info("检测到下播，守望任务自动结束")
                        self._normal_exit = True
                        self.stop(status="completed")
                        break
            if once:
                self._normal_exit = True
                self._session_status = "completed"
                break
            elapsed_total = time.monotonic() - session_started
            if duration_secs > 0 and elapsed_total >= duration_secs:
                logging.info("达到监测时长 %s 分钟，正常结束", duration_minutes)
                self._normal_exit = True
                self.stop(status="completed")
                break
            if duration_secs > 0 and self.last_live_count == 0:
                logging.info("所有跟踪直播已结束，提前结束")
                self._normal_exit = True
                self.stop(status="completed")
                break
            elapsed = time.monotonic() - started
            sleep_for = max(1, interval - elapsed)
            if self._stopping:
                break
            time.sleep(sleep_for)
        self.end_session()
        if self._normal_exit and self.cfg.get("auto_charts_on_exit"):
            self.run_charts()

    def run_charts(self) -> None:
        logging.info("会话结束，自动生成图表...")
        env = dict(os.environ)
        env["PYTHONIOENCODING"] = "utf-8"
        try:
            result = subprocess.run(
                [sys.executable, str(BASE_DIR / "charts.py")],
                cwd=BASE_DIR,
                env=env,
                capture_output=True,
                text=True,
                timeout=300,
            )
            if result.stdout:
                logging.info("图表输出:\n%s", result.stdout.strip())
            if result.stderr:
                logging.warning("图表 stderr:\n%s", result.stderr.strip()[-2000:])
        except Exception as exc:  # noqa: BLE001
            logging.exception("生成图表失败: %s", exc)


def main() -> None:
    parser = argparse.ArgumentParser(description="鸣潮直播实时 CCV 采集器")
    parser.add_argument("--once", action="store_true", help="只采集一轮后退出")
    parser.add_argument("--interval", type=int, default=None, help="轮询间隔秒数，覆盖 config.json")
    parser.add_argument("--duration", type=int, default=None, help="监测时长（分钟），0=不限时")
    parser.add_argument("--auto-charts", action="store_true", help="结束后自动生成图表")
    args = parser.parse_args()

    cfg = load_config()
    if args.interval:
        cfg["poll_interval_seconds"] = args.interval
    if args.duration is not None:
        cfg["monitor_duration_minutes"] = args.duration
    if args.auto_charts:
        cfg["auto_charts_on_exit"] = True
    if not cfg.get("client_id") or not cfg.get("client_secret"):
        print("缺少 Twitch 凭据。请复制 .env.example 为 .env 并填写 TWITCH_CLIENT_ID / TWITCH_CLIENT_SECRET。")
        print("申请地址: https://dev.twitch.tv/console/apps")
        sys.exit(2)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            TimedRotatingFileHandler(
                BASE_DIR / cfg["log_file"],
                when="midnight",
                backupCount=30,
                encoding="utf-8",
            ),
        ],
    )
    logging.info("启动鸣潮实时 CCV 采集器")

    client = TwitchClient(cfg["client_id"], cfg["client_secret"], BASE_DIR / cfg["data_dir"] / "twitch_token.json")
    monitor = Monitor(cfg, client)
    signal.signal(signal.SIGINT, monitor.stop)
    signal.signal(signal.SIGTERM, monitor.stop)
    monitor.run(once=args.once)


if __name__ == "__main__":
    main()
