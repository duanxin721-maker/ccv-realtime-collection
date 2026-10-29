#!/usr/bin/env python3
"""终止任务后自动生成带图表的 xlsx 报告。

对每个有数据的任务：
  1. 读取 data/<task_id>/ 汇总、时序、原始快照
  2. 用 matplotlib 生成 PNG 图表
  3. 用 @oai/artifact-tool 构建 xlsx（数据 + 嵌入图片）

用法:
  python report.py --all
  python report.py --task twitch_ww_default
  python report.py --all --out 报告_20260806
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
BUILD_DIR = BASE_DIR / ".report_build"
TZ = ZoneInfo("Asia/Singapore")

NODE_EXE = Path(os.environ.get("CODEX_NODE_EXE") or shutil.which("node") or "node")
NODE_MODULES = Path(os.environ.get("CODEX_NODE_MODULES") or (BASE_DIR / "node_modules"))

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

for font in (r"C:\Windows\Fonts\msyh.ttc", r"C:\Windows\Fonts\msyhbd.ttc"):
    if Path(font).exists():
        fm.fontManager.addfont(font)
plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "Arial"]
plt.rcParams["axes.unicode_minus"] = False


def safe_title(value: str) -> str:
    out = []
    for ch in str(value or ""):
        code = ord(ch)
        if (
            0x20 <= code <= 0x7E
            or 0x4E00 <= code <= 0x9FFF
            or 0x3000 <= code <= 0x303F
            or 0xFF00 <= code <= 0xFFEF
            or 0x3040 <= code <= 0x30FF
            or 0x3400 <= code <= 0x4DBF
        ):
            out.append(ch)
        else:
            out.append("?")
    return "".join(out)


def to_local(iso: str) -> str:
    if not iso:
        return ""
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso


def load_tasks() -> dict[str, dict]:
    path = DATA_DIR / "tasks.json"
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return {t["id"]: t for t in data.get("tasks", [])}
    except Exception:
        return {}


def task_has_data(task_dir: Path) -> bool:
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


def prepare_json(task_dir: Path, task: dict, inter: Path) -> None:
    inter.mkdir(parents=True, exist_ok=True)
    with (task_dir / "streams_summary.csv").open("r", encoding="utf-8-sig", newline="") as fh:
        summary_rows = list(csv.DictReader(fh))

    series: list[dict] = []
    for row in summary_rows:
        sid = str(row["stream_id"])
        files = list((task_dir / "streams").glob(f"*_{sid}.csv"))
        if not files:
            continue
        with files[0].open("r", encoding="utf-8-sig", newline="") as fh:
            for s in csv.DictReader(fh):
                try:
                    ccv = float(s.get("viewer_count", 0) or 0)
                except Exception:
                    continue
                series.append(
                    {
                        "time_local": to_local(s.get("ts_utc", "")),
                        "channel": row["channel"],
                        "stream_id": sid,
                        "ccv": ccv,
                    }
                )

    raw_rows: list[dict] = []
    raw_dir = task_dir / "raw"
    if raw_dir.exists():
        for raw_path in sorted(raw_dir.glob("*.jsonl")):
            with raw_path.open("r", encoding="utf-8") as fh:
                for line in fh:
                    try:
                        raw_rows.append(json.loads(line))
                    except Exception:
                        continue

    summary_out = [
        {
            "stream_id": r.get("stream_id", ""),
            "channel": r.get("channel", ""),
            "display_name": r.get("display_name", ""),
            "status": r.get("status", ""),
            "sample_count": r.get("sample_count", ""),
            "peak_ccv": r.get("peak_ccv", ""),
            "avg_ccv": r.get("avg_ccv", ""),
            "min_ccv": r.get("min_ccv", ""),
            "duration_min": r.get("duration_min", ""),
            "first_seen_local": to_local(r.get("first_seen_utc", "")),
            "last_seen_local": to_local(r.get("last_seen_utc", "")),
            "title": r.get("title", ""),
            "language": r.get("language", ""),
        }
        for r in summary_rows
    ]
    meta = {
        "task_id": task.get("id", ""),
        "name": task.get("name", ""),
        "platform": task.get("platform", ""),
        "mode": task.get("mode", ""),
        "poll_interval_seconds": task.get("poll_interval_seconds", 30),
    }
    (inter / "summary.json").write_text(json.dumps(summary_out, ensure_ascii=False), encoding="utf-8")
    (inter / "series.json").write_text(json.dumps(series, ensure_ascii=False), encoding="utf-8")
    (inter / "raw.json").write_text(json.dumps(raw_rows, ensure_ascii=False), encoding="utf-8")
    (inter / "meta.json").write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return len(summary_out), len(series), len(raw_rows)


def make_charts(task_dir: Path, img_dir: Path) -> int:
    img_dir.mkdir(parents=True, exist_ok=True)
    with (task_dir / "streams_summary.csv").open("r", encoding="utf-8-sig", newline="") as fh:
        summary = list(csv.DictReader(fh))
    import pandas as pd

    summary = pd.DataFrame(summary).sort_values("peak_ccv", ascending=False)
    streams = []
    for _, row in summary.iterrows():
        sid = str(row["stream_id"])
        files = list((task_dir / "streams").glob(f"*_{sid}.csv"))
        if not files:
            continue
        df = pd.read_csv(files[0], encoding="utf-8-sig")
        df["ts_utc"] = pd.to_datetime(df["ts_utc"], utc=True)
        df["viewer_count"] = pd.to_numeric(df["viewer_count"], errors="coerce")
        df = df.dropna(subset=["viewer_count"])
        if df.empty:
            continue
        streams.append((df, dict(row)))

        local = df["ts_utc"].dt.tz_convert(TZ)
        fig, ax = plt.subplots(figsize=(11, 5), dpi=130)
        ax.plot(local, df["viewer_count"], linewidth=1.4, color="#1f77b4")
        ax.fill_between(local, df["viewer_count"], color="#1f77b4", alpha=0.15)
        ax.set_title(
            f"{safe_title(row['channel'])}  CCV 实时变化\n"
            f"峰值 {int(df['viewer_count'].max())} / 均值 {int(df['viewer_count'].mean())} / 采样 {len(df)} 次 / {row['status']}",
            fontsize=12,
        )
        ax.set_ylabel("CCV")
        ax.grid(alpha=0.3)
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%m-%d %H:%M", tz=TZ))
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(img_dir / f"{row['channel']}_{sid}.png")
        plt.close(fig)

    if streams:
        fig, ax = plt.subplots(figsize=(13, 7), dpi=130)
        for df, meta_row in streams:
            minutes = (df["ts_utc"] - df["ts_utc"].iloc[0]).dt.total_seconds() / 60.0
            ax.plot(
                minutes,
                df["viewer_count"],
                linewidth=1.3,
                label=f"{meta_row['channel']} (峰值 {int(df['viewer_count'].max())})",
            )
        ax.set_title("鸣潮直播 CCV 对比（按开播后分钟对齐）", fontsize=13)
        ax.set_xlabel("开播后分钟数")
        ax.set_ylabel("CCV")
        ax.grid(alpha=0.3)
        ax.legend(loc="best", fontsize=8)
        fig.tight_layout()
        fig.savefig(img_dir / "overview.png")
        plt.close(fig)

        fig, ax = plt.subplots(figsize=(11, 6), dpi=130)
        colors = ["#d62728" if s == "live" else "#1f77b4" for s in summary["status"]]
        bars = ax.bar(summary["channel"], summary["peak_ccv"], color=colors)
        for bar, value in zip(bars, summary["peak_ccv"]):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), str(int(value)), ha="center", va="bottom", fontsize=8)
        ax.set_title("各主播鸣潮直播峰值 CCV", fontsize=13)
        ax.set_ylabel("峰值 CCV")
        ax.grid(axis="y", alpha=0.3)
        fig.autofmt_xdate()
        fig.tight_layout()
        fig.savefig(img_dir / "summary.png")
        plt.close(fig)
    return len(streams)


def ensure_junction() -> Path:
    link = BUILD_DIR / "node_modules"
    BUILD_DIR.mkdir(parents=True, exist_ok=True)
    if not NODE_MODULES.exists():
        raise RuntimeError(
            f"找不到 Node modules: {NODE_MODULES}。请安装/准备 @oai/artifact-tool，"
            "或设置 CODEX_NODE_MODULES 指向包含该包的目录。"
        )
    if link.exists() and not NODE_MODULES.exists():
        shutil.rmtree(link, ignore_errors=True)
    if not link.exists():
        result = subprocess.run(
            ["cmd", "/c", "mklink", "/J", str(link), str(NODE_MODULES)],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"创建 node_modules junction 失败: {result.stderr}")
    return link


def ensure_builder() -> Path:
    ensure_junction()
    target = BUILD_DIR / "build_report.mjs"
    shutil.copy2(BASE_DIR / "build_report.mjs", target)
    return target


def build_task(task: dict, out_dir: Path) -> Path | None:
    task_id = task["id"]
    task_dir = DATA_DIR / task_id
    if not task_has_data(task_dir):
        print(f"[跳过] {task_id} 无数据")
        return None
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    run_name = safe_title(task.get("name") or task_id)
    run_dir = out_dir / f"{run_name}_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=True)
    has_api = False
    summary_path = task_dir / "streams_summary.csv"
    if summary_path.exists():
        try:
            with summary_path.open("r", encoding="utf-8-sig", newline="") as fh:
                has_api = sum(1 for _ in csv.DictReader(fh)) > 0
        except Exception:
            has_api = False
    if has_api:
        builder = ensure_builder()
        work = BUILD_DIR / task_id
        inter = work / "data"
        img = work / "imgs"
        if work.exists():
            shutil.rmtree(work, ignore_errors=True)
        inter.mkdir(parents=True, exist_ok=True)
        img.mkdir(parents=True, exist_ok=True)
        n_summary, n_series, n_raw = prepare_json(task_dir, task, inter)
        n_charts = make_charts(task_dir, img)
        out_xlsx = run_dir / f"{run_name}_数据图表_{stamp}.xlsx"
        env = dict(os.environ)
        env["PYTHONIOENCODING"] = "utf-8"
        result = subprocess.run(
            [str(NODE_EXE), str(builder), str(inter), str(img), str(out_xlsx)],
            cwd=BUILD_DIR,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=300,
        )
        if result.stdout:
            print(result.stdout.strip())
        if result.stderr:
            print("STDERR:", result.stderr.strip()[-1000:])
        if result.returncode != 0:
            print(f"[失败] {task_id}")
            return None
        inspect_file = Path(str(out_xlsx) + ".inspect.ndjson")
        if inspect_file.exists():
            inspect_file.unlink()
        shutil.rmtree(work, ignore_errors=True)
        print(f"[完成API] {task_id}: 汇总{n_summary} 时序{n_series} 原始{n_raw} 图表{n_charts}")
    else:
        n_summary = n_series = n_raw = n_charts = 0
    # Web 通道数据 → 第二个 xlsx（含内嵌图）
    web_csv = task_dir / "web" / "streams.csv"
    if web_csv.exists():
        try:
            with web_csv.open("r", encoding="utf-8") as fh:
                web_rows = sum(1 for _ in fh)
        except Exception:
            web_rows = 0
        if web_rows > 1:
            env2 = dict(os.environ)
            env2["PYTHONIOENCODING"] = "utf-8"
            result2 = subprocess.run(
                [
                    sys.executable,
                    str(BASE_DIR / "_build_web_xlsx.py"),
                    str(task_dir / "web"),
                    run_name,
                    str(run_dir),
                ],
                cwd=BASE_DIR,
                env=env2,
                capture_output=True,
                text=True,
                encoding="utf-8",
                errors="replace",
                timeout=300,
            )
            if result2.stdout:
                print(result2.stdout.strip())
            if result2.stderr:
                print("WEB_STDERR:", result2.stderr.strip()[-1000:])
            if result2.returncode != 0:
                print(f"[Web构建失败] {task_id}")
    copy_snapshot(task_id, run_dir / "数据")
    print(f"[完成] {task_id} -> {run_dir}")
    return run_dir


def copy_snapshot(task_id: str, dest: Path) -> None:
    src = DATA_DIR / task_id
    if not src.exists():
        return
    dest.mkdir(parents=True, exist_ok=True)
    for item in src.iterdir():
        if item.name in ("pause.flag", "stop.flag", "twitch_token.json"):
            continue
        target = dest / item.name
        if item.is_dir():
            shutil.copytree(item, target, dirs_exist_ok=True)
        else:
            shutil.copy2(item, target)


def main() -> None:
    parser = argparse.ArgumentParser(description="生成带图表的 xlsx 报告")
    parser.add_argument("--task", default=None, help="任务 ID")
    parser.add_argument("--all", action="store_true", help="所有有数据的任务")
    parser.add_argument("--out", default=None, help="输出目录（默认 报告_YYYYMMDD）")
    args = parser.parse_args()
    out_dir = Path(args.out) if args.out else BASE_DIR / "输出"
    out_dir.mkdir(parents=True, exist_ok=True)

    tasks = load_tasks()
    if args.task:
        selected = [tasks[args.task]] if args.task in tasks else []
        if not selected:
            print(f"任务不存在: {args.task}")
            sys.exit(1)
    elif args.all:
        selected = list(tasks.values())
    else:
        selected = [t for t in tasks.values() if t.get("status") == "stopped"]
    if not selected:
        print("没有需要生成报告的任务")
        return
    for task in selected:
        build_task(task, out_dir)


if __name__ == "__main__":
    main()
