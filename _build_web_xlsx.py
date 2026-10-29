#!/usr/bin/env python3
"""把网页试点数据（twitch_web_*）构建成带内嵌图表的 xlsx。"""

from __future__ import annotations

import csv
import json
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm

import report

BASE = Path(__file__).resolve().parent
TZ = ZoneInfo("Asia/Singapore")

for font in (r"C:\Windows\Fonts\msyh.ttc", r"C:\Windows\Fonts\msyhbd.ttc"):
    if Path(font).exists():
        fm.fontManager.addfont(font)
plt.rcParams["font.sans-serif"] = ["Microsoft YaHei", "Arial"]
plt.rcParams["axes.unicode_minus"] = False


def to_local(iso: str) -> str:
    if not iso:
        return ""
    try:
        return datetime.fromisoformat(iso.replace("Z", "+00:00")).astimezone(TZ).strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return iso


def main() -> None:
    src = Path(sys.argv[1])
    label = sys.argv[2]
    out_dir = Path(sys.argv[3])
    inter = report.BUILD_DIR / f"{label}_data"
    img = report.BUILD_DIR / f"{label}_imgs"
    for d in (inter, img):
        d.mkdir(parents=True, exist_ok=True)

    rows = []
    with (src / "streams.csv").open("r", encoding="utf-8") as fh:
        for row in csv.DictReader(fh):
            if row.get("viewer_count", "") == "":
                continue
            rows.append(
                {
                    "time_local": to_local(row["ts_utc"]),
                    "channel": label,
                    "stream_id": "web",
                    "ccv": float(row["viewer_count"]),
                }
            )
    meta = json.loads((src / "meta.json").read_text(encoding="utf-8"))
    raw_rows = []
    raw_path = src / "raw.jsonl"
    if raw_path.exists():
        for line in raw_path.read_text(encoding="utf-8").splitlines():
            try:
                raw_rows.append(json.loads(line))
            except Exception:
                continue
    summary = [
        {
            "stream_id": "web",
            "channel": label,
            "display_name": label,
            "status": meta.get("status", ""),
            "sample_count": meta.get("sample_count", ""),
            "peak_ccv": meta.get("peak_ccv", ""),
            "avg_ccv": meta.get("avg_ccv", ""),
            "min_ccv": meta.get("min_ccv", ""),
            "duration_min": "",
            "first_seen_local": to_local(meta.get("started_at_utc", "")),
            "last_seen_local": to_local(meta.get("ended_at_utc", "")),
            "title": f"{label} 网页实时观众数",
            "language": "",
        }
    ]
    meta_out = {
        "task_id": label,
        "name": label,
        "platform": "twitch_web",
        "mode": "channel",
        "poll_interval_seconds": meta.get("interval_seconds", 10),
    }
    (inter / "summary.json").write_text(json.dumps(summary, ensure_ascii=False), encoding="utf-8")
    (inter / "series.json").write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    (inter / "raw.json").write_text(json.dumps(raw_rows, ensure_ascii=False), encoding="utf-8")
    (inter / "meta.json").write_text(json.dumps(meta_out, ensure_ascii=False, indent=2), encoding="utf-8")

    times = [datetime.fromisoformat(r["time_local"]) for r in rows]
    values = [r["ccv"] for r in rows]
    fig, ax = plt.subplots(figsize=(12, 5.5), dpi=140)
    ax.plot(times, values, linewidth=1.5, color="#1f77b4")
    ax.fill_between(times, values, color="#1f77b4", alpha=0.15)
    ax.set_title(
        f"{label} 直播实时 CCV（网页前端，{meta.get('interval_seconds', 10)}秒/次，{meta.get('duration_minutes', 10)}分钟）\n"
        f"峰值 {meta.get('peak_ccv')} / 均值 {meta.get('avg_ccv')} / 最低 {meta.get('min_ccv')} / 采样 {meta.get('sample_count')} 次",
        fontsize=13,
    )
    ax.set_ylabel("CCV")
    ax.grid(alpha=0.3)
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%H:%M:%S"))
    fig.autofmt_xdate()
    fig.tight_layout()
    fig.savefig(img / f"{label}.png")
    plt.close(fig)

    builder = report.ensure_builder()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_xlsx = out_dir / f"{label}_网页实时CCV_数据图表_{stamp}.xlsx"
    env = dict(__import__("os").environ)
    env["PYTHONIOENCODING"] = "utf-8"
    result = subprocess.run(
        [str(report.NODE_EXE), str(builder), str(inter), str(img), str(out_xlsx)],
        cwd=report.BUILD_DIR,
        env=env,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=300,
    )
    print(result.stdout.strip())
    if result.stderr:
        print("STDERR:", result.stderr.strip()[-800:])
    inspect_file = Path(str(out_xlsx) + ".inspect.ndjson")
    if inspect_file.exists():
        inspect_file.unlink()
    print("SAVED", out_xlsx)


if __name__ == "__main__":
    main()
