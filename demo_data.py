#!/usr/bin/env python3
"""生成演示数据（无需 Twitch 凭据即可体验采集-存储-图表全流程）。

用法:
  python demo_data.py
  python charts.py
"""

from __future__ import annotations

import csv
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent

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


def ccv_curve(minutes: int, base: int, peak: int, rng: random.Random) -> list[int]:
    """模拟一场直播的 CCV：爬升-平台-波动，结尾回落。"""
    values = []
    for i in range(minutes):
        progress = i / max(minutes - 1, 1)
        if progress < 0.2:
            growth = progress / 0.2
        elif progress < 0.8:
            growth = 1.0
        else:
            growth = max(0.0, 1.0 - (progress - 0.8) / 0.2)
        target = base + (peak - base) * growth
        noise = rng.uniform(-0.08, 0.08)
        wave = rng.uniform(0, 60) if peak > 800 else rng.uniform(0, 15)
        values.append(max(1, int(target * (1 + noise) + wave)))
    return values


def main() -> None:
    rng = random.Random(20260806)
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    data_dir = BASE_DIR / "data"
    raw_dir = data_dir / "raw"
    streams_dir = data_dir / "streams"
    for d in (data_dir, raw_dir, streams_dir):
        d.mkdir(parents=True, exist_ok=True)

    demo_streams = [
        {
            "channel": "michimochievee",
            "display_name": "MichiMochievee",
            "stream_id": "900000000001",
            "title": "Wuthering Waves 新版本开荒",
            "language": "en",
            "minutes": 180,
            "base": 2800,
            "peak": 4700,
            "start_offset_min": 300,
            "status": "ended",
        },
        {
            "channel": "deme",
            "display_name": "deme",
            "stream_id": "900000000002",
            "title": "鸣潮日常 + 深渊",
            "language": "en",
            "minutes": 150,
            "base": 1200,
            "peak": 2100,
            "start_offset_min": 270,
            "status": "ended",
        },
        {
            "channel": "dizzykitten",
            "display_name": "DizzyKitten",
            "stream_id": "900000000003",
            "title": "潮汐任务全收集",
            "language": "en",
            "minutes": 120,
            "base": 700,
            "peak": 1300,
            "start_offset_min": 240,
            "status": "ended",
        },
        {
            "channel": "nagzz",
            "display_name": "Nagzz",
            "stream_id": "900000000004",
            "title": "Wuthering Waves 鸣潮",
            "language": "en",
            "minutes": 60,
            "base": 400,
            "peak": 780,
            "start_offset_min": 60,
            "status": "live",
        },
        {
            "channel": "rainhoe",
            "display_name": "Rainhoe",
            "stream_id": "900000000005",
            "title": "鸣潮开箱 + 聊天",
            "language": "en",
            "minutes": 90,
            "base": 250,
            "peak": 520,
            "start_offset_min": 90,
            "status": "live",
        },
    ]

    state: dict = {"streams": {}}
    summary_rows: list[dict] = []
    raw_path = raw_dir / f"{now.strftime('%Y-%m-%d')}.jsonl"
    raw_handle = raw_path.open("a", encoding="utf-8")

    try:
        for spec in demo_streams:
            minutes = spec["minutes"]
            values = ccv_curve(minutes, spec["base"], spec["peak"], rng)
            started = now - timedelta(minutes=spec["start_offset_min"])
            is_live = spec["status"] == "live"
            ended = None if is_live else started + timedelta(minutes=minutes)
            first_seen = started
            samples = []

            stream_csv = streams_dir / f"{spec['channel']}_{spec['stream_id']}.csv"
            with stream_csv.open("w", encoding="utf-8-sig", newline="") as fh:
                writer = csv.writer(fh)
                writer.writerow(STREAM_CSV_COLUMNS)
                for i, value in enumerate(values):
                    ts = started + timedelta(minutes=i)
                    if not is_live and ts > now:
                        break
                    row = {
                        "ts_utc": ts.isoformat(),
                        "stream_id": spec["stream_id"],
                        "channel": spec["channel"],
                        "display_name": spec["display_name"],
                        "title": spec["title"],
                        "viewer_count": value,
                        "started_at_utc": started.isoformat(),
                        "language": spec["language"],
                        "game_id": "209738946",
                        "game_name": "Wuthering Waves",
                        "tags": '["Wuthering Waves"]',
                    }
                    raw_handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                    writer.writerow(
                        [
                            ts.isoformat(),
                            str(value),
                            spec["title"],
                            spec["language"],
                            "Wuthering Waves",
                            '["Wuthering Waves"]',
                        ]
                    )
                    samples.append({"ts_utc": ts.isoformat(), "viewer_count": value})

            viewers = [s["viewer_count"] for s in samples]
            stream_entry = {
                "stream_id": spec["stream_id"],
                "channel": spec["channel"],
                "display_name": spec["display_name"],
                "title": spec["title"],
                "language": spec["language"],
                "started_at_utc": started.isoformat(),
                "first_seen_utc": first_seen.isoformat(),
                "last_seen_utc": samples[-1]["ts_utc"] if samples else "",
                "ended_at_utc": ended.isoformat() if ended else "",
                "duration_min": round((samples[-1]["ts_utc"] and (
                    datetime.fromisoformat(samples[-1]["ts_utc"]) - started
                ).total_seconds() / 60.0) if samples else 0, 1),
                "peak_ccv": max(viewers) if viewers else "",
                "avg_ccv": round(sum(viewers) / len(viewers), 1) if viewers else "",
                "min_ccv": min(viewers) if viewers else "",
                "sample_count": len(viewers),
                "status": spec["status"],
                "samples": samples,
            }
            state["streams"][spec["stream_id"]] = stream_entry
            summary_rows.append({c: stream_entry.get(c, "") for c in SUMMARY_COLUMNS})
            print(f"生成 {spec['channel']} ({spec['stream_id']}) {len(samples)} 个采样点, status={spec['status']}")
    finally:
        raw_handle.close()

    with (data_dir / "streams_summary.csv").open("w", encoding="utf-8-sig", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=SUMMARY_COLUMNS)
        writer.writeheader()
        writer.writerows(summary_rows)
    with (data_dir / "state.json").open("w", encoding="utf-8") as fh:
        json.dump(state, fh, ensure_ascii=False, indent=2)
    print("演示数据已生成到 data/，接下来运行: python charts.py")


if __name__ == "__main__":
    main()
