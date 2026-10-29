#!/usr/bin/env node
"use strict";

// YouTube 网页前端模式试点：打开直播视频页，从页面 DOM 读取实时观众数。
// 用法: node yt_web_pilot.js <videoIdOrUrl> [intervalSec=10] [durationMin=10] [outDir]

const fs = require("node:fs");
const path = require("node:path");
const { chromium } = require("playwright");

const args = process.argv.slice(2);
const positional = [];
const flags = {};
for (let i = 0; i < args.length; i += 1) {
  const a = args[i];
  if (a === "--stop-file" || a === "--pause-file") {
    flags[a.slice(2)] = args[i + 1];
    i += 1;
  } else {
    positional.push(a);
  }
}
const rawTarget = positional[0];
const intervalSec = Number(positional[1] || 10);
const durationMin = Number(positional[2] || 10);
const videoId = (() => {
  const value = String(rawTarget || "").trim();
  if (/^[A-Za-z0-9_-]{6,}$/.test(value)) return value;
  const watch = value.match(/[?&]v=([A-Za-z0-9_-]+)/);
  if (watch) return watch[1];
  const short = value.match(/youtu\.be\/([A-Za-z0-9_-]+)/i);
  return short ? short[1] : value;
})();
const outDir = positional[3] || path.join(__dirname, "data", `yt_web_${videoId}`);
const stopFile = flags["stop-file"] ? path.resolve(flags["stop-file"]) : null;
const pauseFile = flags["pause-file"] ? path.resolve(flags["pause-file"]) : null;

if (!videoId) {
  console.error("Usage: node yt_web_pilot.js <videoIdOrUrl> [intervalSec] [durationMin] [outDir]");
  process.exit(2);
}

function utcnow() {
  return new Date().toISOString();
}

async function gotoWithRetry(page, url, attempts = 3) {
  for (let i = 1; i <= attempts; i += 1) {
    try {
      await page.goto(url, { waitUntil: "domcontentloaded", timeout: 90000 });
      return;
    } catch (e) {
      if (i === attempts) throw e;
      console.log(`goto retry ${i}/${attempts}: ${String(e.message).split("\n")[0]}`);
      await new Promise((r) => setTimeout(r, 10000));
    }
  }
}

function parseWatching(text) {
  if (!text) return null;
  const m = String(text).match(
    /([\d,]+(?:\.\d+)?)\s*([万千KMB亿]?)\s*(?:人)?\s*(?:正在观看|watching|观看|viewers)/i,
  );
  if (!m) return null;
  let n = Number(m[1].replace(/,/g, ""));
  const unit = String(m[2] || "").toUpperCase();
  if (unit === "K" || unit === "千") n *= 1000;
  if (unit === "万") n *= 10000;
  if (unit === "M") n *= 1000000;
  if (unit === "B" || unit === "亿") n *= 100000000;
  return Math.round(n);
}

async function main() {
  fs.mkdirSync(outDir, { recursive: true });
  const rawPath = path.join(outDir, "raw.jsonl");
  const csvPath = path.join(outDir, "streams.csv");
  const metaPath = path.join(outDir, "meta.json");
  if (!fs.existsSync(csvPath)) {
    fs.writeFileSync(csvPath, "ts_utc,viewer_count,raw_text,online\n", "utf8");
  }

  const startAt = Date.now();
  const startedIso = utcnow();
  fs.writeFileSync(
    metaPath,
    JSON.stringify({ video_id: videoId, interval_seconds: intervalSec, duration_minutes: durationMin, started_at_utc: startedIso, status: "running" }, null, 2),
    "utf8",
  );

  const samples = [];
  let stopReason = null;
  let runError = null;
  let browser = null;
  try {
    browser = await chromium.launch({
      headless: false,
      args: ["--disable-blink-features=AutomationControlled"],
    });
    const context = await browser.newContext({ viewport: { width: 1366, height: 900 } });
    await context.addInitScript(() => {
      Object.defineProperty(navigator, "webdriver", { get: () => undefined });
    });
    const page = await context.newPage();
    page.setDefaultTimeout(60000);
    await gotoWithRetry(page, `https://www.youtube.com/watch?v=${videoId}`);
    await page.waitForTimeout(6000);
    for (const text of ["Accept all", "I agree", "同意", "全部接受", "Reject all", "全部拒绝", "拒绝"]) {
      const btn = page.locator(`button:has-text("${text}")`).first();
      if (await btn.isVisible().catch(() => false)) {
        await btn.click().catch(() => {});
        await page.waitForTimeout(2000);
        break;
      }
    }

    const deadline = durationMin > 0 ? startAt + durationMin * 60 * 1000 : Infinity;
    while (Date.now() < deadline) {
      if (stopFile && fs.existsSync(stopFile)) {
        console.log("stop file detected, exiting");
        stopReason = "stopped";
        break;
      }
      if (pauseFile && fs.existsSync(pauseFile)) {
        await new Promise((r) => setTimeout(r, 2000));
        continue;
      }
      const ts = utcnow();
      const result = await page.evaluate(() => {
        const pattern = /([\d,]+(?:\.\d+)?[万千KMB亿]?)\s*(?:人)?\s*(?:正在观看|watching|viewers)/i;
        // 首选：视频信息区的悬浮提示（实测实时更新）
        const tooltip = document.querySelector('tp-yt-paper-tooltip[class*="ytd-watch-info-text"]');
        let raw = tooltip ? tooltip.textContent.replace(/\s+/g, " ").trim() : "";
        // 兜底1：主信息区 view-count（更新较慢）
        if (!raw) {
          const viewCount = document.querySelector("ytd-video-view-count-renderer span.view-count");
          raw = viewCount ? viewCount.textContent.replace(/\s+/g, " ").trim() : "";
        }
        return { raw, online: raw !== "" };
      });
      const viewers = parseWatching(result.raw);
      samples.push({ viewers, online: result.online });
      const rawText = result.raw.replace(/,/g, ",");
      fs.appendFileSync(csvPath, `${ts},${viewers ?? ""},"${rawText.replace(/"/g, '""')}",${result.online}\n`, "utf8");
      fs.appendFileSync(
        rawPath,
        JSON.stringify({ ts_utc: ts, video_id: videoId, viewer_count: viewers, raw_text: rawText, online: result.online }) + "\n",
        "utf8",
      );
      console.log(`${new Date(ts).toISOString().slice(11, 19)} viewers=${viewers ?? "?"} raw="${rawText}"`);
      await page.waitForTimeout(Math.max(1, intervalSec * 1000));
    }
  } catch (e) {
    console.error("FAILED", String(e.message).split("\n")[0]);
    runError = e;
  } finally {
    if (browser) await browser.close();
  }

  const values = samples.map((s) => s.viewers).filter((v) => v !== null);
  const status = runError ? "failed" : stopReason === "stopped" ? "stopped" : "completed";
  const meta = {
    video_id: videoId,
    interval_seconds: intervalSec,
    duration_minutes: durationMin,
    started_at_utc: startedIso,
    ended_at_utc: utcnow(),
    status,
    sample_count: samples.length,
    online_count: samples.filter((s) => s.online).length,
    peak_ccv: values.length ? Math.max(...values) : null,
    avg_ccv: values.length ? Math.round((values.reduce((a, b) => a + b, 0) / values.length) * 10) / 10 : null,
    min_ccv: values.length ? Math.min(...values) : null,
  };
  fs.writeFileSync(metaPath, JSON.stringify(meta, null, 2), "utf8");
  console.log("DONE", JSON.stringify(meta));
  if (runError) process.exitCode = 1;
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
