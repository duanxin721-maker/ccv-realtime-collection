#!/usr/bin/env node
"use strict";

// 网页前端模式试点：打开 Twitch 直播页，从 DOM 读取实时观众数。
// 用法: node twitch_web_pilot.js <channel> [intervalSec=10] [durationMin=10] [outDir]

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
const channel = positional[0];
const intervalSec = Number(positional[1] || 10);
const durationMin = Number(positional[2] || 10);
const outDir = positional[3] || path.join(__dirname, "data", `twitch_web_${channel}`);
const stopFile = flags["stop-file"] ? path.resolve(flags["stop-file"]) : null;
const pauseFile = flags["pause-file"] ? path.resolve(flags["pause-file"]) : null;

if (!channel) {
  console.error("Usage: node twitch_web_pilot.js <channel> [intervalSec] [durationMin] [outDir]");
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

async function main() {
  fs.mkdirSync(outDir, { recursive: true });
  const rawPath = path.join(outDir, "raw.jsonl");
  const csvPath = path.join(outDir, "streams.csv");
  const metaPath = path.join(outDir, "meta.json");

  if (!fs.existsSync(csvPath)) {
    fs.writeFileSync(csvPath, "ts_utc,viewer_count,online\n", "utf8");
  }

  const startAt = Date.now();
  const startedIso = utcnow();
  fs.writeFileSync(
    metaPath,
    JSON.stringify({ channel, interval_seconds: intervalSec, duration_minutes: durationMin, started_at_utc: startedIso, status: "running" }, null, 2),
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
    await gotoWithRetry(page, `https://www.twitch.tv/${channel}`);
    await page.waitForTimeout(6000);
    const consent = page.locator("button", { hasText: "继续" }).first();
    if (await consent.isVisible().catch(() => false)) {
      await consent.click();
      await page.waitForTimeout(4000);
    }
    // 点击播放按钮/视频，让观众数覆盖层出现（暂停状态读不到）
    await page.click('button[data-a-target="player-play-button"], video', { timeout: 8000 }).catch(() => {});
    await page.waitForTimeout(3000);

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
      const result = await page.evaluate(({ channelName }) => {
        const el = document.querySelector('[data-a-target="animated-channel-viewers-count"]');
        if (el) {
          const text = el.textContent.trim();
          return { viewers: text ? Number(text.replace(/[^\d]/g, "")) : null, online: !!text };
        }
        // 兜底：在页面文本中找"<频道名> ... N 名观众/viewers"
        const body = document.body.innerText.replace(/\s+/g, " ");
        const esc = (s) => String(s).replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        const m = body.match(
          new RegExp(
            esc(channelName) + "[\\s\\S]{0,100}?([\\d,]+(?:[.\\d]+)?[万千KMB亿]?)\\s*(?:人)?\\s*(?:名观众|viewers?|正在观看)",
            "i",
          ),
        );
        if (m) {
          const n = m[1].replace(/,/g, "");
          let viewers = Number(n);
          if (n.endsWith("万")) viewers = Number(n.replace("万", "")) * 10000;
          else if (n.endsWith("千")) viewers = Number(n.replace("千", "")) * 1000;
          return { viewers: Math.round(viewers), online: true };
        }
        return { viewers: null, online: false };
      }, { channelName: channel });
      samples.push(result);
      const line = `${ts},${result.viewers ?? ""},${result.online}\n`;
      fs.appendFileSync(csvPath, line, "utf8");
      fs.appendFileSync(rawPath, JSON.stringify({ ts_utc: ts, channel, viewer_count: result.viewers, online: result.online }) + "\n", "utf8");
      console.log(`${new Date(ts).toISOString().slice(11, 19)} viewers=${result.viewers ?? "OFFLINE"}`);
      await page.waitForTimeout(Math.max(1, intervalSec * 1000));
    }
  } catch (e) {
    console.error("FAILED", String(e.message).split("\n")[0]);
    runError = e;
  } finally {
    if (browser) await browser.close();
  }

  const viewers = samples.map((s) => s.viewers).filter((v) => v !== null);
  const status = runError ? "failed" : stopReason === "stopped" ? "stopped" : "completed";
  const meta = {
    channel,
    interval_seconds: intervalSec,
    duration_minutes: durationMin,
    started_at_utc: startedIso,
    ended_at_utc: utcnow(),
    status,
    sample_count: samples.length,
    online_count: viewers.length,
    peak_ccv: viewers.length ? Math.max(...viewers) : null,
    avg_ccv: viewers.length ? Math.round((viewers.reduce((a, b) => a + b, 0) / viewers.length) * 10) / 10 : null,
    min_ccv: viewers.length ? Math.min(...viewers) : null,
  };
  fs.writeFileSync(metaPath, JSON.stringify(meta, null, 2), "utf8");
  console.log("DONE", JSON.stringify(meta));
  if (runError) process.exitCode = 1;
}

main().catch((e) => {
  console.error(e);
  process.exitCode = 1;
});
