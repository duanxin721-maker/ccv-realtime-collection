// 从 JSON 中间数据 + PNG 图片构建带嵌入式图表的 xlsx 报告。
// 用法: node build_report.mjs <interDir> <imgDir> <outXlsx>
import fs from "node:fs/promises";
import path from "node:path";
import { SpreadsheetFile, Workbook } from "@oai/artifact-tool";

const [interDir, imgDir, outXlsx] = process.argv.slice(2);
if (!interDir || !imgDir || !outXlsx) {
  console.error("Usage: node build_report.mjs <interDir> <imgDir> <outXlsx>");
  process.exit(2);
}

const summary = JSON.parse(await fs.readFile(path.join(interDir, "summary.json"), "utf8"));
const series = JSON.parse(await fs.readFile(path.join(interDir, "series.json"), "utf8"));
const raw = JSON.parse(await fs.readFile(path.join(interDir, "raw.json"), "utf8"));
const meta = JSON.parse(await fs.readFile(path.join(interDir, "meta.json"), "utf8"));
const imgFiles = (await fs.readdir(imgDir)).filter((f) => f.endsWith(".png")).sort();

function localDate(value) {
  if (!value) return null;
  const m = String(value).match(/^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})$/);
  if (!m) return value;
  return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]), Number(m[4]), Number(m[5]), Number(m[6]));
}

const workbook = Workbook.create();
const sumSheet = workbook.worksheets.add("汇总");
const tsSheet = workbook.worksheets.add("CCV时序");
const rawSheet = workbook.worksheets.add("原始快照");
const helperSheet = workbook.worksheets.add("图表数据");
const chartSheet = workbook.worksheets.add("图表");

// ================= 汇总 =================
sumSheet.showGridLines = false;
const title = `鸣潮直播实时 CCV 监测汇总（${meta.task_id || meta.name || ""}）`;
sumSheet.getRange("A1:L1").merge();
sumSheet.getRange("A1").values = [[title]];
sumSheet.getRange("A1").format = { font: { bold: true, size: 16, color: "#1F4E78" }, fill: "#EAF1F8" };
sumSheet.getRange("A1").format.rowHeightPx = 30;
sumSheet.getRange("A2:L2").merge();
sumSheet.getRange("A2").values = [[
  `任务 ${meta.task_id || "-"} · ${meta.platform || ""}/${meta.mode || ""} · 轮询 ${meta.poll_interval_seconds ?? 30} 秒 · 时区 Asia/Singapore · 生成于 ${new Date().toLocaleString("zh-CN", { hour12: false })}`,
]];
sumSheet.getRange("A2").format = { font: { size: 10, color: "#64748B" } };

const sumHeaders = ["场次ID", "主播", "状态", "采样数", "峰值CCV", "均值CCV", "最低CCV", "采集时长(分)", "首次采样(本地)", "最后采样(本地)", "标题", "语言"];
const headerRow = 4;
const dataStart = headerRow + 1;
const seriesEnd = 1 + series.length;

sumSheet.getRangeByIndexes(headerRow - 1, 0, 1, sumHeaders.length).values = [sumHeaders];
const matrix = summary.map((row) => [
  row.stream_id,
  row.channel,
  row.status,
  row.sample_count === "" ? null : Number(row.sample_count),
  null,
  null,
  null,
  null,
  localDate(row.first_seen_local),
  localDate(row.last_seen_local),
  row.title,
  row.language,
]);
sumSheet.getRangeByIndexes(dataStart - 1, 0, matrix.length, sumHeaders.length).values = matrix;

for (let i = 0; i < summary.length; i += 1) {
  const r = dataStart + i;
  sumSheet.getRange(`E${r}`).formulas = [[`=MAXIFS('CCV时序'!$D$2:$D$${seriesEnd},'CCV时序'!$C$2:$C$${seriesEnd},$A${r})`]];
  sumSheet.getRange(`F${r}`).formulas = [[`=AVERAGEIFS('CCV时序'!$D$2:$D$${seriesEnd},'CCV时序'!$C$2:$C$${seriesEnd},$A${r})`]];
  sumSheet.getRange(`G${r}`).formulas = [[`=MINIFS('CCV时序'!$D$2:$D$${seriesEnd},'CCV时序'!$C$2:$C$${seriesEnd},$A${r})`]];
  sumSheet.getRange(`H${r}`).formulas = [[`=IF(AND(I${r}<>"",J${r}<>""),ROUND((J${r}-I${r})*1440,1),"")`]];
}

sumSheet.tables.add(`A${headerRow}:L${dataStart + matrix.length - 1}`, true, "汇总表").style = "TableStyleMedium2";
sumSheet.freezePanes.freezeRows(headerRow);
sumSheet.getRangeByIndexes(headerRow - 1, 0, 1, sumHeaders.length).format = {
  fill: "#1F4E78",
  font: { bold: true, color: "#FFFFFF" },
  rowHeightPx: 26,
};
sumSheet.getRange(`E${dataStart}:H${dataStart + matrix.length - 1}`).format.numberFormat = "#,##0.0";
sumSheet.getRange(`D${dataStart}:D${dataStart + matrix.length - 1}`).format.numberFormat = "#,##0";
sumSheet.getRange(`I${dataStart}:J${dataStart + matrix.length - 1}`).format.numberFormat = "yyyy-mm-dd hh:mm:ss";
sumSheet.getRange(`A${dataStart}:L${dataStart + matrix.length - 1}`).format.borders = { preset: "all", style: "thin", color: "#D9E2EC" };
sumSheet.getRange(`A${headerRow}:L${dataStart + matrix.length - 1}`).format.autofitColumns();
sumSheet.getRange("K:K").format.columnWidth = 55;
sumSheet.getRange("A:A").format.columnWidth = 22;

// ================= CCV时序 =================
tsSheet.showGridLines = false;
tsSheet.getRange("A1:D1").values = [["时间(本地)", "主播", "场次ID", "CCV"]];
const tsMatrix = series.map((s) => [localDate(s.time_local), s.channel, s.stream_id, s.ccv]);
tsSheet.getRangeByIndexes(1, 0, tsMatrix.length, 4).values = tsMatrix;
tsSheet.tables.add(`A1:D${seriesEnd}`, true, "CCV时序表").style = "TableStyleMedium2";
tsSheet.freezePanes.freezeRows(1);
tsSheet.getRange("A1:D1").format = { fill: "#1F4E78", font: { bold: true, color: "#FFFFFF" }, rowHeightPx: 26 };
tsSheet.getRange(`A2:A${seriesEnd}`).format.numberFormat = "yyyy-mm-dd hh:mm:ss";
tsSheet.getRange(`D2:D${seriesEnd}`).format.numberFormat = "#,##0";
tsSheet.getRange(`A1:D${seriesEnd}`).format.borders = { preset: "all", style: "thin", color: "#D9E2EC" };
tsSheet.getRange(`A1:D${seriesEnd}`).format.autofitColumns();
tsSheet.getRange("A:A").format.columnWidth = 20;
tsSheet.getRange("C:C").format.columnWidth = 22;

// ================= 原始快照 =================
rawSheet.showGridLines = false;
const rawHeaders = ["ts_utc", "session_id", "stream_id", "channel", "display_name", "title", "viewer_count", "started_at_utc", "language", "game_id", "game_name", "tags"];
rawSheet.getRange("A1:L1").values = [rawHeaders];
const rawMatrix = raw.map((r) => [
  r.ts_utc ?? "", r.session_id ?? "", r.stream_id ?? "", r.channel ?? "", r.display_name ?? "",
  r.title ?? "", r.viewer_count ?? "", r.started_at_utc ?? "", r.language ?? "", r.game_id ?? "",
  r.game_name ?? "", r.tags ?? "",
]);
rawSheet.getRangeByIndexes(1, 0, rawMatrix.length, rawHeaders.length).values = rawMatrix;
const rawEnd = 1 + rawMatrix.length;
rawSheet.tables.add(`A1:L${rawEnd}`, true, "原始快照表").style = "TableStyleMedium2";
rawSheet.freezePanes.freezeRows(1);
rawSheet.getRange("A1:L1").format = { fill: "#1F4E78", font: { bold: true, color: "#FFFFFF" }, rowHeightPx: 26 };
rawSheet.getRange(`G2:G${rawEnd}`).format.numberFormat = "#,##0";
rawSheet.getRange(`A1:L${rawEnd}`).format.borders = { preset: "all", style: "thin", color: "#D9E2EC" };
rawSheet.getRange(`A1:L${rawEnd}`).format.autofitColumns();
rawSheet.getRange("F:F").format.columnWidth = 60;

// ================= 图表数据（原生图表辅助区） =================
helperSheet.showGridLines = false;

function columnLetter(index) {
  let n = index + 1;
  let result = "";
  while (n > 0) {
    const rem = (n - 1) % 26;
    result = String.fromCharCode(65 + rem) + result;
    n = Math.floor((n - 1) / 26);
  }
  return result;
}

// 按场次分组
const streamMap = new Map();
for (const s of series) {
  if (!streamMap.has(s.stream_id)) streamMap.set(s.stream_id, []);
  streamMap.get(s.stream_id).push(s);
}
const streamIds = [...streamMap.keys()];

// 每场：开播后分钟 + CCV
const streamBlocks = [];
streamIds.forEach((sid, idx) => {
  const rows = streamMap.get(sid).sort((a, b) => String(a.time_local).localeCompare(String(b.time_local)));
  if (!rows.length) return;
  const t0 = localDate(rows[0].time_local).getTime();
  const label = rows[0].channel || sid;
  const col = idx * 3;
  helperSheet.getRangeByIndexes(0, col, 1, 2).values = [["开播后分钟", label]];
  const matrix = rows.map((r) => {
    const t = localDate(r.time_local);
    return [Math.round(((t.getTime() - t0) / 60000) * 100) / 100, r.ccv];
  });
  helperSheet.getRangeByIndexes(1, col, matrix.length, 2).values = matrix;
  streamBlocks.push({ sid, label, rows, n: matrix.length, col });
});

// 总览：分钟并集
const overviewCols = streamBlocks.map((b) => {
  const t0 = localDate(b.rows[0].time_local).getTime();
  return {
    label: b.label,
    map: new Map(
      b.rows.map((r) => {
        const t = localDate(r.time_local);
        return [Math.round(((t.getTime() - t0) / 60000) * 100) / 100, r.ccv];
      }),
    ),
  };
});
const allMinutes = [...new Set(overviewCols.flatMap((o) => [...o.map.keys()]))].sort((a, b) => a - b);
if (allMinutes.length) {
  helperSheet.getRangeByIndexes(0, streamIds.length * 3, 1, overviewCols.length + 1).values = [
    ["开播后分钟", ...overviewCols.map((o) => o.label)],
  ];
  const ovMatrix = allMinutes.map((m) => [m, ...overviewCols.map((o) => o.map.get(m) ?? null)]);
  helperSheet.getRangeByIndexes(1, streamIds.length * 3, ovMatrix.length, overviewCols.length + 1).values = ovMatrix;
}

// 汇总：主播 + 峰值
helperSheet.getRangeByIndexes(0, streamIds.length * 3 + overviewCols.length + 2, 1, 2).values = [["主播", "峰值CCV"]];
const sumMatrix = summary.map((r) => [r.channel, r.peak_ccv === "" ? null : Number(r.peak_ccv)]);
helperSheet.getRangeByIndexes(1, streamIds.length * 3 + overviewCols.length + 2, sumMatrix.length, 2).values = sumMatrix;

// ================= 图表（原生 Excel 图表，可编辑标题与坐标轴） =================
chartSheet.showGridLines = false;
chartSheet.getRange("A1:G1").merge();
chartSheet.getRange("A1").values = [["原生 Excel 图表：双击可编辑标题、坐标轴范围、数据系列"]];
chartSheet.getRange("A1").format = { font: { bold: true, size: 14, color: "#1F4E78" }, fill: "#EAF1F8" };
chartSheet.getRange("A1").format.rowHeightPx = 26;

// 每场直播一条折线图
streamBlocks.forEach((b, idx) => {
  const catCol = columnLetter(b.col);
  const valCol = columnLetter(b.col + 1);
  const lastRow = b.n + 1;
  const chart = chartSheet.charts.add("line", { hasLegend: false });
  chart.title = `${b.label} CCV 实时变化`;
  const s = chart.series.add(b.label);
  s.categoryFormula = `'图表数据'!$${catCol}$2:$${catCol}$${lastRow}`;
  s.formula = `'图表数据'!$${valCol}$2:$${valCol}$${lastRow}`;
  chart.xAxis = { axisType: "textAxis", title: { text: "开播后分钟" } };
  chart.yAxis = { min: 0, numberFormatCode: "#,##0" };
  const colStart = idx % 2 === 0 ? "B" : "M";
  const colEnd = idx % 2 === 0 ? "K" : "U";
  const rowStart = 2 + Math.floor(idx / 2) * 20;
  const rowEnd = rowStart + 16;
  chart.setPosition(`${colStart}${rowStart}`, `${colEnd}${rowEnd}`);
});

// 总览：全部场次对比
if (allMinutes.length) {
  const ovCol = streamIds.length * 3;
  const catCol = columnLetter(ovCol);
  const ovLastRow = allMinutes.length + 1;
  const ovChart = chartSheet.charts.add("line", { hasLegend: true });
  ovChart.title = "全部直播 CCV 对比（按开播后分钟）";
  overviewCols.forEach((o, i) => {
    const valCol = columnLetter(ovCol + 1 + i);
    const s = ovChart.series.add(o.label);
    s.categoryFormula = `'图表数据'!$${catCol}$2:$${catCol}$${ovLastRow}`;
    s.formula = `'图表数据'!$${valCol}$2:$${valCol}$${ovLastRow}`;
  });
  ovChart.xAxis = { axisType: "textAxis", title: { text: "开播后分钟" } };
  ovChart.yAxis = { min: 0, numberFormatCode: "#,##0" };
  ovChart.setPosition("B22", "U42");
}

// 汇总：峰值柱状图
if (sumMatrix.length) {
  const barCol = streamIds.length * 3 + overviewCols.length + 2;
  const barRange = helperSheet.getRangeByIndexes(0, barCol, sumMatrix.length + 1, 2);
  const barChart = chartSheet.charts.add("bar", barRange);
  barChart.title = "各主播峰值 CCV";
  barChart.yAxis = { min: 0, numberFormatCode: "#,##0" };
  barChart.setPosition("B44", "K58");
}

// ================= 验证 + 导出 =================
const errors = await workbook.inspect({
  kind: "match",
  searchTerm: "#REF!|#DIV/0!|#VALUE!|#NAME\\?|#N/A",
  options: { useRegex: true, maxResults: 100 },
  summary: "formula error scan",
});
console.log("ERRORS:", errors.ndjson);
await fs.mkdir(path.dirname(outXlsx), { recursive: true });
const output = await SpreadsheetFile.exportXlsx(workbook);
await output.save(outXlsx);
console.log("SAVED:", outXlsx);
