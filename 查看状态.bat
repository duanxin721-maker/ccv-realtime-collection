@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo ==== 上次查看（最后成功轮询） ====
type data\heartbeat.json 2>nul
if errorlevel 1 echo （暂无心跳文件，采集器可能未运行）
echo.
echo ==== 采集进程 ====
powershell -NoProfile -Command "$hb = Get-Content data\heartbeat.json -Raw | ConvertFrom-Json; $p = Get-Process -Id $hb.pid -ErrorAction SilentlyContinue; if ($p) { Write-Output ('运行中 PID=' + $hb.pid + ' 会话=' + $hb.session_id) } else { Write-Output ('未运行（最后心跳 ' + $hb.last_tick_utc + '）') }"
echo.
echo ==== 当前跟踪的直播 ====
powershell -NoProfile -Command "Import-Csv data\streams_summary.csv | Where-Object { $_.status -eq 'live' } | Select-Object channel, peak_ccv, avg_ccv, sample_count, duration_min | Format-Table -AutoSize"
pause
