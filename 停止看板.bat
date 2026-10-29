@echo off
chcp 65001 >nul
cd /d "%~dp0"
echo 正在停止采集任务并生成报告...
powershell -NoProfile -Command "Get-CimInstance Win32_Process -Filter \"Name = 'python.exe'\" | Where-Object { $_.CommandLine -like '*server.py*' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force; Write-Output ('已停止控制台 PID ' + $_.ProcessId) }"
echo 正在生成带图表的报告...
set "PYTHON=%~dp0.venv\Scripts\python.exe"
if not exist "%PYTHON%" set "PYTHON=python"
"%PYTHON%" report.py --all
start "" "%~dp0报告_%date:~0,4%%date:~5,2%%date:~8,2%"
pause
