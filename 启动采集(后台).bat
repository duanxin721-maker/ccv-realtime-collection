@echo off
chcp 65001 >nul
cd /d "%~dp0"
powershell -NoProfile -WindowStyle Hidden -Command "$env:PYTHONIOENCODING='utf-8'; $python=Join-Path (Get-Location) '.venv\Scripts\python.exe'; if(-not (Test-Path $python)){ $python=(Get-Command python -ErrorAction Stop).Source }; Start-Process -FilePath $python -ArgumentList 'monitor.py' -WorkingDirectory (Get-Location) -WindowStyle Hidden"
echo 后台采集已启动（每30秒一轮）。日志见 monitor.log，停止请运行 停止采集.bat
pause
