@echo off
chcp 65001 >nul
cd /d "%~dp0"
powershell -NoProfile -WindowStyle Hidden -Command "$env:PYTHONIOENCODING='utf-8'; $python=Join-Path (Get-Location) '.venv\Scripts\python.exe'; if(-not (Test-Path $python)){ $python=(Get-Command python -ErrorAction Stop).Source }; Start-Process -FilePath $python -ArgumentList 'server.py' -WorkingDirectory (Get-Location) -WindowStyle Hidden"
timeout /t 2 >nul
start http://127.0.0.1:8088/
echo 控制台已启动: http://127.0.0.1:8088/ （关闭本窗口不影响控制台）
pause
