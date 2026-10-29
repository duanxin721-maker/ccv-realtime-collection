@echo off
chcp 65001 >nul
cd /d "%~dp0"
type nul > stop.flag
echo 已写入停止标记。采集器将在本轮结束后优雅停止（最多约1分钟），
echo 当前直播会标记为 interrupted，下次启动自动续采。
pause
