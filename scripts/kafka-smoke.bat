@echo off
setlocal
cd /d "%~dp0.."
chcp 65001 >nul
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0kafka-smoke.ps1"
set EXITCODE=%ERRORLEVEL%
exit /b %EXITCODE%
