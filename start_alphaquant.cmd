@echo off
setlocal
cd /d "%~dp0"
if exist ".venv\Scripts\python.exe" (set "PYTHON=.venv\Scripts\python.exe") else (set "PYTHON=python")
if exist ".alphaquant.lock" (
  echo AlphaQuant may already be running. Remove .alphaquant.lock after confirming it is stopped.
  exit /b 2
)
echo %DATE% %TIME% Starting AlphaQuant>>alphaquant_startup.log
echo %PID%>".alphaquant.lock"
"%PYTHON%" -c "import streamlit,pandas,numpy" >>alphaquant_startup.log 2>&1 || goto :failed
"%PYTHON%" -m streamlit run appemergentquant_v3_1.py --server.headless false >>alphaquant_startup.log 2>&1
set "RC=%ERRORLEVEL%"
del ".alphaquant.lock" 2>nul
exit /b %RC%
:failed
echo Dependency validation failed. See alphaquant_startup.log.
del ".alphaquant.lock" 2>nul
exit /b 1
