@echo off
setlocal enabledelayedexpansion
title update - commit and push changes

REM =========================================================================
REM  update.bat  -  Commit local changes and push them to GitHub
REM
REM  Usage:
REM    update.bat                      commit (auto message) + push to origin
REM    update.bat "My commit message"  custom commit message
REM    update.bat "msg" myremote       push to a specific remote
REM    update.bat --all                push to EVERY configured remote
REM    update.bat --pull               pull --rebase first, then commit + push
REM =========================================================================

if "%~1"=="-h" goto :usage
if "%~1"=="/?" goto :usage
if "%~1"=="--help" goto :usage

git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
  echo [FAIL] not inside a git repository
  exit /b 1
)

set "MSG="
set "RMT="
set "DOPULL=0"
set "ALL=0"

:parse
if "%~1"=="" goto :parsed
if /i "%~1"=="--all" ( set "ALL=1" & shift /1 & goto :parse )
if /i "%~1"=="--pull" ( set "DOPULL=1" & shift /1 & goto :parse )
if not defined MSG ( set "MSG=%~1" ) else if not defined RMT ( set "RMT=%~1" )
shift /1
goto :parse

:parsed

REM ---- current branch ----
set "BRANCH=main"
for /f "delims=" %%B in ('git rev-parse --abbrev-ref HEAD 2^>nul') do set "BRANCH=%%B"

REM ---- optional pull before committing ----
if "!DOPULL!"=="1" (
  echo [pull] rebasing on current branch before commit...
  git pull --rebase
  if errorlevel 1 (
    echo [FAIL] pull --rebase failed - resolve conflicts, then rerun update.bat
    exit /b 1
  )
)

REM ---- stage everything ----
git add -A >nul 2>&1

REM ---- detect whether there is anything to commit ----
set "DOC=0"
git diff --cached --quiet >nul 2>&1
if errorlevel 1 set "DOC=1"
git rev-parse --verify HEAD >nul 2>&1
if errorlevel 1 set "DOC=1"

if "!DOC!"=="0" (
  echo [info] working tree clean - nothing to commit
) else (
  if not defined MSG set "MSG=Update - %DATE% %TIME%"
  git commit -q -m "!MSG!"
  if errorlevel 1 (
    echo [FAIL] commit failed
    exit /b 1
  )
  echo [ok] committed: !MSG!
)

REM ---- push to every remote ----
if "!ALL!"=="1" (
  set "FAILED=0"
  for /f "delims=" %%R in ('git remote') do (
    call :pushOne "%%R"
    if errorlevel 1 set "FAILED=1"
  )
  echo.
  if "!FAILED!"=="1" echo Done, but SOME remotes failed - see above.
  if "!FAILED!"=="1" exit /b 1
  echo Done. Pushed to all remotes.
  exit /b 0
)

REM ---- resolve a single remote ----
if defined RMT goto :haveRemote
git remote get-url origin >nul 2>&1
if not errorlevel 1 ( set "RMT=origin" & goto :haveRemote )
for /f "delims=" %%R in ('git remote') do set "RMT=%%R"
if not defined RMT (
  echo [FAIL] no git remote configured - run push-new.bat first
  exit /b 1
)

:haveRemote
call :pushOne "!RMT!"
exit /b %errorlevel%


:pushOne
set "R=%~1"
echo [push] remote=!R!  branch=!BRANCH!
git push -u "!R!" "!BRANCH!"
if not errorlevel 1 (
  echo [ok]   pushed to !R!
  exit /b 0
)
echo [warn] push rejected, trying a rebase against !R!...
git pull --rebase "!R!" "!BRANCH!"
if errorlevel 1 (
  echo [FAIL] rebase failed for !R! - resolve conflicts, then run:
  echo        git push -u "!R!" "!BRANCH!"
  exit /b 1
)
git push -u "!R!" "!BRANCH!"
if errorlevel 1 (
  echo [FAIL] push failed for !R!
  exit /b 1
)
echo [ok]   pushed to !R! after rebase
exit /b 0


:usage
echo.
echo update - commit local changes and push them to GitHub
echo.
echo USAGE
echo   update.bat ["commit message"] [remote]
echo   update.bat --all              push to every configured remote
echo   update.bat --pull             pull --rebase first, then commit + push
echo.
echo EXAMPLES
echo   update.bat
echo   update.bat "Added visual node"
echo   update.bat "Added docs" origin
echo   update.bat --all
echo   update.bat --pull "Sync upstream"
echo.
echo Notes:
echo   - Files are staged automatically with git add -A
echo   - If there is nothing to commit, it still pushes existing commits
echo   - If the remote rejects the push, a pull --rebase is attempted
exit /b 0
