@echo off
setlocal enabledelayedexpansion
title push-new - add GitHub remote and push

REM =========================================================================
REM  push-new.bat  -  Add a GitHub remote URL to this repo and push to it
REM
REM  Usage:
REM    push-new.bat https://github.com/kkm108/my-repo.git
REM    push-new.bat kkm108/my-repo
REM    push-new.bat my-repo                     (uses %GH_OWNER%, else prompts)
REM    push-new.bat my-repo "Commit message"
REM    push-new.bat --bulk repos.txt            (one URL or name per line)
REM    push-new.bat --bulk repos.txt "Commit message"
REM
REM  repos.txt format (lines starting with # are ignored):
REM    https://github.com/kkm108/repo-a.git
REM    kkm108/repo-b
REM    repo-c
REM =========================================================================

if "%~1"=="" goto :usage
if "%~1"=="-h" goto :usage
if "%~1"=="/?" goto :usage
if "%~1"=="--help" goto :usage
if /i "%~1"=="--bulk" goto :bulk

call :pushOne "%~1" "%~2"
exit /b %errorlevel%

:bulk
if "%~2"=="" (
  echo [FAIL] --bulk requires a text file. Example: push-new.bat --bulk repos.txt
  exit /b 1
)
if not exist "%~2" (
  echo [FAIL] file not found: %~2
  exit /b 1
)
set "FAILED=0"
for /f "usebackq tokens=* delims=" %%L in ("%~2") do (
  set "LINE=%%L"
  if defined LINE if not "!LINE:~0,1!"=="#" (
    echo.
    echo ======================================================================
    call :pushOne "!LINE!" "%~3"
    if errorlevel 1 set "FAILED=1"
  )
)
echo.
if "!FAILED!"=="1" (
  echo Done, but SOME repositories failed - check messages above.
  exit /b 1
)
echo Done. All repositories pushed successfully.
exit /b 0


:pushOne
set "INPUT=%~1"
set "MSG=%~2"
if not defined INPUT (
  echo [skip] empty entry
  exit /b 0
)

git rev-parse --is-inside-work-tree >nul 2>&1
if errorlevel 1 (
  echo [FAIL] not inside a git repository
  exit /b 1
)

REM ---- normalize input into a full repository URL ----
set "URL="
echo(!INPUT!| findstr /i /c:"github.com" >nul
if not errorlevel 1 set "URL=!INPUT!"
if defined URL goto :normalize

echo(!INPUT!| findstr /c:"/" >nul
if not errorlevel 1 (
  set "URL=https://github.com/!INPUT!"
  goto :normalize
)

if defined GH_OWNER (
  set "URL=https://github.com/!GH_OWNER!/!INPUT!"
  goto :normalize
)
set /p OWNER=Enter GitHub owner for repo "!INPUT!" : 
if not defined OWNER (
  echo [FAIL] no owner given
  exit /b 1
)
set "URL=https://github.com/!OWNER!/!INPUT!"

:normalize
if /i "!URL:~-4!"==".git" set "URL=!URL:~0,-4!"
set "FULL=!URL!.git"

REM ---- derive remote name from the URL path ----
set "NAME=!URL:*github.com/=!"
if not defined NAME set "NAME=!URL:*gitlab.com/=!"
if not defined NAME (
  echo [FAIL] cannot derive remote name from: !INPUT!
  echo        Use a full URL such as https://github.com/owner/repo
  exit /b 1
)

REM ---- current branch ----
set "BRANCH=main"
for /f "delims=" %%B in ('git rev-parse --abbrev-ref HEAD 2^>nul') do set "BRANCH=%%B"

REM ---- add or update the remote ----
git remote get-url "!NAME!" >nul 2>&1
if errorlevel 1 (
  git remote add "!NAME!" "!FULL!"
  if errorlevel 1 (
    echo [FAIL] could not add remote "!NAME!"
    exit /b 1
  )
  echo [ok]   added remote "!NAME!" -^> !FULL!
) else (
  git remote set-url "!NAME!" "!FULL!"
  echo [ok]   remote "!NAME!" exists, url updated -^> !FULL!
)

REM ---- stage and commit if there is anything new ----
set "DOCOMMIT=0"
git add -A >nul 2>&1
git diff --cached --quiet >nul 2>&1
if errorlevel 1 set "DOCOMMIT=1"
git rev-parse --verify HEAD >nul 2>&1
if errorlevel 1 set "DOCOMMIT=1"

if "!DOCOMMIT!"=="0" (
  echo [info] nothing new to commit
) else (
  if not defined MSG set "MSG=Update - %DATE% %TIME%"
  git commit -q -m "!MSG!"
  if errorlevel 1 (
    echo [FAIL] commit failed
    exit /b 1
  )
  echo [ok]   committed: !MSG!
)

REM ---- push ----
echo [push] remote=!NAME!  branch=!BRANCH!
git push -u "!NAME!" "!BRANCH!"
if not errorlevel 1 (
  echo [ok]   pushed -^> !FULL!
  exit /b 0
)

echo [warn] push rejected, trying a rebase against the remote...
git pull --rebase "!NAME!" "!BRANCH!"
if errorlevel 1 (
  echo [FAIL] rebase failed - resolve conflicts, then run:
  echo        git push -u "!NAME!" "!BRANCH!"
  exit /b 1
)
git push -u "!NAME!" "!BRANCH!"
if errorlevel 1 (
  echo [FAIL] push failed for !FULL!
  exit /b 1
)
echo [ok]   pushed after rebase -^> !FULL!
exit /b 0


:usage
echo.
echo push-new - add a GitHub remote URL and push this repository
echo.
echo USAGE
echo   push-new.bat ^<repo-url^|owner/repo^|repo-name^> ["commit message"]
echo   push-new.bat --bulk ^<file^> ["commit message"]
echo.
echo EXAMPLES
echo   push-new.bat https://github.com/kkm108/Cognitive-Mesh.git
echo   push-new.bat kkm108/new-repo
echo   push-new.bat new-repo "Added docs"
echo   push-new.bat --bulk repos.txt
echo.
echo TIP  Set GH_OWNER once to avoid typing the owner each time:
echo   set GH_OWNER=kkm108
echo.
echo      Files are committed automatically only if something changed.
echo      The remote is added if missing, or its URL is updated if present.
exit /b 0
