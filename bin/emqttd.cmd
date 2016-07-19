@echo off
@setlocal
@setlocal enabledelayedexpansion

@set node_name=emqttd

@rem Get the absolute path to the parent directory,
@rem which is assumed to be the node root.
@for /F "delims=" %%I in ("%~dp0..") do @set node_root=%%~fI

@set releases_dir=%node_root%\releases
@set runner_etc_dir=%node_root%\etc

@rem Parse ERTS version and release version from start_erl.data
@for /F "usebackq tokens=1,2" %%I in ("%releases_dir%\start_erl.data") do @(
    @call :set_trim erts_version %%I
    @call :set_trim release_version %%J
)

@set vm_args=%runner_etc_dir%\vm.args
@set sys_config=%runner_etc_dir%\emqttd.config
@set node_boot_script=%releases_dir%\%release_version%\%node_name%
@set clean_boot_script=%releases_dir%\%release_version%\start_clean

@rem extract erlang cookie from vm.args
@for /f "usebackq tokens=1-2" %%I in (`findstr /b \-setcookie "%vm_args%"`) do @set erlang_cookie=%%J

@set erts_bin=%node_root%\erts-%erts_version%\bin

@set service_name=%node_name%_%release_version%

@set erlsrv="%erts_bin%\erlsrv.exe"
@set epmd="%erts_bin%\epmd.exe"
@set escript="%erts_bin%\escript.exe"
@set werl="%erts_bin%\werl.exe"

@if "%1"=="usage" @goto usage
@if "%1"=="install" @goto install
@if "%1"=="uninstall" @goto uninstall
@if "%1"=="start" @goto start
@if "%1"=="stop" @goto stop
@if "%1"=="restart" @call :stop && @goto start
@if "%1"=="console" @goto console
@if "%1"=="query" @goto query
@if "%1"=="attach" @goto attach
@if "%1"=="upgrade" @goto upgrade
@echo Unknown command: "%1"

:usage
@echo Usage: %~n0 [install^|uninstall^|start^|stop^|restart^|console^|query^|attach^|upgrade]
@goto :EOF

:install
@set description=Erlang node %node_name% in %node_root%
@set start_erl=%node_root%\bin\start_erl.cmd
@set args= ++ %node_name% ++ %node_root%
@%erlsrv% add %service_name% -c "%description%" -sname %node_name% -w "%node_root%" -m "%start_erl%" -args "%args%" -stopaction "init:stop()."
@goto :EOF

:uninstall
@%erlsrv% remove %service_name%
@%epmd% -kill
@goto :EOF

:start
@%erlsrv% start %service_name%
@goto :EOF

:stop
@%erlsrv% stop %service_name%
@goto :EOF

:console
set dest_path=%~dp0
cd /d !dest_path!..\plugins
set current_path=%cd%
set plugins=
for /d %%P in (*) do (
set "plugins=!plugins!"!current_path!\%%P\ebin" "
)
cd /d %node_root%

@start "%node_name% console" %werl% -boot "%node_boot_script%" -config "%sys_config%" -args_file "%vm_args%" -sname %node_name% -pa %plugins%
@goto :EOF

:query
@%erlsrv% list %service_name%
@exit %ERRORLEVEL%
@goto :EOF

:attach
@for /f "usebackq" %%I in (`hostname`) do @set hostname=%%I
start "%node_name% attach" %werl% -boot "%clean_boot_script%" -remsh %node_name%@%hostname% -sname console -setcookie %erlang_cookie%
@goto :EOF

:upgrade
@if "%2"=="" (
    @echo Missing upgrade package argument
    @echo Usage: %~n0 upgrade {package base name}
    @echo NOTE {package base name} MUST NOT include the .tar.gz suffix
    @goto :EOF
)
@%escript% %node_root%\bin\install_upgrade.escript %node_name% %erlang_cookie% %2
@goto :EOF

:set_trim
@set %1=%2
@goto :EOF
