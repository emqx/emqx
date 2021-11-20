:: This batch file handles managing an Erlang node as a Windows service.
::
:: Commands provided:
::
:: * install - install the release as a Windows service
:: * start - start the service and Erlang node
:: * stop - stop the service and Erlang node
:: * restart - run the stop command and start command
:: * uninstall - uninstall the service and kill a running node
:: * ping - check if the node is running
:: * console - start the Erlang release in a `werl` Windows shell
:: * attach - connect to a running node and open an interactive console
:: * list - display a listing of installed Erlang services
:: * usage - display available commands

:: Set variables that describe the release
@set rel_name=emqx
@set rel_vsn={{ release_version }}
@set REL_VSN=%rel_vsn%
@set erts_vsn={{ erts_vsn }}
@set erl_opts={{ erl_opts }}

@set script=%~n0

@set EPMD_ARG=-start_epmd false -epmd_module ekka_epmd -proto_dist ekka
@set ERL_FLAGS=%EPMD_ARG%

:: Discover the release root directory from the directory
:: of this script
@set script_dir=%~dp0
@for %%A in ("%script_dir%\..") do @(
  set rel_root_dir=%%~fA
)

@set rel_dir=%rel_root_dir%\releases\%rel_vsn%
@set RUNNER_ROOT_DIR=%rel_root_dir%
@set RUNNER_ETC_DIR=%rel_root_dir%\etc

@set etc_dir=%rel_root_dir%\etc
@set lib_dir=%rel_root_dir%\lib
@set data_dir=%rel_root_dir%\data
@set emqx_conf=%etc_dir%\emqx.conf

@call :find_erts_dir
@call :find_vm_args
@call :find_sys_config
@call :set_boot_script_var

@set service_name=%rel_name%_%rel_vsn%
@set bindir=%erts_dir%\bin
@set progname=erl.exe
@set clean_boot_script=%rel_root_dir%\bin\start_clean
@set erlsrv="%bindir%\erlsrv.exe"
@set escript="%bindir%\escript.exe"
@set werl="%bindir%\werl.exe"
@set erl_exe="%bindir%\erl.exe"
@set nodetool="%rel_root_dir%\bin\nodetool"
@set cuttlefish="%rel_root_dir%\bin\cuttlefish"
@set node_type="-name"
@set schema_mod="emqx_conf_schema"

@set conf_path="%etc_dir%\emqx.conf"
:: Extract node name from emqx.conf
@for /f "usebackq delims=" %%I in (`"%escript% %nodetool% hocon -s %schema_mod% -c %conf_path% get node.name"`) do @(
  @call :set_trim node_name %%I
)

:: Extract node cookie from emqx.conf
@for /f "usebackq delims=" %%I in (`"%escript% %nodetool% hocon -s %schema_mod% -c %conf_path% get node.cookie"`) do @(
  @call :set_trim node_cookie %%I
)

:: Write the erl.ini file to set up paths relative to this script
@call :write_ini

:: If a start.boot file is not present, copy one from the named .boot file
@if not exist "%rel_dir%\start.boot" (
  copy "%rel_dir%\%rel_name%.boot" "%rel_dir%\start.boot" >nul
)

@if "%1"=="install" @goto install
@if "%1"=="uninstall" @goto uninstall
@if "%1"=="start" @goto start
@if "%1"=="stop" @goto stop
@if "%1"=="restart" @call :stop && @goto start
::@if "%1"=="upgrade" @goto relup
::@if "%1"=="downgrade" @goto relup
@if "%1"=="console" @goto console
@if "%1"=="ping" @goto ping
@if "%1"=="list" @goto list
@if "%1"=="attach" @goto attach
@if "%1"=="" @goto usage
@echo Unknown command: "%1"

@goto :eof

:: Find the ERTS dir
:find_erts_dir
@set possible_erts_dir=%rel_root_dir%\erts-%erts_vsn%
@if exist "%possible_erts_dir%" (
  call :set_erts_dir_from_default
) else (
  call :set_erts_dir_from_erl
)
@goto :eof

:: Set the ERTS dir from the passed in erts_vsn
:set_erts_dir_from_default
@set erts_dir=%possible_erts_dir%
@set rootdir=%rel_root_dir%
@goto :eof

:: Set the ERTS dir from erl
:set_erts_dir_from_erl
@for /f "delims=" %%i in ('where erl') do @(
  set erl=%%i
)
@set dir_cmd="%erl%" -noshell -eval "io:format(\"~s\", [filename:nativename(code:root_dir())])." -s init stop
@for /f %%i in ('%%dir_cmd%%') do @(
  set erl_root=%%i
)
@set erts_dir=%erl_root%\erts-%erts_vsn%
@set rootdir=%erl_root%
@goto :eof

:find_vm_args
@set possible_vm=%etc_dir%\vm.args
@if exist "%possible_vm%" (
  set args_file=-args_file "%possible_vm%"
)
@goto :eof

:: Find the sys.config file
:find_sys_config
@set possible_sys=%etc_dir%\sys.config
@if exist "%possible_sys%" (
  set sys_config=-config "%possible_sys%"
)
@goto :eof

:create_mnesia_dir
@set create_dir_cmd=%escript% %nodetool% mnesia_dir "%data_dir%\mnesia" %node_name%
@for /f "delims=" %%Z in ('%%create_dir_cmd%%') do @(
  set mnesia_dir=%%Z
)
@set mnesia_dir="%mnesia_dir%"
@goto :eof

:: get the current time with hocon
:get_cur_time
@for /f "usebackq tokens=1-6 delims=." %%a in (`"%escript% %nodetool% hocon now_time"`) do @(
  set now_time=%%a.%%b.%%c.%%d.%%e.%%f
)
@goto :eof

:generate_app_config
@call :get_cur_time
%escript% %nodetool% hocon -v -t %now_time% -s %schema_mod% -c "%etc_dir%\emqx.conf" -d "%data_dir%\configs" generate
@set generated_config_args=-config "%data_dir%\configs\app.%now_time%.config" -args_file "%data_dir%\configs\vm.%now_time%.args"
:: create one new line
@echo.>>"%data_dir%\configs\vm.%now_time%.args"
:: write the node type and node name in to vm args file
@echo %node_type% %node_name%>>"%data_dir%\configs\vm.%now_time%.args"
@goto :eof

:: set boot_script variable
:set_boot_script_var
@if exist "%rel_dir%\%rel_name%.boot" (
  set boot_script=%rel_dir%\%rel_name%
) else (
  set boot_script=%rel_dir%\start
)
@goto :eof

:: Write the erl.ini file
:write_ini
@set erl_ini=%erts_dir%\bin\erl.ini
@set converted_bindir=%bindir:\=\\%
@set converted_rootdir=%rootdir:\=\\%
@echo [erlang] > "%erl_ini%"
@echo Bindir=%converted_bindir% >> "%erl_ini%"
@echo Progname=%progname% >> "%erl_ini%"
@echo Rootdir=%converted_rootdir% >> "%erl_ini%"
@goto :eof

:: Display usage information
:usage
@echo usage: %~n0 ^(install^|uninstall^|start^|stop^|restart^|console^|ping^|list^|attach^)
@goto :eof

:: Install the release as a Windows service
:: or install the specified version passed as argument
:install
@call :create_mnesia_dir
@call :generate_app_config
:: Install the service
@set args="-boot %boot_script% %sys_config% %generated_config_args% -mnesia dir '%mnesia_dir%'"
@set description=EMQX node %node_name% in %rootdir%
@if "" == "%2" (
  %erlsrv% add %service_name% %node_type% "%node_name%" -on restart -c "%description%" ^
           -i "emqx" -w "%rootdir%" -m %erl_exe% -args %args% ^
           -st "init:stop()."
  sc config emqx start=delayed-auto
) else (
  :: relup and reldown
  goto relup
)
@goto :eof

:: Uninstall the Windows service
:uninstall
@%erlsrv% remove %service_name%
@goto :eof

:: Start the Windows service
:start
:: window service?
:: @%erlsrv% start %service_name%
@call :create_mnesia_dir
@call :generate_app_config
@set args=-detached %sys_config% %generated_config_args% -mnesia dir '%mnesia_dir%'
@echo off
cd /d "%rel_root_dir%"
@echo on
@start "%rel_name%" %werl% -boot  "%boot_script%" -mode embedded %args%
@goto :eof

:: Stop the Windows service
:stop
:: window service?
:: @%erlsrv% stop %service_name%
@%escript% %nodetool% %node_type% %node_name% -setcookie %node_cookie% stop
@goto :eof

:: Relup and reldown
:relup
@if "" == "%2" (
  echo Missing package argument
  echo Usage: %rel_name% %1 {package base name}
  echo NOTE {package base name} MUST NOT include the .tar.gz suffix
  set ERRORLEVEL=1
  exit /b %ERRORLEVEL%
)
@%escript% "%rootdir%/bin/install_upgrade.escript" "%rel_name%" "%node_name%" "%node_cookie%" "%2"
@goto :eof

:: Start a console
:console
@call :create_mnesia_dir
@call :generate_app_config
@set args=%sys_config% %generated_config_args% -mnesia dir '%mnesia_dir%'
@echo off
cd /d %rel_root_dir%
@echo on
@start "bin\%rel_name% console" %werl% -boot "%boot_script%" -mode embedded %args%
@echo emqx is started!
@goto :eof

:: Ping the running node
:ping
@%escript% %nodetool% ping %node_type% "%node_name%" -setcookie "%node_cookie%"
@goto :eof

:: List installed Erlang services
:list
@%erlsrv% list %service_name%
@goto :eof

:: Attach to a running node
:attach
:: @start "%node_name% attach"
@start "%node_name% attach" %werl% -boot "%clean_boot_script%" ^
  -remsh %node_name% %node_type% console_%node_name% -setcookie %node_cookie%
@goto :eof

:: Trim variable
:set_trim
@set %1=%2
@goto :eof
