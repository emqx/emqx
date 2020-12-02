:: The batch file for emqx_ctl command

@set args=%*

:: Set variables that describe the release
@set rel_name=emqx
@set rel_vsn={{ rel_vsn }}
@set erts_vsn={{ erts_vsn }}
@set erl_opts={{ erl_opts }}

:: Discover the release root directory from the directory
:: of this script
@set script_dir=%~dp0
@for %%A in ("%script_dir%\..") do @(
  set rel_root_dir=%%~fA
)
@set rel_dir=%rel_root_dir%\releases\%rel_vsn%
@set emqx_conf=%rel_root_dir%\etc\emqx.conf

@call :find_erts_dir

@set bindir=%erts_dir%\bin
@set progname=erl.exe
@set escript="%bindir%\escript.exe"
@set nodetool="%rel_root_dir%\bin\nodetool"
@set node_type="-name"

:: Extract node name from emqx.conf
@for /f "usebackq delims=\= tokens=2" %%I in (`findstr /b node\.name "%emqx_conf%"`) do @(
  @call :set_trim node_name %%I
)

:: Extract node cookie from emqx.conf
@for /f "usebackq delims=\= tokens=2" %%I in (`findstr /b node\.cookie "%emqx_conf%"`) do @(
  @call :set_trim node_cookie= %%I
)

:: Write the erl.ini file to set up paths relative to this script
@call :write_ini

:: If a start.boot file is not present, copy one from the named .boot file
@if not exist "%rel_dir%\start.boot" (
  copy "%rel_dir%\%rel_name%.boot" "%rel_dir%\start.boot" >nul
)

@%escript% %nodetool% %node_type% "%node_name%" -setcookie "%node_cookie%" rpc emqx_ctl run_command %args%

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

:: Trim variable
:set_trim
@set %1=%2
@goto :eof

