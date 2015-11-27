@setlocal
@echo off
@setlocal enabledelayedexpansion

@rem Parse arguments. erlsrv.exe prepends erl arguments prior to first ++.
@rem Other args are position dependent.
@set args="%*"
@for /F "delims=++ tokens=1,2,3" %%I in (%args%) do @(
    @set erl_args=%%I
    @call :set_trim node_name %%J
    @rem Trim spaces from the left of %%K (node_root), which may have spaces inside
    @for /f "tokens=* delims= " %%a in ("%%K") do @set node_root=%%a
)

@set releases_dir=%node_root%\releases

@rem parse ERTS version and release version from start_erl.dat
@for /F "usebackq tokens=1,2" %%I in ("%releases_dir%\start_erl.data") do @(
    @call :set_trim erts_version %%I
    @call :set_trim release_version %%J
)

@set erl_exe="%node_root%\erts-%erts_version%\bin\erl.exe"
@set boot_file="%releases_dir%\%release_version%\%node_name%"

@if exist "%releases_dir%\%release_version%\sys.config" (
    @set app_config="%releases_dir%\%release_version%\sys.config"
) else (
    @set app_config="%node_root%\etc\emqttd.config"
)

@if exist "%releases_dir%\%release_version%\vm.args" (
    @set vm_args="%releases_dir%\%release_version%\vm.args"
) else (
    @set vm_args="%node_root%\etc\vm.args"
)

set dest_path=%~dp0
cd /d !dest_path!..\plugins
set current_path=%cd%
set plugins=
for /d %%P in (*) do (
set "plugins=!plugins!"!current_path!\%%P\ebin" "
)
cd /d %node_root%
@%erl_exe% %erl_args% -boot %boot_file% -config %app_config% -args_file %vm_args% -pa %plugins%

:set_trim
@set %1=%2
@goto :EOF
