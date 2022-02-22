:: The batch file for 'emqx ctl' command

@set args=%*

:: Discover the release root directory from the directory
:: of this script
@set script_dir=%~dp0
@for %%A in ("%script_dir%\..") do @(
  set rel_root_dir=%%~fA
)
@%rel_root_dir%\bin\emqx.cmd ctl %args%
