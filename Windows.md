# Build and run EMQX on Windows

NOTE: The instructions and examples are based on Windows 10.

## Build Environment

### Visual studio for C/C++ compile and link

EMQX includes Erlang NIF (Native Implmented Function) components, implemented
in C/C++. To compile and link C/C++ libraries, the easiest way is perhaps to
install Visual Studio.

Visual Studio 2019 is used in our tests.
If you are like me (@zmstone), do not know where to start,
please follow this OTP guide:
https://github.com/erlang/otp/blob/master/HOWTO/INSTALL-WIN32.md

NOTE: To avoid surprises, you may need to add below two paths to `Path` environment variable
and order them before other paths.

```
C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Tools\MSVC\14.28.29910\bin\Hostx64\x64
C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Auxiliary\Build
```

Depending on your visual studio version and OS, the paths may differ.
The first path is for rebar3 port compiler to find `cl.exe` and `link.exe`
The second path is for CMD to setup environment variables.

### Erlang/OTP

Install Erlang/OTP 24.2.1 from https://www.erlang.org/downloads
You may need to edit the `Path` environment variable to allow running
Erlang commands such as `erl` from CMD.

To validate Erlang installation in CMD :

* Start (or restart) CMD

* Execute `erl` command to enter Erlang shell

* Evaluate Erlang expression `halt().` to exit Erlang shell.

e.g.

```
PS C:\Users\zmsto> erl
Eshell V12.2.1  (abort with ^G)
1> halt().
```

### bash

All EMQX build/run scripts are either in `bash` or `escript`.
`escript` is installed as a part of Erlang. To install a `bash`
environment in Windows, there are quite a few options.

Cygwin is what we tested with.

* Add `cygwin\bin` dir to `Path` environment variable
  To do so, search for Edit environment variable in control pannel and
  add `C:\tools\cygwin\bin` (depending on the location where it was installed)
  to `Path` list.

* Validate installation.
  Start (restart) CMD console and execute `which bash`, it should
  print out `/usr/bin/bash`

### Other tools

Some of the unix world tools are required to build EMQX.  Including:

* git
* curl
* make
* jq
* zip / unzip

We recommend using [scoop](https://scoop.sh/), or [Chocolatey](https://chocolatey.org/install) to install the tools.

When using scoop:

```
scoop install git curl make jq zip unzip
```

## Build EMQX source code

* Clone the repo: `git clone https://github.com/emqx/emqx.git`

* Start CMD

* Execute `vcvarsall.bat x86_amd64` to load environment variables

* Change to emqx directory and execute `make`

### Possible errors

* `'cl.exe' is not recognized as an internal or external command`
  This error is likely due to Visual Studio executables are not set in `Path` environment variable.
  To fix it, either add path like `C:\Program Files (x86)\Microsoft Visual Studio\2019\Community\VC\Tools\MSVC\14.28.29910\bin\Hostx64\x64`
  to `Paht`. Or make sure `vcvarsall.bat x86_amd64` is executed prior to the `make` command

* `fatal error C1083: Cannot open include file: 'assert.h': No such file or directory`
  If Visual Studio is installed correctly, this is likely `LIB` and `LIB_PATH` environment
  variables are not set. Make sure `vcvarsall.bat x86_amd64` is executed prior to the `make` command

* `link: extra operand 'some.obj'`
  This is likely due ot the usage of GNU `lnik.exe` but not the one from Visual Studio.
  Exeucte `link.exe --version` to inspect which one is in use. The one installed from
  Visual Studio should print out `Microsoft (R) Incremental Linker`.
  To fix it, Visual Studio's bin paths should be ordered prior to Cygwin's (or similar installation's)
  bin paths in `Path` environment variable.

## Run EMQX

To start EMQX broker.

Execute `_build\emqx\rel\emqx>.\bin\emqx console` or `_build\emqx\rel\emqx>.\bin\emqx start` to start EMQX.

Then execute `_build\emqx\rel\emqx>.\bin\emqx_ctl status` to check status.
If everything works fine, it should print out

```
Node 'emqx@127.0.0.1' 4.3-beta.1 is started
Application emqx 4.3.0 is running
```
