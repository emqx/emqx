%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_plugins_schema).

-behaviour(hocon_schema).

-export([ roots/0
        , fields/1
        , namespace/0
        ]).

-include_lib("typerefl/include/types.hrl").
-include("emqx_plugins.hrl").

namespace() -> "plugin".

roots() -> [?CONF_ROOT].

fields(?CONF_ROOT) ->
    #{fields => root_fields(),
      desc => """
Manage EMQX plugins.
<br>
Plugins can be pre-built as a part of EMQX package,
or installed as a standalone package in a location specified by
<code>install_dir</code> config key
<br>
The standalone-installed plugins are referred to as 'external' plugins.
"""
     };
fields(state) ->
    #{ fields => state_fields(),
       desc => "A per-plugin config to describe the desired state of the plugin."
     }.

state_fields() ->
    [ {name_vsn,
       hoconsc:mk(string(),
                  #{ desc => "The {name}-{version} of the plugin.<br>"
                             "It should match the plugin application name-version as the "
                             "for the plugin release package name<br>"
                             "For example: my_plugin-0.1.0."
                   , required => true
                   })}
    , {enable,
       hoconsc:mk(boolean(),
                  #{ desc => "Set to 'true' to enable this plugin"
                   , required => true
                   })}
    ].

root_fields() ->
    [ {states, fun states/1}
    , {install_dir, fun install_dir/1}
    , {check_interval, fun check_interval/1}
    ].

states(type) -> hoconsc:array(hoconsc:ref(?MODULE, state));
states(required) -> false;
states(default) -> [];
states(desc) -> "An array of plugins in the desired states.<br>"
                "The plugins are started in the defined order";
states(_) -> undefined.

install_dir(type) -> string();
install_dir(required) -> false;
install_dir(default) -> "plugins"; %% runner's root dir
install_dir(T) when T =/= desc -> undefined;
install_dir(desc) -> """
The installation directory for the external plugins.
The plugin beam files and configuration files should reside in
the subdirectory named as <code>emqx_foo_bar-0.1.0</code>.
<br>
NOTE: For security reasons, this directory should **NOT** be writable
by anyone except <code>emqx</code> (or any user which runs EMQX).
""".

check_interval(type) -> emqx_schema:duration();
check_interval(default) -> "5s";
check_interval(T) when T =/= desc -> undefined;
check_interval(desc) -> """
Check interval: check if the status of the plugins in the cluster is consistent, <br>
if the results of 3 consecutive checks are not consistent, then alarm.
""".
