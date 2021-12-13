%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        ]).

-include_lib("typerefl/include/types.hrl").

roots() -> ["plugins"].

fields("plugins") ->
    #{fields => fields(),
      desc => """
Manage EMQ X plugins.
<br>
Plugins can be pre-built as a part of EMQ X package,
or installed as a standalone package in a location specified by
<code>install_dir</code> config key
<br>
The standalone-installed plugins are referred to as 'external' plugins.
"""
     }.

fields() ->
    [ {prebuilt, fun prebuilt/1}
    , {external, fun external/1}
    , {install_dir, fun install_dir/1}
    ].

prebuilt(type) -> hoconsc:map("name", boolean());
prebuilt(nullable) -> true;
prebuilt(T) when T=/= desc -> undefined;
prebuilt(desc) -> """
A map() from plugin name to a boolean (true | false) flag to indicate
whether or not to enable the prebuilt plugin.
<br>
Most of the prebuilt plugins from 4.x are converted into features since 5.0.
""" ++ prebuilt_plugins() ++
"""
<br>
Enabled plugins are loaded (started) as a part of EMQ X node's boot sequence.
Plugins can be loaded on the fly, and enabled from dashbaord UI and/or CLI.
<br>
Example config: <code>{emqx_foo_bar: true, emqx_bazz: false}</code>
""".

external(type) -> hoconsc:map("name", string());
external(nullable) -> true;
external(T) when T =/= desc -> undefined;
external(desc) ->
"""
A map from plugin name to a version number string for enabled ones.
To disable an external plugin, set the value to 'false'.
<br>
Enabled plugins are loaded (started) as a part of EMQ X node's boot sequence.
Plugins can be loaded on the fly, and enabled from dashbaord UI and/or CLI.
<br>
Example config: <code>{emqx_extplug1: \"0.1.0\", emqx_extplug2: false}</code>
""".

install_dir(type) -> string();
install_dir(nullable) -> true;
install_dir(default) -> "plugins"; %% runner's root dir
install_dir(T) when T =/= desc -> undefined;
install_dir(desc) -> """
In which directory are the external plugins installed.
The plugin beam files and configuration files should reside in
the sub-directory named as <code>emqx_foo_bar-0.1.0</code>.
<br>
NOTE: For security reasons, this directory should **NOT** be writable
by anyone expect for <code>emqx</code> (or any user which runs EMQ X)
""".

%% TODO: when we have some prebuilt plugins, change this function to:
%% """
%% The names should be one of
%%   - name1
%%   - name2
%% """
prebuilt_plugins() ->
    "So far, we do not have any prebuilt plugins".
