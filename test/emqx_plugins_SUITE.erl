%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_plugins_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->

    %% Compile extra plugin code

    DataPath = proplists:get_value(data_dir, Config),
    AppPath = filename:join([DataPath, "emqx_mini_plugin"]),
    Cmd = lists:flatten(io_lib:format("cd ~s && make", [AppPath])),

    ct:pal("Executing ~s~n", [Cmd]),
    ct:pal("~n ~s~n", [os:cmd(Cmd)]),

    code:add_path(filename:join([AppPath, "_build", "default", "lib", "emqx_mini_plugin", "ebin"])),

    put(loaded_file, filename:join([DataPath, "loaded_plugins"])),
    emqx_ct_helpers:start_apps([], fun set_sepecial_cfg/1),

    Config.

set_sepecial_cfg(_) ->
    ExpandPath = filename:dirname(code:lib_dir(emqx_mini_plugin)),

    application:set_env(emqx, plugins_loaded_file, get(loaded_file)),
    application:set_env(emqx, expand_plugins_dir, ExpandPath),
    ok.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_load(_) ->
    {error, load_app_fail} = emqx_plugins:load_expand_plugin("./not_existed_path/"),

    {error, not_started} = emqx_plugins:unload(emqx_mini_plugin),
    {ok, _} = emqx_plugins:load(emqx_mini_plugin),
    ok = emqx_plugins:unload(emqx_mini_plugin).
