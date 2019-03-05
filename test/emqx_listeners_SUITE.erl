%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_listeners_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

all() ->
    [start_stop_listeners,
     restart_listeners].

init_per_suite(Config) ->
    NewConfig = generate_config(),
    application:ensure_all_started(esockd),
    application:ensure_all_started(cowboy),
    lists:foreach(fun set_app_env/1, NewConfig),
    Config.

end_per_suite(_Config) ->
    application:stop(esockd),
    application:stop(cowboy).

start_stop_listeners(_) ->
    ok = emqx_listeners:start(),
    ok = emqx_listeners:stop().

restart_listeners(_) ->
    ok = emqx_listeners:start(),
    ok = emqx_listeners:stop(),
    ok = emqx_listeners:restart(),
    ok = emqx_listeners:stop().

generate_config() ->
    Schema = cuttlefish_schema:files([local_path(["priv", "emqx.schema"])]),
    Conf = conf_parse:file([local_path(["etc", "gen.emqx.conf"])]),
    cuttlefish_generator:map(Schema, Conf).

set_app_env({App, Lists}) ->
    lists:foreach(fun({acl_file, _Var}) ->
                      application:set_env(App, acl_file, local_path(["etc", "acl.conf"]));
                     ({plugins_loaded_file, _Var}) ->
                      application:set_env(App, plugins_loaded_file, local_path(["test", "emqx_SUITE_data","loaded_plugins"]));
                     ({Par, Var}) ->
                      application:set_env(App, Par, Var)
                  end, Lists).

local_path(Components, Module) ->
    filename:join([get_base_dir(Module) | Components]).

local_path(Components) ->
    local_path(Components, ?MODULE).

get_base_dir(Module) ->
    {file, Here} = code:is_loaded(Module),
    filename:dirname(filename:dirname(Here)).

get_base_dir() ->
    get_base_dir(?MODULE).
