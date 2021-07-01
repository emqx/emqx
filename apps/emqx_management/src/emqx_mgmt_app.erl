%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_app).

-behaviour(application).

-emqx_plugin(?MODULE).

-define(APP, emqx_management).

-export([ start/2
        , stop/1
        ]).

-include("emqx_mgmt.hrl").

start(_Type, _Args) ->
    Conf = filename:join(emqx:get_env(plugins_etc_dir), 'emqx_management.conf'),
    {ok, RawConf} = hocon:load(Conf),
    #{emqx_management := Config} =
        hocon_schema:check_plain(emqx_management_schema, RawConf, #{atom_key => true}),
    [application:set_env(?APP, Key, maps:get(Key, Config)) || Key <- maps:keys(Config)],
    {ok, Sup} = emqx_mgmt_sup:start_link(),
    ok = ekka_rlog:wait_for_shards([?MANAGEMENT_SHARD], infinity),
    _ = emqx_mgmt_auth:add_default_app(),
    emqx_mgmt_http:start_listeners(),
    emqx_mgmt_cli:load(),
    {ok, Sup}.

stop(_State) ->
    emqx_mgmt_http:stop_listeners().
