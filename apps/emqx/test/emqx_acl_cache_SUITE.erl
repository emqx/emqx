%%--------------------------------------------------------------------
%% Copyright (c) 2019-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_acl_cache_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    toggle_acl(true),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_clean_acl_cache(_) ->
    {ok, Client} = emqtt:start_link([{clientid, <<"emqx_c">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1}">>, 0),
    ct:sleep(100),
    ClientPid = case emqx_cm:lookup_channels(<<"emqx_c">>) of
        [Pid] when is_pid(Pid) ->
            Pid;
        Pids when is_list(Pids) ->
            lists:last(Pids);
        _ -> {error, not_found}
    end,
    Caches = gen_server:call(ClientPid, list_acl_cache),
    ct:log("acl caches: ~p", [Caches]),
    ?assert(length(Caches) > 0),
    erlang:send(ClientPid, clean_acl_cache),
    ?assertEqual(0, length(gen_server:call(ClientPid, list_acl_cache))),
    emqtt:stop(Client).

t_drain_acl_cache(_) ->
    {ok, Client} = emqtt:start_link([{clientid, <<"emqx_c">>}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    emqtt:publish(Client, <<"t1">>, <<"{\"x\":1}">>, 0),
    ct:sleep(100),
    ClientPid = case emqx_cm:lookup_channels(<<"emqx_c">>) of
                    [Pid] when is_pid(Pid) ->
                        Pid;
                    Pids when is_list(Pids) ->
                        lists:last(Pids);
                    _ -> {error, not_found}
                end,
    Caches = gen_server:call(ClientPid, list_acl_cache),
    ct:log("acl caches: ~p", [Caches]),
    ?assert(length(Caches) > 0),
    emqx_acl_cache:drain_cache(),
    ?assertEqual(0, length(gen_server:call(ClientPid, list_acl_cache))),
    ct:sleep(100),
    {ok, _, _} = emqtt:subscribe(Client, <<"t2">>, 0),
    ?assert(length(gen_server:call(ClientPid, list_acl_cache)) > 0),
    emqtt:stop(Client).

toggle_acl(Bool) when is_boolean(Bool) ->
    emqx_config:put_zone_conf(default, [mqtt, authorize, enable], Bool).
