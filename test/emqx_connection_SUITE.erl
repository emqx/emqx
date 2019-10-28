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

-module(emqx_connection_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_clean_acl_cache(_Config) ->
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

t_basic(_) ->
    Topic = <<"TopicA">>,
    {ok, C} = emqtt:start_link([{port, 1883}, {clientid, <<"hello">>}]),
    {ok, _} = emqtt:connect(C),
    {ok, _, [1]} = emqtt:subscribe(C, Topic, qos1),
    {ok, _, [2]} = emqtt:subscribe(C, Topic, qos2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    {ok, _} = emqtt:publish(C, Topic, <<"qos 2">>, 2),
    ?assertEqual(3, length(recv_msgs(3))),
    ok = emqtt:disconnect(C).

recv_msgs(Count) ->
    recv_msgs(Count, []).

recv_msgs(0, Msgs) ->
    Msgs;
recv_msgs(Count, Msgs) ->
    receive
        {publish, Msg} ->
            recv_msgs(Count-1, [Msg|Msgs])
    after 100 ->
        Msgs
    end.

