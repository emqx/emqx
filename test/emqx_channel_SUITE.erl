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

-module(emqx_channel_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

all() ->
    [t_connect_api].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([]).

t_connect_api(_Config) ->
    {ok, T1} = emqx_client:start_link([{host, "localhost"},
                                       {client_id, <<"client1">>},
                                       {username, <<"testuser1">>},
                                       {password, <<"pass1">>}]),
    {ok, _} = emqx_client:connect(T1),
    CPid = emqx_cm:lookup_conn_pid(<<"client1">>),
    ConnStats = emqx_channel:stats(CPid),
    ok = t_stats(ConnStats),
    ConnAttrs = emqx_channel:attrs(CPid),
    ok = t_attrs(ConnAttrs),
    ConnInfo = emqx_channel:info(CPid),
    ok = t_info(ConnInfo),
    SessionPid = emqx_channel:session(CPid),
    true = is_pid(SessionPid),
    emqx_client:disconnect(T1).

t_info(ConnInfo) ->
    ?assertEqual(tcp, maps:get(socktype, ConnInfo)),
    ?assertEqual(running, maps:get(conn_state, ConnInfo)),
    ?assertEqual(<<"client1">>, maps:get(client_id, ConnInfo)),
    ?assertEqual(<<"testuser1">>, maps:get(username, ConnInfo)),
    ?assertEqual(<<"MQTT">>, maps:get(proto_name, ConnInfo)).

t_attrs(AttrsData) ->
    ?assertEqual(<<"client1">>, maps:get(client_id, AttrsData)),
    ?assertEqual(emqx_channel, maps:get(conn_mod, AttrsData)),
    ?assertEqual(<<"testuser1">>, maps:get(username, AttrsData)).

t_stats(StatsData) ->
    ?assertEqual(true, proplists:get_value(recv_oct, StatsData) >= 0),
    ?assertEqual(true, proplists:get_value(mailbox_len, StatsData) >= 0),
    ?assertEqual(true, proplists:get_value(heap_size, StatsData) >= 0),
    ?assertEqual(true, proplists:get_value(reductions, StatsData) >=0),
    ?assertEqual(true, proplists:get_value(recv_pkt, StatsData) =:=1),
    ?assertEqual(true, proplists:get_value(recv_msg, StatsData) >=0),
    ?assertEqual(true, proplists:get_value(send_pkt, StatsData) =:=1).
