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

-module(emqx_connection_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-include_lib("common_test/include/ct.hrl").

-include("emqx_mqtt.hrl").

-define(STATS, [{mailbox_len, _},
                {heap_size, _},
                {reductions, _},
                {recv_pkt, _},
                {recv_msg, _},
                {send_pkt, _},
                {send_msg, _},
                {recv_oct, _},
                {recv_cnt, _},
                {send_oct, _},
                {send_cnt, _},
                {send_pend, _}]).

-define(ATTRS, [{clean_start, _},
                {client_id, _},
                {connected_at, _},
                {is_bridge, _},
                {is_super, _},
                {keepalive, _},
                {mountpoint, _},
                {peercert, _},
                {peername, _},
                {proto_name, _},
                {proto_ver, _},
                {sockname, _},
                {username, _},
                {zone, _}]).

-define(INFO, [{ack_props, _},
               {active_n, _},
               {clean_start, _},
               {client_id, _},
               {conn_props, _},
               {conn_state, _},
               {connected_at, _},
               {enable_acl, _},
               {is_bridge, _},
               {is_super, _},
               {keepalive, _},
               {mountpoint, _},
               {peercert, _},
               {peername, _},
               {proto_name, _},
               {proto_ver, _},
               {pub_limit, _},
               {rate_limit, _},
               {session, _},
               {sockname, _},
               {socktype, _},
               {topic_aliases, _},
               {username, _},
               {zone, _}]).

all() ->
    [t_connect_api].

init_per_suite(Config) ->
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

t_connect_api(_Config) ->
    {ok, T1} = emqx_client:start_link([{host, "localhost"},
                                       {client_id, <<"client1">>},
                                       {username, <<"testuser1">>},
                                       {password, <<"pass1">>}]),
    {ok, _} = emqx_client:connect(T1),
    CPid = emqx_cm:lookup_conn_pid(<<"client1">>),
    ?STATS = emqx_connection:stats(CPid),
    ?ATTRS = emqx_connection:attrs(CPid),
    ?INFO = emqx_connection:info(CPid),
    SessionPid = emqx_connection:session(CPid),
    true = is_pid(SessionPid),
    emqx_client:disconnect(T1).
