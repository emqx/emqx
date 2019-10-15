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

-define(Transport, esockd_transport).

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    ok = meck:new(esockd_transport, [passthrough, no_history]),
    ok = meck:new(emqx_channel, [passthrough, no_history]),
    Config.

end_per_suite(_Config) ->
    ok.

t_start_link_error(_) ->
    process_flag(trap_exit, true),
    ok = meck:expect(esockd_transport, wait, fun(_Sock) -> {error, enotconn} end),
    ok = meck:expect(esockd_transport, fast_close, fun(_Sock) -> ok end),
    {ok, Pid} = emqx_connection:start_link(esockd_transport, socket, []),
    timer:sleep(100),
    ?assertNot(erlang:is_process_alive(Pid)),
    ?assertEqual([{'EXIT', Pid, normal}], proc_mailbox()).

todo_t_basic(_) ->
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

proc_mailbox() ->
    proc_mailbox(self()).
proc_mailbox(Pid) ->
    {messages, Msgs} = erlang:process_info(Pid, messages),
    Msgs.

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

