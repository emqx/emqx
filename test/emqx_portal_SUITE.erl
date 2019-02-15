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

-module(emqx_portal_SUITE).

-export([all/0, init_per_suite/1, end_per_suite/1]).
-export([t_rpc/1,
         t_mqtt/1,
         t_forwards_mngr/1
        ]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_mqtt.hrl").
-include("emqx.hrl").

-define(wait(For, Timeout), emqx_ct_helpers:wait_for(?FUNCTION_NAME, ?LINE, fun() -> For end, Timeout)).

all() -> [t_rpc,
          t_mqtt,
          t_forwards_mngr
         ].

init_per_suite(Config) ->
    case node() of
        nonode@nohost ->
            net_kernel:start(['emqx@127.0.0.1', longnames]);
        _ ->
            ok
    end,
    emqx_ct_broker_helpers:run_setup_steps(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_broker_helpers:run_teardown_steps().

t_forwards_mngr(Config) when is_list(Config) ->
    Subs = [{<<"a">>, 1}, {<<"b">>, 2}],
    Cfg = #{address => node(),
            forwards => [<<"mngr">>],
            connect_module => emqx_portal_rpc,
            mountpoint => <<"forwarded">>,
            subscriptions => Subs
           },
    Name = ?FUNCTION_NAME,
    {ok, Pid} = emqx_portal:start_link(Name, Cfg),
    try
        ?assertEqual([<<"mngr">>], emqx_portal:get_forwards(Name)),
        ?assertEqual(Subs, emqx_portal:get_subscriptions(Pid))
    after
        ok = emqx_portal:stop(Pid)
    end.


%% A loopback RPC to local node
t_rpc(Config) when is_list(Config) ->
    Cfg = #{address => node(),
            forwards => [<<"t_rpc/#">>],
            connect_module => emqx_portal_rpc,
            mountpoint => <<"forwarded">>
           },
    {ok, Pid} = emqx_portal:start_link(?FUNCTION_NAME, Cfg),
    ClientId = <<"ClientId">>,
    try
        {ok, ConnPid} = emqx_mock_client:start_link(ClientId),
        {ok, SPid} = emqx_mock_client:open_session(ConnPid, ClientId, internal),
        %% message from a different client, to avoid getting terminated by no-local
        Msg1 = emqx_message:make(<<"ClientId-2">>, ?QOS_2, <<"t_rpc/one">>, <<"hello">>),
        ok = emqx_session:subscribe(SPid, [{<<"forwarded/t_rpc/one">>, #{qos => ?QOS_1}}]),
        PacketId = 1,
        emqx_session:publish(SPid, PacketId, Msg1),
        ?wait(case emqx_mock_client:get_last_message(ConnPid) of
                  {publish, PacketId, #message{topic = <<"forwarded/t_rpc/one">>}} -> true;
                  Other -> Other
              end, 4000),
        emqx_mock_client:close_session(ConnPid)
    after
        ok = emqx_portal:stop(Pid)
    end.

t_mqtt(Config) when is_list(Config) ->
    SendToTopic = <<"t_mqtt/one">>,
    Mountpoint = <<"forwarded/${node}/">>,
    ForwardedTopic = emqx_topic:join(["forwarded", atom_to_list(node()), SendToTopic]),
    Cfg = #{address => "127.0.0.1:1883",
            forwards => [SendToTopic],
            connect_module => emqx_portal_mqtt,
            mountpoint => Mountpoint,
            username => "user",
            clean_start => true,
            client_id => "bridge_aws",
            keepalive => 60000,
            max_inflight => 32,
            password => "passwd",
            proto_ver => mqttv4,
            queue => #{replayq_dir => "data/t_mqtt/",
                       replayq_seg_bytes => 10000,
                       batch_bytes_limit => 1000,
                       batch_count_limit => 10
                      },
            reconnect_delay_ms => 1000,
            ssl => false,
            start_type => manual,
            %% Consume back to forwarded message for verification
            %% NOTE: this is a indefenite loopback without mocking emqx_portal:import_batch/2
            subscriptions => [{ForwardedTopic, 1}]
           },
    Tester = self(),
    Ref = make_ref(),
    meck:new(emqx_portal, [passthrough, no_history]),
    meck:expect(emqx_portal, import_batch, 2,
                fun(Batch, AckFun) ->
                        Tester ! {Ref, Batch},
                        AckFun()
                end),
    {ok, Pid} = emqx_portal:start_link(?FUNCTION_NAME, Cfg),
    ClientId = <<"client-1">>,
    try
        {ok, ConnPid} = emqx_mock_client:start_link(ClientId),
        {ok, SPid} = emqx_mock_client:open_session(ConnPid, ClientId, internal),
        %% message from a different client, to avoid getting terminated by no-local
        Msgs = lists:seq(1, 10),
        lists:foreach(fun(I) ->
                              Msg = emqx_message:make(<<"client-2">>, ?QOS_1, SendToTopic, integer_to_binary(I)),
                              emqx_session:publish(SPid, I, Msg)
                      end, Msgs),
        ok = receive_and_match_messages(Ref, Msgs),
        emqx_mock_client:close_session(ConnPid)
    after
        ok = emqx_portal:stop(Pid),
        meck:unload(emqx_portal)
    end.

receive_and_match_messages(Ref, Msgs) ->
    TRef = erlang:send_after(timer:seconds(4), self(), {Ref, timeout}),
    try
        do_receive_and_match_messages(Ref, Msgs)
    after
        erlang:cancel_timer(TRef)
    end,
    ok.

do_receive_and_match_messages(_Ref, []) -> ok;
do_receive_and_match_messages(Ref, [I | Rest]) ->
    receive
        {Ref, timeout} -> erlang:error(timeout);
        {Ref, [#{payload := P}]} ->
            ?assertEqual(I, binary_to_integer(P)),
            do_receive_and_match_messages(Ref, Rest)
    end.

