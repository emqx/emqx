%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc `postcondition' callback gets session state (either runtime or
%% stored) so each component of the session can verify that it
%% satisfies the invariants.
-module(emqx_persistent_session_ds_fuzzer).

-behaviour(proper_statem).

-include_lib("proper/include/proper.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-include("emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx/include/emqx_persistent_message.hrl").

%% Commands:
-export([
    connect/2,
    disconnect/1,
    publish/1,
    add_generation/0,
    subscribe/2,
    unsubscribe/1,
    consume/1
]).

%% Misc.
-export([
    sample/1,
    sut_state/0,
    cleanup/0,
    print_cmds/1
]).

%% Proper callbacks:
-export([
    initial_state/0,
    command/1,
    precondition/2,
    postcondition/3,
    next_state/3
]).

%% Trace properties:
-export([
    tprop_packet_id_history/1,
    tprop_qos12_delivery/1
]).

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-define(client, emqx_persistent_session_ds_fuzzer_client).
-define(clientid, <<?MODULE_STRING>>).

%% Configuration for the generators:
-define(wait_publishes_time, 3000).
%% List of topics used in the test:
-define(topics, [<<"t1">>, <<"t2">>, <<"t3">>]).
%% List of clientIDs of simulated publishers:
-define(publishers, [<<"pub1">>, <<"pub2">>, <<"pub3">>]).

%% erlfmt-ignore
-type conninfo() ::
        #{
          %% Pid and monitor reference of the client process (emqtt):
          client_pid := pid() | undefined,
          %% Pid and monitor reference of the session (inside EMQX):
          session_pid := pid() | undefined
         }.

-type sub_opts() ::
    #{
        qos := emqx_types:qos()
    }.

%% erlfmt-ignore
-type s() :: #{
    %% Symbolic fields (known at the generation time):
    %%    State of the session predicted by the model:
    conn_opts := map() | undefined,
    subs := #{emqx_types:topic() => sub_opts()},
    %%    Used to assign timestamps to the messages:
    message_seqno := emqx_ds:time(),
    %%    %% State of the client connection predicted by the model:
    connected := boolean(),
    %%    Set to `true' when new messages are published, and reset to
    %%    `false' by `consume' action (used to avoid generating
    %%    redundand `consume' actions):
    has_data := boolean(),

    %% Dynamic fields:
    %%    Information about the current incarnation of the
    %%    client/session:
    conninfo := conninfo() | undefined | _Symbolic
}.

-type model_state() :: s() | undefined.

%%%%% Trace point kinds:
-define(sessds_test_connect, sessds_test_connect).
-define(sessds_test_disconnect, sessds_test_disconnect).
-define(sessds_test_add_generation, sessds_test_add_generation).
-define(sessds_test_subscribe, sessds_test_subscribe).
-define(sessds_test_unsubscribe, sessds_test_unsubscribe).
-define(sessds_test_consume, sessds_test_consume).

%% Traces for messages sent from the test client to SUT:
-define(sessds_test_out_publish, sessds_test_out_publish).
-define(sessds_test_out_puback, sessds_test_out_puback).
-define(sessds_test_out_pubrec, sessds_test_out_pubrec).
-define(sessds_test_out_pubcomp, sessds_test_out_pubcomp).

%% Traces for messages recieved from the SUT:
-define(sessds_test_in_publish, sessds_test_in_publish).
-define(sessds_test_in_pubrel, sessds_test_in_pubrel).
-define(sessds_test_in_garbage, sessds_test_in_garbage).

-define(sessds_test_client_crash, sessds_test_client_crash).
-define(sessds_test_session_crash, sessds_test_session_crash).
-define(sessds_test_processes_died, sessds_test_processes_died).

%%--------------------------------------------------------------------
%% Proper generators
%%--------------------------------------------------------------------

qos() ->
    range(?QOS_0, ?QOS_2).

%% @doc Static part of the client configuration. It is merged with the
%% randomly generated part.
%% erlfmt-ignore
static_client_opts() ->
    #{
        port => 1883,
        proto => v5,
        clientid => ?clientid,
        %% These properties are imporant for test logic
        %%
        %%   Expiry interval must be large enough to avoid
        %%   automatic session expiration:
        properties => #{'Session-Expiry-Interval' => 1000},
        %%   Clean start is not tested here. It's quite trivial to
        %%   test session cleanup in a regular test, but
        %%   accounting for session state resets in the tests is
        %%   not.
        clean_start => false,
        %%   In order for the model to properly account for
        %%   takeover, clients must not auto-reconnect:
        reconnect => false,
        %%   We want to cover as many scenarios where session has
        %%   un-acked messages as possible:
        auto_ack => never
     }.

%% @doc Proper generator for `emqtt:connect' parameters:
connect_(S) ->
    ?LET(
        ReceiveMaximum,
        range(1, 32),
        begin
            DynamicOpts = #{
                properties => #{'Receive-Maximum' => ReceiveMaximum}
            },
            Opts = emqx_utils_maps:deep_merge(static_client_opts(), DynamicOpts),
            {call, ?MODULE, connect, [S, Opts]}
        end
    ).

%% @doc Proper generator that creates a message in one of the topics.
message(MsgId, #{subs := Subs}) ->
    %% Create bias towards topics that the session is subscribed to:
    TopicFreq = [{5, maps:keys(Subs)}, {1, ?topics}],
    Topics = [{Freq, T} || {Freq, Topics} <- TopicFreq, T <- Topics],
    ?LET(
        {Topic, From, QoS},
        {frequency(Topics), oneof(?publishers), qos()},
        #message{
            id = <<>>,
            qos = QoS,
            from = From,
            topic = Topic,
            %% Note: currently faking time in DS is not trivial. For
            %% example, when clock of
            %% `emqx_persistent_session_ds_subs' deviates from
            %% `emqx_ds_replication_layer''s clock iterators may point
            %% too far into the future. So we have no choice but to
            %% use real clock in this test. However, for the sake of
            %% model determinism we don't assign the timestamp in the
            %% generator and do it later in the action:
            timestamp = undefined,
            %% Message payload is unique:
            payload = <<Topic/binary, " ", From/binary, " ", (integer_to_binary(MsgId))/binary>>
        }
    ).

publish_(S = #{message_seqno := SeqNo}) ->
    ?LET(
        BatchSize,
        range(1, 10),
        ?LET(
            Msgs,
            [message(I, S) || I <- lists:seq(SeqNo, SeqNo + BatchSize)],
            {call, ?MODULE, publish, [Msgs]}
        )
    ).

subscribe_() ->
    ?LET(
        {Topic, QoS},
        {oneof(?topics), qos()},
        {call, ?MODULE, subscribe, [Topic, QoS]}
    ).

unsubscribe_() ->
    ?LET(
        Topic,
        oneof(?topics),
        {call, ?MODULE, unsubscribe, [Topic]}
    ).

%%--------------------------------------------------------------------
%% Operations
%%--------------------------------------------------------------------

%% @doc (Re)connect emqtt client to EMQX. If the client was previously
%% connected, this function will wait for the takeover.
connect(S, Opts = #{clientid := ClientId}) ->
    ?tp(info, ?sessds_test_connect, #{opts => Opts, pid => self()}),
    %% Check metadata of the previous state to catch situations when
    %% the testcase starts from a dirty state:
    true = check_session_metadata(S),
    %% Start the new client:
    {ok, ClientPid} = emqtt:start_link(Opts),
    unlink(ClientPid),
    {ok, _} = emqtt:connect(ClientPid),
    %% Wait for the old client stepdown:
    maybe_wait_stepdown(S),
    %% Update connection info:
    register(?client, ClientPid),
    [SessionPid] = emqx_cm:lookup_channels(local, ClientId),
    %% Return `conninfo()':
    #{
        client_pid => ClientPid,
        session_pid => SessionPid
    }.

%% @doc Shut down emqtt
disconnect(#{conninfo := ConnInfo = #{client_pid := CPid}}) ->
    ?tp(info, ?sessds_test_disconnect, #{pid => CPid}),
    emqtt:stop(client_pid()),
    wait_stepdown(ConnInfo),
    flush_client_messages(CPid).

publish(Batch) ->
    %% Produce traces for each message we're about to publish:
    [?tp(info, ?sessds_test_out_publish, emqx_message:to_map(Msg)) || Msg <- Batch],
    %% We bypass persistent session router for simplicity:
    ok = emqx_ds:store_batch(?PERSISTENT_MESSAGE_DB, [
        Msg#message{timestamp = emqx_message:timestamp_now()}
     || Msg <- Batch
    ]).

add_generation() ->
    ?tp(info, ?sessds_test_add_generation, #{}),
    emqx_ds:add_generation(?PERSISTENT_MESSAGE_DB).

subscribe(Topic, QoS) ->
    ?tp(info, ?sessds_test_subscribe, #{topic => Topic, qos => QoS}),
    emqtt:subscribe(client_pid(), Topic, QoS).

unsubscribe(Topic) ->
    ?tp(info, ?sessds_test_unsubscribe, #{topic => Topic}),
    emqtt:unsubscribe(client_pid(), Topic).

consume(S = #{conninfo := #{client_pid := CPID, session_pid := SPID}}) ->
    %% Set up monitoring to detect crashes early:
    %% Consume and ack all messages we can get from the broker:
    CMRef = monitor(process, CPID),
    SMRef = monitor(process, SPID),
    ?tp_span(
        info,
        ?sessds_test_consume,
        #{},
        receive_ack_loop(S, CMRef, SMRef, ok)
    ).

%% @doc This function receives and acknowledges all MQTT messages sent
%% to the client
receive_ack_loop(
    S = #{conninfo := #{client_pid := CPID}},
    CMRef,
    SMRef,
    Result
) ->
    receive
        %% Handle MQTT packets:
        {publish, Msg = #{client_pid := CPID}} ->
            ?tp(info, ?sessds_test_in_publish, Msg),
            #{packet_id := PID, qos := QoS} = Msg,
            %% Ack:
            case QoS of
                ?QOS_0 ->
                    ok;
                ?QOS_1 ->
                    ?tp(info, ?sessds_test_out_puback, #{packet_id => PID}),
                    emqtt:puback(client_pid(), PID);
                ?QOS_2 ->
                    ?tp(info, ?sessds_test_out_pubrec, #{packet_id => PID}),
                    emqtt:pubrec(client_pid(), PID)
            end,
            receive_ack_loop(S, CMRef, SMRef, Result);
        {pubrel, Msg} ->
            %% FIXME: currently emqtt doesn't supply `client_pid' to
            %% pubrel messages, so it's impossible to discard old
            %% pubrels in case the client restarts. This can create
            %% hard-to-debug situations. Proper solution would be to
            %% fix emqtt, but for now we rely on flushing the messages
            %% in the clause below.
            ?tp(info, ?sessds_test_in_pubrel, Msg),
            #{packet_id := PID} = Msg,
            emqtt:pubcomp(client_pid(), PID),
            ?tp(info, ?sessds_test_out_pubcomp, #{packet_id => PID}),
            receive_ack_loop(S, CMRef, SMRef, Result);
        %% Handle client/session crash:
        {'DOWN', CMRef, process, CPID, Reason} ->
            ?tp(warning, ?sessds_test_client_crash, #{pid => CPID, reason => Reason}),
            flush_emqtt_messages(),
            receive_ack_loop(S, CMRef, SMRef, {error, client_crash});
        {'DOWN', SMRef, process, SessPid, Reason} ->
            ?tp(warning, ?sessds_test_session_crash, #{pid => SessPid, reason => Reason}),
            receive_ack_loop(S, CMRef, SMRef, {error, session_crash});
        Other ->
            %% FIXME: this may include messages from the older
            %% incarnations of the client. Find a better way to deal
            %% with them:
            ?tp(warning, ?sessds_test_in_garbage, #{message => Other}),
            receive_ack_loop(S, CMRef, SMRef, Result)
    after ?wait_publishes_time ->
        demonitor(CMRef, [flush]),
        demonitor(SMRef, [flush]),
        Result
    end.

flush_emqtt_messages() ->
    receive
        {pubrel, #{}} ->
            flush_emqtt_messages();
        {publish, #{}} ->
            flush_emqtt_messages()
    after 0 ->
        ok
    end.

%%--------------------------------------------------------------------
%% Misc. API
%%--------------------------------------------------------------------

sample(Size) ->
    proper_gen:pick(commands(?MODULE), Size).

cleanup() ->
    catch emqtt:stop(client_pid()),
    emqx_cm:kick_session(?clientid),
    emqx_persistent_session_ds:destroy_session(?clientid).

sut_state() ->
    emqx_persistent_session_ds:print_session(?clientid).

%% @doc Pretty-printer for commands
print_cmds(L) ->
    [
        case I of
            ({set, _, {call, ?MODULE, connect, [_, Opts]}}) ->
                io_lib:format("  connect(~0p)~n", [Opts]);
            ({set, _, {call, ?MODULE, publish, [Batch]}}) ->
                Args = [
                    maps:with(
                        [qos, from, topic, payload], emqx_message:to_map(Msg)
                    )
                 || Msg <- Batch
                ],
                io_lib:format("  publish(~n    ~s~n  )~n", [pprint_msgs(Args)]);
            ({set, _, {call, ?MODULE, Fun, _}}) when Fun =:= consume; Fun =:= disconnect ->
                io_lib:format("  ~p(...)~n", [Fun]);
            %% Generic command pretty-printer:
            ({set, _, {call, Module, Fun, Args}}) ->
                ModStr =
                    case Module of
                        ?MODULE -> [];
                        _ -> [atom_to_binary(Module), $:]
                    end,
                io_lib:format("  ~s~p(~s)~n", [ModStr, Fun, pprint_args(Args)]);
            %% Fallback:
            (Other) ->
                io_lib:format("  ~0p~n", [Other])
        end
     || I <- L
    ].

pprint_msgs(Msgs) ->
    lists:join(",\n    ", [io_lib:format("~0p", [I]) || I <- Msgs]).

pprint_args(Args) ->
    lists:join(", ", [io_lib:format("~0p", [I]) || I <- Args]).

%%--------------------------------------------------------------------
%% Trace properties
%%--------------------------------------------------------------------

%% @doc Verify QoS 1/2 flows for each packet ID.
tprop_packet_id_history(Trace) ->
    {_, NFlows} = lists:foldl(
        fun tprop_packet_id_history/2,
        {#{}, 0},
        Trace
    ),
    ct:pal("~p: Verified ~p packet flows.~n", [?FUNCTION_NAME, NFlows]),
    true.

tprop_packet_id_history(I = #{?snk_kind := Kind}, {Acc, NFlows}) ->
    case Kind of
        ?sessds_test_in_publish ->
            {tprop_pid_publish(I, Acc), NFlows + 1};
        %% QoS1:
        ?sessds_test_out_puback ->
            #{packet_id := PID} = I,
            #{PID := {publish, #{qos := ?QOS_1}}} = Acc,
            {maps:remove(PID, Acc), NFlows};
        %% QoS2:
        ?sessds_test_out_pubrec ->
            #{packet_id := PID} = I,
            #{PID := {publish, #{qos := ?QOS_2}}} = Acc,
            {Acc#{PID := pubrec}, NFlows};
        ?sessds_test_in_pubrel ->
            #{packet_id := PID} = I,
            case Acc of
                #{PID := PIDState} ->
                    ?assertMatch(pubrec, PIDState);
                #{} ->
                    ok
            end,
            {Acc#{PID => pubrel}, NFlows};
        ?sessds_test_out_pubcomp ->
            #{packet_id := PID} = I,
            ?assertMatch(#{PID := pubrel}, Acc),
            {maps:remove(PID, Acc), NFlows};
        _ ->
            {Acc, NFlows}
    end.

tprop_pid_publish(#{packet_id := undefined, qos := ?QOS_0}, Acc) ->
    Acc;
tprop_pid_publish(#{packet_id := PID, dup := Dup} = I, Acc) ->
    case Acc of
        #{PID := {publish, Old}} ->
            ?assert(Dup, #{
                msg => "Duplicated message with DUP=false",
                old => Old,
                new => I
            }),
            compare_msgs(Old, I),
            Acc#{PID := {publish, I}};
        #{PID := Old} ->
            error(#{
                msg => "Unexpected packet",
                packet_id => PID,
                old => Old,
                new => I
            });
        #{} ->
            Acc#{PID => {publish, I}}
    end.

%% @doc This property verifies that every message published to the
%% topic while the client is subscribed is eventually delivered. Note:
%% it only verifies the fact of delivery.
tprop_qos12_delivery(Trace) ->
    {_, _, N} = lists:foldl(fun tprop_qos12_delivery/2, {#{}, [], 0}, Trace),
    ct:pal("~p: Confirmed delivery of ~p messages.", [?FUNCTION_NAME, N]),
    true.

tprop_qos12_delivery(#{?snk_kind := Kind} = Event, {Subs, Pending, N}) ->
    case Kind of
        ?sessds_test_subscribe ->
            #{topic := Topic, qos := QoS} = Event,
            case QoS of
                0 ->
                    %% This property ignores QoS 0 subscriptions for
                    %% simplicity. We treat such subscriptions
                    %% identically to "unsubscribed":
                    tprop_qos12_delivery_drop_sub(Topic, Subs, Pending, N);
                _ ->
                    {Subs#{Topic => true}, Pending, N}
            end;
        ?sessds_test_unsubscribe ->
            #{topic := Topic} = Event,
            tprop_qos12_delivery_drop_sub(Topic, Subs, Pending, N);
        ?sessds_test_out_publish ->
            #{topic := Topic, qos := Qos, payload := Payload} = Event,
            case Qos > 0 andalso maps:is_key(Topic, Subs) of
                true ->
                    {Subs, [{Topic, Payload} | Pending], N + 1};
                false ->
                    {Subs, Pending, N}
            end;
        ?sessds_test_in_publish ->
            {Subs, tprop_qos12_delivery_consume_msg(Event, Pending), N};
        ?sessds_test_consume ->
            case Event of
                #{?snk_span := {complete, _}} ->
                    ?assertMatch(
                        [],
                        Pending,
                        "consume action should complete delivery of all pending messages"
                    );
                #{?snk_span := start} ->
                    ok
            end,
            {Subs, Pending, N};
        _ ->
            {Subs, Pending, N}
    end.

tprop_qos12_delivery_drop_sub(Topic, Subs, Pending, N) ->
    {
        maps:remove(Topic, Subs),
        lists:filter(fun({T, _Payload}) -> T =/= Topic end, Pending),
        N
    }.

tprop_qos12_delivery_consume_msg(#{topic := Topic, payload := Payload}, Pending) ->
    Pending -- [{Topic, Payload}].

compare_msgs(Expect, Got) ->
    Fields = [qos, retain, topic, properties, payload],
    ?assertEqual(maps:with(Fields, Expect), maps:with(Fields, Got)).

%%--------------------------------------------------------------------
%% Statem callbacks
%%--------------------------------------------------------------------

%% erlfmt-ignore
command(undefined) ->
    connect_(undefined);
command(S = #{connected := Conn, has_data := HasData, subs := Subs}) ->
    HasSubs = maps:size(Subs) > 0,
    %% Commands that are executed in any state:
    Common =
         %% FIXME: Currently takeover may lead to state corruption due
         %% to serialization problems. `|| not Conn]` effectively
         %% disables takeover. This condition should be removed when
         %% takeover is fixed.
         [{1,  connect_(S)}                     || not Conn] ++
         [{2,  {call, ?MODULE, add_generation, []}},
          %% Publish some messages occasionally even when there are no
          %% subs:
          {2,  publish_(S)}
         ],
    %% Commands that are executed when client is connected:
    Connected =
        [{5,  publish_(S)}                      || HasSubs and not HasData] ++
        [{5,  {call, ?MODULE, consume, [S]}}    || HasData and HasSubs] ++
        [
         {1,  {call, ?MODULE, disconnect, [S]}},
         {1,  unsubscribe_()},
         {3,  subscribe_()}
        ],
    case Conn of
        true  -> frequency(Connected ++ Common);
        false -> frequency(Common)
    end.

-spec initial_state() -> model_state().
initial_state() ->
    undefined.

%% Initial connection:
next_state(undefined, Ret, {call, ?MODULE, connect, [_, Opts]}) ->
    #{
        conn_opts => Opts,
        subs => #{},
        message_seqno => 0,
        connected => true,
        has_data => false,
        conninfo => Ret
    };
%% (Re)connect:
next_state(S, Ret, {call, ?MODULE, connect, [_, Opts]}) ->
    S#{
        conninfo := Ret,
        connected := true,
        conn_opts := Opts
    };
%% Disconnect:
next_state(S, _Ret, {call, ?MODULE, disconnect, _}) ->
    S#{
        connected := false,
        conninfo := undefined
    };
%% Publish/consume messages:
next_state(S = #{message_seqno := T}, _Ret, {call, ?MODULE, publish, [Batch]}) ->
    S#{
        has_data := true,
        message_seqno := T + length(Batch)
    };
next_state(S, _Ret, {call, ?MODULE, consume, _}) ->
    S#{
        has_data := false
    };
%% Add generation:
next_state(S, _Ret, {call, ?MODULE, add_generation, _}) ->
    S;
%% Subscribe/unsubscribe topics:
next_state(S = #{subs := Subs0}, _Ret, {call, ?MODULE, subscribe, [Topic, QoS]}) ->
    Subs = Subs0#{Topic => #{qos => QoS}},
    S#{
        subs := Subs
    };
next_state(S = #{subs := Subs0}, _Ret, {call, ?MODULE, unsubscribe, [Topic]}) ->
    Subs = maps:remove(Topic, Subs0),
    S#{
        subs := Subs
    }.

precondition(_, _) ->
    true.

postcondition(PrevState, Call, Result) ->
    CurrentState = next_state(PrevState, Result, Call),
    case Call of
        {call, ?MODULE, connect, _} ->
            check_session_metadata(CurrentState);
        {call, ?MODULE, consume, _} ->
            Result =:= ok;
        _ ->
            true
    end and check_invariants(CurrentState) and
        check_processes(CurrentState) and
        check_session_metadata(CurrentState).

%%--------------------------------------------------------------------
%% Misc.
%%--------------------------------------------------------------------

%% @doc Check that the processes are alive when they should be
%% according to the model prediction:
check_processes(#{connected := false}) ->
    %% TODO: check that the channel is stopped:
    true;
check_processes(#{connected := true, conninfo := #{client_pid := CPid, session_pid := SPid}}) ->
    SA = is_process_alive(CPid),
    CA = is_process_alive(SPid),
    SA and CA orelse
        begin
            ?tp(
                error,
                ?sessds_test_processes_died,
                #{
                    client => {CPid, CA},
                    session => {SPid, SA}
                }
            ),
            false
        end.

check_session_metadata(undefined) ->
    case emqx_persistent_session_ds_state:print_session(?clientid) of
        undefined ->
            true;
        State ->
            ?tp(error, "Found unexpected session metadata", #{state => State}),
            false
    end;
check_session_metadata(#{}) ->
    case emqx_persistent_session_ds_state:print_session(?clientid) of
        undefined ->
            ?tp(error, "Session metadata was expected, but not found", #{}),
            false;
        _ ->
            true
    end.

check_invariants(ModelState) ->
    emqx_persistent_session_ds:state_invariants(ModelState, sut_state()).

maybe_wait_stepdown(#{connected := true, conninfo := ConnInfo}) ->
    wait_stepdown(ConnInfo);
maybe_wait_stepdown(_) ->
    ok.

wait_stepdown(#{session_pid := SessionPid}) ->
    %% Wait for the old client to stop:
    CMRef = monitor(process, client_pid()),
    receive
        {'DOWN', CMRef, process, _, _} ->
            ok
    after 5_000 ->
        error(timeout_waiting_for_client_down)
    end,
    wait_channel_disappear(?clientid, SessionPid, 100).

wait_channel_disappear(ClientId, Pid, N) ->
    Chans = emqx_cm:lookup_channels(local, ClientId),
    case lists:member(Pid, Chans) of
        false ->
            ok;
        true when N > 0 ->
            timer:sleep(10),
            wait_channel_disappear(ClientId, Pid, N - 1);
        true ->
            error({timeout_waiting_for_takeover, ClientId, Pid, Chans})
    end.

client_pid() ->
    whereis(?client).

flush_client_messages(CPid) ->
    receive
        {_, #{client_pid := CPid}} ->
            flush_client_messages(CPid)
    after 0 ->
        ok
    end.
