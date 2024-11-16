%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Below is the strategy for stateful property-based testing of
%% the durable session.
%%
%% Background: PropER can only work with determinstic systems. But
%% session interacts with a black box (DS), and exhibits
%% non-deterministic behavior due to e.g. uncertain order of event
%% delivery. Therefore, it's hard to define a robust model of the
%% session that PropER could compare with the SUT.
%%
%% Solution: instead of using PropER for end-to-end black box
%% verification, we use it as a fuzzer of sorts to generate random
%% client behaviors.
%%
%% `postcondition' callback gets session state (either runtime or
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

-define(clientid, <<?MODULE_STRING>>).

%% Commands:
-export([
    connect/2,
    disconnect/1,
    publish/1,
    add_generation/0,
    subscribe/3,
    unsubscribe/2,
    consume/1
]).

%% Misc.
-export([
    sample/1,
    killall/1,
    cleanup/0
]).

%% Proper callbacks:
-export([
    initial_state/0,
    command/1,
    precondition/2,
    postcondition/3,
    next_state/3
]).

-type config() ::
    #{
        wait_publishes_time := non_neg_integer(),
        %% List of topics used in the test. They must not overlap.
        %% This list is used for both publishing and subscribing,
        %% therefore wildcards are not supported.
        topics := [emqx_types:topic()],
        %% Static client configuration (port, etc.)
        client_config := map(),
        %% List of client IDs for the simulated publishers:
        publishers := [emqx_types:clientid()]
    }.

%% erlfmt-ignore
-type conninfo() ::
        #{
          %% Pid and monitor reference of the client process (emqtt):
          client_pid := pid() | undefined,
          client_mref := reference() | undefined,
          %% Pid and monitor reference of the session (inside EMQX):
          session_pid := pid() | undefined,
          session_mref := reference() | undefined
         } | undefined.

-type sub_opts() ::
    #{
        qos := emqx_types:qos()
    }.

-type model_state() ::
    #{
        %% Connection options:
        conn_opts := map(),
        subs := #{emqx_types:topic() => sub_opts()}
    }
    | undefined.

%% erlfmt-ignore
-record(s, {
    %% Symbolic fields (known at the generation time):
    %%    Static configuration of the testcase:
    conf :: config(),
    %%    State of the session predicted by the model:
    model_state :: model_state(),
    %%    Used to assign timestamps to the messages:
    faketime = 0 :: emqx_ds:time(),
    %%    %% State of the client connection predicted by the model:
    connected = false :: boolean(),
    %%    Set to `true' when new messages are published, and reset to
    %%    `false' by `consume' action (used to avoid generating
    %%    redundand `consume' actions):
    has_data = false :: boolean(),
    %% Dynamic fields:
    %%    Information about the current incarnation of the
    %%    client/session:
    conninfo :: conninfo() | _Symbolic
}).

%%--------------------------------------------------------------------
%% Proper generators
%%--------------------------------------------------------------------

qos() ->
    range(?QOS_0, ?QOS_2).

%% @doc Proper generator for `emqtt:connect' parameters:
connect_(S = #s{conf = #{client_config := StaticOpts}}) ->
    ?LET(
        {Clean, ReceiveMaximum},
        {frequency([{1, true}, {10099, false}]), range(1, 32)},
        begin
            DynamicOpts = #{
                clean_start => Clean,
                properies => #{'Receive-Maximum' => ReceiveMaximum}
            },
            Opts = emqx_utils_maps:deep_merge(StaticOpts, DynamicOpts),
            {call, ?MODULE, connect, [S, Opts]}
        end
    ).

%% @doc Proper generator that creates a message in one of the topics
%% that the client subscribes.
message(#s{
    faketime = T,
    model_state = #{subs := Subs},
    conf = #{publishers := Pubs, topics := AllTopics}
}) ->
    %% Bias towards topics that the session is subscribed to:
    Topics =
        [{Freq, T} || {Freq, L} <- [{5, maps:keys(Subs)}, {1, AllTopics}], T <- L],
    ?LET(
        {Topic, From, QoS},
        {frequency(Topics), oneof(Pubs), qos()},
        #message{
            id = <<>>,
            qos = QoS,
            from = From,
            topic = Topic,
            timestamp = T,
            payload = <<From/binary, " ", (integer_to_binary(T))/binary>>
        }
    ).

subscribe_(S = #s{conf = #{topics := Topics}}) ->
    ?LET(
        {Topic, QoS},
        {oneof(Topics), qos()},
        {call, ?MODULE, subscribe, [S, Topic, QoS]}
    ).

unsubscribe_(S = #s{conf = #{topics := Topics}}) ->
    ?LET(
        Topic,
        oneof(Topics),
        {call, ?MODULE, unsubscribe, [S, Topic]}
    ).

%%--------------------------------------------------------------------
%% Operations
%%--------------------------------------------------------------------

%% @doc (Re)connect emqtt client to EMQX. If the client was previously
%% connected, this function will wait for the takeover.
connect(S = #s{connected = Connected, conninfo = ConnInfo}, Opts = #{clientid := ClientId}) ->
    ?tp(notice, sessds_test_connect, #{opts => Opts, pid => self()}),
    %% Check metadata of the previous state to catch situations when
    %% the testcase starts from a dirty state:
    true = check_session_metadata(S),
    {ok, ClientPid} = emqtt:start_link(Opts),
    unlink(ClientPid),
    CMRef = monitor(process, ClientPid),
    {ok, _} = emqtt:connect(ClientPid),
    %% Wait for takeover (if the client was previously connected):
    Connected andalso wait_client_down(ConnInfo),
    [SessionPid] = emqx_cm:lookup_channels(local, ClientId),
    SMRef = monitor(process, SessionPid),
    %% If the client was connected previously, we should ensure
    %% takeover has happened:
    #{
        client_pid => ClientPid,
        client_mref => CMRef,
        session_pid => SessionPid,
        session_mref => SMRef
    }.

%% @doc Shut down emqtt
disconnect(#s{conninfo = ConnInfo = #{client_pid := C}}) ->
    ?tp(notice, sessds_test_disconnect, #{pid => C}),
    emqtt:stop(C),
    wait_client_down(ConnInfo).

publish(Msg) ->
    ?tp(notice, sessds_test_publish, emqx_message:to_map(Msg)),
    %% We bypass persistent session router for simplicity:
    emqx_ds:store_batch(?PERSISTENT_MESSAGE_DB, [Msg]).

add_generation() ->
    ?tp(notice, sessds_test_add_generation, #{}),
    emqx_ds:add_generation(?PERSISTENT_MESSAGE_DB).

subscribe(S, Topic, QoS) ->
    ?tp(notice, sessds_test_subscribe, #{topic => Topic, qos => QoS}),
    emqtt:subscribe(client_pid(S), Topic, QoS).

unsubscribe(S, Topic) ->
    ?tp(notice, sessds_test_unsubscribe, #{topic => Topic}),
    emqtt:unsubscribe(client_pid(S), Topic).

consume(S) ->
    %% Consume and ack all messages we can get:
    ?tp_span(
        notice,
        sessds_test_consume,
        #{},
        receive_ack_loop(S, ok)
    ).

receive_ack_loop(
    S = #s{
        conf = #{wait_publishes_time := Timeout},
        conninfo = #{client_pid := CPID, client_mref := CMRef, session_mref := SMRef}
    },
    Result
) ->
    receive
        %% Handle MQTT packets:
        {publish, Msg = #{client_pid := CPID}} ->
            ?tp(notice, sessds_test_in_publish, Msg),
            #{packet_id := PID, qos := QoS} = Msg,
            %% Ack:
            case QoS of
                ?QOS_0 ->
                    ok;
                ?QOS_1 ->
                    ?tp(notice, sessds_test_out_puback, #{packet_id => PID}),
                    emqtt:puback(client_pid(S), PID);
                ?QOS_2 ->
                    ?tp(notice, sessds_test_out_pubrec, #{packet_id => PID}),
                    emqtt:pubrec(client_pid(S), PID)
            end,
            receive_ack_loop(S, Result);
        {pubrel, Msg = #{client_pid := CPID}} ->
            ?tp(notice, sessds_test_in_pubrel, Msg),
            #{packet_id := PID} = Msg,
            emqtt:pubcomp(client_pid(S), PID),
            receive_ack_loop(S, Result);
        %% Handle client/session crash:
        {'DOWN', CMRef, process, CPID, Reason} ->
            ?tp(warning, sessds_test_client_crash, #{pid => CPID, reason => Reason}),
            receive_ack_loop(S, {error, client_crash});
        {'DOWN', SMRef, process, SessPid, Reason} ->
            ?tp(warning, sessds_test_session_crash, #{pid => SessPid, reason => Reason}),
            receive_ack_loop(S, {error, session_crash});
        %%
        Other ->
            %% FIXME: this may include messages from the older
            %% incarnations of the client. Find a better way to deal
            %% with them:
            ?tp(warning, sessds_test_in_garbage, #{message => Other}),
            receive_ack_loop(S, Result)
    after Timeout ->
        Result
    end.

%%--------------------------------------------------------------------
%% Misc. API
%%--------------------------------------------------------------------

-spec default_config() -> config().
default_config() ->
    #{
        wait_publishes_time => 100,
        topics => [<<"t1">>, <<"t2">>, <<"t3">>, <<"t4">>],
        publishers => [<<"pub1">>, <<"pub2">>, <<"pub3">>],
        client_config => #{
            port => 1883,
            proto => v5,
            clientid => ?clientid,
            %% These properties are imporant for test logic
            %%   Expiry interval must be large enough to avoid
            %%   automatic kickout:
            properties => #{'Session-Expiry-Interval' => 1000},
            %%   To test takeover, clients must not auto-reconnect:
            reconnect => false,
            %%   We want to cover as many scenarios where session has
            %%   un-acked messages as possible:
            auto_ack => never
        }
    }.

sample(Size) ->
    proper_gen:pick(commands(?MODULE), Size).

killall(S = #s{connected = Connected}) ->
    Connected andalso catch disconnect(S).

cleanup() ->
    emqx_cm:kick_session(?clientid),
    emqx_persistent_session_ds:destroy_session(?clientid).

%%--------------------------------------------------------------------
%% Statem callbacks
%%--------------------------------------------------------------------

command(S = #s{model_state = undefined}) ->
    connect_(S);
command(S = #s{connected = Conn, has_data = HasData, model_state = #{subs := Subs}}) ->
    HasSubs = maps:size(Subs) > 0,
    %% Commands that are executed in any state:
    Common = [
        {1, connect_(S)},
        {1, {call, ?MODULE, add_generation, []}},
        %% Publish some messages occasionally even when there are no
        %% subs:
        {1, {call, ?MODULE, publish, [message(S)]}}
    ],
    %% Commands that are executed when client is connected:
    Connected =
        [{9, {call, ?MODULE, publish, [message(S)]}} || HasSubs] ++
            [{10, {call, ?MODULE, consume, [S]}} || HasData and HasSubs] ++
            [
                {1, {call, ?MODULE, disconnect, [S]}},
                {5, subscribe_(S)},
                {5, unsubscribe_(S)}
            ],
    case Conn of
        true ->
            frequency(Connected ++ Common);
        false ->
            frequency(Common)
    end.

initial_state() ->
    #s{conf = default_config()}.

%% Start from the blank slate:
next_state(S, Ret, {call, ?MODULE, connect, [_, Opts = #{clean_start := Clean}]}) when
    Clean; S#s.model_state =:= undefined
->
    S#s{
        conninfo = Ret,
        connected = true,
        model_state = #{conn_opts => Opts, subs => #{}}
    };
%% Reconnect:
next_state(
    S = #s{model_state = Sess}, Ret, {call, _, connect, [_, Opts = #{clean_start := false}]}
) ->
    S#s{
        conninfo = Ret,
        connected = true,
        model_state = Sess#{conn_opts => Opts}
    };
%% Disconnect:
next_state(S, _Ret, {call, ?MODULE, disconnect, _}) ->
    S#s{
        connected = false,
        conninfo = undefined
    };
%% Publish/consume messages:
next_state(S = #s{faketime = T}, _Ret, {call, ?MODULE, publish, [Batch]}) ->
    S#s{
        has_data = true,
        faketime = T + 1
    };
next_state(S, _Ret, {call, ?MODULE, consume, _}) ->
    S#s{
        has_data = false
    };
%% Add generation:
next_state(S, _Ret, {call, ?MODULE, add_generation, _}) ->
    S;
%% Subscribe/unsubscribe topics:
next_state(S = #s{model_state = ModelState}, _Ret, {call, ?MODULE, subscribe, [_S, Topic, QoS]}) ->
    #{subs := Subs0} = ModelState,
    Subs = Subs0#{Topic => #{qos => QoS}},
    S#s{
        model_state = ModelState#{subs => Subs}
    };
next_state(S = #s{model_state = ModelState}, _Ret, {call, ?MODULE, unsubscribe, [_S, Topic]}) ->
    #{subs := Subs} = ModelState,
    S#s{
        model_state = ModelState#{subs => maps:remove(Topic, Subs)}
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
check_processes(#s{connected = false}) ->
    %% TODO: check that the channel is stopped:
    true;
check_processes(#s{connected = true, conninfo = #{client_pid := CPid, session_pid := SPid}}) ->
    SA = is_process_alive(CPid),
    CA = is_process_alive(SPid),
    SA and CA orelse
        begin
            ?tp(
                error,
                sessds_test_processes_died,
                #{
                    client => {CPid, CA},
                    session => {SPid, SA}
                }
            ),
            false
        end.

check_session_metadata(#s{model_state = undefined}) ->
    case emqx_persistent_session_ds_state:print_session(?clientid) of
        undefined ->
            true;
        State ->
            ?tp(error, "Found unexpected session metadata", #{state => State}),
            false
    end;
check_session_metadata(#s{model_state = #{}}) ->
    case emqx_persistent_session_ds_state:print_session(?clientid) of
        undefined ->
            ?tp(error, "Session metadata was expected, but not found", #{}),
            false;
        _ ->
            true
    end.

check_invariants(State) ->
    #s{model_state = ModelState} = State,
    emqx_persistent_session_ds:state_invariants(ModelState, sut_state()).

wait_client_down(#{
    client_pid := ClientPid, client_mref := CMRef, session_pid := SessionPid, session_mref := SMRef
}) ->
    %% Wait for the session takeover:
    receive
        {'DOWN', SMRef, process, SessionPid, _Reason} ->
            ok
    after 5_000 ->
        error(timeout_waiting_for_takeover)
    end,
    %% Demonitor client:
    demonitor(CMRef, [flush]),
    ok.

sut_state() ->
    emqx_persistent_session_ds:print_session(?clientid).

client_pid(#s{connected = true, conninfo = #{client_pid := Pid}}) ->
    Pid.
