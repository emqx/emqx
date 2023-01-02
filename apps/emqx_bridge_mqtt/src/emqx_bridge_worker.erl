%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc Bridge works in two layers (1) batching layer (2) transport layer
%% The `bridge' batching layer collects local messages in batches and sends over
%% to remote MQTT node/cluster via `connection' transport layer.
%% In case `REMOTE' is also an EMQX node, `connection' is recommended to be
%% the `gen_rpc' based implementation `emqx_bridge_rpc'. Otherwise `connection'
%% has to be `emqx_bridge_mqtt'.
%%
%% ```
%% +------+                        +--------+
%% | EMQX |                        | REMOTE |
%% |      |                        |        |
%% |   (bridge) <==(connection)==> |        |
%% |      |                        |        |
%% |      |                        |        |
%% +------+                        +--------+
%% '''
%%
%%
%% This module implements 2 kinds of APIs with regards to batching and
%% messaging protocol. (1) A `gen_statem' based local batch collector;
%% (2) APIs for incoming remote batches/messages.
%%
%% Batch collector state diagram
%%
%% [idle] --(0) --> [connecting] --(2)--> [connected]
%%                  |        ^                 |
%%                  |        |                 |
%%                  '--(1)---'--------(3)------'
%%
%% (0): auto or manual start
%% (1): retry timeout
%% (2): successfuly connected to remote node/cluster
%% (3): received {disconnected, Reason} OR
%%      failed to send to remote node/cluster.
%%
%% NOTE: A bridge worker may subscribe to multiple (including wildcard)
%% local topics, and the underlying `emqx_bridge_connect' may subscribe to
%% multiple remote topics, however, worker/connections are not designed
%% to support automatic load-balancing, i.e. in case it can not keep up
%% with the amount of messages comming in, administrator should split and
%% balance topics between worker/connections manually.
%%
%% NOTES:
%% * Local messages are all normalised to QoS-1 when exporting to remote

-module(emqx_bridge_worker).
-behaviour(gen_statem).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% APIs
-export([ start_link/1
        , start_link/2
        , register_metrics/0
        , stop/1
        ]).

%% gen_statem callbacks
-export([ terminate/3
        , code_change/4
        , init/1
        , callback_mode/0
        ]).

%% state functions
-export([ idle/3
        , connected/3
        ]).

%% management APIs
-export([ ensure_started/1
        , ensure_stopped/1
        , ensure_stopped/2
        , status/1
        ]).

-export([ get_forwards/1
        , ensure_forward_present/2
        , ensure_forward_absent/2
        ]).

-export([ get_subscriptions/1
        , ensure_subscription_present/3
        , ensure_subscription_absent/2
        ]).

%% Internal
-export([msg_marshaller/1]).

-export_type([ config/0
             , batch/0
             , ack_ref/0
             ]).

-type id() :: atom() | string() | pid().
-type qos() :: emqx_mqtt_types:qos().
-type config() :: map().
-type batch() :: [emqx_bridge_msg:exp_msg()].
-type ack_ref() :: term().
-type topic() :: emqx_topic:topic().

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-logger_header("[Bridge]").

%% same as default in-flight limit for emqtt
-define(DEFAULT_BATCH_SIZE, 32).
-define(DEFAULT_RECONNECT_DELAY_MS, timer:seconds(5)).
-define(DEFAULT_SEG_BYTES, (1 bsl 20)).
-define(DEFAULT_MAX_TOTAL_SIZE, (1 bsl 31)).
-define(NO_BRIDGE_HANDLER, undefined).

%% @doc Start a bridge worker. Supported configs:
%% start_type: 'manual' (default) or 'auto', when manual, bridge will stay
%%      at 'idle' state until a manual call to start it.
%% connect_module: The module which implements emqx_bridge_connect behaviour
%%      and work as message batch transport layer
%% reconnect_delay_ms: Delay in milli-seconds for the bridge worker to retry
%%      in case of transportation failure.
%% max_inflight: Max number of batches allowed to send-ahead before receiving
%%       confirmation from remote node/cluster
%% mountpoint: The topic mount point for messages sent to remote node/cluster
%%      `undefined', `<<>>' or `""' to disable
%% forwards: Local topics to subscribe.
%% queue.batch_bytes_limit: Max number of bytes to collect in a batch for each
%%      send call towards emqx_bridge_connect
%% queue.batch_count_limit: Max number of messages to collect in a batch for
%%      each send call towards emqx_bridge_connect
%% queue.replayq_dir: Directory where replayq should persist messages
%% queue.replayq_seg_bytes: Size in bytes for each replayq segment file
%%
%% Find more connection specific configs in the callback modules
%% of emqx_bridge_connect behaviour.
start_link(Config) when is_list(Config) ->
    start_link(maps:from_list(Config));
start_link(Config) ->
    gen_statem:start_link(?MODULE, Config, []).

start_link(Name, Config) when is_list(Config) ->
    start_link(Name, maps:from_list(Config));
start_link(Name, Config) ->
    Name1 = name(Name),
    gen_statem:start_link({local, Name1}, ?MODULE, Config#{name => Name1}, []).

ensure_started(Name) ->
    gen_statem:call(name(Name), ensure_started).

%% @doc Manually stop bridge worker. State idempotency ensured.
ensure_stopped(Id) ->
    ensure_stopped(Id, 1000).

ensure_stopped(Id, Timeout) ->
    Pid = case id(Id) of
              P when is_pid(P) -> P;
              N -> whereis(N)
          end,
    case Pid of
        undefined ->
            ok;
        _ ->
            MRef = monitor(process, Pid),
            unlink(Pid),
            _ = gen_statem:call(id(Id), ensure_stopped, Timeout),
            receive
                {'DOWN', MRef, _, _, _} ->
                    ok
            after
                Timeout ->
                    exit(Pid, kill)
            end
    end.

stop(Pid) -> gen_statem:stop(Pid).

status(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, status);
status(Id) ->
    gen_statem:call(name(Id), status).

%% @doc Return all forwards (local subscriptions).
-spec get_forwards(id()) -> [topic()].
get_forwards(Id) -> gen_statem:call(id(Id), get_forwards, timer:seconds(1000)).

%% @doc Return all subscriptions (subscription over mqtt connection to remote broker).
-spec get_subscriptions(id()) -> [{emqx_topic:topic(), qos()}].
get_subscriptions(Id) -> gen_statem:call(id(Id), get_subscriptions).

%% @doc Add a new forward (local topic subscription).
-spec ensure_forward_present(id(), topic()) -> ok.
ensure_forward_present(Id, Topic) ->
    gen_statem:call(id(Id), {ensure_present, forwards, topic(Topic)}).

%% @doc Ensure a forward topic is deleted.
-spec ensure_forward_absent(id(), topic()) -> ok.
ensure_forward_absent(Id, Topic) ->
    gen_statem:call(id(Id), {ensure_absent, forwards, topic(Topic)}).

%% @doc Ensure subscribed to remote topic.
%% NOTE: only applicable when connection module is emqx_bridge_mqtt
%%       return `{error, no_remote_subscription_support}' otherwise.
-spec ensure_subscription_present(id(), topic(), qos()) -> ok | {error, any()}.
ensure_subscription_present(Id, Topic, QoS) ->
    gen_statem:call(id(Id), {ensure_present, subscriptions, {topic(Topic), QoS}}).

%% @doc Ensure unsubscribed from remote topic.
%% NOTE: only applicable when connection module is emqx_bridge_mqtt
-spec ensure_subscription_absent(id(), topic()) -> ok.
ensure_subscription_absent(Id, Topic) ->
    gen_statem:call(id(Id), {ensure_absent, subscriptions, topic(Topic)}).

callback_mode() -> [state_functions].

%% @doc Config should be a map().
init(Config) ->
    erlang:process_flag(trap_exit, true),
    ConnectModule = maps:get(connect_module, Config),
    Subscriptions = maps:get(subscriptions, Config, []),
    Forwards = maps:get(forwards, Config, []),
    Queue = open_replayq(Config),
    State = init_opts(Config),
    Topics = [iolist_to_binary(T) || T <- Forwards],
    Subs = check_subscriptions(Subscriptions),
    ConnectCfg = get_conn_cfg(Config),
    self() ! idle,
    {ok, idle, State#{connect_module => ConnectModule,
                      connect_cfg => ConnectCfg,
                      forwards => Topics,
                      subscriptions => Subs,
                      replayq => Queue
                     }}.

init_opts(Config) ->
    IfRecordMetrics = maps:get(if_record_metrics, Config, true),
    ReconnDelayMs = maps:get(reconnect_delay_ms, Config, ?DEFAULT_RECONNECT_DELAY_MS),
    StartType = maps:get(start_type, Config, manual),
    BridgeHandler = maps:get(bridge_handler, Config, ?NO_BRIDGE_HANDLER),
    Mountpoint = maps:get(forward_mountpoint, Config, undefined),
    ReceiveMountpoint = maps:get(receive_mountpoint, Config, undefined),
    MaxInflightSize = maps:get(max_inflight, Config, ?DEFAULT_BATCH_SIZE),
    BatchSize = maps:get(batch_size, Config, ?DEFAULT_BATCH_SIZE),
    Name = maps:get(name, Config, undefined),
    #{start_type => StartType,
      reconnect_delay_ms => ReconnDelayMs,
      batch_size => BatchSize,
      mountpoint => format_mountpoint(Mountpoint),
      receive_mountpoint => ReceiveMountpoint,
      inflight => [],
      max_inflight => MaxInflightSize,
      connection => undefined,
      bridge_handler => BridgeHandler,
      if_record_metrics => IfRecordMetrics,
      name => Name}.

open_replayq(Config) ->
    QCfg = maps:get(queue, Config, #{}),
    Dir = maps:get(replayq_dir, QCfg, undefined),
    SegBytes = maps:get(replayq_seg_bytes, QCfg, ?DEFAULT_SEG_BYTES),
    MaxTotalSize = maps:get(max_total_size, QCfg, ?DEFAULT_MAX_TOTAL_SIZE),
    QueueConfig = case Dir =:= undefined orelse Dir =:= "" of
        true -> #{mem_only => true};
        false -> #{dir => Dir, seg_bytes => SegBytes, max_total_size => MaxTotalSize}
    end,
    replayq:open(QueueConfig#{sizer => fun emqx_bridge_msg:estimate_size/1,
                              marshaller => fun ?MODULE:msg_marshaller/1}).

check_subscriptions(Subscriptions) ->
    lists:map(fun({Topic, QoS}) ->
        Topic1 = iolist_to_binary(Topic),
        true = emqx_topic:validate({filter, Topic1}),
        {Topic1, QoS}
    end, Subscriptions).

get_conn_cfg(Config) ->
    maps:without([connect_module,
                  queue,
                  reconnect_delay_ms,
                  forwards,
                  mountpoint,
                  name
                 ], Config).

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _StateName, #{replayq := Q} = State) ->
    _ = disconnect(State),
    _ = replayq:close(Q),
    ok.

%% ensure_started will be deprecated in the future
idle({call, From}, ensure_started, State) ->
    case do_connect(State) of
        {ok, State1} ->
            {next_state, connected, State1, [{reply, From, ok}, {state_timeout, 0, connected}]};
        {error, Reason, _State} ->
            {keep_state_and_data, [{reply, From, {error, Reason}}]}
    end;
%% @doc Standing by for manual start.
idle(info, idle, #{start_type := manual}) ->
    keep_state_and_data;
%% @doc Standing by for auto start.
idle(info, idle, #{start_type := auto} = State) ->
    connecting(State);
idle(state_timeout, reconnect, State) ->
    connecting(State);

idle(info, {batch_ack, Ref}, State) ->
    NewState = handle_batch_ack(State, Ref),
    {keep_state, NewState};

idle(Type, Content, State) ->
    common(idle, Type, Content, State).

connecting(#{reconnect_delay_ms := ReconnectDelayMs} = State) ->
    case do_connect(State) of
        {ok, State1} ->
            {next_state, connected, State1, {state_timeout, 0, connected}};
        _ ->
            {keep_state_and_data, {state_timeout, ReconnectDelayMs, reconnect}}
    end.

connected(state_timeout, connected, #{inflight := Inflight} = State) ->
    case retry_inflight(State#{inflight := []}, Inflight) of
        {ok, NewState} ->
            {keep_state, NewState, {next_event, internal, maybe_send}};
        {error, NewState} ->
            {keep_state, NewState}
    end;
connected(internal, maybe_send, State) ->
    {_, NewState} = pop_and_send(State),
    {keep_state, NewState};

connected(info, {disconnected, Conn, Reason},
          #{connection := Connection, name := Name, reconnect_delay_ms := ReconnectDelayMs} = State) ->
    ?tp(info, disconnected, #{name => Name, reason => Reason}),
    case Conn =:= maps:get(client_pid, Connection, undefined)  of
        true ->
            {next_state, idle, State#{connection => undefined}, {state_timeout, ReconnectDelayMs, reconnect}};
        false ->
            keep_state_and_data
    end;
connected(info, {batch_ack, Ref}, State) ->
    NewState = handle_batch_ack(State, Ref),
    {keep_state, NewState, {next_event, internal, maybe_send}};
connected(Type, Content, State) ->
    common(connected, Type, Content, State).

%% Common handlers
common(StateName, {call, From}, status, _State) ->
    {keep_state_and_data, [{reply, From, StateName}]};
common(_StateName, {call, From}, ensure_started, _State) ->
    {keep_state_and_data, [{reply, From, connected}]};
common(_StateName, {call, From}, ensure_stopped, _State) ->
    {stop_and_reply, {shutdown, manual}, [{reply, From, ok}]};
common(_StateName, {call, From}, get_forwards, #{forwards := Forwards}) ->
    {keep_state_and_data, [{reply, From, Forwards}]};
common(_StateName, {call, From}, get_subscriptions, #{subscriptions := Subs}) ->
    {keep_state_and_data, [{reply, From, Subs}]};
common(_StateName, {call, From}, {ensure_present, What, Topic}, State) ->
    {Result, NewState} = ensure_present(What, Topic, State),
    {keep_state, NewState, [{reply, From, Result}]};
common(_StateName, {call, From}, {ensure_absent, What, Topic}, State) ->
    {Result, NewState} = ensure_absent(What, Topic, State),
    {keep_state, NewState, [{reply, From, Result}]};
common(_StateName, info, {deliver, _, Msg},
       State = #{replayq := Q, if_record_metrics := IfRecordMetric}) ->
    Msgs = collect([Msg]),
    bridges_metrics_inc(IfRecordMetric,
                        'bridge.mqtt.message_received',
                        length(Msgs)
                       ),
    NewQ = replayq:append(Q, Msgs),
    {keep_state, State#{replayq => NewQ}, {next_event, internal, maybe_send}};
common(_StateName, info, {'EXIT', _, _}, State) ->
    {keep_state, State};
common(StateName, Type, Content, #{name := Name} = State) ->
    ?LOG(notice, "Bridge ~p discarded ~p type event at state ~p:~p",
          [Name, Type, StateName, Content]),
    {keep_state, State}.

eval_bridge_handler(State = #{bridge_handler := ?NO_BRIDGE_HANDLER}, _Msg) ->
    State;
eval_bridge_handler(State = #{bridge_handler := Handler}, Msg) ->
    Handler(Msg),
    State.

ensure_present(Key, Topic, State) ->
    Topics = maps:get(Key, State),
    case is_topic_present(Topic, Topics) of
        true ->
            {ok, State};
        false ->
            R = do_ensure_present(Key, Topic, State),
            {R, State#{Key := lists:usort([Topic | Topics])}}
    end.

ensure_absent(Key, Topic, State) ->
    Topics = maps:get(Key, State),
    case is_topic_present(Topic, Topics) of
        true ->
            R = do_ensure_absent(Key, Topic, State),
            {R, State#{Key := ensure_topic_absent(Topic, Topics)}};
        false ->
            {ok, State}
    end.

ensure_topic_absent(_Topic, []) -> [];
ensure_topic_absent(Topic, [{_, _} | _] = L) -> lists:keydelete(Topic, 1, L);
ensure_topic_absent(Topic, L) -> lists:delete(Topic, L).

is_topic_present({Topic, _QoS}, Topics) ->
    is_topic_present(Topic, Topics);
is_topic_present(Topic, Topics) ->
    lists:member(Topic, Topics) orelse false =/= lists:keyfind(Topic, 1, Topics).

do_connect(#{forwards := Forwards,
             subscriptions := Subs,
             connect_module := ConnectModule,
             connect_cfg := ConnectCfg,
             inflight := Inflight,
             name := Name} = State) ->
    ok = subscribe_local_topics(Forwards, Name),
    case emqx_bridge_connect:start(ConnectModule, ConnectCfg#{subscriptions => Subs}) of
        {ok, Conn} ->
            Res = eval_bridge_handler(State#{connection => Conn}, connected),
            ?tp(info, connected, #{name => Name, inflight => length(Inflight)}),
            {ok, Res};
        {error, Reason} ->
            {error, Reason, State}
    end.

do_ensure_present(forwards, Topic, #{name := Name}) ->
    subscribe_local_topic(Topic, Name);
do_ensure_present(subscriptions, _Topic, #{connection := undefined}) ->
    {error, no_connection};
do_ensure_present(subscriptions, _Topic, #{connect_module := emqx_bridge_rpc}) ->
    {error, no_remote_subscription_support};
do_ensure_present(subscriptions, {Topic, QoS}, #{connect_module := ConnectModule,
                                                 connection := Conn}) ->
    ConnectModule:ensure_subscribed(Conn, Topic, QoS).

do_ensure_absent(forwards, Topic, _) ->
    do_unsubscribe(Topic);
do_ensure_absent(subscriptions, _Topic, #{connection := undefined}) ->
    {error, no_connection};
do_ensure_absent(subscriptions, _Topic, #{connect_module := emqx_bridge_rpc}) ->
    {error, no_remote_subscription_support};
do_ensure_absent(subscriptions, Topic, #{connect_module := ConnectModule,
                                         connection := Conn}) ->
    ConnectModule:ensure_unsubscribed(Conn, Topic).

collect(Acc) ->
    receive
        {deliver, _, Msg} ->
            collect([Msg | Acc])
    after
        0 ->
            lists:reverse(Acc)
    end.

%% Retry all inflight (previously sent but not acked) batches.
retry_inflight(State, []) -> {ok, State};
retry_inflight(State, [#{q_ack_ref := QAckRef, batch := Batch} | Rest] = OldInf) ->
    case do_send(State, QAckRef, Batch) of
        {ok, State1} ->
            retry_inflight(State1, Rest);
        {error, #{inflight := NewInf} = State1} ->
            {error, State1#{inflight := NewInf ++ OldInf}}
    end.

pop_and_send(#{inflight := Inflight, max_inflight := Max } = State) ->
    pop_and_send_loop(State, Max - length(Inflight)).

pop_and_send_loop(State, 0) ->
    ?tp(debug, inflight_full, #{}),
    {ok, State};
pop_and_send_loop(#{replayq := Q, connect_module := Module} = State, N) ->
    case replayq:is_empty(Q) of
        true ->
            ?tp(debug, replayq_drained, #{}),
            {ok, State};
        false ->
            BatchSize = case Module of
                emqx_bridge_rpc -> maps:get(batch_size, State);
                _ -> 1
            end,
            Opts = #{count_limit => BatchSize, bytes_limit => 999999999},
            {Q1, QAckRef, Batch} = replayq:pop(Q, Opts),
            case do_send(State#{replayq := Q1}, QAckRef, Batch) of
                {ok, NewState} -> pop_and_send_loop(NewState, N - 1);
                {error, NewState} -> {error, NewState}
            end
    end.

%% Assert non-empty batch because we have a is_empty check earlier.
do_send(#{inflight := Inflight,
          connect_module := Module,
          connection := Connection,
          mountpoint := Mountpoint,
          if_record_metrics := IfRecordMetrics} = State, QAckRef, [_ | _] = Batch) ->
    ExportMsg = fun(Message) ->
                    bridges_metrics_inc(IfRecordMetrics, 'bridge.mqtt.message_sent'),
                    emqx_bridge_msg:to_export(Module, Mountpoint, Message)
                end,
    case Module:send(Connection, [ExportMsg(M) || M <- Batch]) of
        {ok, Refs} ->
            {ok, State#{inflight := Inflight ++ [#{q_ack_ref => QAckRef,
                                                   send_ack_ref => map_set(Refs),
                                                   batch => Batch}]}};
        {error, Reason} ->
            ?LOG(info, "mqtt_bridge_produce_failed ~p", [Reason]),
            {error, State}
    end.

%% map as set, ack-reference -> 1
map_set(Ref) when is_reference(Ref) ->
    %% QoS-0 or RPC call returns a reference
    map_set([Ref]);
map_set(List) ->
    map_set(List, #{}).

map_set([], Set) -> Set;
map_set([H | T], Set) -> map_set(T, Set#{H => 1}).

handle_batch_ack(#{inflight := Inflight0, replayq := Q} = State, Ref) ->
    Inflight1 = do_ack(Inflight0, Ref),
    Inflight = drop_acked_batches(Q, Inflight1),
    State#{inflight := Inflight}.

do_ack([], Ref) ->
    ?LOG(debug, "stale_batch_ack_reference ~p", [Ref]),
    [];
do_ack([#{send_ack_ref := Refs} = First | Rest], Ref) ->
    case maps:is_key(Ref, Refs) of
        true ->
            NewRefs = maps:without([Ref], Refs),
            [First#{send_ack_ref := NewRefs} | Rest];
        false ->
            [First | do_ack(Rest, Ref)]
    end.

%% Drop the consecutive header of the inflight list having empty send_ack_ref
drop_acked_batches(_Q, []) ->
    ?tp(debug, inflight_drained, #{}),
    [];
drop_acked_batches(Q, [#{send_ack_ref := Refs,
                         q_ack_ref := QAckRef} | Rest] = All) ->
    case maps:size(Refs) of
        0 ->
            %% all messages are acked by bridge target
            %% now it's safe to ack replayq (delete from disk)
            ok = replayq:ack(Q, QAckRef),
            %% continue to check more sent batches
            drop_acked_batches(Q, Rest);
        _ ->
            %% the head (oldest) inflight batch is not acked, keep waiting
            All
    end.

subscribe_local_topics(Topics, Name) ->
    lists:foreach(fun(Topic) -> subscribe_local_topic(Topic, Name) end, Topics).

subscribe_local_topic(Topic, Name) ->
    do_subscribe(Topic, Name).

topic(T) -> iolist_to_binary(T).

validate(RawTopic) ->
    Topic = topic(RawTopic),
    try emqx_topic:validate(Topic) of
        _Success -> Topic
    catch
        error:Reason ->
            error({bad_topic, Topic, Reason})
    end.

do_subscribe(RawTopic, Name) ->
    TopicFilter = validate(RawTopic),
    {Topic, SubOpts} = emqx_topic:parse(TopicFilter, #{qos => ?QOS_1}),
    emqx_broker:subscribe(Topic, Name, SubOpts).

do_unsubscribe(RawTopic) ->
    TopicFilter = validate(RawTopic),
    {Topic, _SubOpts} = emqx_topic:parse(TopicFilter),
    emqx_broker:unsubscribe(Topic).

disconnect(#{connection := Conn,
             connect_module := Module
            } = State) when Conn =/= undefined ->
    Module:stop(Conn),
    State0 = State#{connection => undefined},
    eval_bridge_handler(State0, disconnected);
disconnect(State) ->
    eval_bridge_handler(State, disconnected).

%% Called only when replayq needs to dump it to disk.
msg_marshaller(Bin) when is_binary(Bin) -> emqx_bridge_msg:from_binary(Bin);
msg_marshaller(Msg) -> emqx_bridge_msg:to_binary(Msg).

format_mountpoint(undefined) ->
    undefined;
format_mountpoint(Prefix) ->
    binary:replace(iolist_to_binary(Prefix), <<"${node}">>, atom_to_binary(node(), utf8)).

name(Id) -> list_to_atom(lists:concat([?MODULE, "_", Id])).

id(Pid) when is_pid(Pid) -> Pid;
id(Name) -> name(Name).

register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1,
                  ['bridge.mqtt.message_sent',
                   'bridge.mqtt.message_received'
                  ]).

bridges_metrics_inc(true, Metric) ->
    emqx_metrics:inc(Metric);
bridges_metrics_inc(_IsRecordMetric, _Metric) ->
    ok.

bridges_metrics_inc(true, Metric, Value) ->
    emqx_metrics:inc(Metric, Value);
bridges_metrics_inc(_IsRecordMetric, _Metric, _Value) ->
    ok.
