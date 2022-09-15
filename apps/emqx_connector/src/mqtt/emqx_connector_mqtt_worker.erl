%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%% has to be `emqx_connector_mqtt_mod'.
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
%% (2): successfully connected to remote node/cluster
%% (3): received {disconnected, Reason} OR
%%      failed to send to remote node/cluster.
%%
%% NOTE: A bridge worker may subscribe to multiple (including wildcard)
%% local topics, and the underlying `emqx_bridge_connect' may subscribe to
%% multiple remote topics, however, worker/connections are not designed
%% to support automatic load-balancing, i.e. in case it can not keep up
%% with the amount of messages coming in, administrator should split and
%% balance topics between worker/connections manually.
%%
%% NOTES:
%% * Local messages are all normalised to QoS-1 when exporting to remote

-module(emqx_connector_mqtt_worker).
-behaviour(gen_statem).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/logger.hrl").

%% APIs
-export([
    start_link/1,
    stop/1
]).

%% gen_statem callbacks
-export([
    terminate/3,
    code_change/4,
    init/1,
    callback_mode/0
]).

%% state functions
-export([
    idle/3,
    connected/3
]).

%% management APIs
-export([
    ensure_started/1,
    ensure_stopped/1,
    status/1,
    ping/1,
    send_to_remote/2,
    send_to_remote_async/3
]).

-export([get_forwards/1]).

-export([get_subscriptions/1]).

-export_type([
    config/0,
    ack_ref/0
]).

-type id() :: atom() | string() | pid().
-type qos() :: emqx_types:qos().
-type config() :: map().
-type ack_ref() :: term().
-type topic() :: emqx_types:topic().

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%% same as default in-flight limit for emqtt
-define(DEFAULT_INFLIGHT_SIZE, 32).
-define(DEFAULT_RECONNECT_DELAY_MS, timer:seconds(5)).
-define(DEFAULT_SEG_BYTES, (1 bsl 20)).
-define(DEFAULT_MAX_TOTAL_SIZE, (1 bsl 31)).

%% @doc Start a bridge worker. Supported configs:
%% start_type: 'manual' (default) or 'auto', when manual, bridge will stay
%%      at 'idle' state until a manual call to start it.
%% connect_module: The module which implements emqx_bridge_connect behaviour
%%      and work as message batch transport layer
%% reconnect_interval: Delay in milli-seconds for the bridge worker to retry
%%      in case of transportation failure.
%% max_inflight: Max number of batches allowed to send-ahead before receiving
%%       confirmation from remote node/cluster
%% mountpoint: The topic mount point for messages sent to remote node/cluster
%%      `undefined', `<<>>' or `""' to disable
%% forwards: Local topics to subscribe.
%%
%% Find more connection specific configs in the callback modules
%% of emqx_bridge_connect behaviour.
start_link(Opts) when is_list(Opts) ->
    start_link(maps:from_list(Opts));
start_link(Opts) ->
    case maps:get(name, Opts, undefined) of
        undefined ->
            gen_statem:start_link(?MODULE, Opts, []);
        Name ->
            Name1 = name(Name),
            gen_statem:start_link({local, Name1}, ?MODULE, Opts#{name => Name1}, [])
    end.

ensure_started(Name) ->
    gen_statem:call(name(Name), ensure_started).

%% @doc Manually stop bridge worker. State idempotency ensured.
ensure_stopped(Name) ->
    gen_statem:call(name(Name), ensure_stopped, 5000).

stop(Pid) -> gen_statem:stop(Pid).

status(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, status);
status(Name) ->
    gen_statem:call(name(Name), status).

ping(Pid) when is_pid(Pid) ->
    gen_statem:call(Pid, ping);
ping(Name) ->
    gen_statem:call(name(Name), ping).

send_to_remote(Pid, Msg) when is_pid(Pid) ->
    gen_statem:call(Pid, {send_to_remote, Msg});
send_to_remote(Name, Msg) ->
    gen_statem:call(name(Name), {send_to_remote, Msg}).

send_to_remote_async(Pid, Msg, Callback) when is_pid(Pid) ->
    gen_statem:cast(Pid, {send_to_remote_async, Msg, Callback});
send_to_remote_async(Name, Msg, Callback) ->
    gen_statem:cast(name(Name), {send_to_remote_async, Msg, Callback}).

%% @doc Return all forwards (local subscriptions).
-spec get_forwards(id()) -> [topic()].
get_forwards(Name) -> gen_statem:call(name(Name), get_forwards, timer:seconds(1000)).

%% @doc Return all subscriptions (subscription over mqtt connection to remote broker).
-spec get_subscriptions(id()) -> [{emqx_types:topic(), qos()}].
get_subscriptions(Name) -> gen_statem:call(name(Name), get_subscriptions).

callback_mode() -> [state_functions].

%% @doc Config should be a map().
init(#{name := Name} = ConnectOpts) ->
    ?SLOG(debug, #{
        msg => "starting_bridge_worker",
        name => Name
    }),
    erlang:process_flag(trap_exit, true),
    State = init_state(ConnectOpts),
    self() ! idle,
    {ok, idle, State#{
        connect_opts => pre_process_opts(ConnectOpts)
    }}.

init_state(Opts) ->
    ReconnDelayMs = maps:get(reconnect_interval, Opts, ?DEFAULT_RECONNECT_DELAY_MS),
    StartType = maps:get(start_type, Opts, manual),
    Mountpoint = maps:get(forward_mountpoint, Opts, undefined),
    MaxInflightSize = maps:get(max_inflight, Opts, ?DEFAULT_INFLIGHT_SIZE),
    Name = maps:get(name, Opts, undefined),
    #{
        start_type => StartType,
        reconnect_interval => ReconnDelayMs,
        mountpoint => format_mountpoint(Mountpoint),
        max_inflight => MaxInflightSize,
        connection => undefined,
        name => Name
    }.

pre_process_opts(#{subscriptions := InConf, forwards := OutConf} = ConnectOpts) ->
    ConnectOpts#{
        subscriptions => pre_process_in_out(in, InConf),
        forwards => pre_process_in_out(out, OutConf)
    }.

pre_process_in_out(_, undefined) ->
    undefined;
pre_process_in_out(in, #{local := LC} = Conf) when is_map(Conf) ->
    Conf#{local => pre_process_in_out_common(LC)};
pre_process_in_out(in, Conf) when is_map(Conf) ->
    %% have no 'local' field in the config
    undefined;
pre_process_in_out(out, #{remote := RC} = Conf) when is_map(Conf) ->
    Conf#{remote => pre_process_in_out_common(RC)};
pre_process_in_out(out, Conf) when is_map(Conf) ->
    %% have no 'remote' field in the config
    undefined.

pre_process_in_out_common(Conf0) ->
    Conf1 = pre_process_conf(topic, Conf0),
    Conf2 = pre_process_conf(qos, Conf1),
    Conf3 = pre_process_conf(payload, Conf2),
    pre_process_conf(retain, Conf3).

pre_process_conf(Key, Conf) ->
    case maps:find(Key, Conf) of
        error ->
            Conf;
        {ok, Val} when is_binary(Val) ->
            Conf#{Key => emqx_plugin_libs_rule:preproc_tmpl(Val)};
        {ok, Val} ->
            Conf#{Key => Val}
    end.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

terminate(_Reason, _StateName, State) ->
    _ = disconnect(State),
    maybe_destroy_session(State).

maybe_destroy_session(#{connect_opts := ConnectOpts = #{clean_start := false}} = State) ->
    try
        %% Destroy session if clean_start is not set.
        %% Ignore any crashes, just refresh the clean_start = true.
        _ = do_connect(State#{connect_opts => ConnectOpts#{clean_start => true}}),
        _ = disconnect(State),
        ok
    catch
        _:_ ->
            ok
    end;
maybe_destroy_session(_State) ->
    ok.

%% ensure_started will be deprecated in the future
idle({call, From}, ensure_started, State) ->
    case do_connect(State) of
        {ok, State1} ->
            {next_state, connected, State1, [{reply, From, ok}, {state_timeout, 0, connected}]};
        {error, Reason, _State} ->
            {keep_state_and_data, [{reply, From, {error, Reason}}]}
    end;
idle({call, From}, {send_to_remote, _}, _State) ->
    {keep_state_and_data, [{reply, From, {error, {recoverable_error, not_connected}}}]};
%% @doc Standing by for manual start.
idle(info, idle, #{start_type := manual}) ->
    keep_state_and_data;
%% @doc Standing by for auto start.
idle(info, idle, #{start_type := auto} = State) ->
    connecting(State);
idle(state_timeout, reconnect, State) ->
    connecting(State);
idle(Type, Content, State) ->
    common(idle, Type, Content, State).

connecting(#{reconnect_interval := ReconnectDelayMs} = State) ->
    case do_connect(State) of
        {ok, State1} ->
            {next_state, connected, State1, {state_timeout, 0, connected}};
        _ ->
            {keep_state_and_data, {state_timeout, ReconnectDelayMs, reconnect}}
    end.

connected(state_timeout, connected, State) ->
    %% nothing to do
    {keep_state, State};
connected({call, From}, {send_to_remote, Msg}, State) ->
    case do_send(State, Msg) of
        {ok, NState} ->
            {keep_state, NState, [{reply, From, ok}]};
        {error, Reason} ->
            {keep_state_and_data, [[reply, From, {error, Reason}]]}
    end;
connected(cast, {send_to_remote_async, Msg, Callback}, State) ->
    _ = do_send_async(State, Msg, Callback),
    {keep_state, State};
connected(
    info,
    {disconnected, Conn, Reason},
    #{connection := Connection, name := Name, reconnect_interval := ReconnectDelayMs} = State
) ->
    ?tp(info, disconnected, #{name => Name, reason => Reason}),
    case Conn =:= maps:get(client_pid, Connection, undefined) of
        true ->
            {next_state, idle, State#{connection => undefined},
                {state_timeout, ReconnectDelayMs, reconnect}};
        false ->
            keep_state_and_data
    end;
connected(Type, Content, State) ->
    common(connected, Type, Content, State).

%% Common handlers
common(StateName, {call, From}, status, _State) ->
    {keep_state_and_data, [{reply, From, StateName}]};
common(_StateName, {call, From}, ping, #{connection := Conn} = _State) ->
    Reply = emqx_connector_mqtt_mod:ping(Conn),
    {keep_state_and_data, [{reply, From, Reply}]};
common(_StateName, {call, From}, ensure_stopped, #{connection := undefined} = _State) ->
    {keep_state_and_data, [{reply, From, ok}]};
common(_StateName, {call, From}, ensure_stopped, #{connection := Conn} = State) ->
    Reply = emqx_connector_mqtt_mod:stop(Conn),
    {next_state, idle, State#{connection => undefined}, [{reply, From, Reply}]};
common(_StateName, {call, From}, get_forwards, #{connect_opts := #{forwards := Forwards}}) ->
    {keep_state_and_data, [{reply, From, Forwards}]};
common(_StateName, {call, From}, get_subscriptions, #{connection := Connection}) ->
    {keep_state_and_data, [{reply, From, maps:get(subscriptions, Connection, #{})}]};
common(_StateName, {call, From}, Req, _State) ->
    {keep_state_and_data, [{reply, From, {error, {unsupported_request, Req}}}]};
common(_StateName, info, {'EXIT', _, _}, State) ->
    {keep_state, State};
common(StateName, Type, Content, #{name := Name} = State) ->
    ?SLOG(error, #{
        msg => "bridge_discarded_event",
        name => Name,
        type => Type,
        state_name => StateName,
        content => Content
    }),
    {keep_state, State}.

do_connect(
    #{
        connect_opts := ConnectOpts,
        name := Name
    } = State
) ->
    case emqx_connector_mqtt_mod:start(ConnectOpts) of
        {ok, Conn} ->
            ?tp(info, connected, #{name => Name}),
            {ok, State#{connection => Conn}};
        {error, Reason} ->
            ConnectOpts1 = obfuscate(ConnectOpts),
            ?SLOG(error, #{
                msg => "failed_to_connect",
                config => ConnectOpts1,
                reason => Reason
            }),
            {error, Reason, State}
    end.

do_send(#{connect_opts := #{forwards := undefined}}, Msg) ->
    ?SLOG(error, #{
        msg =>
            "cannot_forward_messages_to_remote_broker"
            "_as_'egress'_is_not_configured",
        messages => Msg
    });
do_send(
    #{
        connection := Connection,
        mountpoint := Mountpoint,
        connect_opts := #{forwards := Forwards}
    } = State,
    Msg
) ->
    Vars = emqx_connector_mqtt_msg:make_pub_vars(Mountpoint, Forwards),
    ExportMsg = emqx_connector_mqtt_msg:to_remote_msg(Msg, Vars),
    ?SLOG(debug, #{
        msg => "publish_to_remote_broker",
        message => Msg,
        vars => Vars
    }),
    case emqx_connector_mqtt_mod:send(Connection, ExportMsg) of
        ok ->
            {ok, State};
        {ok, #{reason_code := RC}} when
            RC =:= ?RC_SUCCESS;
            RC =:= ?RC_NO_MATCHING_SUBSCRIBERS
        ->
            {ok, State};
        {ok, #{reason_code := RC, reason_code_name := RCN}} ->
            ?SLOG(warning, #{
                msg => "publish_to_remote_node_falied",
                message => Msg,
                reason_code => RC,
                reason_code_name => RCN
            }),
            {error, RCN};
        {error, Reason} ->
            ?SLOG(info, #{
                msg => "mqtt_bridge_produce_failed",
                reason => Reason
            }),
            {error, Reason}
    end.

do_send_async(#{connect_opts := #{forwards := undefined}}, Msg, _Callback) ->
    %% TODO: eval callback with undefined error
    ?SLOG(error, #{
        msg =>
            "cannot_forward_messages_to_remote_broker"
            "_as_'egress'_is_not_configured",
        messages => Msg
    });
do_send_async(
    #{
        connection := Connection,
        mountpoint := Mountpoint,
        connect_opts := #{forwards := Forwards}
    },
    Msg,
    Callback
) ->
    Vars = emqx_connector_mqtt_msg:make_pub_vars(Mountpoint, Forwards),
    ExportMsg = emqx_connector_mqtt_msg:to_remote_msg(Msg, Vars),
    ?SLOG(debug, #{
        msg => "publish_to_remote_broker",
        message => Msg,
        vars => Vars
    }),
    emqx_connector_mqtt_mod:send_async(Connection, ExportMsg, Callback).

disconnect(#{connection := Conn} = State) when Conn =/= undefined ->
    emqx_connector_mqtt_mod:stop(Conn),
    State#{connection => undefined};
disconnect(State) ->
    State.

format_mountpoint(undefined) ->
    undefined;
format_mountpoint(Prefix) ->
    binary:replace(iolist_to_binary(Prefix), <<"${node}">>, atom_to_binary(node(), utf8)).

name(Id) -> list_to_atom(str(Id)).

obfuscate(Map) ->
    maps:fold(
        fun(K, V, Acc) ->
            case is_sensitive(K) of
                true -> [{K, '***'} | Acc];
                false -> [{K, V} | Acc]
            end
        end,
        [],
        Map
    ).

is_sensitive(password) -> true;
is_sensitive(ssl_opts) -> true;
is_sensitive(_) -> false.

str(A) when is_atom(A) ->
    atom_to_list(A);
str(B) when is_binary(B) ->
    binary_to_list(B);
str(S) when is_list(S) ->
    S.
