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

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/logger.hrl").

%% APIs
-export([
    start_link/2,
    stop/1
]).

%% management APIs
-export([
    connect/1,
    status/1,
    ping/1,
    info/1,
    send_to_remote/2,
    send_to_remote_async/3
]).

-export([handle_publish/3]).
-export([handle_disconnect/1]).

-export_type([
    config/0,
    ack_ref/0
]).

-type name() :: term().
% -type qos() :: emqx_types:qos().
-type config() :: map().
-type ack_ref() :: term().
% -type topic() :: emqx_types:topic().

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(REF(Name), {via, gproc, ?NAME(Name)}).
-define(NAME(Name), {n, l, Name}).

%% @doc Start a bridge worker. Supported configs:
%% mountpoint: The topic mount point for messages sent to remote node/cluster
%%      `undefined', `<<>>' or `""' to disable
%% forwards: Local topics to subscribe.
%%
%% Find more connection specific configs in the callback modules
%% of emqx_bridge_connect behaviour.
-spec start_link(name(), map()) ->
    {ok, pid()} | {error, _Reason}.
start_link(Name, BridgeOpts) ->
    ?SLOG(debug, #{
        msg => "client_starting",
        name => Name,
        options => BridgeOpts
    }),
    Conf = init_config(Name, BridgeOpts),
    Options = mk_client_options(Conf, BridgeOpts),
    case emqtt:start_link(Options) of
        {ok, Pid} ->
            true = gproc:reg_other(?NAME(Name), Pid, Conf),
            {ok, Pid};
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "client_start_failed",
                config => emqx_utils:redact(BridgeOpts),
                reason => Reason
            }),
            Error
    end.

init_config(Name, Opts) ->
    Mountpoint = maps:get(forward_mountpoint, Opts, undefined),
    Subscriptions = maps:get(subscriptions, Opts, undefined),
    Forwards = maps:get(forwards, Opts, undefined),
    #{
        mountpoint => format_mountpoint(Mountpoint),
        subscriptions => pre_process_subscriptions(Subscriptions, Name, Opts),
        forwards => pre_process_forwards(Forwards)
    }.

mk_client_options(Conf, BridgeOpts) ->
    Server = iolist_to_binary(maps:get(server, BridgeOpts)),
    HostPort = emqx_connector_mqtt_schema:parse_server(Server),
    Mountpoint = maps:get(receive_mountpoint, BridgeOpts, undefined),
    Subscriptions = maps:get(subscriptions, Conf),
    Vars = emqx_connector_mqtt_msg:make_pub_vars(Mountpoint, Subscriptions),
    CleanStart =
        case Subscriptions of
            #{remote := _} ->
                maps:get(clean_start, BridgeOpts);
            undefined ->
                %% NOTE
                %% We are ignoring the user configuration here because there's currently no reliable way
                %% to ensure proper session recovery according to the MQTT spec.
                true
        end,
    Opts = maps:without(
        [
            address,
            auto_reconnect,
            conn_type,
            mountpoint,
            forwards,
            receive_mountpoint,
            subscriptions
        ],
        BridgeOpts
    ),
    Opts#{
        msg_handler => mk_client_event_handler(Vars, #{server => Server}),
        hosts => [HostPort],
        clean_start => CleanStart,
        force_ping => true,
        proto_ver => maps:get(proto_ver, BridgeOpts, v4)
    }.

mk_client_event_handler(Vars, Opts) when Vars /= undefined ->
    #{
        publish => {fun ?MODULE:handle_publish/3, [Vars, Opts]},
        disconnected => {fun ?MODULE:handle_disconnect/1, []}
    };
mk_client_event_handler(undefined, _Opts) ->
    undefined.

connect(Name) ->
    #{subscriptions := Subscriptions} = get_config(Name),
    case emqtt:connect(get_pid(Name)) of
        {ok, Properties} ->
            case subscribe_remote_topics(Name, Subscriptions) of
                ok ->
                    {ok, Properties};
                {ok, _, _RCs} ->
                    {ok, Properties};
                {error, Reason} = Error ->
                    ?SLOG(error, #{
                        msg => "client_subscribe_failed",
                        subscriptions => Subscriptions,
                        reason => Reason
                    }),
                    Error
            end;
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "client_connect_failed",
                reason => Reason
            }),
            Error
    end.

subscribe_remote_topics(Ref, #{remote := #{topic := FromTopic, qos := QoS}}) ->
    emqtt:subscribe(ref(Ref), FromTopic, QoS);
subscribe_remote_topics(_Ref, undefined) ->
    ok.

stop(Ref) ->
    emqtt:stop(ref(Ref)).

info(Ref) ->
    emqtt:info(ref(Ref)).

status(Ref) ->
    try
        case proplists:get_value(socket, info(Ref)) of
            Socket when Socket /= undefined ->
                connected;
            undefined ->
                connecting
        end
    catch
        exit:{noproc, _} ->
            disconnected
    end.

ping(Ref) ->
    emqtt:ping(ref(Ref)).

send_to_remote(Name, MsgIn) ->
    trycall(fun() -> do_send(Name, export_msg(Name, MsgIn)) end).

do_send(Name, {true, Msg}) ->
    case emqtt:publish(get_pid(Name), Msg) of
        ok ->
            ok;
        {ok, #{reason_code := RC}} when
            RC =:= ?RC_SUCCESS;
            RC =:= ?RC_NO_MATCHING_SUBSCRIBERS
        ->
            ok;
        {ok, #{reason_code := RC, reason_code_name := Reason}} ->
            ?SLOG(warning, #{
                msg => "remote_publish_failed",
                message => Msg,
                reason_code => RC,
                reason_code_name => Reason
            }),
            {error, Reason};
        {error, Reason} ->
            ?SLOG(info, #{
                msg => "client_failed",
                reason => Reason
            }),
            {error, Reason}
    end;
do_send(_Name, false) ->
    ok.

send_to_remote_async(Name, MsgIn, Callback) ->
    trycall(fun() -> do_send_async(Name, export_msg(Name, MsgIn), Callback) end).

do_send_async(Name, {true, Msg}, Callback) ->
    Pid = get_pid(Name),
    ok = emqtt:publish_async(Pid, Msg, _Timeout = infinity, Callback),
    {ok, Pid};
do_send_async(_Name, false, _Callback) ->
    ok.

ref(Pid) when is_pid(Pid) ->
    Pid;
ref(Term) ->
    ?REF(Term).

trycall(Fun) ->
    try
        Fun()
    catch
        throw:noproc ->
            {error, disconnected};
        exit:{noproc, _} ->
            {error, disconnected}
    end.

format_mountpoint(undefined) ->
    undefined;
format_mountpoint(Prefix) ->
    binary:replace(iolist_to_binary(Prefix), <<"${node}">>, atom_to_binary(node(), utf8)).

pre_process_subscriptions(undefined, _, _) ->
    undefined;
pre_process_subscriptions(
    #{remote := RC, local := LC} = Conf,
    BridgeName,
    BridgeOpts
) when is_map(Conf) ->
    Conf#{
        remote => pre_process_in_remote(RC, BridgeName, BridgeOpts),
        local => pre_process_in_out_common(LC)
    };
pre_process_subscriptions(Conf, _, _) when is_map(Conf) ->
    %% have no 'local' field in the config
    undefined.

pre_process_forwards(undefined) ->
    undefined;
pre_process_forwards(#{remote := RC} = Conf) when is_map(Conf) ->
    Conf#{remote => pre_process_in_out_common(RC)};
pre_process_forwards(Conf) when is_map(Conf) ->
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

pre_process_in_remote(#{qos := QoSIn} = Conf, BridgeName, BridgeOpts) ->
    QoS = downgrade_ingress_qos(QoSIn),
    case QoS of
        QoSIn ->
            ok;
        _ ->
            ?SLOG(warning, #{
                msg => "downgraded_unsupported_ingress_qos",
                qos_configured => QoSIn,
                qos_used => QoS,
                name => BridgeName,
                options => BridgeOpts
            })
    end,
    Conf#{qos => QoS}.

downgrade_ingress_qos(2) ->
    1;
downgrade_ingress_qos(QoS) ->
    QoS.

get_pid(Name) ->
    case gproc:where(?NAME(Name)) of
        Pid when is_pid(Pid) ->
            Pid;
        undefined ->
            throw(noproc)
    end.

get_config(Name) ->
    try
        gproc:lookup_value(?NAME(Name))
    catch
        error:badarg ->
            throw(noproc)
    end.

export_msg(Name, Msg) ->
    case get_config(Name) of
        #{forwards := Forwards = #{}, mountpoint := Mountpoint} ->
            {true, export_msg(Mountpoint, Forwards, Msg)};
        #{forwards := undefined} ->
            ?SLOG(error, #{
                msg => "forwarding_unavailable",
                message => Msg,
                reason => "egress is not configured"
            }),
            false
    end.

export_msg(Mountpoint, Forwards, Msg) ->
    Vars = emqx_connector_mqtt_msg:make_pub_vars(Mountpoint, Forwards),
    emqx_connector_mqtt_msg:to_remote_msg(Msg, Vars).

%%

handle_publish(#{properties := Props} = MsgIn, Vars, Opts) ->
    Msg = import_msg(MsgIn, Opts),
    ?SLOG(debug, #{
        msg => "publish_local",
        message => Msg,
        vars => Vars
    }),
    case Vars of
        #{on_message_received := {Mod, Func, Args}} ->
            _ = erlang:apply(Mod, Func, [Msg | Args]);
        _ ->
            ok
    end,
    maybe_publish_local(Msg, Vars, Props).

handle_disconnect(_Reason) ->
    ok.

maybe_publish_local(Msg, Vars, Props) ->
    case emqx_utils_maps:deep_get([local, topic], Vars, undefined) of
        %% local topic is not set, discard it
        undefined ->
            ok;
        _ ->
            emqx_broker:publish(emqx_connector_mqtt_msg:to_broker_msg(Msg, Vars, Props))
    end.

import_msg(
    #{
        dup := Dup,
        payload := Payload,
        properties := Props,
        qos := QoS,
        retain := Retain,
        topic := Topic
    },
    #{server := Server}
) ->
    #{
        id => emqx_guid:to_hexstr(emqx_guid:gen()),
        server => Server,
        payload => Payload,
        topic => Topic,
        qos => QoS,
        dup => Dup,
        retain => Retain,
        pub_props => printable_maps(Props),
        message_received_at => erlang:system_time(millisecond)
    }.

printable_maps(undefined) ->
    #{};
printable_maps(Headers) ->
    maps:fold(
        fun
            ('User-Property', V0, AccIn) when is_list(V0) ->
                AccIn#{
                    'User-Property' => maps:from_list(V0),
                    'User-Property-Pairs' => [
                        #{
                            key => Key,
                            value => Value
                        }
                     || {Key, Value} <- V0
                    ]
                };
            (K, V0, AccIn) ->
                AccIn#{K => V0}
        end,
        #{},
        Headers
    ).
