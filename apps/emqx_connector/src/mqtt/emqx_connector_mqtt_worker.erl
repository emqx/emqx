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
    status/1,
    ping/1,
    info/1,
    send_to_remote/3,
    send_to_remote_async/4
]).

-export([handle_publish/3]).
-export([handle_disconnect/1]).

-export_type([config/0]).

-type template() :: emqx_plugin_libs_rule:tmpl_token().

-type name() :: term().
-type options() :: #{
    % endpoint
    server := iodata(),
    % emqtt client options
    proto_ver := v3 | v4 | v5,
    username := binary(),
    password := binary(),
    clientid := binary(),
    clean_start := boolean(),
    max_inflight := pos_integer(),
    connect_timeout := pos_integer(),
    retry_interval := timeout(),
    bridge_mode := boolean(),
    ssl := boolean(),
    ssl_opts := proplists:proplist(),
    % bridge options
    subscriptions := map(),
    forwards := map()
}.

-type config() :: #{
    subscriptions := subscriptions() | undefined,
    forwards := forwards() | undefined
}.

-type subscriptions() :: #{
    remote := #{
        topic := emqx_topic:topic(),
        qos => emqx_types:qos()
    },
    local := #{
        topic => template(),
        qos => template() | emqx_types:qos(),
        retain => template() | boolean(),
        payload => template() | undefined
    },
    on_message_received := {module(), atom(), [term()]}
}.

-type forwards() :: #{
    local => #{
        topic => emqx_topic:topic()
    },
    remote := #{
        topic := template(),
        qos => template() | emqx_types:qos(),
        retain => template() | boolean(),
        payload => template() | undefined
    }
}.

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%% @doc Start a bridge worker. Supported configs:
%% mountpoint: The topic mount point for messages sent to remote node/cluster
%%      `undefined', `<<>>' or `""' to disable
%% forwards: Local topics to subscribe.
%%
%% Find more connection specific configs in the callback modules
%% of emqx_bridge_connect behaviour.
-spec start_link(name(), options()) ->
    {ok, pid(), {emqtt:properties(), config()}} | {error, _Reason}.
start_link(Name, BridgeOpts) ->
    ?SLOG(debug, #{
        msg => "client_starting",
        name => Name,
        options => BridgeOpts
    }),
    Config = init_config(Name, BridgeOpts),
    Options = mk_client_options(Config, BridgeOpts),
    case emqtt:start_link(Options) of
        {ok, Pid} ->
            connect(Pid, Name, Config);
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "client_start_failed",
                config => emqx_utils:redact(BridgeOpts),
                reason => Reason
            }),
            Error
    end.

connect(Pid, Name, Config = #{subscriptions := Subscriptions}) ->
    case emqtt:connect(Pid) of
        {ok, Props} ->
            case subscribe_remote_topics(Pid, Subscriptions) of
                ok ->
                    {ok, Pid, {Props, Config}};
                {ok, _, _RCs} ->
                    {ok, Pid, {Props, Config}};
                {error, Reason} = Error ->
                    ?SLOG(error, #{
                        msg => "client_subscribe_failed",
                        subscriptions => Subscriptions,
                        reason => Reason
                    }),
                    _ = emqtt:stop(Pid),
                    Error
            end;
        {error, Reason} = Error ->
            ?SLOG(warning, #{
                msg => "client_connect_failed",
                reason => Reason,
                name => Name
            }),
            _ = emqtt:stop(Pid),
            Error
    end.

subscribe_remote_topics(Pid, #{remote := #{topic := RemoteTopic, qos := QoS}}) ->
    emqtt:subscribe(Pid, RemoteTopic, QoS);
subscribe_remote_topics(_Ref, undefined) ->
    ok.

init_config(Name, Opts) ->
    Subscriptions = maps:get(subscriptions, Opts, undefined),
    Forwards = maps:get(forwards, Opts, undefined),
    #{
        subscriptions => pre_process_subscriptions(Subscriptions, Name, Opts),
        forwards => pre_process_forwards(Forwards)
    }.

mk_client_options(Config, BridgeOpts) ->
    Server = iolist_to_binary(maps:get(server, BridgeOpts)),
    HostPort = emqx_connector_mqtt_schema:parse_server(Server),
    Subscriptions = maps:get(subscriptions, Config),
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
    Opts = maps:with(
        [
            proto_ver,
            username,
            password,
            clientid,
            max_inflight,
            connect_timeout,
            retry_interval,
            bridge_mode,
            ssl,
            ssl_opts
        ],
        BridgeOpts
    ),
    Opts#{
        msg_handler => mk_client_event_handler(Subscriptions, #{server => Server}),
        hosts => [HostPort],
        clean_start => CleanStart,
        force_ping => true
    }.

mk_client_event_handler(Vars, Opts) when Vars /= undefined ->
    #{
        publish => {fun ?MODULE:handle_publish/3, [Vars, Opts]},
        disconnected => {fun ?MODULE:handle_disconnect/1, []}
    };
mk_client_event_handler(undefined, _Opts) ->
    undefined.

stop(Pid) ->
    emqtt:stop(Pid).

info(Pid) ->
    emqtt:info(Pid).

status(Pid) ->
    try
        case proplists:get_value(socket, info(Pid)) of
            Socket when Socket /= undefined ->
                connected;
            undefined ->
                connecting
        end
    catch
        exit:{noproc, _} ->
            disconnected
    end.

ping(Pid) ->
    emqtt:ping(Pid).

send_to_remote(Pid, MsgIn, Conf) ->
    do_send(Pid, export_msg(MsgIn, Conf)).

do_send(Pid, {true, Msg}) ->
    case emqtt:publish(Pid, Msg) of
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

send_to_remote_async(Pid, MsgIn, Callback, Conf) ->
    do_send_async(Pid, export_msg(MsgIn, Conf), Callback).

do_send_async(Pid, {true, Msg}, Callback) ->
    ok = emqtt:publish_async(Pid, Msg, _Timeout = infinity, Callback),
    {ok, Pid};
do_send_async(_Pid, false, _Callback) ->
    ok.

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

export_msg(Msg, #{forwards := Forwards = #{}}) ->
    {true, emqx_connector_mqtt_msg:to_remote_msg(Msg, Forwards)};
export_msg(Msg, #{forwards := undefined}) ->
    ?SLOG(error, #{
        msg => "forwarding_unavailable",
        message => Msg,
        reason => "egress is not configured"
    }),
    false.

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
