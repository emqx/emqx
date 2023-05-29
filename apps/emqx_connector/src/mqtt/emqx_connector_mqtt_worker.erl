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

-module(emqx_connector_mqtt_worker).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx.hrl").

%% APIs
-export([
    init/2,
    connect/1,
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

-export([handle_publish/4]).
-export([handle_disconnect/1]).

-export_type([config/0]).

-type template() :: emqx_plugin_libs_rule:tmpl_token().

-type name() :: term().
-type options() :: #{
    % endpoint
    server := iodata(),
    pool_size := pos_integer(),
    % emqtt client options
    proto_ver := v3 | v4 | v5,
    username := binary(),
    password := binary(),
    clientid := binary(),
    clean_start := boolean(),
    max_inflight := pos_integer(),
    connect_timeout := pos_integer(),
    retry_interval := timeout(),
    keepalive := non_neg_integer(),
    bridge_mode := boolean(),
    ssl := boolean(),
    ssl_opts := proplists:proplist(),
    % bridge options
    ingress := map(),
    egress := map()
}.

-type client_option() ::
    emqtt:option()
    | {pool_size, pos_integer()}
    | {name, name()}
    | {ingress, ingress() | undefined}.

-type config() :: egress() | undefined.

-type ingress() :: #{
    remote := #{
        topic := emqx_topic:topic(),
        qos => emqx_types:qos()
    },
    local := msgvars(),
    on_message_received := {module(), atom(), [term()]}
}.

-type egress() :: #{
    local => #{
        topic => emqx_topic:topic()
    },
    remote := msgvars()
}.

-type msgvars() :: #{
    topic => template(),
    qos => template() | emqx_types:qos(),
    retain => template() | boolean(),
    payload => template() | undefined
}.

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-spec init(name(), options()) ->
    {ok, [client_option()], config()}.
init(Name, BridgeOpts) ->
    Ingress = pre_process_ingress(maps:get(ingress, BridgeOpts), Name, BridgeOpts),
    Egress = pre_process_egress(maps:get(egress, BridgeOpts)),
    ClientOpts = mk_client_options(Name, Ingress, BridgeOpts),
    {ok, maps:to_list(ClientOpts), Egress}.

%% @doc Start a bridge worker.
-spec connect([client_option() | {ecpool_worker_id, pos_integer()}]) ->
    {ok, pid()} | {error, _Reason}.
connect(ClientOpts0) ->
    ?SLOG(debug, #{
        msg => "client_starting",
        options => emqx_utils:redact(ClientOpts0)
    }),
    {value, {_, Name}, ClientOpts1} = lists:keytake(name, 1, ClientOpts0),
    {value, {_, WorkerId}, ClientOpts} = lists:keytake(ecpool_worker_id, 1, ClientOpts1),
    case emqtt:start_link(mk_emqtt_opts(WorkerId, ClientOpts)) of
        {ok, Pid} ->
            connect(Pid, Name, WorkerId, ClientOpts);
        {error, Reason} = Error ->
            ?SLOG(error, #{
                msg => "client_start_failed",
                config => emqx_utils:redact(ClientOpts),
                reason => Reason
            }),
            Error
    end.

mk_emqtt_opts(WorkerId, ClientOpts) ->
    ClientId = proplists:get_value(clientid, ClientOpts),
    lists:keystore(clientid, 1, ClientOpts, {clientid, mk_clientid(WorkerId, ClientId)}).

mk_clientid(WorkerId, ClientId) ->
    iolist_to_binary([ClientId, $: | integer_to_list(WorkerId)]).

connect(Pid, Name, WorkerId, ClientOpts) ->
    case emqtt:connect(Pid) of
        {ok, _Props} ->
            Ingress = proplists:get_value(ingress, ClientOpts),
            case subscribe_remote_topic(Pid, WorkerId, Ingress) of
                false ->
                    {ok, Pid};
                {ok, _, _RCs} ->
                    {ok, Pid};
                {error, Reason} = Error ->
                    ?SLOG(error, #{
                        msg => "client_subscribe_failed",
                        ingress => Ingress,
                        reason => Reason
                    }),
                    _ = catch emqtt:stop(Pid),
                    Error
            end;
        {error, Reason} = Error ->
            ?SLOG(warning, #{
                msg => "client_connect_failed",
                reason => Reason,
                name => Name
            }),
            _ = catch emqtt:stop(Pid),
            Error
    end.

subscribe_remote_topic(Pid, WorkerId, #{remote := #{topic := RemoteTopic, qos := QoS}}) ->
    case emqx_topic:parse(RemoteTopic) of
        {_Filter, #{share := _Name}} ->
            % NOTE: this is shared subscription, each worker may subscribe
            emqtt:subscribe(Pid, RemoteTopic, QoS);
        {_Filter, #{}} when WorkerId =:= 1 ->
            % NOTE: this is regular subscription, only the first worker should subscribe
            emqtt:subscribe(Pid, RemoteTopic, QoS);
        {_Filter, #{}} ->
            false
    end;
subscribe_remote_topic(_Ref, _, undefined) ->
    false.

mk_client_options(Name, Ingress, BridgeOpts) ->
    Server = iolist_to_binary(maps:get(server, BridgeOpts)),
    HostPort = emqx_connector_mqtt_schema:parse_server(Server),
    CleanStart =
        case Ingress of
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
            pool_size,
            proto_ver,
            username,
            password,
            clientid,
            max_inflight,
            connect_timeout,
            retry_interval,
            keepalive,
            bridge_mode,
            ssl,
            ssl_opts
        ],
        BridgeOpts
    ),
    Opts#{
        name => Name,
        ingress => Ingress,
        msg_handler => mk_client_event_handler(Ingress, #{server => Server}),
        hosts => [HostPort],
        clean_start => CleanStart,
        force_ping => true
    }.

mk_client_event_handler(Ingress = #{}, Opts) ->
    OnMessage = maps:get(on_message_received, Ingress, undefined),
    LocalPublish =
        case Ingress of
            #{local := Local = #{topic := _}} ->
                Local;
            #{} ->
                undefined
        end,
    #{
        publish => {fun ?MODULE:handle_publish/4, [OnMessage, LocalPublish, Opts]},
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

do_send(Pid, Msg) when Msg /= undefined ->
    emqtt:publish(Pid, Msg);
do_send(_Name, undefined) ->
    ok.

send_to_remote_async(Pid, MsgIn, Callback, Conf) ->
    do_send_async(Pid, export_msg(MsgIn, Conf), Callback).

do_send_async(Pid, Msg, Callback) when Msg /= undefined ->
    ok = emqtt:publish_async(Pid, Msg, _Timeout = infinity, Callback),
    {ok, Pid};
do_send_async(_Pid, undefined, _Callback) ->
    ok.

pre_process_ingress(
    #{remote := RC, local := LC} = Conf,
    BridgeName,
    BridgeOpts
) when is_map(Conf) ->
    Conf#{
        remote => pre_process_in_remote(RC, BridgeName, BridgeOpts),
        local => pre_process_common(LC)
    };
pre_process_ingress(Conf, _, _) when is_map(Conf) ->
    %% have no 'local' field in the config
    undefined.

pre_process_egress(#{remote := RC} = Conf) when is_map(Conf) ->
    Conf#{remote => pre_process_common(RC)};
pre_process_egress(Conf) when is_map(Conf) ->
    %% have no 'remote' field in the config
    undefined.

pre_process_common(Conf0) ->
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

export_msg(Msg, #{remote := Remote}) ->
    to_remote_msg(Msg, Remote);
export_msg(Msg, undefined) ->
    ?SLOG(error, #{
        msg => "forwarding_unavailable",
        message => Msg,
        reason => "egress is not configured"
    }),
    undefined.

%%

handle_publish(#{properties := Props} = MsgIn, OnMessage, LocalPublish, Opts) ->
    Msg = import_msg(MsgIn, Opts),
    ?SLOG(debug, #{
        msg => "publish_local",
        message => Msg
    }),
    maybe_on_message_received(Msg, OnMessage),
    maybe_publish_local(Msg, LocalPublish, Props).

handle_disconnect(_Reason) ->
    ok.

maybe_on_message_received(Msg, {Mod, Func, Args}) ->
    erlang:apply(Mod, Func, [Msg | Args]);
maybe_on_message_received(_Msg, undefined) ->
    ok.

maybe_publish_local(Msg, Local = #{}, Props) ->
    emqx_broker:publish(to_broker_msg(Msg, Local, Props));
maybe_publish_local(_Msg, undefined, _Props) ->
    ok.

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

%% Shame that we have to know the callback module here
%% would be great if we can get rid of #mqtt_msg{} record
%% and use #message{} in all places.
-spec to_remote_msg(emqx_types:message() | map(), msgvars()) ->
    #mqtt_msg{}.
to_remote_msg(#message{flags = Flags} = Msg, Vars) ->
    {EventMsg, _} = emqx_rule_events:eventmsg_publish(Msg),
    to_remote_msg(EventMsg#{retain => maps:get(retain, Flags, false)}, Vars);
to_remote_msg(
    MapMsg,
    #{
        topic := TopicToken,
        qos := QoSToken,
        retain := RetainToken
    } = Remote
) when is_map(MapMsg) ->
    Topic = replace_vars_in_str(TopicToken, MapMsg),
    Payload = process_payload(Remote, MapMsg),
    QoS = replace_simple_var(QoSToken, MapMsg),
    Retain = replace_simple_var(RetainToken, MapMsg),
    PubProps = maps:get(pub_props, MapMsg, #{}),
    #mqtt_msg{
        qos = QoS,
        retain = Retain,
        topic = Topic,
        props = emqx_utils:pub_props_to_packet(PubProps),
        payload = Payload
    }.

%% published from remote node over a MQTT connection
to_broker_msg(Msg, Vars, undefined) ->
    to_broker_msg(Msg, Vars, #{});
to_broker_msg(
    #{dup := Dup} = MapMsg,
    #{
        topic := TopicToken,
        qos := QoSToken,
        retain := RetainToken
    } = Local,
    Props
) ->
    Topic = replace_vars_in_str(TopicToken, MapMsg),
    Payload = process_payload(Local, MapMsg),
    QoS = replace_simple_var(QoSToken, MapMsg),
    Retain = replace_simple_var(RetainToken, MapMsg),
    PubProps = maps:get(pub_props, MapMsg, #{}),
    set_headers(
        Props#{properties => emqx_utils:pub_props_to_packet(PubProps)},
        emqx_message:set_flags(
            #{dup => Dup, retain => Retain},
            emqx_message:make(bridge, QoS, Topic, Payload)
        )
    ).

process_payload(From, MapMsg) ->
    do_process_payload(maps:get(payload, From, undefined), MapMsg).

do_process_payload(undefined, Msg) ->
    emqx_utils_json:encode(Msg);
do_process_payload(Tks, Msg) ->
    replace_vars_in_str(Tks, Msg).

%% Replace a string contains vars to another string in which the placeholders are replace by the
%% corresponding values. For example, given "a: ${var}", if the var=1, the result string will be:
%% "a: 1".
replace_vars_in_str(Tokens, Data) when is_list(Tokens) ->
    emqx_plugin_libs_rule:proc_tmpl(Tokens, Data, #{return => full_binary});
replace_vars_in_str(Val, _Data) ->
    Val.

%% Replace a simple var to its value. For example, given "${var}", if the var=1, then the result
%% value will be an integer 1.
replace_simple_var(Tokens, Data) when is_list(Tokens) ->
    [Var] = emqx_plugin_libs_rule:proc_tmpl(Tokens, Data, #{return => rawlist}),
    Var;
replace_simple_var(Val, _Data) ->
    Val.

set_headers(Val, Msg) ->
    emqx_message:set_headers(Val, Msg).
