%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_events).

-include("rule_engine.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").
-include_lib("emqx_bridge/include/emqx_bridge_resource.hrl").

-export([
    reload/0,
    load/1,
    unload/0,
    unload/1,
    event_names/0,
    event_name/1,
    event_topics_enum/0,
    event_topic/1,
    eventmsg_publish/1
]).

-export([
    on_client_connected/3,
    on_client_disconnected/4,
    on_client_connack/4,
    on_client_check_authz_complete/6,
    on_client_check_authn_complete/3,
    on_session_subscribed/4,
    on_session_unsubscribed/4,
    on_message_publish/2,
    on_message_dropped/4,
    on_message_transformation_failed/3,
    on_schema_validation_failed/3,
    on_message_delivered/3,
    on_message_acked/3,
    on_delivery_dropped/4,
    on_bridge_message_received/2
]).

-export([
    event_info/0,
    columns/1,
    columns_with_exam/1
]).

-ifdef(TEST).
-export([
    reason/1,
    hook_fun/1,
    printable_maps/1
]).
-endif.

-elvis([{elvis_style, dont_repeat_yourself, disable}]).

event_names() ->
    [
        'client.connected',
        'client.disconnected',
        'client.connack',
        'client.check_authz_complete',
        'session.subscribed',
        'session.unsubscribed',
        'message.publish',
        'message.delivered',
        'message.acked',
        'message.dropped',
        'message.transformation_failed',
        'schema.validation_failed',
        'delivery.dropped'
    ].

%% for documentation purposes
event_topics_enum() ->
    [
        '$events/client_connected',
        '$events/client_disconnected',
        '$events/client_connack',
        '$events/client_check_authz_complete',
        '$events/session_subscribed',
        '$events/session_unsubscribed',
        '$events/message_delivered',
        '$events/message_acked',
        '$events/message_dropped',
        '$events/message_transformation_failed',
        '$events/schema_validation_failed',
        '$events/delivery_dropped'
        % '$events/message_publish' % not possible to use in SELECT FROM
    ].

reload() ->
    lists:foreach(
        fun(Rule) ->
            ok = emqx_rule_engine:load_hooks_for_rule(Rule)
        end,
        emqx_rule_engine:get_rules()
    ).

load(Topic) ->
    HookPoint = event_name(Topic),
    HookFun = hook_fun_name(HookPoint),
    emqx_hooks:put(
        HookPoint, {?MODULE, HookFun, [#{event_topic => Topic}]}, ?HP_RULE_ENGINE
    ).

unload() ->
    lists:foreach(
        fun(HookPoint) ->
            emqx_hooks:del(HookPoint, {?MODULE, hook_fun_name(HookPoint)})
        end,
        event_names()
    ).

unload(Topic) ->
    HookPoint = event_name(Topic),
    emqx_hooks:del(HookPoint, {?MODULE, hook_fun_name(HookPoint)}).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------
on_message_publish(Message = #message{topic = Topic}, _Conf) ->
    case ignore_sys_message(Message) of
        true ->
            ok;
        false ->
            case emqx_rule_engine:get_rules_for_topic(Topic) of
                [] ->
                    ok;
                Rules ->
                    %% ENVs are the fields that can't be refereced by the SQL, but can be used
                    %% from actions. e.g. The 'headers' field in the internal record `#message{}`.
                    {Columns, Envs} = eventmsg_publish(Message),
                    emqx_rule_runtime:apply_rules(Rules, Columns, Envs)
            end
    end,
    {ok, Message}.

on_bridge_message_received(Message, Conf = #{event_topic := BridgeTopic}) ->
    apply_event(BridgeTopic, fun() -> with_basic_columns(BridgeTopic, Message, #{}) end, Conf).

on_client_connected(ClientInfo, ConnInfo, Conf) ->
    apply_event(
        'client.connected',
        fun() -> eventmsg_connected(ClientInfo, ConnInfo) end,
        Conf
    ).

on_client_connack(ConnInfo, Reason, _, Conf) ->
    apply_event(
        'client.connack',
        fun() -> eventmsg_connack(ConnInfo, Reason) end,
        Conf
    ).

%% TODO: support full action in major release
on_client_check_authz_complete(
    ClientInfo, ?authz_action(PubSub), Topic, Result, AuthzSource, Conf
) ->
    apply_event(
        'client.check_authz_complete',
        fun() ->
            eventmsg_check_authz_complete(
                ClientInfo,
                PubSub,
                Topic,
                Result,
                AuthzSource
            )
        end,
        Conf
    ).

on_client_check_authn_complete(ClientInfo, Result, Conf) ->
    apply_event(
        'client.check_authn_complete',
        fun() ->
            eventmsg_check_authn_complete(
                ClientInfo,
                Result
            )
        end,
        Conf
    ).

on_client_disconnected(ClientInfo, Reason, ConnInfo, Conf) ->
    apply_event(
        'client.disconnected',
        fun() -> eventmsg_disconnected(ClientInfo, ConnInfo, Reason) end,
        Conf
    ).

on_session_subscribed(ClientInfo, Topic, SubOpts, Conf) ->
    apply_event(
        'session.subscribed',
        fun() ->
            eventmsg_sub_or_unsub(
                'session.subscribed', ClientInfo, emqx_topic:maybe_format_share(Topic), SubOpts
            )
        end,
        Conf
    ).

on_session_unsubscribed(ClientInfo, Topic, SubOpts, Conf) ->
    apply_event(
        'session.unsubscribed',
        fun() ->
            eventmsg_sub_or_unsub(
                'session.unsubscribed', ClientInfo, emqx_topic:maybe_format_share(Topic), SubOpts
            )
        end,
        Conf
    ).

on_message_dropped(Message, _, Reason, Conf) ->
    case ignore_sys_message(Message) of
        true ->
            ok;
        false ->
            apply_event(
                'message.dropped',
                fun() -> eventmsg_dropped(Message, Reason) end,
                Conf
            )
    end,
    {ok, Message}.

on_message_transformation_failed(Message, TransformationContext, Conf) ->
    case ignore_sys_message(Message) of
        true ->
            ok;
        false ->
            apply_event(
                'message.transformation_failed',
                fun() -> eventmsg_transformation_failed(Message, TransformationContext) end,
                Conf
            )
    end,
    {ok, Message}.

on_schema_validation_failed(Message, ValidationContext, Conf) ->
    case ignore_sys_message(Message) of
        true ->
            ok;
        false ->
            apply_event(
                'schema.validation_failed',
                fun() -> eventmsg_validation_failed(Message, ValidationContext) end,
                Conf
            )
    end,
    {ok, Message}.

on_message_delivered(ClientInfo, Message, Conf) ->
    case ignore_sys_message(Message) of
        true ->
            ok;
        false ->
            apply_event(
                'message.delivered',
                fun() -> eventmsg_delivered(ClientInfo, Message) end,
                Conf
            )
    end,
    {ok, Message}.

on_message_acked(ClientInfo, Message, Conf) ->
    case ignore_sys_message(Message) of
        true ->
            ok;
        false ->
            apply_event(
                'message.acked',
                fun() -> eventmsg_acked(ClientInfo, Message) end,
                Conf
            )
    end,
    {ok, Message}.

on_delivery_dropped(ClientInfo, Message, Reason, Conf) ->
    case ignore_sys_message(Message) of
        true ->
            ok;
        false ->
            apply_event(
                'delivery.dropped',
                fun() -> eventmsg_delivery_dropped(ClientInfo, Message, Reason) end,
                Conf
            )
    end,
    {ok, Message}.

%%--------------------------------------------------------------------
%% Event Messages
%%--------------------------------------------------------------------

eventmsg_publish(
    Message = #message{
        id = Id,
        from = ClientId,
        qos = QoS,
        flags = Flags,
        topic = Topic,
        headers = Headers,
        payload = Payload,
        timestamp = Timestamp
    }
) ->
    with_basic_columns(
        'message.publish',
        #{
            id => emqx_guid:to_hexstr(Id),
            clientid => ClientId,
            username => emqx_message:get_header(username, Message, undefined),
            payload => Payload,
            peerhost => ntoa(emqx_message:get_header(peerhost, Message, undefined)),
            peername => ntoa(emqx_message:get_header(peername, Message, undefined)),
            topic => Topic,
            qos => QoS,
            flags => Flags,
            pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
            publish_received_at => Timestamp,
            client_attrs => emqx_message:get_header(client_attrs, Message, #{})
        },
        #{headers => Headers}
    ).

eventmsg_connected(
    ClientInfo = #{
        clientid := ClientId,
        username := Username,
        is_bridge := IsBridge,
        mountpoint := Mountpoint
    },
    ConnInfo = #{
        peername := PeerName,
        sockname := SockName,
        clean_start := CleanStart,
        proto_name := ProtoName,
        proto_ver := ProtoVer,
        connected_at := ConnectedAt
    }
) ->
    Keepalive = maps:get(keepalive, ConnInfo, 0),
    ConnProps = maps:get(conn_props, ConnInfo, #{}),
    RcvMax = maps:get(receive_maximum, ConnInfo, 0),
    ExpiryInterval = maps:get(expiry_interval, ConnInfo, 0),
    with_basic_columns(
        'client.connected',
        #{
            clientid => ClientId,
            username => Username,
            mountpoint => Mountpoint,
            peername => ntoa(PeerName),
            sockname => ntoa(SockName),
            proto_name => ProtoName,
            proto_ver => ProtoVer,
            keepalive => Keepalive,
            clean_start => CleanStart,
            receive_maximum => RcvMax,
            expiry_interval => ExpiryInterval div 1000,
            is_bridge => IsBridge,
            conn_props => printable_maps(ConnProps),
            connected_at => ConnectedAt,
            client_attrs => maps:get(client_attrs, ClientInfo, #{})
        },
        #{}
    ).

eventmsg_disconnected(
    ClientInfo = #{
        clientid := ClientId,
        username := Username
    },
    ConnInfo = #{
        peername := PeerName,
        sockname := SockName,
        proto_name := ProtoName,
        proto_ver := ProtoVer,
        disconnected_at := DisconnectedAt
    },
    Reason
) ->
    with_basic_columns(
        'client.disconnected',
        #{
            reason => reason(Reason),
            clientid => ClientId,
            username => Username,
            peername => ntoa(PeerName),
            sockname => ntoa(SockName),
            proto_name => ProtoName,
            proto_ver => ProtoVer,
            disconn_props => printable_maps(maps:get(disconn_props, ConnInfo, #{})),
            disconnected_at => DisconnectedAt,
            client_attrs => maps:get(client_attrs, ClientInfo, #{})
        },
        #{}
    ).

eventmsg_connack(
    ConnInfo = #{
        clientid := ClientId,
        clean_start := CleanStart,
        username := Username,
        peername := PeerName,
        sockname := SockName,
        proto_name := ProtoName,
        proto_ver := ProtoVer
    },
    Reason
) ->
    Keepalive = maps:get(keepalive, ConnInfo, 0),
    ConnProps = maps:get(conn_props, ConnInfo, #{}),
    ExpiryInterval = maps:get(expiry_interval, ConnInfo, 0),
    with_basic_columns(
        'client.connack',
        #{
            reason_code => reason(Reason),
            clientid => ClientId,
            clean_start => CleanStart,
            username => Username,
            peername => ntoa(PeerName),
            sockname => ntoa(SockName),
            proto_name => ProtoName,
            proto_ver => ProtoVer,
            keepalive => Keepalive,
            expiry_interval => ExpiryInterval,
            conn_props => printable_maps(ConnProps)
        },
        #{}
    ).

eventmsg_check_authz_complete(
    ClientInfo = #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost,
        peername := PeerName
    },
    PubSub,
    Topic,
    Result,
    AuthzSource
) ->
    with_basic_columns(
        'client.check_authz_complete',
        #{
            clientid => ClientId,
            username => Username,
            peername => ntoa(PeerName),
            peerhost => ntoa(PeerHost),
            topic => Topic,
            action => PubSub,
            authz_source => AuthzSource,
            result => Result,
            client_attrs => maps:get(client_attrs, ClientInfo, #{})
        },
        #{}
    ).

eventmsg_check_authn_complete(
    ClientInfo = #{
        clientid := ClientId,
        username := Username,
        peername := PeerName
    },
    Result
) ->
    #{
        reason_code := Reason,
        is_superuser := IsSuperuser,
        is_anonymous := IsAnonymous
    } = maps:merge(
        #{is_anonymous => false, is_superuser => false}, Result
    ),
    with_basic_columns(
        'client.check_authn_complete',
        #{
            clientid => ClientId,
            username => Username,
            peername => ntoa(PeerName),
            reason_code => force_to_bin(Reason),
            is_anonymous => IsAnonymous,
            is_superuser => IsSuperuser,
            client_attrs => maps:get(client_attrs, ClientInfo, #{})
        },
        #{}
    ).

eventmsg_sub_or_unsub(
    Event,
    ClientInfo = #{
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost,
        peername := PeerName
    },
    Topic,
    SubOpts = #{qos := QoS}
) ->
    PropKey = sub_unsub_prop_key(Event),
    with_basic_columns(
        Event,
        #{
            clientid => ClientId,
            username => Username,
            peerhost => ntoa(PeerHost),
            peername => ntoa(PeerName),
            PropKey => printable_maps(maps:get(PropKey, SubOpts, #{})),
            topic => Topic,
            qos => QoS,
            client_attrs => maps:get(client_attrs, ClientInfo, #{})
        },
        #{}
    ).

eventmsg_dropped(
    Message = #message{
        id = Id,
        from = ClientId,
        qos = QoS,
        flags = Flags,
        topic = Topic,
        headers = Headers,
        payload = Payload,
        timestamp = Timestamp
    },
    Reason
) ->
    with_basic_columns(
        'message.dropped',
        #{
            id => emqx_guid:to_hexstr(Id),
            reason => Reason,
            clientid => ClientId,
            username => emqx_message:get_header(username, Message, undefined),
            payload => Payload,
            peerhost => ntoa(emqx_message:get_header(peerhost, Message, undefined)),
            peername => ntoa(emqx_message:get_header(peername, Message, undefined)),
            topic => Topic,
            qos => QoS,
            flags => Flags,
            pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
            publish_received_at => Timestamp
        },
        #{headers => Headers}
    ).

eventmsg_transformation_failed(
    Message = #message{
        id = Id,
        from = ClientId,
        qos = QoS,
        flags = Flags,
        topic = Topic,
        headers = Headers,
        payload = Payload,
        timestamp = Timestamp
    },
    TransformationContext
) ->
    #{name := TransformationName} = TransformationContext,
    with_basic_columns(
        'message.transformation_failed',
        #{
            id => emqx_guid:to_hexstr(Id),
            transformation => TransformationName,
            clientid => ClientId,
            username => emqx_message:get_header(username, Message, undefined),
            payload => Payload,
            peername => ntoa(emqx_message:get_header(peername, Message, undefined)),
            topic => Topic,
            qos => QoS,
            flags => Flags,
            pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
            publish_received_at => Timestamp
        },
        #{headers => Headers}
    ).

eventmsg_validation_failed(
    Message = #message{
        id = Id,
        from = ClientId,
        qos = QoS,
        flags = Flags,
        topic = Topic,
        headers = Headers,
        payload = Payload,
        timestamp = Timestamp
    },
    ValidationContext
) ->
    #{name := ValidationName} = ValidationContext,
    with_basic_columns(
        'schema.validation_failed',
        #{
            id => emqx_guid:to_hexstr(Id),
            validation => ValidationName,
            clientid => ClientId,
            username => emqx_message:get_header(username, Message, undefined),
            payload => Payload,
            peerhost => ntoa(emqx_message:get_header(peerhost, Message, undefined)),
            peername => ntoa(emqx_message:get_header(peername, Message, undefined)),
            topic => Topic,
            qos => QoS,
            flags => Flags,
            pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
            publish_received_at => Timestamp
        },
        #{headers => Headers}
    ).

eventmsg_delivered(
    _ClientInfo = #{
        peerhost := PeerHost,
        peername := PeerName,
        clientid := ReceiverCId,
        username := ReceiverUsername
    },
    Message = #message{
        id = Id,
        from = ClientId,
        qos = QoS,
        flags = Flags,
        topic = Topic,
        headers = Headers,
        payload = Payload,
        timestamp = Timestamp
    }
) ->
    with_basic_columns(
        'message.delivered',
        #{
            id => emqx_guid:to_hexstr(Id),
            from_clientid => ClientId,
            from_username => emqx_message:get_header(username, Message, undefined),
            clientid => ReceiverCId,
            username => ReceiverUsername,
            payload => Payload,
            peerhost => ntoa(PeerHost),
            peername => ntoa(PeerName),
            topic => Topic,
            qos => QoS,
            flags => Flags,
            pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
            publish_received_at => Timestamp
        },
        #{headers => Headers}
    ).

eventmsg_acked(
    _ClientInfo = #{
        peerhost := PeerHost,
        peername := PeerName,
        clientid := ReceiverCId,
        username := ReceiverUsername
    },
    Message = #message{
        id = Id,
        from = ClientId,
        qos = QoS,
        flags = Flags,
        topic = Topic,
        headers = Headers,
        payload = Payload,
        timestamp = Timestamp
    }
) ->
    with_basic_columns(
        'message.acked',
        #{
            id => emqx_guid:to_hexstr(Id),
            from_clientid => ClientId,
            from_username => emqx_message:get_header(username, Message, undefined),
            clientid => ReceiverCId,
            username => ReceiverUsername,
            payload => Payload,
            peerhost => ntoa(PeerHost),
            peername => ntoa(PeerName),
            topic => Topic,
            qos => QoS,
            flags => Flags,
            pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
            puback_props => printable_maps(emqx_message:get_header(puback_props, Message, #{})),
            publish_received_at => Timestamp
        },
        #{headers => Headers}
    ).

eventmsg_delivery_dropped(
    _ClientInfo = #{
        peerhost := PeerHost,
        peername := PeerName,
        clientid := ReceiverCId,
        username := ReceiverUsername
    },
    Message = #message{
        id = Id,
        from = ClientId,
        qos = QoS,
        flags = Flags,
        topic = Topic,
        headers = Headers,
        payload = Payload,
        timestamp = Timestamp
    },
    Reason
) ->
    with_basic_columns(
        'delivery.dropped',
        #{
            id => emqx_guid:to_hexstr(Id),
            reason => Reason,
            from_clientid => ClientId,
            from_username => emqx_message:get_header(username, Message, undefined),
            clientid => ReceiverCId,
            username => ReceiverUsername,
            payload => Payload,
            peerhost => ntoa(PeerHost),
            peername => ntoa(PeerName),
            topic => Topic,
            qos => QoS,
            flags => Flags,
            pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
            publish_received_at => Timestamp
        },
        #{headers => Headers}
    ).

sub_unsub_prop_key('session.subscribed') -> sub_props;
sub_unsub_prop_key('session.unsubscribed') -> unsub_props.

with_basic_columns(EventName, Columns, Envs) when is_map(Columns) ->
    {
        Columns#{
            event => EventName,
            timestamp => erlang:system_time(millisecond),
            node => node()
        },
        Envs
    }.

%%--------------------------------------------------------------------
%% rules applying
%%--------------------------------------------------------------------
apply_event(EventName, GenEventMsg, _Conf) ->
    EventTopic = event_topic(EventName),
    case emqx_rule_engine:get_rules_for_topic(EventTopic) of
        [] ->
            ok;
        Rules ->
            %% delay the generating of eventmsg after we have found some rules to apply
            {Columns, Envs} = GenEventMsg(),
            emqx_rule_runtime:apply_rules(Rules, Columns, Envs)
    end.

%%--------------------------------------------------------------------
%% Columns
%%--------------------------------------------------------------------
columns(Event) ->
    [Key || {Key, _ExampleVal} <- columns_with_exam(Event)].

event_info() ->
    [
        event_info_message_publish(),
        event_info_message_deliver(),
        event_info_message_acked(),
        event_info_message_dropped(),
        event_info_client_connected(),
        event_info_client_disconnected(),
        event_info_client_connack(),
        event_info_client_check_authz_complete(),
        event_info_client_check_authn_complete(),
        event_info_session_subscribed(),
        event_info_session_unsubscribed(),
        event_info_delivery_dropped(),
        event_info_bridge_mqtt()
    ] ++ ee_event_info().

-if(?EMQX_RELEASE_EDITION == ee).
%% ELSE (?EMQX_RELEASE_EDITION == ee).
event_info_schema_validation_failed() ->
    event_info_common(
        'schema.validation_failed',
        {<<"schema validation failed">>, <<"schema 验证失败"/utf8>>},
        {<<"messages that do not pass configured validations">>, <<"未通过验证的消息"/utf8>>},
        <<"SELECT * FROM \"$events/schema_validation_failed\" WHERE topic =~ 't/#'">>
    ).
event_info_message_transformation_failed() ->
    event_info_common(
        'message.transformation_failed',
        {<<"message transformation failed">>, <<"message 验证失败"/utf8>>},
        {<<"messages that do not pass configured transformation">>, <<"未通过验证的消息"/utf8>>},
        <<"SELECT * FROM \"$events/message_transformation_failed\" WHERE topic =~ 't/#'">>
    ).
ee_event_info() ->
    [
        event_info_schema_validation_failed(),
        event_info_message_transformation_failed()
    ].
-else.
%% END (?EMQX_RELEASE_EDITION == ee).

ee_event_info() ->
    [].
-endif.

event_info_message_publish() ->
    event_info_common(
        'message.publish',
        {<<"message publish">>, <<"消息发布"/utf8>>},
        {<<"message publish">>, <<"消息发布"/utf8>>},
        <<"SELECT payload.msg as msg FROM \"t/#\" WHERE msg = 'hello'">>
    ).
event_info_message_deliver() ->
    event_info_common(
        'message.delivered',
        {<<"message delivered">>, <<"消息已投递"/utf8>>},
        {<<"message delivered">>, <<"消息已投递"/utf8>>},
        <<"SELECT * FROM \"$events/message_delivered\" WHERE topic =~ 't/#'">>
    ).
event_info_message_acked() ->
    event_info_common(
        'message.acked',
        {<<"message acked">>, <<"消息应答"/utf8>>},
        {<<"message acked">>, <<"消息应答"/utf8>>},
        <<"SELECT * FROM \"$events/message_acked\" WHERE topic =~ 't/#'">>
    ).
event_info_message_dropped() ->
    event_info_common(
        'message.dropped',
        {<<"message routing-drop">>, <<"消息转发丢弃"/utf8>>},
        {<<"messages are discarded during routing, usually because there are no subscribers">>,
            <<"消息在转发的过程中被丢弃，一般是由于没有订阅者"/utf8>>},
        <<"SELECT * FROM \"$events/message_dropped\" WHERE topic =~ 't/#'">>
    ).
event_info_delivery_dropped() ->
    event_info_common(
        'delivery.dropped',
        {<<"message delivery-drop">>, <<"消息投递丢弃"/utf8>>},
        {<<"messages are discarded during delivery, i.e. because the message queue is full">>,
            <<"消息在投递的过程中被丢弃，比如由于消息队列已满"/utf8>>},
        <<"SELECT * FROM \"$events/delivery_dropped\" WHERE topic =~ 't/#'">>
    ).
event_info_client_connected() ->
    event_info_common(
        'client.connected',
        {<<"client connected">>, <<"连接建立"/utf8>>},
        {<<"client connected">>, <<"连接建立"/utf8>>},
        <<"SELECT * FROM \"$events/client_connected\"">>
    ).
event_info_client_disconnected() ->
    event_info_common(
        'client.disconnected',
        {<<"client disconnected">>, <<"连接断开"/utf8>>},
        {<<"client disconnected">>, <<"连接断开"/utf8>>},
        <<"SELECT * FROM \"$events/client_disconnected\" WHERE topic =~ 't/#'">>
    ).
event_info_client_connack() ->
    event_info_common(
        'client.connack',
        {<<"client connack">>, <<"连接确认"/utf8>>},
        {<<"client connack">>, <<"连接确认"/utf8>>},
        <<"SELECT * FROM \"$events/client_connack\"">>
    ).
event_info_client_check_authz_complete() ->
    event_info_common(
        'client.check_authz_complete',
        {<<"client check authz complete">>, <<"授权结果"/utf8>>},
        {<<"client check authz complete">>, <<"授权结果"/utf8>>},
        <<"SELECT * FROM \"$events/client_check_authz_complete\"">>
    ).
event_info_client_check_authn_complete() ->
    event_info_common(
        'client.check_authn_complete',
        {<<"client check authn complete">>, <<"认证结果"/utf8>>},
        {<<"client check authn complete">>, <<"认证结果"/utf8>>},
        <<"SELECT * FROM \"$events/client_check_authn_complete\"">>
    ).
event_info_session_subscribed() ->
    event_info_common(
        'session.subscribed',
        {<<"session subscribed">>, <<"会话订阅完成"/utf8>>},
        {<<"session subscribed">>, <<"会话订阅完成"/utf8>>},
        <<"SELECT * FROM \"$events/session_subscribed\" WHERE topic =~ 't/#'">>
    ).
event_info_session_unsubscribed() ->
    event_info_common(
        'session.unsubscribed',
        {<<"session unsubscribed">>, <<"会话取消订阅完成"/utf8>>},
        {<<"session unsubscribed">>, <<"会话取消订阅完成"/utf8>>},
        <<"SELECT * FROM \"$events/session_unsubscribed\" WHERE topic =~ 't/#'">>
    ).
event_info_bridge_mqtt() ->
    event_info_common(
        <<"$bridges/mqtt:*">>,
        {<<"MQTT bridge message">>, <<"MQTT 桥接消息"/utf8>>},
        {<<"received a message from MQTT bridge">>, <<"收到来自 MQTT 桥接的消息"/utf8>>},
        <<"SELECT * FROM \"$bridges/mqtt:my_mqtt_bridge\" WHERE topic =~ 't/#'">>
    ).

event_info_common(Event, {TitleEN, TitleZH}, {DescrEN, DescrZH}, SqlExam) ->
    #{
        event => event_topic(Event),
        title => #{en => TitleEN, zh => TitleZH},
        description => #{en => DescrEN, zh => DescrZH},
        test_columns => test_columns(Event),
        columns => columns(Event),
        sql_example => SqlExam
    }.

test_columns('message.dropped') ->
    [{<<"reason">>, [<<"no_subscribers">>, <<"the reason of dropping">>]}] ++
        test_columns('message.publish');
test_columns('message.publish') ->
    [
        {<<"clientid">>, [<<"c_emqx">>, <<"the clientid of the sender">>]},
        {<<"username">>, [<<"u_emqx">>, <<"the username of the sender">>]},
        {<<"topic">>, [<<"t/a">>, <<"the topic of the MQTT message">>]},
        {<<"qos">>, [1, <<"the QoS of the MQTT message">>]},
        {<<"payload">>, [<<"{\"msg\": \"hello\"}">>, <<"the payload of the MQTT message">>]}
    ];
test_columns('delivery.dropped') ->
    [{<<"reason">>, [<<"queue_full">>, <<"the reason of dropping">>]}] ++
        test_columns('message.delivered');
test_columns('message.acked') ->
    test_columns('message.delivered');
test_columns('message.delivered') ->
    [
        {<<"from_clientid">>, [<<"c_emqx_1">>, <<"the clientid of the sender">>]},
        {<<"from_username">>, [<<"u_emqx_1">>, <<"the username of the sender">>]},
        {<<"clientid">>, [<<"c_emqx_2">>, <<"the clientid of the receiver">>]},
        {<<"username">>, [<<"u_emqx_2">>, <<"the username of the receiver">>]},
        {<<"topic">>, [<<"t/a">>, <<"the topic of the MQTT message">>]},
        {<<"qos">>, [1, <<"the QoS of the MQTT message">>]},
        {<<"payload">>, [<<"{\"msg\": \"hello\"}">>, <<"the payload of the MQTT message">>]}
    ];
test_columns('client.connected') ->
    [
        {<<"clientid">>, [<<"c_emqx">>, <<"the clientid if the client">>]},
        {<<"username">>, [<<"u_emqx">>, <<"the username if the client">>]},
        {<<"peername">>, [<<"127.0.0.1:52918">>, <<"the IP address and port of the client">>]}
    ];
test_columns('client.disconnected') ->
    [
        {<<"clientid">>, [<<"c_emqx">>, <<"the clientid if the client">>]},
        {<<"username">>, [<<"u_emqx">>, <<"the username if the client">>]},
        {<<"reason">>, [<<"normal">>, <<"the reason for shutdown">>]}
    ];
test_columns('client.connack') ->
    [
        {<<"clientid">>, [<<"c_emqx">>, <<"the clientid if the client">>]},
        {<<"username">>, [<<"u_emqx">>, <<"the username if the client">>]},
        {<<"reason_code">>, [<<"success">>, <<"the reason code">>]}
    ];
test_columns('client.check_authz_complete') ->
    [
        {<<"clientid">>, [<<"c_emqx">>, <<"the clientid if the client">>]},
        {<<"username">>, [<<"u_emqx">>, <<"the username if the client">>]},
        {<<"topic">>, [<<"t/1">>, <<"the topic of the MQTT message">>]},
        {<<"action">>, [<<"publish">>, <<"the action of publish or subscribe">>]},
        {<<"result">>, [<<"allow">>, <<"the authz check complete result">>]}
    ];
test_columns('client.check_authn_complete') ->
    [
        {<<"clientid">>, [<<"c_emqx">>, <<"the clientid if the client">>]},
        {<<"username">>, [<<"u_emqx">>, <<"the username if the client">>]},
        {<<"reason_code">>, [<<"success">>, <<"the reason code">>]},
        {<<"is_superuser">>, [true, <<"Whether this is a superuser">>]},
        {<<"is_anonymous">>, [false, <<"Whether this is a superuser">>]}
    ];
test_columns('session.unsubscribed') ->
    test_columns('session.subscribed');
test_columns('session.subscribed') ->
    [
        {<<"clientid">>, [<<"c_emqx">>, <<"the clientid if the client">>]},
        {<<"username">>, [<<"u_emqx">>, <<"the username if the client">>]},
        {<<"topic">>, [<<"t/a">>, <<"the topic of the MQTT message">>]},
        {<<"qos">>, [1, <<"the QoS of the MQTT message">>]}
    ];
test_columns(<<"$bridges/mqtt", _/binary>>) ->
    [
        {<<"topic">>, [<<"t/a">>, <<"the topic of the MQTT message">>]},
        {<<"qos">>, [1, <<"the QoS of the MQTT message">>]},
        {<<"payload">>, [<<"{\"msg\": \"hello\"}">>, <<"the payload of the MQTT message">>]}
    ];
test_columns(Event) ->
    ee_test_columns(Event).

-if(?EMQX_RELEASE_EDITION == ee).
ee_test_columns('schema.validation_failed') ->
    [{<<"validation">>, <<"myvalidation">>}] ++
        test_columns('message.publish');
ee_test_columns('message.transformation_failed') ->
    [{<<"transformation">>, <<"mytransformation">>}] ++
        test_columns('message.publish').
%% ELSE (?EMQX_RELEASE_EDITION == ee).
-else.
-spec ee_test_columns(_) -> no_return().
ee_test_columns(Event) ->
    error({unknown_event, Event}).
%% END (?EMQX_RELEASE_EDITION == ee).
-endif.

columns_with_exam('message.publish') ->
    [
        {<<"event">>, 'message.publish'},
        {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())},
        {<<"clientid">>, <<"c_emqx">>},
        {<<"username">>, <<"u_emqx">>},
        {<<"payload">>, <<"{\"msg\": \"hello\"}">>},
        {<<"peerhost">>, <<"192.168.0.10">>},
        {<<"peername">>, <<"192.168.0.10:32781">>},
        {<<"topic">>, <<"t/a">>},
        {<<"qos">>, 1},
        {<<"flags">>, #{}},
        {<<"publish_received_at">>, erlang:system_time(millisecond)},
        columns_example_props(pub_props),
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()},
        columns_example_client_attrs()
    ];
columns_with_exam('message.delivered') ->
    columns_message_ack_delivered('message.delivered');
columns_with_exam('message.acked') ->
    [columns_example_props(puback_props)] ++
        columns_message_ack_delivered('message.acked');
columns_with_exam('message.dropped') ->
    [
        {<<"event">>, 'message.dropped'},
        {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())},
        {<<"reason">>, no_subscribers},
        {<<"clientid">>, <<"c_emqx">>},
        {<<"username">>, <<"u_emqx">>},
        {<<"payload">>, <<"{\"msg\": \"hello\"}">>},
        {<<"peerhost">>, <<"192.168.0.10">>},
        {<<"peername">>, <<"192.168.0.10:32781">>},
        {<<"topic">>, <<"t/a">>},
        {<<"qos">>, 1},
        {<<"flags">>, #{}},
        {<<"publish_received_at">>, erlang:system_time(millisecond)},
        columns_example_props(pub_props),
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()}
    ];
columns_with_exam('schema.validation_failed') ->
    [
        {<<"event">>, 'schema.validation_failed'},
        {<<"validation">>, <<"my_validation">>},
        {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())},
        {<<"clientid">>, <<"c_emqx">>},
        {<<"username">>, <<"u_emqx">>},
        {<<"payload">>, <<"{\"msg\": \"hello\"}">>},
        {<<"peerhost">>, <<"192.168.0.10">>},
        {<<"peername">>, <<"192.168.0.10:32781">>},
        {<<"topic">>, <<"t/a">>},
        {<<"qos">>, 1},
        {<<"flags">>, #{}},
        {<<"publish_received_at">>, erlang:system_time(millisecond)},
        columns_example_props(pub_props),
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()}
    ];
columns_with_exam('message.transformation_failed') ->
    [
        {<<"event">>, 'message.transformation_failed'},
        {<<"validation">>, <<"my_transformation">>},
        {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())},
        {<<"clientid">>, <<"c_emqx">>},
        {<<"username">>, <<"u_emqx">>},
        {<<"payload">>, <<"{\"msg\": \"hello\"}">>},
        {<<"peername">>, <<"192.168.0.10:56431">>},
        {<<"topic">>, <<"t/a">>},
        {<<"qos">>, 1},
        {<<"flags">>, #{}},
        {<<"publish_received_at">>, erlang:system_time(millisecond)},
        columns_example_props(pub_props),
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()}
    ];
columns_with_exam('delivery.dropped') ->
    [
        {<<"event">>, 'delivery.dropped'},
        {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())},
        {<<"reason">>, queue_full},
        {<<"from_clientid">>, <<"c_emqx_1">>},
        {<<"from_username">>, <<"u_emqx_1">>},
        {<<"clientid">>, <<"c_emqx_2">>},
        {<<"username">>, <<"u_emqx_2">>},
        {<<"payload">>, <<"{\"msg\": \"hello\"}">>},
        {<<"peerhost">>, <<"192.168.0.10">>},
        {<<"peername">>, <<"192.168.0.10:32781">>},
        {<<"topic">>, <<"t/a">>},
        {<<"qos">>, 1},
        {<<"flags">>, #{}},
        columns_example_props(pub_props),
        {<<"publish_received_at">>, erlang:system_time(millisecond)},
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()}
    ];
columns_with_exam('client.connected') ->
    [
        {<<"event">>, 'client.connected'},
        {<<"clientid">>, <<"c_emqx">>},
        {<<"username">>, <<"u_emqx">>},
        {<<"mountpoint">>, undefined},
        {<<"peername">>, <<"192.168.0.10:56431">>},
        {<<"sockname">>, <<"0.0.0.0:1883">>},
        {<<"proto_name">>, <<"MQTT">>},
        {<<"proto_ver">>, 5},
        {<<"keepalive">>, 60},
        {<<"clean_start">>, true},
        {<<"expiry_interval">>, 3600},
        {<<"is_bridge">>, false},
        columns_example_props(conn_props),
        {<<"connected_at">>, erlang:system_time(millisecond)},
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()},
        columns_example_client_attrs()
    ];
columns_with_exam('client.disconnected') ->
    [
        {<<"event">>, 'client.disconnected'},
        {<<"reason">>, normal},
        {<<"clientid">>, <<"c_emqx">>},
        {<<"username">>, <<"u_emqx">>},
        {<<"peername">>, <<"192.168.0.10:56431">>},
        {<<"sockname">>, <<"0.0.0.0:1883">>},
        {<<"proto_name">>, <<"MQTT">>},
        {<<"proto_ver">>, 5},
        columns_example_props(disconn_props),
        {<<"disconnected_at">>, erlang:system_time(millisecond)},
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()},
        columns_example_client_attrs()
    ];
columns_with_exam('client.connack') ->
    [
        {<<"event">>, 'client.connected'},
        {<<"reason_code">>, success},
        {<<"clientid">>, <<"c_emqx">>},
        {<<"username">>, <<"u_emqx">>},
        {<<"peername">>, <<"192.168.0.10:56431">>},
        {<<"sockname">>, <<"0.0.0.0:1883">>},
        {<<"proto_name">>, <<"MQTT">>},
        {<<"proto_ver">>, 5},
        {<<"keepalive">>, 60},
        {<<"clean_start">>, true},
        {<<"expiry_interval">>, 3600},
        {<<"connected_at">>, erlang:system_time(millisecond)},
        columns_example_props(conn_props),
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()}
    ];
columns_with_exam('client.check_authz_complete') ->
    [
        {<<"event">>, 'client.check_authz_complete'},
        {<<"clientid">>, <<"c_emqx">>},
        {<<"username">>, <<"u_emqx">>},
        {<<"peerhost">>, <<"192.168.0.10">>},
        {<<"peername">>, <<"192.168.0.10:32781">>},
        {<<"topic">>, <<"t/a">>},
        {<<"action">>, <<"publish">>},
        {<<"authz_source">>, <<"cache">>},
        {<<"result">>, <<"allow">>},
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()},
        columns_example_client_attrs()
    ];
columns_with_exam('client.check_authn_complete') ->
    [
        {<<"event">>, 'client.check_authz_complete'},
        {<<"clientid">>, <<"c_emqx">>},
        {<<"username">>, <<"u_emqx">>},
        {<<"peername">>, <<"192.168.0.10:56431">>},
        {<<"reason_code">>, <<"success">>},
        {<<"is_superuser">>, true},
        {<<"is_anonymous">>, false},
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()},
        columns_example_client_attrs()
    ];
columns_with_exam('session.subscribed') ->
    [columns_example_props(sub_props), columns_example_client_attrs()] ++
        columns_message_sub_unsub('session.subscribed');
columns_with_exam('session.unsubscribed') ->
    [columns_example_props(unsub_props), columns_example_client_attrs()] ++
        columns_message_sub_unsub('session.unsubscribed');
columns_with_exam(<<"$bridges/mqtt", _/binary>> = EventName) ->
    [
        {<<"event">>, EventName},
        {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())},
        {<<"payload">>, <<"{\"msg\": \"hello\"}">>},
        {<<"server">>, <<"192.168.0.10:1883">>},
        {<<"topic">>, <<"t/a">>},
        {<<"qos">>, 1},
        {<<"dup">>, false},
        {<<"retain">>, false},
        columns_example_props(pub_props),
        %% the time that we receiced the message from remote broker
        {<<"message_received_at">>, erlang:system_time(millisecond)},
        %% the time that the rule is triggered
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()}
    ].

columns_message_sub_unsub(EventName) ->
    [
        {<<"event">>, EventName},
        {<<"clientid">>, <<"c_emqx">>},
        {<<"username">>, <<"u_emqx">>},
        {<<"peerhost">>, <<"192.168.0.10">>},
        {<<"peername">>, <<"192.168.0.10:32781">>},
        {<<"topic">>, <<"t/a">>},
        {<<"qos">>, 1},
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()}
    ].

columns_message_ack_delivered(EventName) ->
    [
        {<<"event">>, EventName},
        {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())},
        {<<"from_clientid">>, <<"c_emqx_1">>},
        {<<"from_username">>, <<"u_emqx_1">>},
        {<<"clientid">>, <<"c_emqx_2">>},
        {<<"username">>, <<"u_emqx_2">>},
        {<<"payload">>, <<"{\"msg\": \"hello\"}">>},
        {<<"peerhost">>, <<"192.168.0.10">>},
        {<<"peername">>, <<"192.168.0.10:32781">>},
        {<<"topic">>, <<"t/a">>},
        {<<"qos">>, 1},
        {<<"flags">>, #{}},
        {<<"publish_received_at">>, erlang:system_time(millisecond)},
        columns_example_props(pub_props),
        {<<"timestamp">>, erlang:system_time(millisecond)},
        {<<"node">>, node()}
    ].

columns_example_props(PropType) ->
    Props = columns_example_props_specific(PropType),
    UserProps = #{
        'User-Property' => #{<<"foo">> => <<"bar">>},
        'User-Property-Pairs' => [
            #{key => <<"foo">>}, #{value => <<"bar">>}
        ]
    },
    {PropType, maps:merge(Props, UserProps)}.

columns_example_props_specific(pub_props) ->
    #{
        'Payload-Format-Indicator' => 0,
        'Message-Expiry-Interval' => 30
    };
columns_example_props_specific(puback_props) ->
    #{'Reason-String' => <<"OK">>};
columns_example_props_specific(conn_props) ->
    #{
        'Session-Expiry-Interval' => 7200,
        'Receive-Maximum' => 32
    };
columns_example_props_specific(disconn_props) ->
    #{
        'Session-Expiry-Interval' => 7200,
        'Reason-String' => <<"Redirect to another server">>,
        'Server Reference' => <<"192.168.22.129">>
    };
columns_example_props_specific(sub_props) ->
    #{};
columns_example_props_specific(unsub_props) ->
    #{}.

columns_example_client_attrs() ->
    {<<"client_attrs">>, #{
        <<"test">> => <<"example">>
    }}.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

hook_fun_name(HookPoint) ->
    HookFun = hook_fun(HookPoint),
    {name, HookFunName} = erlang:fun_info(HookFun, name),
    HookFunName.

%% return static function references to help static code checks
hook_fun(?BRIDGE_HOOKPOINT(_)) -> fun ?MODULE:on_bridge_message_received/2;
hook_fun('client.connected') -> fun ?MODULE:on_client_connected/3;
hook_fun('client.disconnected') -> fun ?MODULE:on_client_disconnected/4;
hook_fun('client.connack') -> fun ?MODULE:on_client_connack/4;
hook_fun('client.check_authz_complete') -> fun ?MODULE:on_client_check_authz_complete/6;
hook_fun('client.check_authn_complete') -> fun ?MODULE:on_client_check_authn_complete/3;
hook_fun('session.subscribed') -> fun ?MODULE:on_session_subscribed/4;
hook_fun('session.unsubscribed') -> fun ?MODULE:on_session_unsubscribed/4;
hook_fun('message.delivered') -> fun ?MODULE:on_message_delivered/3;
hook_fun('message.acked') -> fun ?MODULE:on_message_acked/3;
hook_fun('message.dropped') -> fun ?MODULE:on_message_dropped/4;
hook_fun('message.transformation_failed') -> fun ?MODULE:on_message_transformation_failed/3;
hook_fun('schema.validation_failed') -> fun ?MODULE:on_schema_validation_failed/3;
hook_fun('delivery.dropped') -> fun ?MODULE:on_delivery_dropped/4;
hook_fun('message.publish') -> fun ?MODULE:on_message_publish/2;
hook_fun(Event) -> error({invalid_event, Event}).

reason(Reason) when is_atom(Reason) -> Reason;
reason({shutdown, Reason}) when is_atom(Reason) -> Reason;
reason({Error, _}) when is_atom(Error) -> Error;
reason(_) -> internal_error.

force_to_bin(Bin) when is_binary(Bin) ->
    Bin;
force_to_bin(Term) ->
    try
        emqx_utils_conv:bin(Term)
    catch
        _:_ ->
            emqx_utils_conv:bin(lists:flatten(io_lib:format("~p", [Term])))
    end.

ntoa(undefined) ->
    undefined;
ntoa(IpOrIpPort) ->
    iolist_to_binary(emqx_utils:ntoa(IpOrIpPort)).

event_name(?BRIDGE_HOOKPOINT(_) = Bridge) -> Bridge;
event_name(<<"$events/client_connected">>) -> 'client.connected';
event_name(<<"$events/client_disconnected">>) -> 'client.disconnected';
event_name(<<"$events/client_connack">>) -> 'client.connack';
event_name(<<"$events/client_check_authz_complete">>) -> 'client.check_authz_complete';
event_name(<<"$events/client_check_authn_complete">>) -> 'client.check_authn_complete';
event_name(<<"$events/session_subscribed">>) -> 'session.subscribed';
event_name(<<"$events/session_unsubscribed">>) -> 'session.unsubscribed';
event_name(<<"$events/message_delivered">>) -> 'message.delivered';
event_name(<<"$events/message_acked">>) -> 'message.acked';
event_name(<<"$events/message_dropped">>) -> 'message.dropped';
event_name(<<"$events/message_transformation_failed">>) -> 'message.transformation_failed';
event_name(<<"$events/schema_validation_failed">>) -> 'schema.validation_failed';
event_name(<<"$events/delivery_dropped">>) -> 'delivery.dropped';
event_name(_) -> 'message.publish'.

event_topic(?BRIDGE_HOOKPOINT(_) = Bridge) -> Bridge;
event_topic('client.connected') -> <<"$events/client_connected">>;
event_topic('client.disconnected') -> <<"$events/client_disconnected">>;
event_topic('client.connack') -> <<"$events/client_connack">>;
event_topic('client.check_authz_complete') -> <<"$events/client_check_authz_complete">>;
event_topic('client.check_authn_complete') -> <<"$events/client_check_authn_complete">>;
event_topic('session.subscribed') -> <<"$events/session_subscribed">>;
event_topic('session.unsubscribed') -> <<"$events/session_unsubscribed">>;
event_topic('message.delivered') -> <<"$events/message_delivered">>;
event_topic('message.acked') -> <<"$events/message_acked">>;
event_topic('message.dropped') -> <<"$events/message_dropped">>;
event_topic('message.transformation_failed') -> <<"$events/message_transformation_failed">>;
event_topic('schema.validation_failed') -> <<"$events/schema_validation_failed">>;
event_topic('delivery.dropped') -> <<"$events/delivery_dropped">>;
event_topic('message.publish') -> <<"$events/message_publish">>.

printable_maps(undefined) ->
    #{};
printable_maps(Headers) ->
    maps:fold(
        fun
            (K, V0, AccIn) when K =:= peerhost; K =:= peername; K =:= sockname ->
                AccIn#{K => ntoa(V0)};
            ('User-Property', V0, AccIn) when is_list(V0) ->
                AccIn#{
                    %% The 'User-Property' field is for the convenience of querying properties
                    %% using the '.' syntax, e.g. "SELECT 'User-Property'.foo as foo"
                    %% However, this does not allow duplicate property keys. To allow
                    %% duplicate keys, we have to use the 'User-Property-Pairs' field instead.
                    'User-Property' => maps:from_list(V0),
                    'User-Property-Pairs' => [
                        #{
                            key => Key,
                            value => Value
                        }
                     || {Key, Value} <- V0
                    ]
                };
            (_K, V, AccIn) when is_tuple(V) ->
                %% internal headers
                AccIn;
            (K, V, AccIn) ->
                AccIn#{K => V}
        end,
        #{'User-Property' => #{}},
        Headers
    ).

ignore_sys_message(#message{flags = Flags}) ->
    ConfigRootKey = emqx_rule_engine_schema:namespace(),
    maps:get(sys, Flags, false) andalso
        emqx:get_config([ConfigRootKey, ignore_sys_message]).
