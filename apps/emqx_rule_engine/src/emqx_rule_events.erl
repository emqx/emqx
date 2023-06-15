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

-module(emqx_rule_events).

-logger_header("[RuleEvents]").

-include("rule_engine.hrl").
-include_lib("emqx/include/emqx.hrl").

-export([ load/1
        , unload/0
        , unload/1
        , event_name/1
        , eventmsg_publish/1
        ]).

-export([ on_client_connected/3
        , on_client_disconnected/4
        , on_client_connack/4
        , on_session_subscribed/4
        , on_session_unsubscribed/4
        , on_message_publish/2
        , on_message_dropped/4
        , on_message_delivered/3
        , on_message_acked/3
        , on_delivery_dropped/4
        , on_client_check_acl_complete/6
        ]).

-export([ event_info/0
        , columns/1
        , columns_with_exam/1
        ]).

-define(SUPPORTED_HOOK,
        [ 'client.connected'
        , 'client.disconnected'
        , 'client.connack'
        , 'session.subscribed'
        , 'session.unsubscribed'
        , 'message.publish'
        , 'message.delivered'
        , 'message.acked'
        , 'message.dropped'
        , 'delivery.dropped'
        , 'client.check_acl_complete'
        ]).

-ifdef(TEST).
-export([ reason/1
        , hook_fun/1
        , printable_maps/1
        ]).
-endif.

load(Topic) ->
    HookPoint = event_name(Topic),
    emqx_hooks:put(HookPoint, {?MODULE, hook_fun(HookPoint),
        [hook_conf(HookPoint, env())]}).

unload() ->
    lists:foreach(fun(HookPoint) ->
            emqx_hooks:del(HookPoint, {?MODULE, hook_fun(HookPoint)})
        end, ?SUPPORTED_HOOK).

unload(Topic) ->
    HookPoint = event_name(Topic),
    emqx_hooks:del(HookPoint, {?MODULE, hook_fun(HookPoint)}).

env() ->
    application:get_all_env(?APP).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

on_message_publish(Message = #message{flags = #{event := true}},
                   _Env) ->
    {ok, Message};
on_message_publish(Message = #message{flags = #{sys := true}},
                   #{ignore_sys_message := true}) ->
    {ok, Message};
on_message_publish(Message = #message{topic = Topic}, _Env) ->
    case emqx_rule_registry:get_active_rules_for(Topic) of
        [] -> ok;
        Rules -> emqx_rule_runtime:apply_rules(Rules, eventmsg_publish(Message))
    end,
    {ok, Message}.

on_client_connected(ClientInfo, ConnInfo, Env) ->
    may_publish_and_apply('client.connected',
        fun() -> eventmsg_connected(ClientInfo, ConnInfo) end, Env).

on_client_disconnected(ClientInfo, Reason, ConnInfo, Env) ->
    may_publish_and_apply('client.disconnected',
        fun() -> eventmsg_disconnected(ClientInfo, ConnInfo, Reason) end, Env).

on_client_connack(ConnInfo, Reason, _, Env) ->
    may_publish_and_apply('client.connack',
                          fun() -> eventmsg_connack(ConnInfo, Reason) end, Env).

on_client_check_acl_complete(ClientInfo, PubSub, Topic, Result, IsCache, Env) ->
    may_publish_and_apply('client.check_acl_complete',
                          fun() -> eventmsg_check_acl_complete(ClientInfo,
                                                               PubSub,
                                                               Topic,
                                                               Result,
                                                               IsCache) end, Env).

on_session_subscribed(ClientInfo, Topic, SubOpts, Env) ->
    may_publish_and_apply('session.subscribed',
        fun() -> eventmsg_sub_or_unsub('session.subscribed', ClientInfo, Topic, SubOpts) end, Env).

on_session_unsubscribed(ClientInfo, Topic, SubOpts, Env) ->
    may_publish_and_apply('session.unsubscribed',
        fun() -> eventmsg_sub_or_unsub('session.unsubscribed', ClientInfo, Topic, SubOpts) end, Env).

on_message_dropped(Message = #message{flags = #{sys := true}},
                   _, _, #{ignore_sys_message := true}) ->
    {ok, Message};
on_message_dropped(Message, _, Reason, Env) ->
    may_publish_and_apply('message.dropped',
        fun() -> eventmsg_dropped(Message, Reason) end, Env),
    {ok, Message}.

on_message_delivered(_ClientInfo, Message = #message{flags = #{sys := true}},
                   #{ignore_sys_message := true}) ->
    {ok, Message};
on_message_delivered(ClientInfo, Message, Env) ->
    may_publish_and_apply('message.delivered',
        fun() -> eventmsg_delivered(ClientInfo, Message) end, Env),
    {ok, Message}.

on_message_acked(_ClientInfo, Message = #message{flags = #{sys := true}},
                   #{ignore_sys_message := true}) ->
    {ok, Message};
on_message_acked(ClientInfo, Message, Env) ->
    may_publish_and_apply('message.acked',
        fun() -> eventmsg_acked(ClientInfo, Message) end, Env),
    {ok, Message}.

on_delivery_dropped(_ClientInfo, Message = #message{flags = #{sys := true}},
                   _Reason, #{ignore_sys_message := true}) ->
    {ok, Message};
on_delivery_dropped(ClientInfo, Message, Reason, Env) ->
    may_publish_and_apply('delivery.dropped',
        fun() -> eventmsg_delivery_dropped(ClientInfo, Message, Reason) end, Env),
    {ok, Message}.

%%--------------------------------------------------------------------
%% Event Messages
%%--------------------------------------------------------------------

eventmsg_publish(Message = #message{id = Id, from = ClientId, qos = QoS, flags = Flags, topic = Topic, headers = Headers, payload = Payload, timestamp = Timestamp}) ->
    with_basic_columns('message.publish',
        #{id => emqx_guid:to_hexstr(Id),
          clientid => ClientId,
          username => emqx_message:get_header(username, Message, undefined),
          payload => Payload,
          peerhost => ntoa(emqx_message:get_header(peerhost, Message, undefined)),
          topic => Topic,
          qos => QoS,
          flags => Flags,
          pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
          %% the column 'headers' will be removed in the next major release
          headers => printable_maps(Headers),
          publish_received_at => Timestamp
        }).

eventmsg_connected(_ClientInfo = #{
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
                    connected_at := ConnectedAt,
                    receive_maximum := RcvMax
                   }) ->
    Keepalive = maps:get(keepalive, ConnInfo, 0),
    ConnProps = maps:get(conn_props, ConnInfo, #{}),
    ExpiryInterval = maps:get(expiry_interval, ConnInfo, 0),
    with_basic_columns('client.connected',
        #{clientid => ClientId,
          username => Username,
          mountpoint => Mountpoint,
          peername => ntoa(PeerName),
          sockname => ntoa(SockName),
          proto_name => ProtoName,
          proto_ver => ProtoVer,
          keepalive => Keepalive,
          clean_start => CleanStart,
          receive_maximum => RcvMax,
          expiry_interval => ExpiryInterval,
          is_bridge => IsBridge,
          conn_props => printable_maps(ConnProps),
          connected_at => ConnectedAt
        }).

eventmsg_disconnected(_ClientInfo = #{
                       clientid := ClientId,
                       username := Username
                      },
                      ConnInfo = #{
                        peername := PeerName,
                        sockname := SockName,
                        proto_name := ProtoName,
                        proto_ver := ProtoVer,
                        disconnected_at := DisconnectedAt
                      }, Reason) ->
    with_basic_columns('client.disconnected',
        #{reason => reason(Reason),
          clientid => ClientId,
          username => Username,
          peername => ntoa(PeerName),
          sockname => ntoa(SockName),
          proto_name => ProtoName,
          proto_ver => ProtoVer,
          disconn_props => printable_maps(maps:get(disconn_props, ConnInfo, #{})),
          disconnected_at => DisconnectedAt
        }).

eventmsg_connack(ConnInfo = #{
                    clientid := ClientId,
                    clean_start := CleanStart,
                    username := Username,
                    peername := PeerName,
                    sockname := SockName,
                    proto_name := ProtoName,
                    proto_ver := ProtoVer
                   }, Reason) ->
    Keepalive = maps:get(keepalive, ConnInfo, 0),
    ConnProps = maps:get(conn_props, ConnInfo, #{}),
    ExpiryInterval = maps:get(expiry_interval, ConnInfo, 0),
    with_basic_columns('client.connack',
        #{reason_code => reason(Reason),
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
        }).
eventmsg_check_acl_complete(_ClientInfo = #{
                                            clientid := ClientId,
                                            username := Username,
                                            peerhost := PeerHost
                                           }, PubSub, Topic, Result, IsCache) ->
    with_basic_columns('client.check_acl_complete',
                       #{clientid => ClientId,
                         username => Username,
                         peerhost => ntoa(PeerHost),
                         topic    => Topic,
                         action   => PubSub,
                         is_cache => IsCache,
                         result   => Result
                        }).

eventmsg_sub_or_unsub(Event, _ClientInfo = #{
                    clientid := ClientId,
                    username := Username,
                    peerhost := PeerHost
                   }, Topic, SubOpts = #{qos := QoS}) ->
    PropKey = sub_unsub_prop_key(Event),
    with_basic_columns(Event,
        #{clientid => ClientId,
          username => Username,
          peerhost => ntoa(PeerHost),
          PropKey => printable_maps(maps:get(PropKey, SubOpts, #{})),
          topic => Topic,
          qos => QoS
        }).

eventmsg_dropped(Message = #message{id = Id, from = ClientId, qos = QoS, flags = Flags, topic = Topic, headers = Headers, payload = Payload, timestamp = Timestamp}, Reason) ->
    with_basic_columns('message.dropped',
        #{id => emqx_guid:to_hexstr(Id),
          reason => Reason,
          clientid => ClientId,
          username => emqx_message:get_header(username, Message, undefined),
          payload => Payload,
          peerhost => ntoa(emqx_message:get_header(peerhost, Message, undefined)),
          topic => Topic,
          qos => QoS,
          flags => Flags,
          %% the column 'headers' will be removed in the next major release
          headers => printable_maps(Headers),
          pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
          publish_received_at => Timestamp
        }).

eventmsg_delivery_dropped(_ClientInfo = #{
            peerhost := PeerHost,
            clientid := ReceiverCId,
            username := ReceiverUsername
        },
        Message = #message{id = Id, from = ClientId, qos = QoS, flags = Flags, topic = Topic,
            headers = Headers, payload = Payload, timestamp = Timestamp},
        Reason) ->
    with_basic_columns('delivery.dropped',
        #{id => emqx_guid:to_hexstr(Id),
          reason => Reason,
          from_clientid => ClientId,
          from_username => emqx_message:get_header(username, Message, undefined),
          clientid => ReceiverCId,
          username => ReceiverUsername,
          payload => Payload,
          peerhost => ntoa(PeerHost),
          topic => Topic,
          qos => QoS,
          flags => Flags,
          %% the column 'headers' will be removed in the next major release
          headers => printable_maps(Headers),
          pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
          publish_received_at => Timestamp
        }).

eventmsg_delivered(_ClientInfo = #{
                    peerhost := PeerHost,
                    clientid := ReceiverCId,
                    username := ReceiverUsername
                  }, Message = #message{id = Id, from = ClientId, qos = QoS, flags = Flags, topic = Topic, headers = Headers, payload = Payload, timestamp = Timestamp}) ->
    with_basic_columns('message.delivered',
        #{id => emqx_guid:to_hexstr(Id),
          from_clientid => ClientId,
          from_username => emqx_message:get_header(username, Message, undefined),
          clientid => ReceiverCId,
          username => ReceiverUsername,
          payload => Payload,
          peerhost => ntoa(PeerHost),
          topic => Topic,
          qos => QoS,
          flags => Flags,
          %% the column 'headers' will be removed in the next major release
          headers => printable_maps(Headers),
          pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
          publish_received_at => Timestamp
        }).

eventmsg_acked(_ClientInfo = #{
                    peerhost := PeerHost,
                    clientid := ReceiverCId,
                    username := ReceiverUsername
                  }, Message = #message{id = Id, from = ClientId, qos = QoS, flags = Flags, topic = Topic, headers = Headers, payload = Payload, timestamp = Timestamp}) ->
    with_basic_columns('message.acked',
        #{id => emqx_guid:to_hexstr(Id),
          from_clientid => ClientId,
          from_username => emqx_message:get_header(username, Message, undefined),
          clientid => ReceiverCId,
          username => ReceiverUsername,
          payload => Payload,
          peerhost => ntoa(PeerHost),
          topic => Topic,
          qos => QoS,
          flags => Flags,
          %% the column 'headers' will be removed in the next major release
          headers => printable_maps(Headers),
          pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
          puback_props => printable_maps(emqx_message:get_header(puback_props, Message, #{})),
          publish_received_at => Timestamp
        }).

sub_unsub_prop_key('session.subscribed') -> sub_props;
sub_unsub_prop_key('session.unsubscribed') -> unsub_props.

with_basic_columns(EventName, Data) when is_map(Data) ->
    Data#{event => EventName,
          timestamp => erlang:system_time(millisecond),
          node => node()
        }.

%%--------------------------------------------------------------------
%% Events publishing and rules applying
%%--------------------------------------------------------------------

may_publish_and_apply(EventName, GenEventMsg, #{enabled := true, qos := QoS}) ->
    EventTopic = event_topic(EventName),
    EventMsg = GenEventMsg(),
    case emqx_json:safe_encode(EventMsg) of
        {ok, Payload} ->
            _ = emqx_broker:safe_publish(make_msg(QoS, EventTopic, Payload)),
            ok;
        {error, _Reason} ->
            ?LOG(error, "Failed to encode event msg for ~p, msg: ~p", [EventName, EventMsg])
    end,
    emqx_rule_runtime:apply_rules(emqx_rule_registry:get_active_rules_for(EventTopic), EventMsg);
may_publish_and_apply(EventName, GenEventMsg, _Env) ->
    EventTopic = event_topic(EventName),
    case emqx_rule_registry:get_active_rules_for(EventTopic) of
        [] -> ok;
        Rules -> emqx_rule_runtime:apply_rules(Rules, GenEventMsg())
    end.

make_msg(QoS, Topic, Payload) ->
    emqx_message:set_flags(#{sys => true, event => true},
        emqx_message:make(emqx_events, QoS, Topic, iolist_to_binary(Payload))).

%%--------------------------------------------------------------------
%% Columns
%%--------------------------------------------------------------------
columns(Event) ->
    [Key || {Key, _ExampleVal} <- columns_with_exam(Event)].

event_info() ->
    [ event_info_message_publish()
    , event_info_message_deliver()
    , event_info_message_acked()
    , event_info_message_dropped()
    , event_info_delivery_dropped()
    , event_info_client_connected()
    , event_info_client_disconnected()
    , event_info_client_connack()
    , event_info_session_subscribed()
    , event_info_session_unsubscribed()
    , event_info_client_check_acl_complete()
    ].

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
        {<<"messages are discarded during forwarding, usually because there are no subscribers">>,
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
event_info_client_check_acl_complete() ->
    event_info_common(
      'client.check_acl_complete',
      {<<"client check acl complete">>, <<"鉴权结果"/utf8>>},
      {<<"client check acl complete">>, <<"鉴权结果"/utf8>>},
      <<"SELECT * FROM \"$events/client_check_acl_complete\"">>
     ).

event_info_common(Event, {TitleEN, TitleZH}, {DescrEN, DescrZH}, SqlExam) ->
    #{event => event_topic(Event),
      title => #{en => TitleEN, zh => TitleZH},
      description => #{en => DescrEN, zh => DescrZH},
      test_columns => test_columns(Event),
      columns => columns(Event),
      sql_example => SqlExam
    }.

test_columns('message.dropped') ->
    [ {<<"reason">>, <<"no_subscribers">>}
    ] ++ test_columns('message.publish');
test_columns('message.publish') ->
    [ {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"topic">>, <<"t/a">>}
    , {<<"qos">>, 1}
    , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
    ];
test_columns('message.acked') ->
    test_columns('message.delivered');
test_columns('message.delivered') ->
    [ {<<"from_clientid">>, <<"c_emqx_1">>}
    , {<<"from_username">>, <<"u_emqx_1">>}
    , {<<"clientid">>, <<"c_emqx_2">>}
    , {<<"username">>, <<"u_emqx_2">>}
    , {<<"topic">>, <<"t/a">>}
    , {<<"qos">>, 1}
    , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
    ];
test_columns('delivery.dropped') ->
    [ {<<"reason">>, <<"queue_full">>}
    ] ++ test_columns('message.delivered');
test_columns('client.connected') ->
    [ {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"peername">>, <<"127.0.0.1:52918">>}
    ];
test_columns('client.disconnected') ->
    [ {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"reason">>, <<"normal">>}
    ];
test_columns('client.connack') ->
    [ {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"reason_code">>, <<"sucess">>}
    ];
test_columns('session.unsubscribed') ->
    test_columns('session.subscribed');
test_columns('session.subscribed') ->
    [ {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"topic">>, <<"t/a">>}
    , {<<"qos">>, 1}
    ];
test_columns('client.check_acl_complete') ->
    [ {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"topic">>, <<"t/1">>}
    , {<<"action">>, <<"publish">>}
    , {<<"result">>, <<"allow">>}
    ].

columns_with_exam('message.publish') ->
    [ {<<"event">>, 'message.publish'}
    , {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())}
    , {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
    , {<<"peerhost">>, <<"192.168.0.10">>}
    , {<<"topic">>, <<"t/a">>}
    , {<<"qos">>, 1}
    , {<<"flags">>, #{}}
    , {<<"publish_received_at">>, erlang:system_time(millisecond)}
    , columns_example_props(pub_props)
    , {<<"timestamp">>, erlang:system_time(millisecond)}
    , {<<"node">>, node()}
    ];
columns_with_exam('message.delivered') ->
    [ {<<"event">>, 'message.delivered'}
    , {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())}
    , {<<"from_clientid">>, <<"c_emqx_1">>}
    , {<<"from_username">>, <<"u_emqx_1">>}
    , {<<"clientid">>, <<"c_emqx_2">>}
    , {<<"username">>, <<"u_emqx_2">>}
    , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
    , {<<"peerhost">>, <<"192.168.0.10">>}
    , {<<"topic">>, <<"t/a">>}
    , {<<"qos">>, 1}
    , {<<"flags">>, #{}}
    , {<<"publish_received_at">>, erlang:system_time(millisecond)}
    , columns_example_props(pub_props)
    , {<<"timestamp">>, erlang:system_time(millisecond)}
    , {<<"node">>, node()}
    ];
columns_with_exam('message.acked') ->
    [ {<<"event">>, 'message.acked'}
    , {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())}
    , {<<"from_clientid">>, <<"c_emqx_1">>}
    , {<<"from_username">>, <<"u_emqx_1">>}
    , {<<"clientid">>, <<"c_emqx_2">>}
    , {<<"username">>, <<"u_emqx_2">>}
    , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
    , {<<"peerhost">>, <<"192.168.0.10">>}
    , {<<"topic">>, <<"t/a">>}
    , {<<"qos">>, 1}
    , {<<"flags">>, #{}}
    , {<<"publish_received_at">>, erlang:system_time(millisecond)}
    , columns_example_props(pub_props)
    , columns_example_props(puback_props)
    , {<<"timestamp">>, erlang:system_time(millisecond)}
    , {<<"node">>, node()}
    ];
columns_with_exam('message.dropped') ->
    [ {<<"event">>, 'message.dropped'}
    , {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())}
    , {<<"reason">>, no_subscribers}
    , {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
    , {<<"peerhost">>, <<"192.168.0.10">>}
    , {<<"topic">>, <<"t/a">>}
    , {<<"qos">>, 1}
    , {<<"flags">>, #{}}
    , {<<"publish_received_at">>, erlang:system_time(millisecond)}
    , columns_example_props(pub_props)
    , {<<"timestamp">>, erlang:system_time(millisecond)}
    , {<<"node">>, node()}
    ];
columns_with_exam('delivery.dropped') ->
    [ {<<"event">>, 'delivery.dropped'}
    , {<<"id">>, emqx_guid:to_hexstr(emqx_guid:gen())}
    , {<<"reason">>, queue_full}
    , {<<"from_clientid">>, <<"c_emqx_1">>}
    , {<<"from_username">>, <<"u_emqx_1">>}
    , {<<"clientid">>, <<"c_emqx_2">>}
    , {<<"username">>, <<"u_emqx_2">>}
    , {<<"payload">>, <<"{\"msg\": \"hello\"}">>}
    , {<<"peerhost">>, <<"192.168.0.10">>}
    , {<<"topic">>, <<"t/a">>}
    , {<<"qos">>, 1}
    , {<<"flags">>, #{}}
    , {<<"publish_received_at">>, erlang:system_time(millisecond)}
    , {<<"timestamp">>, erlang:system_time(millisecond)}
    , {<<"node">>, node()}
    ];
columns_with_exam('client.connected') ->
    [ {<<"event">>, 'client.connected'}
    , {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"mountpoint">>, undefined}
    , {<<"peername">>, <<"192.168.0.10:56431">>}
    , {<<"sockname">>, <<"0.0.0.0:1883">>}
    , {<<"proto_name">>, <<"MQTT">>}
    , {<<"proto_ver">>, 5}
    , {<<"keepalive">>, 60}
    , {<<"clean_start">>, true}
    , {<<"expiry_interval">>, 3600}
    , {<<"is_bridge">>, false}
    , {<<"connected_at">>, erlang:system_time(millisecond)}
    , columns_example_props(conn_props)
    , {<<"timestamp">>, erlang:system_time(millisecond)}
    , {<<"node">>, node()}
    ];
columns_with_exam('client.disconnected') ->
    [ {<<"event">>, 'client.disconnected'}
    , {<<"reason">>, normal}
    , {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"peername">>, <<"192.168.0.10:56431">>}
    , {<<"sockname">>, <<"0.0.0.0:1883">>}
    , {<<"proto_name">>, <<"MQTT">>}
    , {<<"proto_ver">>, 5}
    , {<<"disconnected_at">>, erlang:system_time(millisecond)}
    , columns_example_props(disconn_props)
    , {<<"timestamp">>, erlang:system_time(millisecond)}
    , {<<"node">>, node()}
    ];
columns_with_exam('client.connack') ->
    [ {<<"event">>, 'client.connected'}
    , {<<"reason_code">>, success}
    , {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"peername">>, <<"192.168.0.10:56431">>}
    , {<<"sockname">>, <<"0.0.0.0:1883">>}
    , {<<"proto_name">>, <<"MQTT">>}
    , {<<"proto_ver">>, 5}
    , {<<"keepalive">>, 60}
    , {<<"clean_start">>, true}
    , {<<"expiry_interval">>, 3600}
    , {<<"connected_at">>, erlang:system_time(millisecond)}
    , columns_example_props(conn_props)
    , {<<"timestamp">>, erlang:system_time(millisecond)}
    , {<<"node">>, node()}
    ];
columns_with_exam('session.subscribed') ->
    [ {<<"event">>, 'session.subscribed'}
    , {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"peerhost">>, <<"192.168.0.10">>}
    , {<<"topic">>, <<"t/a">>}
    , {<<"qos">>, 1}
    , columns_example_props(sub_props)
    , {<<"timestamp">>, erlang:system_time(millisecond)}
    , {<<"node">>, node()}
    ];
columns_with_exam('session.unsubscribed') ->
    [ {<<"event">>, 'session.unsubscribed'}
    , {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"peerhost">>, <<"192.168.0.10">>}
    , {<<"topic">>, <<"t/a">>}
    , {<<"qos">>, 1}
    , columns_example_props(unsub_props)
    , {<<"timestamp">>, erlang:system_time(millisecond)}
    , {<<"node">>, node()}
    ];
columns_with_exam('client.check_acl_complete') ->
    [ {<<"event">>, 'client.check_acl_complete'}
    , {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"peerhost">>, <<"192.168.0.10">>}
    , {<<"topic">>, <<"t/a">>}
    , {<<"action">>, <<"publish">>}
    , {<<"is_cache">>, <<"false">>}
    , {<<"result">>, <<"allow">>}
    , {<<"timestamp">>, erlang:system_time(millisecond)}
    , {<<"node">>, node()}
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
    #{ 'Payload-Format-Indicator' => 0
     , 'Message-Expiry-Interval' => 30
     };
columns_example_props_specific(puback_props) ->
    #{ 'Reason-String' => <<"OK">>
     };
columns_example_props_specific(conn_props) ->
    #{ 'Session-Expiry-Interval' => 7200
     , 'Receive-Maximum' => 32
     };
columns_example_props_specific(disconn_props) ->
    #{ 'Session-Expiry-Interval' => 7200
     , 'Reason-String' => <<"Redirect to another server">>
     , 'Server Reference' => <<"192.168.22.129">>
     };
columns_example_props_specific(sub_props) ->
    #{};
columns_example_props_specific(unsub_props) ->
    #{}.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

hook_conf(HookPoint, Env) ->
    Events = proplists:get_value(events, Env, []),
    IgnoreSys = proplists:get_value(ignore_sys_message, Env, true),
    case lists:keyfind(HookPoint, 1, Events) of
        {_, on, QoS} -> #{enabled => true, qos => QoS, ignore_sys_message => IgnoreSys};
        _ -> #{enabled => false, qos => 1, ignore_sys_message => IgnoreSys}
    end.

hook_fun(Event) ->
    case string:split(atom_to_list(Event), ".") of
        [Prefix, Name] ->
            list_to_atom(lists:append(["on_", Prefix, "_", Name]));
        [_] ->
            error(invalid_event, Event)
    end.

reason(Reason) when is_atom(Reason) -> Reason;
reason({shutdown, Reason}) when is_atom(Reason) -> Reason;
reason({Error, _}) when is_atom(Error) -> Error;
reason(_) -> internal_error.

ntoa(undefined) -> undefined;
ntoa({IpAddr, Port}) ->
    iolist_to_binary([inet:ntoa(IpAddr), ":", integer_to_list(Port)]);
ntoa(IpAddr) ->
    iolist_to_binary(inet:ntoa(IpAddr)).

event_name(<<"$events/client_connected", _/binary>>) -> 'client.connected';
event_name(<<"$events/client_disconnected", _/binary>>) -> 'client.disconnected';
event_name(<<"$events/client_connack", _/binary>>) -> 'client.connack';
event_name(<<"$events/session_subscribed", _/binary>>) -> 'session.subscribed';
event_name(<<"$events/session_unsubscribed", _/binary>>) ->
    'session.unsubscribed';
event_name(<<"$events/message_delivered", _/binary>>) -> 'message.delivered';
event_name(<<"$events/message_acked", _/binary>>) -> 'message.acked';
event_name(<<"$events/message_dropped", _/binary>>) -> 'message.dropped';
event_name(<<"$events/delivery_dropped", _/binary>>) -> 'delivery.dropped';
event_name(<<"$events/client_check_acl_complete", _/binary>>) -> 'client.check_acl_complete';
event_name(_) -> 'message.publish'.

event_topic('client.connected') -> <<"$events/client_connected">>;
event_topic('client.disconnected') -> <<"$events/client_disconnected">>;
event_topic('client.connack') -> <<"$events/client_connack">>;
event_topic('session.subscribed') -> <<"$events/session_subscribed">>;
event_topic('session.unsubscribed') -> <<"$events/session_unsubscribed">>;
event_topic('message.delivered') -> <<"$events/message_delivered">>;
event_topic('message.acked') -> <<"$events/message_acked">>;
event_topic('message.dropped') -> <<"$events/message_dropped">>;
event_topic('delivery.dropped') -> <<"$events/delivery_dropped">>;
event_topic('message.publish') -> <<"$events/message_publish">>;
event_topic('client.check_acl_complete') -> <<"$events/client_check_acl_complete">>.

printable_maps(undefined) -> #{};
printable_maps(Headers) ->
    maps:fold(
        fun (K, V0, AccIn) when K =:= peerhost; K =:= peername; K =:= sockname ->
                AccIn#{K => ntoa(V0)};
            ('User-Property', V0, AccIn) when is_list(V0) ->
                AccIn#{
                    %% The 'User-Property' field is for the convenience of querying properties
                    %% using the '.' syntax, e.g. "SELECT 'User-Property'.foo as foo"
                    %% However, this does not allow duplicate property keys. To allow
                    %% duplicate keys, we have to use the 'User-Property-Pairs' field instead.
                    'User-Property' => maps:from_list(V0),
                    'User-Property-Pairs' => [#{
                        key => Key,
                        value => Value
                     } || {Key, Value} <- V0]
                };
            (K, V, AccIn) when is_map(V) ->
                AccIn#{K => printable_maps(V)};
            (K, V, AccIn) when is_list(V) ->
                AccIn#{K => printable_list(V)};
            (_K, V, AccIn) when is_tuple(V) ->
                %% internal header, remove it
                AccIn;
            (K, V, AccIn) ->
                AccIn#{K => V}
        end, #{}, Headers).

printable_list(L) ->
    lists:filtermap(fun printable_element/1, L).

printable_element(E) when is_map(E) ->
    {true, printable_maps(E)};
printable_element(E) when is_tuple(E) ->
    false;
printable_element(E) when is_list(E) ->
    {true, printable_list(E)};
printable_element(E) ->
    {true, E}.
