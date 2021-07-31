%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-logger_header("[RuleEvents]").

-export([ load/1
        , unload/0
        , unload/1
        , event_name/1
        , eventmsg_publish/1
        ]).

-export([ on_client_connected/3
        , on_client_disconnected/4
        , on_session_subscribed/4
        , on_session_unsubscribed/4
        , on_message_publish/2
        , on_message_dropped/4
        , on_message_delivered/3
        , on_message_acked/3
        ]).

-export([ event_info/0
        , columns/1
        , columns_with_exam/1
        ]).

-define(SUPPORTED_HOOK,
        [ 'client.connected'
        , 'client.disconnected'
        , 'session.subscribed'
        , 'session.unsubscribed'
        , 'message.publish'
        , 'message.delivered'
        , 'message.acked'
        , 'message.dropped'
        ]).

-ifdef(TEST).
-export([ reason/1
        , hook_fun/1
        , printable_maps/1
        ]).
-endif.

load(Topic) ->
    HookPoint = event_name(Topic),
    emqx_hooks:put(HookPoint, {?MODULE, hook_fun(HookPoint), [[]]}).

unload() ->
    lists:foreach(fun(HookPoint) ->
            emqx_hooks:del(HookPoint, {?MODULE, hook_fun(HookPoint)})
        end, ?SUPPORTED_HOOK).

unload(Topic) ->
    HookPoint = event_name(Topic),
    emqx_hooks:del(HookPoint, {?MODULE, hook_fun(HookPoint)}).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------
on_message_publish(Message = #message{topic = Topic}, _Env) ->
    case ignore_sys_message(Message) of
        true ->
            ok;
        false ->
            case emqx_rule_registry:get_rules_for(Topic) of
                [] -> ok;
                Rules -> emqx_rule_runtime:apply_rules(Rules, eventmsg_publish(Message))
            end
    end,
    {ok, Message}.

on_client_connected(ClientInfo, ConnInfo, Env) ->
    apply_event('client.connected',
        fun() -> eventmsg_connected(ClientInfo, ConnInfo) end, Env).

on_client_disconnected(ClientInfo, Reason, ConnInfo, Env) ->
    apply_event('client.disconnected',
        fun() -> eventmsg_disconnected(ClientInfo, ConnInfo, Reason) end, Env).

on_session_subscribed(ClientInfo, Topic, SubOpts, Env) ->
    apply_event('session.subscribed',
        fun() -> eventmsg_sub_or_unsub('session.subscribed', ClientInfo, Topic, SubOpts) end, Env).

on_session_unsubscribed(ClientInfo, Topic, SubOpts, Env) ->
    apply_event('session.unsubscribed',
        fun() -> eventmsg_sub_or_unsub('session.unsubscribed', ClientInfo, Topic, SubOpts) end, Env).

on_message_dropped(Message, _, Reason, Env) ->
    case ignore_sys_message(Message) of
        true -> ok;
        false ->
            apply_event('message.dropped',
                fun() -> eventmsg_dropped(Message, Reason) end, Env)
    end,
    {ok, Message}.

on_message_delivered(ClientInfo, Message, Env) ->
    case ignore_sys_message(Message) of
        true -> ok;
        false ->
            apply_event('message.delivered',
                fun() -> eventmsg_delivered(ClientInfo, Message) end, Env)
    end,
    {ok, Message}.

on_message_acked(ClientInfo, Message, Env) ->
    case ignore_sys_message(Message) of
        true -> ok;
        false ->
            apply_event('message.acked',
                fun() -> eventmsg_acked(ClientInfo, Message) end, Env)
    end,
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
                   _ConnInfo = #{
                    peername := PeerName,
                    sockname := SockName,
                    clean_start := CleanStart,
                    proto_name := ProtoName,
                    proto_ver := ProtoVer,
                    keepalive := Keepalive,
                    connected_at := ConnectedAt,
                    conn_props := ConnProps,
                    receive_maximum := RcvMax,
                    expiry_interval := ExpiryInterval
                   }) ->
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
          expiry_interval => ExpiryInterval div 1000,
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
                        disconnected_at := DisconnectedAt
                      }, Reason) ->
    with_basic_columns('client.disconnected',
        #{reason => reason(Reason),
          clientid => ClientId,
          username => Username,
          peername => ntoa(PeerName),
          sockname => ntoa(SockName),
          disconn_props => printable_maps(maps:get(disconn_props, ConnInfo, #{})),
          disconnected_at => DisconnectedAt
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
%% rules applying
%%--------------------------------------------------------------------
apply_event(EventName, GenEventMsg, _Env) ->
    EventTopic = event_topic(EventName),
    case emqx_rule_registry:get_rules_for(EventTopic) of
        [] -> ok;
        Rules -> emqx_rule_runtime:apply_rules(Rules, GenEventMsg())
    end.

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
    , event_info_client_connected()
    , event_info_client_disconnected()
    , event_info_session_subscribed()
    , event_info_session_unsubscribed()
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
        {<<"message dropped">>, <<"消息丢弃"/utf8>>},
        {<<"message dropped">>, <<"消息丢弃"/utf8>>},
        <<"SELECT * FROM \"$events/message_dropped\" WHERE topic =~ 't/#'">>
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

event_info_common(Event, {TitleEN, TitleZH}, {DescrEN, DescrZH}, SqlExam) ->
    #{event => event_topic(Event),
      title => #{en => TitleEN, zh => TitleZH},
      description => #{en => DescrEN, zh => DescrZH},
      test_columns => test_columns(Event),
      columns => columns(Event),
      sql_example => SqlExam
    }.

test_columns('message.dropped') ->
    test_columns('message.publish');
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
test_columns('session.unsubscribed') ->
    test_columns('session.subscribed');
test_columns('session.subscribed') ->
    [ {<<"clientid">>, <<"c_emqx">>}
    , {<<"username">>, <<"u_emqx">>}
    , {<<"topic">>, <<"t/a">>}
    , {<<"qos">>, 1}
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
    , {<<"headers">>, undefined}
    , {<<"publish_received_at">>, erlang:system_time(millisecond)}
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
    , {<<"disconnected_at">>, erlang:system_time(millisecond)}
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
    , {<<"timestamp">>, erlang:system_time(millisecond)}
    , {<<"node">>, node()}
    ].

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

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
event_name(<<"$events/session_subscribed", _/binary>>) -> 'session.subscribed';
event_name(<<"$events/session_unsubscribed", _/binary>>) ->
    'session.unsubscribed';
event_name(<<"$events/message_delivered", _/binary>>) -> 'message.delivered';
event_name(<<"$events/message_acked", _/binary>>) -> 'message.acked';
event_name(<<"$events/message_dropped", _/binary>>) -> 'message.dropped';
event_name(_) -> 'message.publish'.

event_topic('client.connected') -> <<"$events/client_connected">>;
event_topic('client.disconnected') -> <<"$events/client_disconnected">>;
event_topic('session.subscribed') -> <<"$events/session_subscribed">>;
event_topic('session.unsubscribed') -> <<"$events/session_unsubscribed">>;
event_topic('message.delivered') -> <<"$events/message_delivered">>;
event_topic('message.acked') -> <<"$events/message_acked">>;
event_topic('message.dropped') -> <<"$events/message_dropped">>;
event_topic('message.publish') -> <<"$events/message_publish">>.

printable_maps(undefined) -> #{};
printable_maps(Headers) ->
    maps:fold(
        fun (K, V0, AccIn) when K =:= peerhost; K =:= peername; K =:= sockname ->
                AccIn#{K => ntoa(V0)};
            ('User-Property', V0, AccIn) when is_list(V0) ->
                AccIn#{
                    'User-Property' => maps:from_list(V0),
                    'User-Property-Pairs' => [#{
                        key => Key,
                        value => Value
                     } || {Key, Value} <- V0]
                };
            (K, V0, AccIn) -> AccIn#{K => V0}
        end, #{}, Headers).

ignore_sys_message(#message{flags = Flags}) ->
    maps:get(sys, Flags, false) andalso
      emqx_config:get([emqx_rule_engine, ignore_sys_message]).
