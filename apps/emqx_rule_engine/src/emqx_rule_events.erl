%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx_libs/include/emqx.hrl").
-include_lib("emqx_libs/include/logger.hrl").

-logger_header("[RuleEvents]").

-export([ load/1
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

load(Env) ->
    [emqx_hooks:add(HookPoint, {?MODULE, hook_fun(HookPoint), [hook_conf(HookPoint, Env)]})
     || HookPoint <- ?SUPPORTED_HOOK],
    ok.

unload(_Env) ->
    [emqx_hooks:del(HookPoint, {?MODULE, hook_fun(HookPoint)})
     || HookPoint <- ?SUPPORTED_HOOK],
    ok.

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
    case emqx_rule_registry:get_rules_for(Topic) of
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

eventmsg_dropped(Message = #message{id = Id, from = ClientId, qos = QoS, flags = Flags, topic = Topic, payload = Payload, timestamp = Timestamp}, Reason) ->
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
          pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
          publish_received_at => Timestamp
        }).

eventmsg_delivered(_ClientInfo = #{
                    peerhost := PeerHost,
                    clientid := ReceiverCId,
                    username := ReceiverUsername
                  }, Message = #message{id = Id, from = ClientId, qos = QoS, flags = Flags, topic = Topic, payload = Payload, timestamp = Timestamp}) ->
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
          pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
          publish_received_at => Timestamp
        }).

eventmsg_acked(_ClientInfo = #{
                    peerhost := PeerHost,
                    clientid := ReceiverCId,
                    username := ReceiverUsername
                  }, Message = #message{id = Id, from = ClientId, qos = QoS, flags = Flags, topic = Topic, payload = Payload, timestamp = Timestamp}) ->
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
            emqx_broker:safe_publish(make_msg(QoS, EventTopic, Payload));
        {error, _Reason} ->
            ?LOG(error, "Failed to encode event msg for ~p, msg: ~p", [EventName, EventMsg])
    end,
    emqx_rule_runtime:apply_rules(emqx_rule_registry:get_rules_for(EventTopic), EventMsg);
may_publish_and_apply(EventName, GenEventMsg, _Env) ->
    EventTopic = event_topic(EventName),
    case emqx_rule_registry:get_rules_for(EventTopic) of
        [] -> ok;
        Rules -> emqx_rule_runtime:apply_rules(Rules, GenEventMsg())
    end.

make_msg(QoS, Topic, Payload) ->
    emqx_message:set_flags(#{sys => true, event => true},
        emqx_message:make(emqx_events, QoS, Topic, iolist_to_binary(Payload))).

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
    iolist_to_binary([inet:ntoa(IpAddr),":",integer_to_list(Port)]);
ntoa(IpAddr) ->
    iolist_to_binary(inet:ntoa(IpAddr)).

event_name(<<"$events/client_connected", _/binary>>) -> 'client.connected';
event_name(<<"$events/client_disconnected", _/binary>>) -> 'client.disconnected';
event_name(<<"$events/session_subscribed", _/binary>>) -> 'session.subscribed';
event_name(<<"$events/session_unsubscribed", _/binary>>) -> 'session.unsubscribed';
event_name(<<"$events/message_delivered", _/binary>>) -> 'message.delivered';
event_name(<<"$events/message_acked", _/binary>>) -> 'message.acked';
event_name(<<"$events/message_dropped", _/binary>>) -> 'message.dropped'.

event_topic('client.connected') -> <<"$events/client_connected">>;
event_topic('client.disconnected') -> <<"$events/client_disconnected">>;
event_topic('session.subscribed') -> <<"$events/session_subscribed">>;
event_topic('session.unsubscribed') -> <<"$events/session_unsubscribed">>;
event_topic('message.delivered') -> <<"$events/message_delivered">>;
event_topic('message.acked') -> <<"$events/message_acked">>;
event_topic('message.dropped') -> <<"$events/message_dropped">>.

printable_maps(undefined) -> #{};
printable_maps(Headers) ->
    maps:fold(
        fun (K, V0, AccIn) when K =:= peerhost; K =:= peername; K =:= sockname ->
                AccIn#{K => ntoa(V0)};
            ('User-Property', V0, AccIn) when is_list(V0) ->
                AccIn#{'User-Property' => maps:from_list(V0)};
            (K, V0, AccIn) -> AccIn#{K => V0}
        end, #{}, Headers).
