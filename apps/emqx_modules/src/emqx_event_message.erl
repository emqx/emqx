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

-module(emqx_event_message).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include("emqx_modules.hrl").

-export([ list/0
        , update/1
        , enable/0
        , disable/0
        , post_config_update/5
        , init_conf_handler/0
        ]).

-export([ on_client_connected/2
        , on_client_disconnected/3
        , on_client_subscribed/3
        , on_client_unsubscribed/3
        , on_message_dropped/3
        , on_message_delivered/2
        , on_message_acked/2
        ]).

-ifdef(TEST).
-export([reason/1]).
-endif.

init_conf_handler() ->
    emqx_conf:add_handler([event_message], ?MODULE).

list() ->
    emqx_conf:get([event_message], #{}).

update(Params) ->
    case emqx_conf:update([event_message],
                          Params,
                          #{rawconf_with_defaults => true, override_to => cluster}) of
        {ok, #{raw_config := NewEventMessage}} ->
            {ok, NewEventMessage};
        {error, Reason} ->
            {error, Reason}
    end.

post_config_update(_KeyPath, _Config, NewConf, _OldConf, _AppEnvs) ->
    disable(),
    enable(maps:to_list(NewConf)).

enable() ->
    enable(maps:to_list(list())).

disable() ->
    foreach_with(fun check_enable/2, fun emqx_hooks:del/2, maps:to_list(list())).

%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

on_client_connected(ClientInfo, ConnInfo) ->
    Payload0 = common_infos(ClientInfo, ConnInfo),
    Payload = Payload0#{
                        keepalive       => maps:get(keepalive, ConnInfo, 0),
                        clean_start     => maps:get(clean_start, ConnInfo, true),
                        expiry_interval => maps:get(expiry_interval, ConnInfo, 0)
                       },
    publish_event_msg(<<"$event/client_connected">>, Payload).

on_client_disconnected(ClientInfo,
                       Reason, ConnInfo = #{disconnected_at := DisconnectedAt}) ->

    Payload0 = common_infos(ClientInfo, ConnInfo),
    Payload = Payload0#{
                        reason => reason(Reason),
                        disconnected_at => DisconnectedAt
                       },
    publish_event_msg(<<"$event/client_disconnected">>, Payload).

on_client_subscribed(_ClientInfo = #{clientid := ClientId,
                                     username := Username},
                     Topic, SubOpts) ->
    Payload = #{clientid => ClientId,
                username => Username,
                topic => Topic,
                subopts => SubOpts,
                ts => erlang:system_time(millisecond)
               },
    publish_event_msg(<<"$event/client_subscribed">>, Payload).

on_client_unsubscribed(_ClientInfo = #{clientid := ClientId,
                                       username := Username},
                       Topic, _SubOpts) ->
    Payload = #{clientid => ClientId,
                username => Username,
                topic => Topic,
                ts => erlang:system_time(millisecond)
               },
    publish_event_msg(<<"$event/client_unsubscribed">>, Payload).

on_message_dropped(Message = #message{from = ClientId}, _, Reason) ->
    case ignore_sys_message(Message) of
        true -> ok;
        false ->
            Payload0 = base_message(Message),
            Payload = Payload0#{
                reason => Reason,
                clientid => ClientId,
                username => emqx_message:get_header(username, Message, undefined),
                peerhost => ntoa(emqx_message:get_header(peerhost, Message, undefined))
            },
            publish_event_msg(<<"$event/message_dropped">>, Payload)
    end,
    {ok, Message}.

on_message_delivered(_ClientInfo = #{
                         peerhost := PeerHost,
                         clientid := ReceiverCId,
                         username := ReceiverUsername},
                     #message{from = ClientId} = Message) ->
    case ignore_sys_message(Message) of
        true -> ok;
        false ->
            Payload0 = base_message(Message),
            Payload = Payload0#{
                from_clientid => ClientId,
                from_username => emqx_message:get_header(username, Message, undefined),
                clientid => ReceiverCId,
                username => ReceiverUsername,
                peerhost => ntoa(PeerHost)
            },
            publish_event_msg(<<"$event/message_delivered">>, Payload)
    end,
    {ok, Message}.

on_message_acked(_ClientInfo = #{
                    peerhost := PeerHost,
                    clientid := ReceiverCId,
                    username := ReceiverUsername},
                 #message{from = ClientId} = Message) ->
    case ignore_sys_message(Message) of
        true -> ok;
        false ->
            Payload0 = base_message(Message),
            Payload = Payload0#{
                from_clientid => ClientId,
                from_username => emqx_message:get_header(username, Message, undefined),
                clientid => ReceiverCId,
                username => ReceiverUsername,
                peerhost => ntoa(PeerHost)
            },
            publish_event_msg(<<"$event/message_acked">>, Payload)
    end,
    {ok, Message}.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------
common_infos(
  _ClientInfo = #{clientid := ClientId,
                  username := Username,
                  peerhost := PeerHost,
                  sockport := SockPort
                 },
  _ConnInfo = #{proto_name := ProtoName,
                proto_ver := ProtoVer,
                connected_at := ConnectedAt
               }) ->
    #{clientid => ClientId,
      username => Username,
      ipaddress => ntoa(PeerHost),
      sockport => SockPort,
      proto_name => ProtoName,
      proto_ver => ProtoVer,
      connected_at => ConnectedAt,
      ts => erlang:system_time(millisecond)
     }.

make_msg(Topic, Payload) ->
    emqx_message:set_flag(
      sys, emqx_message:make(
             ?MODULE, 0, Topic, iolist_to_binary(Payload))).

-compile({inline, [reason/1]}).
reason(Reason) when is_atom(Reason) -> Reason;
reason({shutdown, Reason}) when is_atom(Reason) -> Reason;
reason({Error, _}) when is_atom(Error) -> Error;
reason(_) -> internal_error.

ntoa(undefined) -> undefined;
ntoa({IpAddr, Port}) ->
    iolist_to_binary([inet:ntoa(IpAddr), ":", integer_to_list(Port)]);
ntoa(IpAddr) ->
    iolist_to_binary(inet:ntoa(IpAddr)).

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

base_message(Message) ->
    #message{
        id = Id,
        qos = QoS,
        flags = Flags,
        topic = Topic,
        headers = Headers,
        payload = Payload,
        timestamp = Timestamp} = Message,
    #{
        id => emqx_guid:to_hexstr(Id),
        payload => Payload,
        topic => Topic,
        qos => QoS,
        flags => Flags,
        headers => printable_maps(Headers),
        pub_props => printable_maps(emqx_message:get_header(properties, Message, #{})),
        publish_received_at => Timestamp
    }.

ignore_sys_message(#message{flags = Flags}) ->
    maps:get(sys, Flags, false).

publish_event_msg(Topic, Payload) ->
    _ = emqx_broker:safe_publish(make_msg(Topic, emqx_json:encode(Payload))),
    ok.

enable(List) ->
    foreach_with(fun check_enable/2, fun emqx_hooks:put/2, List).

check_enable(Handler, {client_connected, true}) ->
    Handler('client.connected', {?MODULE, on_client_connected, []});
check_enable(Handler, {client_disconnected, true}) ->
    Handler('client.disconnected', {?MODULE, on_client_disconnected, []});
check_enable(Handler, {client_subscribed, true}) ->
    Handler('session.subscribed', {?MODULE, on_client_subscribed, []});
check_enable(Handler, {client_unsubscribed, true}) ->
    Handler('session.unsubscribed', {?MODULE, on_client_unsubscribed, []});
check_enable(Handler, {message_delivered, true}) ->
    Handler('message.delivered', {?MODULE, on_message_delivered, []});
check_enable(Handler, {message_acked, true}) ->
    Handler('message.acked', {?MODULE, on_message_acked, []});
check_enable(Handler, {message_dropped, true}) ->
    Handler('message.dropped', {?MODULE, on_message_dropped, []});
check_enable(_Handler, {_Topic, _Enable}) ->
    ok.

foreach_with(Fun, With, [H | T]) ->
    Fun(With, H),
    foreach_with(Fun, With, T);
foreach_with(_Fun, _With, []) ->
    ok.
