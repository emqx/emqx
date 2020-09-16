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

-module(emqx_exhook_handler).

-include("emqx_exhook.hrl").
-include_lib("emqx_libs/include/emqx.hrl").
-include_lib("emqx_libs/include/logger.hrl").

-logger_header("[ExHook]").

-export([ on_client_connect/2
        , on_client_connack/3
        , on_client_connected/2
        , on_client_disconnected/3
        , on_client_authenticate/2
        , on_client_check_acl/4
        , on_client_subscribe/3
        , on_client_unsubscribe/3
        ]).

%% Session Lifecircle Hooks
-export([ on_session_created/2
        , on_session_subscribed/3
        , on_session_unsubscribed/3
        , on_session_resumed/2
        , on_session_discarded/2
        , on_session_takeovered/2
        , on_session_terminated/3
        ]).

%% Utils
-export([ message/1
        , validator/1
        , assign_to_message/2
        , clientinfo/1
        , stringfy/1
        ]).

-import(emqx_exhook,
        [ cast/2
        , call_fold/4
        ]).

-exhooks([ {'client.connect',      {?MODULE, on_client_connect,       []}}
         , {'client.connack',      {?MODULE, on_client_connack,       []}}
         , {'client.connected',    {?MODULE, on_client_connected,     []}}
         , {'client.disconnected', {?MODULE, on_client_disconnected,  []}}
         , {'client.authenticate', {?MODULE, on_client_authenticate,  []}}
         , {'client.check_acl',    {?MODULE, on_client_check_acl,     []}}
         , {'client.subscribe',    {?MODULE, on_client_subscribe,     []}}
         , {'client.unsubscribe',  {?MODULE, on_client_unsubscribe,   []}}
         , {'session.created',     {?MODULE, on_session_created,      []}}
         , {'session.subscribed',  {?MODULE, on_session_subscribed,   []}}
         , {'session.unsubscribed',{?MODULE, on_session_unsubscribed, []}}
         , {'session.resumed',     {?MODULE, on_session_resumed,      []}}
         , {'session.discarded',   {?MODULE, on_session_discarded,    []}}
         , {'session.takeovered',  {?MODULE, on_session_takeovered,   []}}
         , {'session.terminated',  {?MODULE, on_session_terminated,   []}}
         ]).

%%--------------------------------------------------------------------
%% Clients
%%--------------------------------------------------------------------

on_client_connect(ConnInfo, _Props) ->
    cast('client_connect', [conninfo(ConnInfo), props(_Props)]).

on_client_connack(ConnInfo, Rc, _Props) ->
    cast('client_connack', [conninfo(ConnInfo), Rc, props(_Props)]).

on_client_connected(ClientInfo, _ConnInfo) ->
    cast('client_connected', [clientinfo(ClientInfo)]).

on_client_disconnected(ClientInfo, {shutdown, Reason}, ConnInfo) when is_atom(Reason) ->
    on_client_disconnected(ClientInfo, Reason, ConnInfo);
on_client_disconnected(ClientInfo, Reason, _ConnInfo) ->
    cast('client_disconnected', [clientinfo(ClientInfo), stringfy(Reason)]).

on_client_authenticate(ClientInfo, AuthResult) ->
    AccArg = maps:get(auth_result, AuthResult, undefined) == success,
    Name   = 'client_authenticate',
    case call_fold(Name, [clientinfo(ClientInfo)], AccArg, validator(Name)) of
        {stop, Bool} when is_boolean(Bool) ->
            Result = case Bool of true -> success; _ -> not_authorized end,
            {stop, AuthResult#{auth_result => Result, anonymous => false}};
        _ ->
            {ok, AuthResult}
    end.

on_client_check_acl(ClientInfo, PubSub, Topic, Result) ->
    AccArg = Result == allow,
    Name   = 'client_check_acl',
    case call_fold(Name, [clientinfo(ClientInfo), PubSub, Topic], AccArg, validator(Name)) of
        {stop, Bool} when is_boolean(Bool) ->
            NResult = case Bool of true -> allow; _ -> deny end,
            {stop, NResult};
        _ -> {ok, Result}
    end.

on_client_subscribe(ClientInfo, Props, TopicFilters) ->
    cast('client_subscribe', [clientinfo(ClientInfo), props(Props), topicfilters(TopicFilters)]).

on_client_unsubscribe(Clientinfo, Props, TopicFilters) ->
    cast('client_unsubscribe', [clientinfo(Clientinfo), props(Props), topicfilters(TopicFilters)]).

%%--------------------------------------------------------------------
%% Session
%%--------------------------------------------------------------------

on_session_created(ClientInfo, _SessInfo) ->
    cast('session_created', [clientinfo(ClientInfo)]).

on_session_subscribed(Clientinfo, Topic, SubOpts) ->
    cast('session_subscribed', [clientinfo(Clientinfo), Topic, props(SubOpts)]).

on_session_unsubscribed(ClientInfo, Topic, _SubOpts) ->
    cast('session_unsubscribed', [clientinfo(ClientInfo), Topic]).

on_session_resumed(ClientInfo, _SessInfo) ->
    cast('session_resumed', [clientinfo(ClientInfo)]).

on_session_discarded(ClientInfo, _SessInfo) ->
    cast('session_discarded', [clientinfo(ClientInfo)]).

on_session_takeovered(ClientInfo, _SessInfo) ->
    cast('session_takeovered', [clientinfo(ClientInfo)]).

on_session_terminated(ClientInfo, Reason, _SessInfo) ->
    cast('session_terminated', [clientinfo(ClientInfo), stringfy(Reason)]).

%%--------------------------------------------------------------------
%% Types

props(undefined) -> [];
props(M) when is_map(M) -> maps:to_list(M).

conninfo(_ConnInfo =
         #{clientid := ClientId, username := Username, peername := {Peerhost, _},
           sockname := {_, SockPort}, proto_name := ProtoName, proto_ver := ProtoVer,
           keepalive := Keepalive}) ->
    [{node, node()},
     {clientid, ClientId},
     {username, maybe(Username)},
     {peerhost, ntoa(Peerhost)},
     {sockport, SockPort},
     {proto_name, ProtoName},
     {proto_ver, ProtoVer},
     {keepalive, Keepalive}].

clientinfo(ClientInfo =
           #{clientid := ClientId, username := Username, peerhost := PeerHost,
             sockport := SockPort, protocol := Protocol, mountpoint := Mountpoiont}) ->
    [{node, node()},
     {clientid, ClientId},
     {username, maybe(Username)},
     {password, maybe(maps:get(password, ClientInfo, undefined))},
     {peerhost, ntoa(PeerHost)},
     {sockport, SockPort},
     {protocol, Protocol},
     {mountpoint, maybe(Mountpoiont)},
     {is_superuser, maps:get(is_superuser, ClientInfo, false)},
     {anonymous, maps:get(anonymous, ClientInfo, true)}].

message(#message{id = Id, qos = Qos, from = From, topic = Topic, payload = Payload, timestamp = Ts}) ->
    [{node, node()},
     {id, hexstr(Id)},
     {qos, Qos},
     {from, From},
     {topic, Topic},
     {payload, Payload},
     {timestamp, Ts}].

topicfilters(Tfs = [{_, _}|_]) ->
    [{Topic, Qos} || {Topic, #{qos := Qos}} <- Tfs];
topicfilters(Tfs) ->
    Tfs.

ntoa({0,0,0,0,0,16#ffff,AB,CD}) ->
    list_to_binary(inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256}));
ntoa(IP) ->
    list_to_binary(inet_parse:ntoa(IP)).

maybe(undefined) -> <<"">>;
maybe(B) -> B.

%% @private
stringfy(Term) when is_binary(Term) ->
    Term;
stringfy(Term) when is_atom(Term) ->
    atom_to_binary(Term, utf8);
stringfy(Term) when is_tuple(Term) ->
    iolist_to_binary(io_lib:format("~p", [Term])).

hexstr(B) ->
    iolist_to_binary([io_lib:format("~2.16.0B", [X]) || X <- binary_to_list(B)]).

%%--------------------------------------------------------------------
%% Validator funcs

validator(Name) ->
    fun(V) -> validate_acc_arg(Name, V) end.

validate_acc_arg('client_authenticate', V) when is_boolean(V) -> true;
validate_acc_arg('client_check_acl',    V) when is_boolean(V) -> true;
validate_acc_arg('message_publish',     V) when is_list(V) -> validate_msg(V, true);
validate_acc_arg(_,                     _) -> false.

validate_msg([], Bool) ->
    Bool;
validate_msg(_, false) ->
    false;
validate_msg([{topic, T} | More], _) ->
    validate_msg(More, is_binary(T));
validate_msg([{payload, P} | More], _) ->
    validate_msg(More, is_binary(P));
validate_msg([{qos, Q} | More], _) ->
    validate_msg(More, Q =< 2 andalso Q >= 0);
validate_msg([{timestamp, T} | More], _) ->
    validate_msg(More, is_integer(T));
validate_msg([_ | More], _) ->
    validate_msg(More, true).

%%--------------------------------------------------------------------
%% Misc

assign_to_message([], Message) ->
    Message;
assign_to_message([{topic, Topic}|More], Message) ->
    assign_to_message(More, Message#message{topic = Topic});
assign_to_message([{qos, Qos}|More], Message) ->
    assign_to_message(More, Message#message{qos = Qos});
assign_to_message([{payload, Payload}|More], Message) ->
    assign_to_message(More, Message#message{payload = Payload});
assign_to_message([_|More], Message) ->
    assign_to_message(More, Message).
