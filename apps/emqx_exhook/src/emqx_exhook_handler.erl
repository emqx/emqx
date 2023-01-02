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

-module(emqx_exhook_handler).

-include("emqx_exhook.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

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

-export([ on_message_publish/1
        , on_message_dropped/3
        , on_message_delivered/2
        , on_message_acked/2
        ]).

%% Utils
-export([ message/1
        , headers/1
        , stringfy/1
        , merge_responsed_bool/2
        , merge_responsed_message/2
        , assign_to_message/2
        , clientinfo/1
        ]).

-import(emqx_exhook,
        [ cast/2
        , call_fold/3
        ]).


-elvis([{elvis_style, god_modules, disable}]).

-define(STOP_OR_OK(Res),
        (Res =:= ok orelse Res =:= stop)).

%%--------------------------------------------------------------------
%% Clients
%%--------------------------------------------------------------------

on_client_connect(ConnInfo, Props) ->
    Req = #{conninfo => conninfo(ConnInfo),
            props => properties(Props)
           },
    cast('client.connect', Req).

on_client_connack(ConnInfo, Rc, Props) ->
    Req = #{conninfo => conninfo(ConnInfo),
            result_code => stringfy(Rc),
            props => properties(Props)},
    cast('client.connack', Req).

on_client_connected(ClientInfo, _ConnInfo) ->
    Req = #{clientinfo => clientinfo(ClientInfo)},
    cast('client.connected', Req).

on_client_disconnected(ClientInfo, Reason, _ConnInfo) ->
    Req = #{clientinfo => clientinfo(ClientInfo),
            reason => stringfy(Reason)
           },
    cast('client.disconnected', Req).

on_client_authenticate(ClientInfo, AuthResult) ->
    %% XXX: Bool is missing more information about the atom of the result
    %%      So, the `Req` has missed detailed info too.
    %%
    %%      The return value of `call_fold` just a bool, that has missed
    %%      detailed info too.
    %%
    Bool = maps:get(auth_result, AuthResult, undefined) == success,
    Req = #{clientinfo => clientinfo(ClientInfo),
            result => Bool
           },

    case call_fold('client.authenticate', Req,
                   fun merge_responsed_bool/2) of
        {StopOrOk, #{result := Result0}}
          when is_boolean(Result0) andalso ?STOP_OR_OK(StopOrOk) ->
            Result = case Result0 of true -> success; _ -> not_authorized end,
            {StopOrOk, AuthResult#{auth_result => Result, anonymous => false}};
        ignore ->
            {ok, AuthResult}
    end.

on_client_check_acl(ClientInfo, PubSub, Topic, Result) ->
    Bool = Result == allow,
    Type = case PubSub of
               publish -> 'PUBLISH';
               subscribe -> 'SUBSCRIBE'
           end,
    Req = #{clientinfo => clientinfo(ClientInfo),
            type => Type,
            topic => Topic,
            result => Bool
           },
    case call_fold('client.check_acl', Req,
                   fun merge_responsed_bool/2) of
        {StopOrOk, #{result := Result0}}
          when is_boolean(Result0) andalso ?STOP_OR_OK(StopOrOk) ->
            NResult = case Result0 of true -> allow; _ -> deny end,
            {StopOrOk, NResult};
        ignore -> {ok, Result}
    end.

on_client_subscribe(ClientInfo, Props, TopicFilters) ->
    Req = #{clientinfo => clientinfo(ClientInfo),
            props => properties(Props),
            topic_filters => topicfilters(TopicFilters)
           },
    cast('client.subscribe', Req).

on_client_unsubscribe(ClientInfo, Props, TopicFilters) ->
    Req = #{clientinfo => clientinfo(ClientInfo),
            props => properties(Props),
            topic_filters => topicfilters(TopicFilters)
           },
    cast('client.unsubscribe', Req).

%%--------------------------------------------------------------------
%% Session
%%--------------------------------------------------------------------

on_session_created(ClientInfo, _SessInfo) ->
    Req = #{clientinfo => clientinfo(ClientInfo)},
    cast('session.created', Req).

on_session_subscribed(ClientInfo, Topic, SubOpts) ->
    Req = #{clientinfo => clientinfo(ClientInfo),
            topic => Topic,
            subopts => maps:with([qos, share, rh, rap, nl], SubOpts)
           },
    cast('session.subscribed', Req).

on_session_unsubscribed(ClientInfo, Topic, _SubOpts) ->
    Req = #{clientinfo => clientinfo(ClientInfo),
            topic => Topic
           },
    cast('session.unsubscribed', Req).

on_session_resumed(ClientInfo, _SessInfo) ->
    Req = #{clientinfo => clientinfo(ClientInfo)},
    cast('session.resumed', Req).

on_session_discarded(ClientInfo, _SessInfo) ->
    Req = #{clientinfo => clientinfo(ClientInfo)},
    cast('session.discarded', Req).

on_session_takeovered(ClientInfo, _SessInfo) ->
    Req = #{clientinfo => clientinfo(ClientInfo)},
    cast('session.takeovered', Req).

on_session_terminated(ClientInfo, Reason, _SessInfo) ->
    Req = #{clientinfo => clientinfo(ClientInfo),
            reason => stringfy(Reason)},
    cast('session.terminated', Req).

%%--------------------------------------------------------------------
%% Message
%%--------------------------------------------------------------------

on_message_publish(#message{topic = <<"$SYS/", _/binary>>}) ->
    ok;
on_message_publish(Message) ->
    Req = #{message => message(Message)},
    case call_fold('message.publish', Req,
                   fun emqx_exhook_handler:merge_responsed_message/2) of
        {StopOrOk, #{message := NMessage}}
          when ?STOP_OR_OK(StopOrOk) ->
            {StopOrOk, assign_to_message(NMessage, Message)};
        ignore ->
            {ok, Message}
    end.

on_message_dropped(#message{topic = <<"$SYS/", _/binary>>}, _By, _Reason) ->
    ok;
on_message_dropped(Message, _By, Reason) ->
    Req = #{message => message(Message),
            reason => stringfy(Reason)
           },
    cast('message.dropped', Req).

on_message_delivered(_ClientInfo, #message{topic = <<"$SYS/", _/binary>>}) ->
    ok;
on_message_delivered(ClientInfo, Message) ->
    Req = #{clientinfo => clientinfo(ClientInfo),
            message => message(Message)
           },
    cast('message.delivered', Req).

on_message_acked(_ClientInfo, #message{topic = <<"$SYS/", _/binary>>}) ->
    ok;
on_message_acked(ClientInfo, Message) ->
    Req = #{clientinfo => clientinfo(ClientInfo),
            message => message(Message)
           },
    cast('message.acked', Req).

%%--------------------------------------------------------------------
%% Types

properties(undefined) -> [];
properties(M) when is_map(M) ->
    maps:fold(fun(K, V, Acc) ->
        [#{name => stringfy(K),
           value => stringfy(V)} | Acc]
    end, [], M).

conninfo(_ConnInfo =
           #{clientid := ClientId, username := Username, peername := {Peerhost, _},
             sockname := {_, SockPort}, proto_name := ProtoName, proto_ver := ProtoVer,
             keepalive := Keepalive}) ->
    #{node => stringfy(node()),
      clientid => ClientId,
      username => maybe(Username),
      peerhost => ntoa(Peerhost),
      sockport => SockPort,
      proto_name => ProtoName,
      proto_ver => stringfy(ProtoVer),
      keepalive => Keepalive}.

clientinfo(ClientInfo =
            #{clientid := ClientId, username := Username, peerhost := PeerHost,
              sockport := SockPort, protocol := Protocol, mountpoint := Mountpoiont}) ->
    #{node => stringfy(node()),
      clientid => ClientId,
      username => maybe(Username),
      password => maybe(maps:get(password, ClientInfo, undefined)),
      peerhost => ntoa(PeerHost),
      sockport => SockPort,
      protocol => stringfy(Protocol),
      mountpoint => maybe(Mountpoiont),
      is_superuser => maps:get(is_superuser, ClientInfo, false),
      anonymous => maps:get(anonymous, ClientInfo, true),
      cn => maybe(maps:get(cn, ClientInfo, undefined)),
      dn => maybe(maps:get(dn, ClientInfo, undefined))}.

message(#message{id = Id, qos = Qos, from = From, topic = Topic,
                 payload = Payload, timestamp = Ts, headers = Headers}) ->
    #{node => stringfy(node()),
      id => emqx_guid:to_hexstr(Id),
      qos => Qos,
      from => stringfy(From),
      topic => Topic,
      payload => Payload,
      timestamp => Ts,
      headers => headers(Headers)
     }.

headers(Headers) ->
    Ls = [username, protocol, peerhost, allow_publish],
    maps:fold(
      fun
          (_, undefined, Acc) ->
              Acc; %% Ignore undefined value
          (K, V, Acc) ->
              case lists:member(K, Ls) of
                  true ->
                      Acc#{atom_to_binary(K) => bin(K, V)};
                  _ ->
                      Acc
              end
    end, #{}, Headers).

bin(K, V) when K == username;
               K == protocol;
               K == allow_publish ->
    bin(V);
bin(peerhost, V) ->
    bin(inet:ntoa(V)).

bin(V) when is_binary(V) -> V;
bin(V) when is_atom(V) -> atom_to_binary(V);
bin(V) when is_list(V) -> iolist_to_binary(V).

assign_to_message(InMessage = #{qos := Qos, topic := Topic,
                                payload := Payload}, Message) ->
    NMsg = Message#message{qos = Qos, topic = Topic, payload = Payload},
    enrich_header(maps:get(headers, InMessage, #{}), NMsg).

enrich_header(Headers, Message) ->
    case maps:get(<<"allow_publish">>, Headers, undefined) of
        <<"false">> ->
            emqx_message:set_header(allow_publish, false, Message);
        <<"true">> ->
            emqx_message:set_header(allow_publish, true, Message);
        _ ->
            Message
    end.

topicfilters(Tfs) when is_list(Tfs) ->
    [#{name => Topic, qos => Qos} || {Topic, #{qos := Qos}} <- Tfs].

ntoa({0,0,0,0,0,16#ffff,AB,CD}) ->
    list_to_binary(inet_parse:ntoa({AB bsr 8, AB rem 256, CD bsr 8, CD rem 256}));
ntoa(IP) ->
    list_to_binary(inet_parse:ntoa(IP)).

maybe(undefined) -> <<>>;
maybe(B) -> B.

%% @private
stringfy(Term) when is_binary(Term) ->
    Term;
stringfy(Term) when is_integer(Term) ->
    integer_to_binary(Term);
stringfy(Term) when is_atom(Term) ->
    atom_to_binary(Term, utf8);
stringfy(Term) when is_list(Term) ->
    list_to_binary(Term);
stringfy(Term) ->
    unicode:characters_to_binary((io_lib:format("~0p", [Term]))).

%%--------------------------------------------------------------------
%% Acc funcs

%% see exhook.proto
merge_responsed_bool(_Req, #{type := 'IGNORE'}) ->
    ignore;
merge_responsed_bool(Req, #{type := Type, value := {bool_result, NewBool}})
  when is_boolean(NewBool) ->
    {ret(Type), Req#{result => NewBool}};
merge_responsed_bool(_Req, Resp) ->
    ?LOG(warning, "Unknown responsed value ~0p to merge to callback chain", [Resp]),
    ignore.

merge_responsed_message(_Req, #{type := 'IGNORE'}) ->
    ignore;
merge_responsed_message(Req, #{type := Type, value := {message, NMessage}}) ->
    {ret(Type), Req#{message => NMessage}};
merge_responsed_message(_Req, Resp) ->
    ?LOG(warning, "Unknown responsed value ~0p to merge to callback chain", [Resp]),
    ignore.

ret('CONTINUE') -> ok;
ret('STOP_AND_RETURN') -> stop.
