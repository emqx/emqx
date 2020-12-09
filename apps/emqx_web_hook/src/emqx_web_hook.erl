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

-module(emqx_web_hook).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-define(APP, emqx_web_hook).

-logger_header("[WebHook]").

-import(inet, [ntoa/1]).

%% APIs
-export([ register_metrics/0
        , load/0
        , unload/0
        ]).

%% Hooks callback
-export([ on_client_connect/3
        , on_client_connack/4
        , on_client_connected/3
        , on_client_disconnected/4
        , on_client_subscribe/4
        , on_client_unsubscribe/4
        ]).

-export([ on_session_subscribed/4
        , on_session_unsubscribed/4
        , on_session_terminated/4
        ]).
-export([ on_message_publish/2
        , on_message_delivered/3
        , on_message_acked/3
        ]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

register_metrics() ->
    lists:foreach(fun emqx_metrics:ensure/1,
                  ['webhook.client_connect',
                   'webhook.client_connack',
                   'webhook.client_connected',
                   'webhook.client_disconnected',
                   'webhook.client_subscribe',
                   'webhook.client_unsubscribe',
                   'webhook.session_subscribed',
                   'webhook.session_unsubscribed',
                   'webhook.session_terminated',
                   'webhook.message_publish',
                   'webhook.message_delivered',
                   'webhook.message_acked']).

load() ->
    lists:foreach(
      fun({Hook, Fun, Filter}) ->
        emqx:hook(Hook, {?MODULE, Fun, [{Filter}]})
      end, parse_rule(application:get_env(?APP, rules, []))).

unload() ->
    lists:foreach(
      fun({Hook, Fun, _Filter}) ->
          emqx:unhook(Hook, {?MODULE, Fun})
      end, parse_rule(application:get_env(?APP, rules, []))).

%%--------------------------------------------------------------------
%% Client connect
%%--------------------------------------------------------------------

on_client_connect(ConnInfo = #{clientid := ClientId, username := Username, peername := {Peerhost, _}}, _ConnProp, _Env) ->
    emqx_metrics:inc('webhook.client_connect'),
    Params = #{ action => client_connect
              , node => node()
              , clientid => ClientId
              , username => maybe(Username)
              , ipaddress => iolist_to_binary(ntoa(Peerhost))
              , keepalive => maps:get(keepalive, ConnInfo)
              , proto_ver => maps:get(proto_ver, ConnInfo)
              },
    send_http_request(Params).

%%--------------------------------------------------------------------
%% Client connack
%%--------------------------------------------------------------------

on_client_connack(ConnInfo = #{clientid := ClientId, username := Username, peername := {Peerhost, _}}, Rc, _AckProp, _Env) ->
    emqx_metrics:inc('webhook.client_connack'),
    Params = #{ action => client_connack
              , node => node()
              , clientid => ClientId
              , username => maybe(Username)
              , ipaddress => iolist_to_binary(ntoa(Peerhost))
              , keepalive => maps:get(keepalive, ConnInfo)
              , proto_ver => maps:get(proto_ver, ConnInfo)
              , conn_ack => Rc
              },
    send_http_request(Params).

%%--------------------------------------------------------------------
%% Client connected
%%--------------------------------------------------------------------

on_client_connected(#{clientid := ClientId, username := Username, peerhost := Peerhost}, ConnInfo, _Env) ->
    emqx_metrics:inc('webhook.client_connected'),
    Params = #{ action => client_connected
              , node => node()
              , clientid => ClientId
              , username => maybe(Username)
              , ipaddress => iolist_to_binary(ntoa(Peerhost))
              , keepalive => maps:get(keepalive, ConnInfo)
              , proto_ver => maps:get(proto_ver, ConnInfo)
              , connected_at => maps:get(connected_at, ConnInfo)
              },
    send_http_request(Params).

%%--------------------------------------------------------------------
%% Client disconnected
%%--------------------------------------------------------------------

on_client_disconnected(ClientInfo, {shutdown, Reason}, ConnInfo, Env) when is_atom(Reason) ->
    on_client_disconnected(ClientInfo, Reason, ConnInfo, Env);
on_client_disconnected(#{clientid := ClientId, username := Username}, Reason, ConnInfo, _Env) ->
    emqx_metrics:inc('webhook.client_disconnected'),
    Params = #{ action => client_disconnected
              , node => node()
              , clientid => ClientId
              , username => maybe(Username)
              , reason => stringfy(maybe(Reason))
              , disconnected_at => maps:get(disconnected_at, ConnInfo, erlang:system_time(millisecond))
              },
    send_http_request(Params).

%%--------------------------------------------------------------------
%% Client subscribe
%%--------------------------------------------------------------------

on_client_subscribe(#{clientid := ClientId, username := Username}, _Properties, TopicTable, {Filter}) ->
    lists:foreach(fun({Topic, Opts}) ->
      with_filter(
        fun() ->
          emqx_metrics:inc('webhook.client_subscribe'),
          Params = #{ action => client_subscribe
                    , node => node()
                    , clientid => ClientId
                    , username => maybe(Username)
                    , topic => Topic
                    , opts => Opts
                    },
          send_http_request(Params)
        end, Topic, Filter)
    end, TopicTable).

%%--------------------------------------------------------------------
%% Client unsubscribe
%%--------------------------------------------------------------------

on_client_unsubscribe(#{clientid := ClientId, username := Username}, _Properties, TopicTable, {Filter}) ->
    lists:foreach(fun({Topic, Opts}) ->
      with_filter(
        fun() ->
          emqx_metrics:inc('webhook.client_unsubscribe'),
          Params = #{ action => client_unsubscribe
                    , node => node()
                    , clientid => ClientId
                    , username => maybe(Username)
                    , topic => Topic
                    , opts => Opts
                    },
          send_http_request(Params)
        end, Topic, Filter)
    end, TopicTable).

%%--------------------------------------------------------------------
%% Session subscribed
%%--------------------------------------------------------------------

on_session_subscribed(#{clientid := ClientId, username := Username}, Topic, Opts, {Filter}) ->
    with_filter(
      fun() ->
        emqx_metrics:inc('webhook.session_subscribed'),
        Params = #{ action => session_subscribed
                  , node => node()
                  , clientid => ClientId
                  , username => maybe(Username)
                  , topic => Topic
                  , opts => Opts
                  },
        send_http_request(Params)
      end, Topic, Filter).

%%--------------------------------------------------------------------
%% Session unsubscribed
%%--------------------------------------------------------------------

on_session_unsubscribed(#{clientid := ClientId, username := Username}, Topic, _Opts, {Filter}) ->
    with_filter(
      fun() ->
        emqx_metrics:inc('webhook.session_unsubscribed'),
        Params = #{ action => session_unsubscribed
                  , node => node()
                  , clientid => ClientId
                  , username => maybe(Username)
                  , topic => Topic
                  },
        send_http_request(Params)
      end, Topic, Filter).

%%--------------------------------------------------------------------
%% Session terminated
%%--------------------------------------------------------------------

on_session_terminated(Info, {shutdown, Reason}, SessInfo, Env) when is_atom(Reason) ->
    on_session_terminated(Info, Reason, SessInfo, Env);
on_session_terminated(#{clientid := ClientId, username := Username}, Reason, _SessInfo, _Env) ->
    emqx_metrics:inc('webhook.session_terminated'),
    Params = #{ action => session_terminated
              , node => node()
              , clientid => ClientId
              , username => maybe(Username)
              , reason => stringfy(maybe(Reason))
              },
    send_http_request(Params).

%%--------------------------------------------------------------------
%% Message publish
%%--------------------------------------------------------------------

on_message_publish(Message = #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    {ok, Message};
on_message_publish(Message = #message{topic = Topic}, {Filter}) ->
    with_filter(
      fun() ->
        emqx_metrics:inc('webhook.message_publish'),
        {FromClientId, FromUsername} = parse_from(Message),
        Params = #{ action => message_publish
                  , node => node()
                  , from_client_id => FromClientId
                  , from_username => FromUsername
                  , topic => Message#message.topic
                  , qos => Message#message.qos
                  , retain => emqx_message:get_flag(retain, Message)
                  , payload => encode_payload(Message#message.payload)
                  , ts => Message#message.timestamp
                  },
        send_http_request(Params),
        {ok, Message}
      end, Message, Topic, Filter).

%%--------------------------------------------------------------------
%% Message deliver
%%--------------------------------------------------------------------

on_message_delivered(_ClientInfo,#message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    ok;
on_message_delivered(#{clientid := ClientId, username := Username},
                     Message = #message{topic = Topic}, {Filter}) ->
  with_filter(
    fun() ->
      emqx_metrics:inc('webhook.message_delivered'),
      {FromClientId, FromUsername} = parse_from(Message),
      Params = #{ action => message_delivered
                , node => node()
                , clientid => ClientId
                , username => maybe(Username)
                , from_client_id => FromClientId
                , from_username => FromUsername
                , topic => Message#message.topic
                , qos => Message#message.qos
                , retain => emqx_message:get_flag(retain, Message)
                , payload => encode_payload(Message#message.payload)
                , ts => Message#message.timestamp
                },
      send_http_request(Params)
    end, Topic, Filter).

%%--------------------------------------------------------------------
%% Message acked
%%--------------------------------------------------------------------

on_message_acked(_ClientInfo, #message{topic = <<"$SYS/", _/binary>>}, _Env) ->
    ok;
on_message_acked(#{clientid := ClientId, username := Username},
                 Message = #message{topic = Topic}, {Filter}) ->
    with_filter(
      fun() ->
        emqx_metrics:inc('webhook.message_acked'),
        {FromClientId, FromUsername} = parse_from(Message),
        Params = #{ action => message_acked
                  , node => node()
                  , clientid => ClientId
                  , username => maybe(Username)
                  , from_client_id => FromClientId
                  , from_username => FromUsername
                  , topic => Message#message.topic
                  , qos => Message#message.qos
                  , retain => emqx_message:get_flag(retain, Message)
                  , payload => encode_payload(Message#message.payload)
                  , ts => Message#message.timestamp
                  },
        send_http_request(Params)
      end, Topic, Filter).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

send_http_request(Params) ->
    Params1 = emqx_json:encode(Params),
    Url = application:get_env(?APP, url, "http://127.0.0.1"),
    Headers = application:get_env(?APP, headers, []),
    ?LOG(debug, "Send to: ~0p, params: ~0s", [Url, Params1]),
    case request_(post, {Url, Headers, "application/json", Params1}, [{timeout, 5000}], [], 0) of
        {ok, _} -> ok;
        {error, Reason} ->
            ?LOG(error, "HTTP request error: ~p", [Reason]), ok
    end.

request_(Method, Req, HTTPOpts, Opts, Times) ->
    %% Resend request, when TCP closed by remotely
    NHttpOpts = case application:get_env(?APP, ssl, false) of
        true -> [{ssl, application:get_env(?APP, ssloptions, [])} | HTTPOpts];
        _ -> HTTPOpts
    end,
    case httpc:request(Method, Req, NHttpOpts, Opts) of
        {error, socket_closed_remotely} when Times < 3 ->
            timer:sleep(trunc(math:pow(10, Times))),
            request_(Method, Req, HTTPOpts, Opts, Times+1);
        Other -> Other
    end.

parse_rule(Rules) ->
    parse_rule(Rules, []).
parse_rule([], Acc) ->
    lists:reverse(Acc);
parse_rule([{Rule, Conf} | Rules], Acc) ->
    Params = emqx_json:decode(iolist_to_binary(Conf)),
    Action = proplists:get_value(<<"action">>, Params),
    Filter = proplists:get_value(<<"topic">>, Params),
    parse_rule(Rules, [{list_to_atom(Rule), binary_to_existing_atom(Action, utf8), Filter} | Acc]).

with_filter(Fun, _, undefined) ->
    Fun(), ok;
with_filter(Fun, Topic, Filter) ->
    case emqx_topic:match(Topic, Filter) of
        true  -> Fun(), ok;
        false -> ok
    end.

with_filter(Fun, _, _, undefined) ->
    Fun();
with_filter(Fun, Msg, Topic, Filter) ->
    case emqx_topic:match(Topic, Filter) of
        true  -> Fun();
        false -> {ok, Msg}
    end.

parse_from(Message) ->
    {emqx_message:from(Message), maybe(emqx_message:get_header(username, Message))}.

encode_payload(Payload) ->
    encode_payload(Payload, application:get_env(?APP, encode_payload, undefined)).

encode_payload(Payload, base62) -> emqx_base62:encode(Payload);
encode_payload(Payload, base64) -> base64:encode(Payload);
encode_payload(Payload, _) -> Payload.

stringfy(Term) when is_atom(Term); is_binary(Term) ->
    Term;
stringfy(Term) ->
    unicode:characters_to_binary((io_lib:format("~0p", [Term]))).

maybe(undefined) -> null;
maybe(Str) -> Str.

