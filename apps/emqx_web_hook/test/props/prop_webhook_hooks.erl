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

-module(prop_webhook_hooks).

-include_lib("proper/include/proper.hrl").

-import(emqx_ct_proper_types,
        [ conninfo/0
        , clientinfo/0
        , sessioninfo/0
        , message/0
        , connack_return_code/0
        , topictab/0
        , topic/0
        , subopts/0
        ]).

-define(ALL(Vars, Types, Exprs),
        ?SETUP(fun() ->
            State = do_setup(),
            fun() -> do_teardown(State) end
         end, ?FORALL(Vars, Types, Exprs))).

%%--------------------------------------------------------------------
%% Properties
%%--------------------------------------------------------------------

prop_client_connect() ->
    ?ALL({ConnInfo, ConnProps, Env},
         {conninfo(), conn_properties(), empty_env()},
       begin
           ok = emqx_web_hook:on_client_connect(ConnInfo, ConnProps, Env),
           Body = receive_http_request_body(),
           Body = emqx_json:encode(
                    #{action => client_connect,
                      node => stringfy(node()),
                      clientid => maps:get(clientid, ConnInfo),
                      username => maybe(maps:get(username, ConnInfo)),
                      ipaddress => peer2addr(maps:get(peername, ConnInfo)),
                      keepalive => maps:get(keepalive, ConnInfo),
                      proto_ver => maps:get(proto_ver, ConnInfo)
                     }),
           true
       end).

prop_client_connack() ->
    ?ALL({ConnInfo, Rc, AckProps, Env},
         {conninfo(), connack_return_code(), ack_properties(), empty_env()},
        begin
            ok = emqx_web_hook:on_client_connack(ConnInfo, Rc, AckProps, Env),
            Body = receive_http_request_body(),
            Body = emqx_json:encode(
                     #{action => client_connack,
                       node => stringfy(node()),
                       clientid => maps:get(clientid, ConnInfo),
                       username => maybe(maps:get(username, ConnInfo)),
                       ipaddress => peer2addr(maps:get(peername, ConnInfo)),
                       keepalive => maps:get(keepalive, ConnInfo),
                       proto_ver => maps:get(proto_ver, ConnInfo),
                       conn_ack => Rc
                       }),
            true
        end).

prop_client_connected() ->
    ?ALL({ClientInfo, ConnInfo, Env},
         {clientinfo(), conninfo(), empty_env()},
        begin
            ok = emqx_web_hook:on_client_connected(ClientInfo, ConnInfo, Env),
            Body = receive_http_request_body(),
            Body = emqx_json:encode(
                     #{action => client_connected,
                       node => stringfy(node()),
                       clientid => maps:get(clientid, ClientInfo),
                       username => maybe(maps:get(username, ClientInfo)),
                       ipaddress => peer2addr(maps:get(peerhost, ClientInfo)),
                       keepalive => maps:get(keepalive, ConnInfo),
                       proto_ver => maps:get(proto_ver, ConnInfo),
                       connected_at => maps:get(connected_at, ConnInfo)
                      }),
            true
        end).

prop_client_disconnected() ->
    ?ALL({ClientInfo, Reason, ConnInfo, Env},
         {clientinfo(), shutdown_reason(), disconnected_conninfo(), empty_env()},
        begin
            ok = emqx_web_hook:on_client_disconnected(ClientInfo, Reason, ConnInfo, Env),
            Body = receive_http_request_body(),
            Body = emqx_json:encode(
                     #{action => client_disconnected,
                       node => stringfy(node()),
                       clientid => maps:get(clientid, ClientInfo),
                       username => maybe(maps:get(username, ClientInfo)),
                       disconnected_at => maps:get(disconnected_at, ConnInfo),
                       reason => stringfy(Reason)
                      }),
            true
        end).

prop_client_subscribe() ->
    ?ALL({ClientInfo, SubProps, TopicTab, Env},
         {clientinfo(), sub_properties(), topictab(), topic_filter_env()},
        begin
            ok = emqx_web_hook:on_client_subscribe(ClientInfo, SubProps, TopicTab, Env),

            Matched = filter_topictab(TopicTab, Env),

            lists:foreach(fun({Topic, Opts}) ->
                Body = receive_http_request_body(),
                Body = emqx_json:encode(
                         #{action => client_subscribe,
                           node => stringfy(node()),
                           clientid => maps:get(clientid, ClientInfo),
                           username => maybe(maps:get(username, ClientInfo)),
                           topic => Topic,
                           opts => Opts})
            end, Matched),
            true
        end).

prop_client_unsubscribe() ->
    ?ALL({ClientInfo, SubProps, TopicTab, Env},
         {clientinfo(), unsub_properties(), topictab(), topic_filter_env()},
        begin
            ok = emqx_web_hook:on_client_unsubscribe(ClientInfo, SubProps, TopicTab, Env),

            Matched = filter_topictab(TopicTab, Env),

            lists:foreach(fun({Topic, Opts}) ->
                Body = receive_http_request_body(),
                Body = emqx_json:encode(
                         #{action => client_unsubscribe,
                           node => stringfy(node()),
                           clientid => maps:get(clientid, ClientInfo),
                           username => maybe(maps:get(username, ClientInfo)),
                           topic => Topic,
                           opts => Opts})
            end, Matched),
            true
        end).

prop_session_subscribed() ->
    ?ALL({ClientInfo, Topic, SubOpts, Env},
         {clientinfo(), topic(), subopts(), topic_filter_env()},
        begin
            ok = emqx_web_hook:on_session_subscribed(ClientInfo, Topic, SubOpts, Env),
            filter_topic_match(Topic, Env) andalso begin
                Body = receive_http_request_body(),
                Body1 = emqx_json:encode(
                         #{action => session_subscribed,
                           node => stringfy(node()),
                           clientid => maps:get(clientid, ClientInfo),
                           username => maybe(maps:get(username, ClientInfo)),
                           topic => Topic,
                           opts => SubOpts
                          }),
                Body = Body1
            end,
            true
        end).

prop_session_unsubscribed() ->
    ?ALL({ClientInfo, Topic, SubOpts, Env},
         {clientinfo(), topic(), subopts(), empty_env()},
        begin
            ok = emqx_web_hook:on_session_unsubscribed(ClientInfo, Topic, SubOpts, Env),
            filter_topic_match(Topic, Env) andalso begin
                Body = receive_http_request_body(),
                Body = emqx_json:encode(
                         #{action => session_unsubscribed,
                           node => stringfy(node()),
                           clientid => maps:get(clientid, ClientInfo),
                           username => maybe(maps:get(username, ClientInfo)),
                           topic => Topic
                          })
            end,
            true
        end).

prop_session_terminated() ->
    ?ALL({ClientInfo, Reason, SessInfo, Env},
         {clientinfo(), shutdown_reason(), sessioninfo(), empty_env()},
        begin
            ok = emqx_web_hook:on_session_terminated(ClientInfo, Reason, SessInfo, Env),
            Body = receive_http_request_body(),
            Body = emqx_json:encode(
                     #{action => session_terminated,
                       node => stringfy(node()),
                       clientid => maps:get(clientid, ClientInfo),
                       username => maybe(maps:get(username, ClientInfo)),
                       reason => stringfy(Reason)
                      }),
            true
        end).

prop_message_publish() ->
    ?ALL({Msg, Env, Encode}, {message(), topic_filter_env(), payload_encode()},
        begin
            application:set_env(emqx_web_hook, encode_payload, Encode),
            {ok, Msg} = emqx_web_hook:on_message_publish(Msg, Env),
            application:unset_env(emqx_web_hook, encode_payload),

            (not emqx_message:is_sys(Msg))
                andalso filter_topic_match(emqx_message:topic(Msg), Env)
                andalso begin
                    Body = receive_http_request_body(),
                    Body = emqx_json:encode(
                             #{action => message_publish,
                               node => stringfy(node()),
                               from_client_id => emqx_message:from(Msg),
                               from_username => maybe(emqx_message:get_header(username, Msg)),
                               topic => emqx_message:topic(Msg),
                               qos => emqx_message:qos(Msg),
                               retain => emqx_message:get_flag(retain, Msg),
                               payload => encode(emqx_message:payload(Msg), Encode),
                               ts => emqx_message:timestamp(Msg)
                              })
                end,
            true
        end).

prop_message_delivered() ->
    ?ALL({ClientInfo, Msg, Env, Encode}, {clientinfo(), message(), topic_filter_env(), payload_encode()},
        begin
            application:set_env(emqx_web_hook, encode_payload, Encode),
            ok = emqx_web_hook:on_message_delivered(ClientInfo, Msg, Env),
            application:unset_env(emqx_web_hook, encode_payload),

            (not emqx_message:is_sys(Msg))
                andalso filter_topic_match(emqx_message:topic(Msg), Env)
                andalso begin
                    Body = receive_http_request_body(),
                    Body = emqx_json:encode(
                             #{action => message_delivered,
                               node => stringfy(node()),
                               clientid => maps:get(clientid, ClientInfo),
                               username => maybe(maps:get(username, ClientInfo)),
                               from_client_id => emqx_message:from(Msg),
                               from_username => maybe(emqx_message:get_header(username, Msg)),
                               topic => emqx_message:topic(Msg),
                               qos => emqx_message:qos(Msg),
                               retain => emqx_message:get_flag(retain, Msg),
                               payload => encode(emqx_message:payload(Msg), Encode),
                               ts => emqx_message:timestamp(Msg)
                              })
                end,
            true
        end).

prop_message_acked() ->
    ?ALL({ClientInfo, Msg, Env, Encode}, {clientinfo(), message(), empty_env(), payload_encode()},
        begin
            application:set_env(emqx_web_hook, encode_payload, Encode),
            ok = emqx_web_hook:on_message_acked(ClientInfo, Msg, Env),
            application:unset_env(emqx_web_hook, encode_payload),

            (not emqx_message:is_sys(Msg))
                andalso filter_topic_match(emqx_message:topic(Msg), Env)
                andalso begin
                    Body = receive_http_request_body(),
                    Body = emqx_json:encode(
                             #{action => message_acked,
                               node => stringfy(node()),
                               clientid => maps:get(clientid, ClientInfo),
                               username => maybe(maps:get(username, ClientInfo)),
                               from_client_id => emqx_message:from(Msg),
                               from_username => maybe(emqx_message:get_header(username, Msg)),
                               topic => emqx_message:topic(Msg),
                               qos => emqx_message:qos(Msg),
                               retain => emqx_message:get_flag(retain, Msg),
                               payload => encode(emqx_message:payload(Msg), Encode),
                               ts => emqx_message:timestamp(Msg)
                              })
                end,
            true
        end).

prop_try_again() ->
    Setup = fun() ->
                logger:set_module_level(emqx_web_hook, emergency),
                meck:new(httpc, [passthrough, no_history]),
                meck:expect(httpc, request,
                    fun(Method, {Url, [], ContentType, Body}, _HttpOpts, _Opt) ->
                        self() ! {Method, Url, ContentType, Body}, {error, get(code)}
                    end),
                meck:new(emqx_metrics, [passthrough, no_history]),
                meck:expect(emqx_metrics, inc, fun(_) -> ok end)
            end,
    Teardown = fun() ->
                   meck:unload(httpc),
                   meck:unload(emqx_metrics),
                   logger:set_module_level(emqx_web_hook, debug)
               end,
    ?SETUP(fun() -> Setup(), Teardown end,
        ?FORALL({ConnInfo, ConnProps, Env, Code},
                {conninfo(), conn_properties(), empty_env(), http_code()},
                begin
                    %% pre-set error code
                    put(code, Code),
                    %% run hook
                    ok = emqx_web_hook:on_client_connect(ConnInfo, ConnProps, Env),

                    Bodys = receive_http_request_bodys(),
                    Body = emqx_json:encode(
                             #{action => client_connect,
                               node => stringfy(node()),
                               clientid => maps:get(clientid, ConnInfo),
                               username => maybe(maps:get(username, ConnInfo)),
                               ipaddress => peer2addr(maps:get(peername, ConnInfo)),
                               keepalive => maps:get(keepalive, ConnInfo),
                               proto_ver => maps:get(proto_ver, ConnInfo)
                              }),
                    [ B = Body || B <- Bodys],
                    if Code == socket_closed_remotely ->
                           4 = length(Bodys);
                       true -> ok
                    end,
                    true
                end)).

%%--------------------------------------------------------------------
%% Helper
%%--------------------------------------------------------------------
do_setup() ->
    Self = self(),
    meck:new(httpc, [passthrough, no_history]),
    meck:expect(httpc, request,
                fun(Method, {Url, [], ContentType, Body}, _HttpOpts, _Opt) ->
                    Self ! {Method, Url, ContentType, Body}, {ok, ok}
                end),

    meck:new(emqx_metrics, [passthrough, no_history]),
    meck:expect(emqx_metrics, inc, fun(_) -> ok end),
    ok.

do_teardown(_) ->
    meck:unload(httpc),
    meck:unload(emqx_metrics).

maybe(undefined) -> null;
maybe(T) -> T.

peer2addr({Host, _}) ->
    list_to_binary(inet:ntoa(Host));
peer2addr(Host) ->
    list_to_binary(inet:ntoa(Host)).

ensure_to_binary(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8);
ensure_to_binary(Bin) when is_binary(Bin) -> Bin.

stringfy({shutdown, Reason}) ->
    stringfy(Reason);
stringfy(Term) when is_atom(Term); is_binary(Term) ->
    Term;
stringfy(Term) ->
    unicode:characters_to_binary(io_lib:format("~0p", [Term])).

receive_http_request_body() ->
    receive
        {post, "http://127.0.0.1", "application/json", Body} ->
            Body
    after 100 ->
        exit(waiting_message_timeout)
    end.

receive_http_request_bodys() ->
    receive_http_request_bodys_([]).

receive_http_request_bodys_(Acc) ->
    receive
        {post, "http://127.0.0.1", "application/json", Body} ->
           receive_http_request_bodys_([Body|Acc])
    after 1000 ->
          lists:reverse(Acc)
    end.

filter_topictab(TopicTab, {undefined}) ->
    TopicTab;
filter_topictab(TopicTab, {TopicFilter}) ->
    lists:filter(fun({Topic, _}) -> emqx_topic:match(Topic, TopicFilter) end, TopicTab).

filter_topic_match(_Topic, {undefined}) ->
    true;
filter_topic_match(Topic, {TopicFilter}) ->
    emqx_topic:match(Topic, TopicFilter).

encode(Bin, base64) ->
    base64:encode(Bin);
encode(Bin, base62) ->
    emqx_base62:encode(Bin);
encode(Bin, _) ->
    Bin.

%%--------------------------------------------------------------------
%% Generators
%%--------------------------------------------------------------------

conn_properties() ->
    #{}.

ack_properties() ->
    #{}.

sub_properties() ->
    #{}.

unsub_properties() ->
    #{}.

shutdown_reason() ->
    oneof([any(), {shutdown, atom()}]).

empty_env() ->
    {undefined}.

topic_filter_env() ->
    oneof([{<<"#">>}, {undefined}, {topic()}]).

payload_encode() ->
    oneof([base62, base64, undefined]).

http_code() ->
    oneof([socket_closed_remotely, others]).

disconnected_conninfo() ->
    ?LET(Info, conninfo(),
         begin
           Info#{disconnected_at => erlang:system_time(millisecond)}
         end).
