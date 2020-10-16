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

-module(emqx_exproto_echo_svr).

-behavior(emqx_exproto_v_1_connection_handler_bhvr).

-export([ start/0
        , stop/1
        ]).

-export([ frame_connect/2
        , frame_connack/1
        , frame_publish/3
        , frame_puback/1
        , frame_subscribe/2
        , frame_suback/1
        , frame_unsubscribe/1
        , frame_unsuback/1
        , frame_disconnect/0
        ]).

-export([ on_socket_created/2
        , on_received_bytes/2
        , on_socket_closed/2
        , on_timer_timeout/2
        , on_received_messages/2
        ]).

-define(HTTP, #{grpc_opts => #{service_protos => [emqx_exproto_pb],
                               services => #{'emqx.exproto.v1.ConnectionHandler' => ?MODULE}},
                listen_opts => #{port => 9001,
                                 socket_options => []},
                pool_opts => #{size => 8},
                transport_opts => #{ssl => false}}).

-define(CLIENT, emqx_exproto_v_1_connection_adapter_client).
-define(send(Req), ?CLIENT:send(Req, #{channel => ct_test_channel})).
-define(close(Req), ?CLIENT:close(Req, #{channel => ct_test_channel})).
-define(authenticate(Req), ?CLIENT:authenticate(Req, #{channel => ct_test_channel})).
-define(start_timer(Req), ?CLIENT:start_timer(Req, #{channel => ct_test_channel})).
-define(publish(Req), ?CLIENT:publish(Req, #{channel => ct_test_channel})).
-define(subscribe(Req), ?CLIENT:subscribe(Req, #{channel => ct_test_channel})).
-define(unsubscribe(Req), ?CLIENT:unsubscribe(Req, #{channel => ct_test_channel})).

-define(TYPE_CONNECT, 1).
-define(TYPE_CONNACK, 2).
-define(TYPE_PUBLISH, 3).
-define(TYPE_PUBACK, 4).
-define(TYPE_SUBSCRIBE, 5).
-define(TYPE_SUBACK, 6).
-define(TYPE_UNSUBSCRIBE, 7).
-define(TYPE_UNSUBACK, 8).
-define(TYPE_DISCONNECT, 9).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

start() ->
    application:ensure_all_started(grpcbox),
    [start_channel(), start_server()].

start_channel() ->
    grpcbox_channel_sup:start_child(ct_test_channel, [{http, "localhost", 9100, []}], #{}).

start_server() ->
    grpcbox:start_server(?HTTP).

stop([ChannPid, SvrPid]) ->
    supervisor:terminate_child(grpcbox_channel_sup, ChannPid),
    supervisor:terminate_child(grpcbox_services_simple_sup, SvrPid).

%%--------------------------------------------------------------------
%% Protocol Adapter callbacks
%%--------------------------------------------------------------------

-spec on_socket_created(ctx:ctx(), emqx_exproto_pb:created_socket_request()) ->
    {ok, emqx_exproto_pb:empty_success(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
on_socket_created(Ctx, Req) ->
    io:format("~p: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

-spec on_received_bytes(ctx:ctx(), emqx_exproto_pb:received_bytes_request()) ->
    {ok, emqx_exproto_pb:empty_success(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
on_received_bytes(Ctx, Req = #{conn := Conn, bytes := Bytes}) ->
    io:format("~p: ~0p~n", [?FUNCTION_NAME, Req]),
    #{<<"type">> := Type} = Params = emqx_json:decode(Bytes, [return_maps]),
    _ = handle_in(Conn, Type, Params),
    {ok, #{}, Ctx}.

-spec on_socket_closed(ctx:ctx(), emqx_exproto_pb:socket_closed_request()) ->
    {ok, emqx_exproto_pb:empty_success(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
on_socket_closed(Ctx, Req) ->
    io:format("~p: ~0p~n", [?FUNCTION_NAME, Req]),
    {ok, #{}, Ctx}.

on_timer_timeout(Ctx, Req = #{conn := Conn, type := 'KEEPALIVE'}) ->
    io:format("~p: ~0p~n", [?FUNCTION_NAME, Req]),
    handle_out(Conn, ?TYPE_DISCONNECT),
    ?close(#{conn => Conn}),
    {ok, #{}, Ctx}.

-spec on_received_messages(ctx:ctx(), emqx_exproto_pb:received_messages_request()) ->
    {ok, emqx_exproto_pb:empty_success(), ctx:ctx()} | grpcbox_stream:grpc_error_response().
on_received_messages(Ctx, Req = #{conn := Conn, messages := Messages}) ->
    io:format("~p: ~0p~n", [?FUNCTION_NAME, Req]),
    lists:foreach(fun(Message) ->
        handle_out(Conn, ?TYPE_PUBLISH, Message)
    end, Messages),
    {ok, #{}, Ctx}.

%%--------------------------------------------------------------------
%% The Protocol Example:
%%  CONN:
%%   {"type": 1, "clientinfo": {...}}
%%
%%  CONNACK:
%%   {"type": 2, "code": 0}
%%
%%  PUBLISH:
%%   {"type": 3, "topic": "xxx", "payload": "", "qos": 0}
%%
%%  PUBACK:
%%   {"type": 4, "code": 0}
%%
%%  SUBSCRIBE:
%%   {"type": 5, "topic": "xxx", "qos": 1}
%%
%%  SUBACK:
%%   {"type": 6, "code": 0}
%%
%%  DISCONNECT:
%%   {"type": 7, "code": 1}
%%--------------------------------------------------------------------

handle_in(Conn, ?TYPE_CONNECT, #{<<"clientinfo">> := ClientInfo, <<"password">> := Password}) ->
    NClientInfo = maps:from_list([{binary_to_atom(K, utf8), V} || {K, V} <- maps:to_list(ClientInfo)]),
    case ?authenticate(#{conn => Conn, clientinfo => NClientInfo, password => Password}) of
        {ok, #{code := 'SUCCESS'}, _} ->
            case maps:get(keepalive, NClientInfo, 0) of
                0 -> ok;
                Intv ->
                    io:format("Try call start_timer with ~ps", [Intv]),
                    ?start_timer(#{conn => Conn, type => 'KEEPALIVE', interval => Intv})
            end,
            handle_out(Conn, ?TYPE_CONNACK, 0);
        _ ->
            handle_out(Conn, ?TYPE_CONNACK, 1),
            ?close(#{conn => Conn})
    end;
handle_in(Conn, ?TYPE_PUBLISH, #{<<"topic">> := Topic,
                                 <<"qos">> := Qos,
                                 <<"payload">> := Payload}) ->
    case ?publish(#{conn => Conn, topic => Topic, qos => Qos, payload => Payload}) of
        {ok, #{code := 'SUCCESS'}, _} ->
            handle_out(Conn, ?TYPE_PUBACK, 0);
        _ ->
            handle_out(Conn, ?TYPE_PUBACK, 1)
    end;
handle_in(Conn, ?TYPE_SUBSCRIBE, #{<<"qos">> := Qos, <<"topic">> := Topic}) ->
    case ?subscribe(#{conn => Conn, topic => Topic, qos => Qos}) of
        {ok, #{code := 'SUCCESS'}, _} ->
            handle_out(Conn, ?TYPE_SUBACK, 0);
        _ ->
            handle_out(Conn, ?TYPE_SUBACK, 1)
    end;
handle_in(Conn, ?TYPE_UNSUBSCRIBE, #{<<"topic">> := Topic}) ->
    case ?unsubscribe(#{conn => Conn, topic => Topic}) of
        {ok, #{code := 'SUCCESS'}, _} ->
            handle_out(Conn, ?TYPE_UNSUBACK, 0);
        _ ->
            handle_out(Conn, ?TYPE_UNSUBACK, 1)
    end;

handle_in(Conn, ?TYPE_DISCONNECT, _) ->
    ?close(#{conn => Conn}).

handle_out(Conn, ?TYPE_CONNACK, Code) ->
    ?send(#{conn => Conn, bytes => frame_connack(Code)});
handle_out(Conn, ?TYPE_PUBACK, Code) ->
    ?send(#{conn => Conn, bytes => frame_puback(Code)});
handle_out(Conn, ?TYPE_SUBACK, Code) ->
    ?send(#{conn => Conn, bytes => frame_suback(Code)});
handle_out(Conn, ?TYPE_UNSUBACK, Code) ->
    ?send(#{conn => Conn, bytes => frame_unsuback(Code)});
handle_out(Conn, ?TYPE_PUBLISH, #{qos := Qos, topic := Topic, payload := Payload}) ->
    ?send(#{conn => Conn, bytes => frame_publish(Topic, Qos, Payload)}).

handle_out(Conn, ?TYPE_DISCONNECT) ->
    ?send(#{conn => Conn, bytes => frame_disconnect()}).

%%--------------------------------------------------------------------
%% Frame

frame_connect(ClientInfo, Password) ->
    emqx_json:encode(#{type => ?TYPE_CONNECT,
                       clientinfo => ClientInfo,
                       password => Password}).
frame_connack(Code) ->
    emqx_json:encode(#{type => ?TYPE_CONNACK, code => Code}).

frame_publish(Topic, Qos, Payload) ->
    emqx_json:encode(#{type => ?TYPE_PUBLISH,
                       topic => Topic,
                       qos => Qos,
                       payload => Payload}).

frame_puback(Code) ->
    emqx_json:encode(#{type => ?TYPE_PUBACK, code => Code}).

frame_subscribe(Topic, Qos) ->
    emqx_json:encode(#{type => ?TYPE_SUBSCRIBE, topic => Topic, qos => Qos}).

frame_suback(Code) ->
    emqx_json:encode(#{type => ?TYPE_SUBACK, code => Code}).

frame_unsubscribe(Topic) ->
    emqx_json:encode(#{type => ?TYPE_UNSUBSCRIBE, topic => Topic}).

frame_unsuback(Code) ->
    emqx_json:encode(#{type => ?TYPE_UNSUBACK, code => Code}).

frame_disconnect() ->
    emqx_json:encode(#{type => ?TYPE_DISCONNECT}).
