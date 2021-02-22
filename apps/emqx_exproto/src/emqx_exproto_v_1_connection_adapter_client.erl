%%%-------------------------------------------------------------------
%% @doc Client module for grpc service emqx.exproto.v1.ConnectionAdapter.
%% @end
%%%-------------------------------------------------------------------

%% this module was generated and should not be modified manually

-module(emqx_exproto_v_1_connection_adapter_client).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("grpc/include/grpc.hrl").

-define(SERVICE, 'emqx.exproto.v1.ConnectionAdapter').
-define(PROTO_MODULE, 'emqx_exproto_pb').
-define(MARSHAL(T), fun(I) -> ?PROTO_MODULE:encode_msg(I, T) end).
-define(UNMARSHAL(T), fun(I) -> ?PROTO_MODULE:decode_msg(I, T) end).
-define(DEF(Path, Req, Resp, MessageType),
        #{path => Path,
          service =>?SERVICE,
          message_type => MessageType,
          marshal => ?MARSHAL(Req),
          unmarshal => ?UNMARSHAL(Resp)}).

-spec send(emqx_exproto_pb:send_bytes_request())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
send(Req) ->
    send(Req, #{}, #{}).

-spec send(emqx_exproto_pb:send_bytes_request(), grpc:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
send(Req, Options) ->
    send(Req, #{}, Options).

-spec send(emqx_exproto_pb:send_bytes_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
send(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exproto.v1.ConnectionAdapter/Send">>,
                           send_bytes_request, code_response, <<"emqx.exproto.v1.SendBytesRequest">>),
                      Req, Metadata, Options).

-spec close(emqx_exproto_pb:close_socket_request())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
close(Req) ->
    close(Req, #{}, #{}).

-spec close(emqx_exproto_pb:close_socket_request(), grpc:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
close(Req, Options) ->
    close(Req, #{}, Options).

-spec close(emqx_exproto_pb:close_socket_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
close(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exproto.v1.ConnectionAdapter/Close">>,
                           close_socket_request, code_response, <<"emqx.exproto.v1.CloseSocketRequest">>),
                      Req, Metadata, Options).

-spec authenticate(emqx_exproto_pb:authenticate_request())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
authenticate(Req) ->
    authenticate(Req, #{}, #{}).

-spec authenticate(emqx_exproto_pb:authenticate_request(), grpc:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
authenticate(Req, Options) ->
    authenticate(Req, #{}, Options).

-spec authenticate(emqx_exproto_pb:authenticate_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
authenticate(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exproto.v1.ConnectionAdapter/Authenticate">>,
                           authenticate_request, code_response, <<"emqx.exproto.v1.AuthenticateRequest">>),
                      Req, Metadata, Options).

-spec start_timer(emqx_exproto_pb:timer_request())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
start_timer(Req) ->
    start_timer(Req, #{}, #{}).

-spec start_timer(emqx_exproto_pb:timer_request(), grpc:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
start_timer(Req, Options) ->
    start_timer(Req, #{}, Options).

-spec start_timer(emqx_exproto_pb:timer_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
start_timer(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exproto.v1.ConnectionAdapter/StartTimer">>,
                           timer_request, code_response, <<"emqx.exproto.v1.TimerRequest">>),
                      Req, Metadata, Options).

-spec publish(emqx_exproto_pb:publish_request())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
publish(Req) ->
    publish(Req, #{}, #{}).

-spec publish(emqx_exproto_pb:publish_request(), grpc:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
publish(Req, Options) ->
    publish(Req, #{}, Options).

-spec publish(emqx_exproto_pb:publish_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
publish(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exproto.v1.ConnectionAdapter/Publish">>,
                           publish_request, code_response, <<"emqx.exproto.v1.PublishRequest">>),
                      Req, Metadata, Options).

-spec subscribe(emqx_exproto_pb:subscribe_request())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
subscribe(Req) ->
    subscribe(Req, #{}, #{}).

-spec subscribe(emqx_exproto_pb:subscribe_request(), grpc:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
subscribe(Req, Options) ->
    subscribe(Req, #{}, Options).

-spec subscribe(emqx_exproto_pb:subscribe_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
subscribe(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exproto.v1.ConnectionAdapter/Subscribe">>,
                           subscribe_request, code_response, <<"emqx.exproto.v1.SubscribeRequest">>),
                      Req, Metadata, Options).

-spec unsubscribe(emqx_exproto_pb:unsubscribe_request())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
unsubscribe(Req) ->
    unsubscribe(Req, #{}, #{}).

-spec unsubscribe(emqx_exproto_pb:unsubscribe_request(), grpc:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
unsubscribe(Req, Options) ->
    unsubscribe(Req, #{}, Options).

-spec unsubscribe(emqx_exproto_pb:unsubscribe_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, term()}.
unsubscribe(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exproto.v1.ConnectionAdapter/Unsubscribe">>,
                           unsubscribe_request, code_response, <<"emqx.exproto.v1.UnsubscribeRequest">>),
                      Req, Metadata, Options).

