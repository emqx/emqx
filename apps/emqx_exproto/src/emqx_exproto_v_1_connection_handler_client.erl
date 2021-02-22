%%%-------------------------------------------------------------------
%% @doc Client module for grpc service emqx.exproto.v1.ConnectionHandler.
%% @end
%%%-------------------------------------------------------------------

%% this module was generated and should not be modified manually

-module(emqx_exproto_v_1_connection_handler_client).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("grpc/include/grpc.hrl").

-define(SERVICE, 'emqx.exproto.v1.ConnectionHandler').
-define(PROTO_MODULE, 'emqx_exproto_pb').
-define(MARSHAL(T), fun(I) -> ?PROTO_MODULE:encode_msg(I, T) end).
-define(UNMARSHAL(T), fun(I) -> ?PROTO_MODULE:decode_msg(I, T) end).
-define(DEF(Path, Req, Resp, MessageType),
        #{path => Path,
          service =>?SERVICE,
          message_type => MessageType,
          marshal => ?MARSHAL(Req),
          unmarshal => ?UNMARSHAL(Resp)}).

-spec on_socket_created(grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
on_socket_created(Options) ->
    on_socket_created(#{}, Options).

-spec on_socket_created(grpc:metadata(), grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
on_socket_created(Metadata, Options) ->
    grpc_client:open(?DEF(<<"/emqx.exproto.v1.ConnectionHandler/OnSocketCreated">>,
                          socket_created_request, empty_success, <<"emqx.exproto.v1.SocketCreatedRequest">>),
                     Metadata, Options).

-spec on_socket_closed(grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
on_socket_closed(Options) ->
    on_socket_closed(#{}, Options).

-spec on_socket_closed(grpc:metadata(), grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
on_socket_closed(Metadata, Options) ->
    grpc_client:open(?DEF(<<"/emqx.exproto.v1.ConnectionHandler/OnSocketClosed">>,
                          socket_closed_request, empty_success, <<"emqx.exproto.v1.SocketClosedRequest">>),
                     Metadata, Options).

-spec on_received_bytes(grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
on_received_bytes(Options) ->
    on_received_bytes(#{}, Options).

-spec on_received_bytes(grpc:metadata(), grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
on_received_bytes(Metadata, Options) ->
    grpc_client:open(?DEF(<<"/emqx.exproto.v1.ConnectionHandler/OnReceivedBytes">>,
                          received_bytes_request, empty_success, <<"emqx.exproto.v1.ReceivedBytesRequest">>),
                     Metadata, Options).

-spec on_timer_timeout(grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
on_timer_timeout(Options) ->
    on_timer_timeout(#{}, Options).

-spec on_timer_timeout(grpc:metadata(), grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
on_timer_timeout(Metadata, Options) ->
    grpc_client:open(?DEF(<<"/emqx.exproto.v1.ConnectionHandler/OnTimerTimeout">>,
                          timer_timeout_request, empty_success, <<"emqx.exproto.v1.TimerTimeoutRequest">>),
                     Metadata, Options).

-spec on_received_messages(grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
on_received_messages(Options) ->
    on_received_messages(#{}, Options).

-spec on_received_messages(grpc:metadata(), grpc_client:options())
    -> {ok, grpc_client:grpcstream()}
     | {error, term()}.
on_received_messages(Metadata, Options) ->
    grpc_client:open(?DEF(<<"/emqx.exproto.v1.ConnectionHandler/OnReceivedMessages">>,
                          received_messages_request, empty_success, <<"emqx.exproto.v1.ReceivedMessagesRequest">>),
                     Metadata, Options).

