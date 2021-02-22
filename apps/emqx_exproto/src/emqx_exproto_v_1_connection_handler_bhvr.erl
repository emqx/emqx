%%%-------------------------------------------------------------------
%% @doc Behaviour to implement for grpc service emqx.exproto.v1.ConnectionHandler.
%% @end
%%%-------------------------------------------------------------------

%% this module was generated and should not be modified manually

-module(emqx_exproto_v_1_connection_handler_bhvr).

-callback on_socket_created(grpc_stream:stream(), grpc:metadata())
    -> {ok, grpc_stream:stream()}.

-callback on_socket_closed(grpc_stream:stream(), grpc:metadata())
    -> {ok, grpc_stream:stream()}.

-callback on_received_bytes(grpc_stream:stream(), grpc:metadata())
    -> {ok, grpc_stream:stream()}.

-callback on_timer_timeout(grpc_stream:stream(), grpc:metadata())
    -> {ok, grpc_stream:stream()}.

-callback on_received_messages(grpc_stream:stream(), grpc:metadata())
    -> {ok, grpc_stream:stream()}.

