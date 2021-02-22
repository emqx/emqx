%%%-------------------------------------------------------------------
%% @doc Behaviour to implement for grpc service emqx.exproto.v1.ConnectionAdapter.
%% @end
%%%-------------------------------------------------------------------

%% this module was generated and should not be modified manually

-module(emqx_exproto_v_1_connection_adapter_bhvr).

-callback send(emqx_exproto_pb:send_bytes_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback close(emqx_exproto_pb:close_socket_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback authenticate(emqx_exproto_pb:authenticate_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback start_timer(emqx_exproto_pb:timer_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback publish(emqx_exproto_pb:publish_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback subscribe(emqx_exproto_pb:subscribe_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback unsubscribe(emqx_exproto_pb:unsubscribe_request(), grpc:metadata())
    -> {ok, emqx_exproto_pb:code_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

