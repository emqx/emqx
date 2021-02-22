%%%-------------------------------------------------------------------
%% @doc Behaviour to implement for grpc service emqx.exhook.v1.HookProvider.
%% @end
%%%-------------------------------------------------------------------

%% this module was generated and should not be modified manually

-module(emqx_exhook_v_1_hook_provider_bhvr).

-callback on_provider_loaded(emqx_exhook_pb:provider_loaded_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:loaded_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_provider_unloaded(emqx_exhook_pb:provider_unloaded_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_client_connect(emqx_exhook_pb:client_connect_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_client_connack(emqx_exhook_pb:client_connack_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_client_connected(emqx_exhook_pb:client_connected_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_client_disconnected(emqx_exhook_pb:client_disconnected_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_client_authenticate(emqx_exhook_pb:client_authenticate_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_client_check_acl(emqx_exhook_pb:client_check_acl_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_client_subscribe(emqx_exhook_pb:client_subscribe_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_client_unsubscribe(emqx_exhook_pb:client_unsubscribe_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_session_created(emqx_exhook_pb:session_created_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_session_subscribed(emqx_exhook_pb:session_subscribed_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_session_unsubscribed(emqx_exhook_pb:session_unsubscribed_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_session_resumed(emqx_exhook_pb:session_resumed_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_session_discarded(emqx_exhook_pb:session_discarded_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_session_takeovered(emqx_exhook_pb:session_takeovered_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_session_terminated(emqx_exhook_pb:session_terminated_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_message_publish(emqx_exhook_pb:message_publish_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_message_delivered(emqx_exhook_pb:message_delivered_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_message_dropped(emqx_exhook_pb:message_dropped_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

-callback on_message_acked(emqx_exhook_pb:message_acked_request(), grpc:metadata())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, grpc_stream:error_response()}.

