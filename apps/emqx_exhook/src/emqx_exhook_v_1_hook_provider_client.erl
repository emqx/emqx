%%%-------------------------------------------------------------------
%% @doc Client module for grpc service emqx.exhook.v1.HookProvider.
%% @end
%%%-------------------------------------------------------------------

%% this module was generated and should not be modified manually

-module(emqx_exhook_v_1_hook_provider_client).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("grpc/include/grpc.hrl").

-define(SERVICE, 'emqx.exhook.v1.HookProvider').
-define(PROTO_MODULE, 'emqx_exhook_pb').
-define(MARSHAL(T), fun(I) -> ?PROTO_MODULE:encode_msg(I, T) end).
-define(UNMARSHAL(T), fun(I) -> ?PROTO_MODULE:decode_msg(I, T) end).
-define(DEF(Path, Req, Resp, MessageType),
        #{path => Path,
          service =>?SERVICE,
          message_type => MessageType,
          marshal => ?MARSHAL(Req),
          unmarshal => ?UNMARSHAL(Resp)}).

-spec on_provider_loaded(emqx_exhook_pb:provider_loaded_request())
    -> {ok, emqx_exhook_pb:loaded_response(), grpc:metadata()}
     | {error, term()}.
on_provider_loaded(Req) ->
    on_provider_loaded(Req, #{}, #{}).

-spec on_provider_loaded(emqx_exhook_pb:provider_loaded_request(), grpc:options())
    -> {ok, emqx_exhook_pb:loaded_response(), grpc:metadata()}
     | {error, term()}.
on_provider_loaded(Req, Options) ->
    on_provider_loaded(Req, #{}, Options).

-spec on_provider_loaded(emqx_exhook_pb:provider_loaded_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:loaded_response(), grpc:metadata()}
     | {error, term()}.
on_provider_loaded(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnProviderLoaded">>,
                           provider_loaded_request, loaded_response, <<"emqx.exhook.v1.ProviderLoadedRequest">>),
                      Req, Metadata, Options).

-spec on_provider_unloaded(emqx_exhook_pb:provider_unloaded_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_provider_unloaded(Req) ->
    on_provider_unloaded(Req, #{}, #{}).

-spec on_provider_unloaded(emqx_exhook_pb:provider_unloaded_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_provider_unloaded(Req, Options) ->
    on_provider_unloaded(Req, #{}, Options).

-spec on_provider_unloaded(emqx_exhook_pb:provider_unloaded_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_provider_unloaded(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnProviderUnloaded">>,
                           provider_unloaded_request, empty_success, <<"emqx.exhook.v1.ProviderUnloadedRequest">>),
                      Req, Metadata, Options).

-spec on_client_connect(emqx_exhook_pb:client_connect_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_connect(Req) ->
    on_client_connect(Req, #{}, #{}).

-spec on_client_connect(emqx_exhook_pb:client_connect_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_connect(Req, Options) ->
    on_client_connect(Req, #{}, Options).

-spec on_client_connect(emqx_exhook_pb:client_connect_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_connect(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnClientConnect">>,
                           client_connect_request, empty_success, <<"emqx.exhook.v1.ClientConnectRequest">>),
                      Req, Metadata, Options).

-spec on_client_connack(emqx_exhook_pb:client_connack_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_connack(Req) ->
    on_client_connack(Req, #{}, #{}).

-spec on_client_connack(emqx_exhook_pb:client_connack_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_connack(Req, Options) ->
    on_client_connack(Req, #{}, Options).

-spec on_client_connack(emqx_exhook_pb:client_connack_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_connack(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnClientConnack">>,
                           client_connack_request, empty_success, <<"emqx.exhook.v1.ClientConnackRequest">>),
                      Req, Metadata, Options).

-spec on_client_connected(emqx_exhook_pb:client_connected_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_connected(Req) ->
    on_client_connected(Req, #{}, #{}).

-spec on_client_connected(emqx_exhook_pb:client_connected_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_connected(Req, Options) ->
    on_client_connected(Req, #{}, Options).

-spec on_client_connected(emqx_exhook_pb:client_connected_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_connected(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnClientConnected">>,
                           client_connected_request, empty_success, <<"emqx.exhook.v1.ClientConnectedRequest">>),
                      Req, Metadata, Options).

-spec on_client_disconnected(emqx_exhook_pb:client_disconnected_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_disconnected(Req) ->
    on_client_disconnected(Req, #{}, #{}).

-spec on_client_disconnected(emqx_exhook_pb:client_disconnected_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_disconnected(Req, Options) ->
    on_client_disconnected(Req, #{}, Options).

-spec on_client_disconnected(emqx_exhook_pb:client_disconnected_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_disconnected(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnClientDisconnected">>,
                           client_disconnected_request, empty_success, <<"emqx.exhook.v1.ClientDisconnectedRequest">>),
                      Req, Metadata, Options).

-spec on_client_authenticate(emqx_exhook_pb:client_authenticate_request())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, term()}.
on_client_authenticate(Req) ->
    on_client_authenticate(Req, #{}, #{}).

-spec on_client_authenticate(emqx_exhook_pb:client_authenticate_request(), grpc:options())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, term()}.
on_client_authenticate(Req, Options) ->
    on_client_authenticate(Req, #{}, Options).

-spec on_client_authenticate(emqx_exhook_pb:client_authenticate_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, term()}.
on_client_authenticate(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnClientAuthenticate">>,
                           client_authenticate_request, valued_response, <<"emqx.exhook.v1.ClientAuthenticateRequest">>),
                      Req, Metadata, Options).

-spec on_client_check_acl(emqx_exhook_pb:client_check_acl_request())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, term()}.
on_client_check_acl(Req) ->
    on_client_check_acl(Req, #{}, #{}).

-spec on_client_check_acl(emqx_exhook_pb:client_check_acl_request(), grpc:options())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, term()}.
on_client_check_acl(Req, Options) ->
    on_client_check_acl(Req, #{}, Options).

-spec on_client_check_acl(emqx_exhook_pb:client_check_acl_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, term()}.
on_client_check_acl(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnClientCheckAcl">>,
                           client_check_acl_request, valued_response, <<"emqx.exhook.v1.ClientCheckAclRequest">>),
                      Req, Metadata, Options).

-spec on_client_subscribe(emqx_exhook_pb:client_subscribe_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_subscribe(Req) ->
    on_client_subscribe(Req, #{}, #{}).

-spec on_client_subscribe(emqx_exhook_pb:client_subscribe_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_subscribe(Req, Options) ->
    on_client_subscribe(Req, #{}, Options).

-spec on_client_subscribe(emqx_exhook_pb:client_subscribe_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_subscribe(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnClientSubscribe">>,
                           client_subscribe_request, empty_success, <<"emqx.exhook.v1.ClientSubscribeRequest">>),
                      Req, Metadata, Options).

-spec on_client_unsubscribe(emqx_exhook_pb:client_unsubscribe_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_unsubscribe(Req) ->
    on_client_unsubscribe(Req, #{}, #{}).

-spec on_client_unsubscribe(emqx_exhook_pb:client_unsubscribe_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_unsubscribe(Req, Options) ->
    on_client_unsubscribe(Req, #{}, Options).

-spec on_client_unsubscribe(emqx_exhook_pb:client_unsubscribe_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_client_unsubscribe(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnClientUnsubscribe">>,
                           client_unsubscribe_request, empty_success, <<"emqx.exhook.v1.ClientUnsubscribeRequest">>),
                      Req, Metadata, Options).

-spec on_session_created(emqx_exhook_pb:session_created_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_created(Req) ->
    on_session_created(Req, #{}, #{}).

-spec on_session_created(emqx_exhook_pb:session_created_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_created(Req, Options) ->
    on_session_created(Req, #{}, Options).

-spec on_session_created(emqx_exhook_pb:session_created_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_created(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnSessionCreated">>,
                           session_created_request, empty_success, <<"emqx.exhook.v1.SessionCreatedRequest">>),
                      Req, Metadata, Options).

-spec on_session_subscribed(emqx_exhook_pb:session_subscribed_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_subscribed(Req) ->
    on_session_subscribed(Req, #{}, #{}).

-spec on_session_subscribed(emqx_exhook_pb:session_subscribed_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_subscribed(Req, Options) ->
    on_session_subscribed(Req, #{}, Options).

-spec on_session_subscribed(emqx_exhook_pb:session_subscribed_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_subscribed(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnSessionSubscribed">>,
                           session_subscribed_request, empty_success, <<"emqx.exhook.v1.SessionSubscribedRequest">>),
                      Req, Metadata, Options).

-spec on_session_unsubscribed(emqx_exhook_pb:session_unsubscribed_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_unsubscribed(Req) ->
    on_session_unsubscribed(Req, #{}, #{}).

-spec on_session_unsubscribed(emqx_exhook_pb:session_unsubscribed_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_unsubscribed(Req, Options) ->
    on_session_unsubscribed(Req, #{}, Options).

-spec on_session_unsubscribed(emqx_exhook_pb:session_unsubscribed_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_unsubscribed(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnSessionUnsubscribed">>,
                           session_unsubscribed_request, empty_success, <<"emqx.exhook.v1.SessionUnsubscribedRequest">>),
                      Req, Metadata, Options).

-spec on_session_resumed(emqx_exhook_pb:session_resumed_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_resumed(Req) ->
    on_session_resumed(Req, #{}, #{}).

-spec on_session_resumed(emqx_exhook_pb:session_resumed_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_resumed(Req, Options) ->
    on_session_resumed(Req, #{}, Options).

-spec on_session_resumed(emqx_exhook_pb:session_resumed_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_resumed(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnSessionResumed">>,
                           session_resumed_request, empty_success, <<"emqx.exhook.v1.SessionResumedRequest">>),
                      Req, Metadata, Options).

-spec on_session_discarded(emqx_exhook_pb:session_discarded_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_discarded(Req) ->
    on_session_discarded(Req, #{}, #{}).

-spec on_session_discarded(emqx_exhook_pb:session_discarded_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_discarded(Req, Options) ->
    on_session_discarded(Req, #{}, Options).

-spec on_session_discarded(emqx_exhook_pb:session_discarded_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_discarded(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnSessionDiscarded">>,
                           session_discarded_request, empty_success, <<"emqx.exhook.v1.SessionDiscardedRequest">>),
                      Req, Metadata, Options).

-spec on_session_takeovered(emqx_exhook_pb:session_takeovered_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_takeovered(Req) ->
    on_session_takeovered(Req, #{}, #{}).

-spec on_session_takeovered(emqx_exhook_pb:session_takeovered_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_takeovered(Req, Options) ->
    on_session_takeovered(Req, #{}, Options).

-spec on_session_takeovered(emqx_exhook_pb:session_takeovered_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_takeovered(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnSessionTakeovered">>,
                           session_takeovered_request, empty_success, <<"emqx.exhook.v1.SessionTakeoveredRequest">>),
                      Req, Metadata, Options).

-spec on_session_terminated(emqx_exhook_pb:session_terminated_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_terminated(Req) ->
    on_session_terminated(Req, #{}, #{}).

-spec on_session_terminated(emqx_exhook_pb:session_terminated_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_terminated(Req, Options) ->
    on_session_terminated(Req, #{}, Options).

-spec on_session_terminated(emqx_exhook_pb:session_terminated_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_session_terminated(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnSessionTerminated">>,
                           session_terminated_request, empty_success, <<"emqx.exhook.v1.SessionTerminatedRequest">>),
                      Req, Metadata, Options).

-spec on_message_publish(emqx_exhook_pb:message_publish_request())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, term()}.
on_message_publish(Req) ->
    on_message_publish(Req, #{}, #{}).

-spec on_message_publish(emqx_exhook_pb:message_publish_request(), grpc:options())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, term()}.
on_message_publish(Req, Options) ->
    on_message_publish(Req, #{}, Options).

-spec on_message_publish(emqx_exhook_pb:message_publish_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:valued_response(), grpc:metadata()}
     | {error, term()}.
on_message_publish(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnMessagePublish">>,
                           message_publish_request, valued_response, <<"emqx.exhook.v1.MessagePublishRequest">>),
                      Req, Metadata, Options).

-spec on_message_delivered(emqx_exhook_pb:message_delivered_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_message_delivered(Req) ->
    on_message_delivered(Req, #{}, #{}).

-spec on_message_delivered(emqx_exhook_pb:message_delivered_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_message_delivered(Req, Options) ->
    on_message_delivered(Req, #{}, Options).

-spec on_message_delivered(emqx_exhook_pb:message_delivered_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_message_delivered(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnMessageDelivered">>,
                           message_delivered_request, empty_success, <<"emqx.exhook.v1.MessageDeliveredRequest">>),
                      Req, Metadata, Options).

-spec on_message_dropped(emqx_exhook_pb:message_dropped_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_message_dropped(Req) ->
    on_message_dropped(Req, #{}, #{}).

-spec on_message_dropped(emqx_exhook_pb:message_dropped_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_message_dropped(Req, Options) ->
    on_message_dropped(Req, #{}, Options).

-spec on_message_dropped(emqx_exhook_pb:message_dropped_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_message_dropped(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnMessageDropped">>,
                           message_dropped_request, empty_success, <<"emqx.exhook.v1.MessageDroppedRequest">>),
                      Req, Metadata, Options).

-spec on_message_acked(emqx_exhook_pb:message_acked_request())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_message_acked(Req) ->
    on_message_acked(Req, #{}, #{}).

-spec on_message_acked(emqx_exhook_pb:message_acked_request(), grpc:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_message_acked(Req, Options) ->
    on_message_acked(Req, #{}, Options).

-spec on_message_acked(emqx_exhook_pb:message_acked_request(), grpc:metadata(), grpc_client:options())
    -> {ok, emqx_exhook_pb:empty_success(), grpc:metadata()}
     | {error, term()}.
on_message_acked(Req, Metadata, Options) ->
    grpc_client:unary(?DEF(<<"/emqx.exhook.v1.HookProvider/OnMessageAcked">>,
                           message_acked_request, empty_success, <<"emqx.exhook.v1.MessageAckedRequest">>),
                      Req, Metadata, Options).

