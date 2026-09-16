%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(__EMQX_BRIDGE_GCP_PUBSUB_CONSUMER_GRPC_HRL__).
-define(__EMQX_BRIDGE_GCP_PUBSUB_CONSUMER_GRPC_HRL__, true).

-define(CONNECTOR_TYPE, gcp_pubsub_consumer_grpc).
-define(CONNECTOR_TYPE_BIN, <<"gcp_pubsub_consumer_grpc">>).

-define(SOURCE_TYPE, gcp_pubsub_consumer_grpc).
-define(SOURCE_TYPE_BIN, <<"gcp_pubsub_consumer_grpc">>).

-include("emqx_bridge_gcp_pubsub.hrl").

-define(TOP_SUP, emqx_bridge_gcp_pubsub_sup).
-define(SOURCE_SUP, emqx_bridge_gcp_pubsub_consumer_grpc_sup).

-define(PARSE_SERVER_OPTS, #{single_server => true, supported_schemes => ["http", "https"]}).

-define(ack_deadline, ack_deadline).
-define(auth_ctx, auth_ctx).
-define(client_pool, client_pool).
-define(gcp_client, gcp_client).
-define(handle, handle).
-define(hookpoints, hookpoints).
-define(idx, idx).
-define(max_outstanding_messages, max_outstanding_messages).
-define(namespace, namespace).
-define(pending_acks, pending_acks).
-define(pool, pool).
-define(request_ttl, request_ttl).
-define(source_res_id, source_res_id).
-define(subscription_resource, subscription_resource).
-define(topic_resource, topic_resource).
-define(undefined, undefined).

%% END ifndef(__EMQX_BRIDGE_GCP_PUBSUB_CONSUMER_GRPC_HRL__)
-endif.
