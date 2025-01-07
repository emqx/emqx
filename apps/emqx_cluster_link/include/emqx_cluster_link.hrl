%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(TOPIC_PREFIX, "$LINK/cluster/").
-define(TOPIC_PREFIX_WILDCARD, <<?TOPIC_PREFIX "#">>).

-define(ROUTE_TOPIC_PREFIX, ?TOPIC_PREFIX "route/").
-define(MSG_TOPIC_PREFIX, ?TOPIC_PREFIX "msg/").
-define(RESP_TOPIC_PREFIX, ?TOPIC_PREFIX "resp/").

-define(MY_CLUSTER_NAME, emqx_cluster_link_config:cluster()).
-define(ROUTE_TOPIC, <<?ROUTE_TOPIC_PREFIX, (?MY_CLUSTER_NAME)/binary>>).
-define(MSG_FWD_TOPIC, <<?MSG_TOPIC_PREFIX, (?MY_CLUSTER_NAME)/binary>>).
-define(RESP_TOPIC(Actor), <<?RESP_TOPIC_PREFIX, (?MY_CLUSTER_NAME)/binary, "/", Actor/binary>>).

%% Fairly compact text encoding.
-define(SHARED_ROUTE_ID(Topic, Group), <<"$s/", Group/binary, "/", Topic/binary>>).
-define(PERSISTENT_ROUTE_ID(Topic, ID), <<"$p/", ID/binary, "/", Topic/binary>>).

-define(METRIC_NAME, cluster_link).

-define(route_metric, 'routes').
-define(PERSISTENT_SHARED_ROUTE_ID(Topic, Group, ID),
    <<"$sp/", Group/binary, "/", ID/binary, "/", Topic/binary>>
).
