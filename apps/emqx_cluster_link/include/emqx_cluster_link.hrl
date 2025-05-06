%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(TOPIC_PREFIX, "$LINK/cluster/").
-define(TOPIC_PREFIX_WILDCARD, <<?TOPIC_PREFIX "#">>).

-define(ROUTE_TOPIC_PREFIX, ?TOPIC_PREFIX "route/").
-define(MSG_TOPIC_PREFIX, ?TOPIC_PREFIX "msg/").
-define(RESP_TOPIC_PREFIX, ?TOPIC_PREFIX "resp/").

-define(ROUTE_TOPIC(CLUSTER), <<?ROUTE_TOPIC_PREFIX, (CLUSTER)/binary>>).
-define(MSG_FWD_TOPIC(CLUSTER), <<?MSG_TOPIC_PREFIX, (CLUSTER)/binary>>).
-define(RESP_TOPIC(CLUSTER, ACTOR),
    <<?RESP_TOPIC_PREFIX, (CLUSTER)/binary, "/", (atom_to_binary(ACTOR))/binary>>
).

-define(PS_ROUTE_ACTOR, 'ps-routes-v1').

%% Fairly compact text encoding.
-define(SHARED_ROUTE_ID(Topic, Group),
    <<"$s/", Group/binary, "/", Topic/binary>>
).
-define(PERSISTENT_ROUTE_ID(Topic, ID),
    <<"$p/", ID/binary, "/", Topic/binary>>
).
-define(PERSISTENT_SHARED_ROUTE_ID(Topic, Group, ID),
    <<"$sp/", Group/binary, "/", ID/binary, "/", Topic/binary>>
).

-define(METRIC_NAME, cluster_link).

-define(route_metric, 'routes').
