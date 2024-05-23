%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(TOPIC_PREFIX, "$LINK/cluster/").
-define(ROUTE_TOPIC_PREFIX, ?TOPIC_PREFIX "route/").
-define(MSG_TOPIC_PREFIX, ?TOPIC_PREFIX "msg/").

%% Fairly compact text encoding.
-define(SHARED_ROUTE_ID(Topic, Group), <<"$s/", Group/binary, "/", Topic/binary>>).
-define(PERSISTENT_ROUTE_ID(Topic, ID), <<"$p/", ID/binary, "/", Topic/binary>>).
