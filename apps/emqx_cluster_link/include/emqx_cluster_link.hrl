%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-define(TOPIC_PREFIX, "$LINK/cluster/").
-define(CTRL_TOPIC_PREFIX, ?TOPIC_PREFIX "ctrl/").
-define(ROUTE_TOPIC_PREFIX, ?TOPIC_PREFIX "route/").
-define(MSG_TOPIC_PREFIX, ?TOPIC_PREFIX "msg/").

-define(DEST(FromClusterName), {external, {link, FromClusterName}}).
