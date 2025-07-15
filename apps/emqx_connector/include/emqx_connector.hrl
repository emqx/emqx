%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-ifndef(EMQX_CONNECTOR_HRL).
-define(EMQX_CONNECTOR_HRL, true).

-define(VALID, emqx_resource_validator).
-define(NOT_EMPTY(MSG), ?VALID:not_empty(MSG)).
-define(MAX(MAXV), ?VALID:max(number, MAXV)).
-define(MIN(MINV), ?VALID:min(number, MINV)).

-define(MYSQL_DEFAULT_PORT, 3306).
-define(MONGO_DEFAULT_PORT, 27017).
-define(REDIS_DEFAULT_PORT, 6379).
-define(CLICKHOUSE_DEFAULT_PORT, 8123).

-define(AUTO_RECONNECT_INTERVAL, 2).

-define(SERVERS_DESC,
    "A Node list for Cluster to connect to. The nodes should be separated with commas, such as: `Node[,Node].`<br/>"
    "For each Node should be: "
).

-define(SERVER_DESC(TYPE, DEFAULT_PORT),
    "The IPv4 or IPv6 address or the hostname to connect to.<br/>"
    "A host entry has the following form: `Host[:Port]`.<br/>"
    "The " ++ TYPE ++ " default port " ++ DEFAULT_PORT ++ " is used if `[:Port]` is not specified."
).

-define(CONNECTOR_RESOURCE_GROUP, <<"connector">>).

-endif.
