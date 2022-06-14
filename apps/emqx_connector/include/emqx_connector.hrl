%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-define(VALID, emqx_resource_validator).
-define(NOT_EMPTY(MSG), ?VALID:not_empty(MSG)).
-define(MAX(MAXV), ?VALID:max(number, MAXV)).
-define(MIN(MINV), ?VALID:min(number, MINV)).

-define(MYSQL_DEFAULT_PORT, 3306).
-define(MONGO_DEFAULT_PORT, 27017).
-define(REDIS_DEFAULT_PORT, 6379).
-define(PGSQL_DEFAULT_PORT, 5432).

-define(SERVERS_DESC,
    "A Node list for Cluster to connect to. The nodes should be separated with commas, such as: `Node[,Node].`\n"
    "For each Node should be: "
).

-define(SERVER_DESC(TYPE, DEFAULT_PORT),
    "\n"
    "The IPv4 or IPv6 address or the hostname to connect to.</br>\n"
    "A host entry has the following form: `Host[:Port]`.</br>\n"
    "The " ++ TYPE ++ " default port " ++ DEFAULT_PORT ++ " is used if `[:Port]` is not specified."
).

-define(CONNECTOR_RESOURCE_GROUP, <<"emqx_connector">>).
