%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
% -define(MAX(MAXV), ?VALID:max(number, MAXV)).
-define(MIN(MINV), ?VALID:min(number, MINV)).

-define(MYSQL_DEFAULT_PORT, 3306).
-define(MONGO_DEFAULT_PORT, 27017).
-define(REDIS_DEFAULT_PORT, 6379).

-define(AUTO_RECONNECT_INTERVAL, 2).

-define(CONNECTOR_RESOURCE_GROUP, <<"emqx_connector">>).
