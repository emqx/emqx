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

%% Definitions for Hook Priorities

%% == Highest Priority
-define(HP_PSK, 1000).
-define(HP_REWRITE, 1000).
-define(HP_AUTHN, 990).
-define(HP_AUTHZ, 980).
-define(HP_SYS_MSGS, 960).
-define(HP_TOPIC_METRICS, 950).
-define(HP_RETAINER, 940).
-define(HP_AUTO_SUB, 930).

-define(HP_RULE_ENGINE, 900).

%% apps that can work with the republish action
-define(HP_SLOW_SUB, 980).
-define(HP_BRIDGE, 970).
-define(HP_DELAY_PUB, 960).

%% apps that can stop the hooks
-define(HP_EXHOOK, 100).
%% == Lowest Priority
