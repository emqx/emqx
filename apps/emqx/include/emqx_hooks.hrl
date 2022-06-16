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

%% Highest Priority = 1000, don't change this value as the plugins may depend on it.
-define(HP_HIGHEST, 1000).

%% hooks used by the emqx core app
-define(HP_PSK, 990).
-define(HP_REWRITE, 980).
-define(HP_AUTHN, 970).
-define(HP_AUTHZ, 960).
-define(HP_SYS_MSGS, 950).
-define(HP_TOPIC_METRICS, 940).
-define(HP_RETAINER, 930).
-define(HP_AUTO_SUB, 920).
-define(HP_RULE_ENGINE, 900).
%% apps that can work with the republish action
-define(HP_SLOW_SUB, 880).
-define(HP_BRIDGE, 870).
-define(HP_DELAY_PUB, 860).
%% apps that can stop the hooks chain from continuing
-define(HP_EXHOOK, 100).

%% == Lowest Priority = 0, don't change this value as the plugins may depend on it.
-define(HP_LOWEST, 0).
