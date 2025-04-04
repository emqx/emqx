%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-define(HP_SCHEMA_VALIDATION, 945).
-define(HP_MESSAGE_TRANSFORMATION, 943).
-define(HP_TOPIC_METRICS, 940).
-define(HP_RETAINER, 930).
-define(HP_AUTO_SUB, 920).
-define(HP_RULE_ENGINE, 900).
%% apps that can work with the republish action
-define(HP_SLOW_SUB, 880).
-define(HP_BRIDGE, 870).
-define(HP_DELAY_PUB, 860).
%% apps that can stop the hooks chain from continuing
-define(HP_NODE_REBALANCE, 110).
-define(HP_EXHOOK, 100).

%% == Lowest Priority = 0, don't change this value as the plugins may depend on it.
-define(HP_LOWEST, 0).
