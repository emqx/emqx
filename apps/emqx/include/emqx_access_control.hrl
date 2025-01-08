%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-ifndef(EMQX_ACCESS_CONTROL_HRL).
-define(EMQX_ACCESS_CONTROL_HRL, true).

-define(EMQX_AUTHORIZATION_CONFIG_ROOT_NAME, "authorization").
-define(EMQX_AUTHORIZATION_CONFIG_ROOT_NAME_ATOM, authorization).
-define(EMQX_AUTHORIZATION_CONFIG_ROOT_NAME_BINARY, <<"authorization">>).

-define(DEFAULT_ACTION_QOS, 0).
-define(DEFAULT_ACTION_RETAIN, false).

-define(AUTHZ_SUBSCRIBE_MATCH_MAP(QOS), #{action_type := subscribe, qos := QOS}).
-define(AUTHZ_SUBSCRIBE(QOS), #{action_type => subscribe, qos => QOS}).
-define(AUTHZ_SUBSCRIBE, ?AUTHZ_SUBSCRIBE(?DEFAULT_ACTION_QOS)).

-define(AUTHZ_PUBLISH_MATCH_MAP(QOS, RETAIN), #{
    action_type := publish, qos := QOS, retain := RETAIN
}).
-define(AUTHZ_PUBLISH(QOS, RETAIN), #{action_type => publish, qos => QOS, retain => RETAIN}).
-define(AUTHZ_PUBLISH(QOS), ?AUTHZ_PUBLISH(QOS, ?DEFAULT_ACTION_RETAIN)).
-define(AUTHZ_PUBLISH, ?AUTHZ_PUBLISH(?DEFAULT_ACTION_QOS)).

-define(authz_action(PUBSUB, QOS), #{action_type := PUBSUB, qos := QOS}).
-define(authz_action(PUBSUB), ?authz_action(PUBSUB, _)).
-define(authz_action, ?authz_action(_)).

-define(AUTHN_TRACE_TAG, "AUTHN").

-endif.
