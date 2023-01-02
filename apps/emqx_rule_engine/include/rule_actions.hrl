%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-compile({parse_transform, emqx_rule_actions_trans}).

-type selected_data() :: map().
-type env_vars() :: map().
-type bindings() :: list({atom(), term()}).

-define(BINDING_KEYS, '__bindings__').

-define(LOG_RULE_ACTION(Level, Metadata, Fmt, Args),
        emqx_rule_utils:log_action(Level, Metadata, Fmt, Args)).

-define(bound_v(Key, ENVS0),
    maps:get(Key,
        maps:get(?BINDING_KEYS, ENVS0, #{}))).

-define(JWT_TABLE, emqx_rule_engine_jwt_table).
