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

-module(emqx_rule_registry_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_ct:all(?MODULE).

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, Config) ->
    Config.

% t_mnesia(_) ->
%     error('TODO').

% t_dump(_) ->
%     error('TODO').

% t_start_link(_) ->
%     error('TODO').

% t_get_rules_for(_) ->
%     error('TODO').

% t_add_rules(_) ->
%     error('TODO').

% t_remove_rules(_) ->
%     error('TODO').

% t_add_action(_) ->
%     error('TODO').

% t_remove_action(_) ->
%     error('TODO').

% t_remove_actions(_) ->
%     error('TODO').

% t_init(_) ->
%     error('TODO').

% t_handle_call(_) ->
%     error('TODO').

% t_handle_cast(_) ->
%     error('TODO').

% t_handle_info(_) ->
%     error('TODO').

% t_terminate(_) ->
%     error('TODO').

% t_code_change(_) ->
%     error('TODO').

% t_get_resource_types(_) ->
%     error('TODO').

% t_get_resources_by_type(_) ->
%     error('TODO').

% t_get_actions_for(_) ->
%     error('TODO').

% t_get_actions(_) ->
%     error('TODO').

% t_get_action_instance_params(_) ->
%     error('TODO').

% t_remove_action_instance_params(_) ->
%     error('TODO').

% t_remove_resource_params(_) ->
%     error('TODO').

% t_add_action_instance_params(_) ->
%     error('TODO').

% t_add_resource_params(_) ->
%     error('TODO').

% t_find_action(_) ->
%     error('TODO').

% t_get_rules(_) ->
%     error('TODO').

% t_get_resources(_) ->
%     error('TODO').

% t_remove_resource(_) ->
%     error('TODO').

% t_find_resource_params(_) ->
%     error('TODO').

% t_add_resource(_) ->
%     error('TODO').

% t_find_resource_type(_) ->
%     error('TODO').

% t_remove_rule(_) ->
%     error('TODO').

% t_add_rule(_) ->
%     error('TODO').

% t_register_resource_types(_) ->
%     error('TODO').

% t_add_actions(_) ->
%     error('TODO').

% t_unregister_resource_types_of(_) ->
%     error('TODO').

% t_remove_actions_of(_) ->
%     error('TODO').

% t_get_rule(_) ->
%     error('TODO').

% t_find_resource(_) ->
%     error('TODO').

