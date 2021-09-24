%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Define the default actions.
-module(emqx_rule_outputs).
-include_lib("emqx/include/logger.hrl").

-export([ console/2
        , get_selected_data/2
        ]).

-spec console(map(), map()) -> any().
console(Selected, #{metadata := #{rule_id := RuleId}} = Envs) ->
    ?ULOG("[rule output] ~s~n"
          "\tOutput Data: ~p~n"
          "\tEnvs: ~p~n", [RuleId, Selected, Envs]).

get_selected_data(Selected, _Envs) ->
     Selected.
