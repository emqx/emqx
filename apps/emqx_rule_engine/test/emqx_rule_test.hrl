%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% Test Suite funcs
-import(emqx_rule_test_lib,
        [ stop_apps/0
        , start_apps/0
        ]).

%% RULE helper funcs
-import(emqx_rule_test_lib,
        [ create_simple_repub_rule/2
        , create_simple_repub_rule/3
        , make_simple_debug_resource_type/0
        , init_events_counters/0
        ]).
