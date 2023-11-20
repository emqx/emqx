%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge_v2_tests).

-include_lib("eunit/include/eunit.hrl").

resource_opts_union_connector_actions_test() ->
    %% The purpose of this test is to ensure we have split `resource_opts' fields
    %% consciouly between connector and actions, in particular when/if we introduce new
    %% fields there.
    AllROFields = non_deprecated_fields(emqx_resource_schema:create_opts([])),
    ActionROFields = non_deprecated_fields(emqx_bridge_v2_schema:resource_opts_fields()),
    ConnectorROFields = non_deprecated_fields(emqx_connector_schema:resource_opts_fields()),
    UnionROFields = lists:usort(ConnectorROFields ++ ActionROFields),
    ?assertEqual(
        lists:usort(AllROFields),
        UnionROFields,
        #{
            missing_fields => AllROFields -- UnionROFields,
            unexpected_fields => UnionROFields -- AllROFields,
            action_fields => ActionROFields,
            connector_fields => ConnectorROFields
        }
    ),
    ok.

non_deprecated_fields(Fields) ->
    [K || {K, Schema} <- Fields, not hocon_schema:is_deprecated(Schema)].
