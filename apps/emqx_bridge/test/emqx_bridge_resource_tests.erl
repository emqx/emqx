%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge_resource_tests).

-include_lib("eunit/include/eunit.hrl").

bridge_hookpoint_test_() ->
    BridgeId = emqx_bridge_resource:bridge_id(type, name),
    BridgeHookpoint = emqx_bridge_resource:bridge_hookpoint(BridgeId),
    [
        ?_assertEqual(<<"$bridges/type:name">>, BridgeHookpoint),
        ?_assertEqual(
            {ok, BridgeId},
            emqx_bridge_resource:bridge_hookpoint_to_bridge_id(BridgeHookpoint)
        ),
        ?_assertEqual(
            {error, bad_bridge_hookpoint},
            emqx_bridge_resource:bridge_hookpoint_to_bridge_id(BridgeId)
        )
    ].
