%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
