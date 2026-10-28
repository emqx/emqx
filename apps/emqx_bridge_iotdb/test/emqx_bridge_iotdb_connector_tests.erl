%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iotdb_connector_tests).

-include_lib("eunit/include/eunit.hrl").

active_health_check_not_supported_test() ->
    ?assertNot(
        lists:keymember(
            active_health_check, 1, emqx_bridge_iotdb_connector:fields("config_restapi")
        )
    ).
