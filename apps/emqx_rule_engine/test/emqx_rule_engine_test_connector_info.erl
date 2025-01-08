%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_test_connector_info).

-behaviour(emqx_connector_info).

-export([
    type_name/0,
    bridge_types/0,
    resource_callback_module/0,
    config_schema/0,
    schema_module/0,
    api_schema/1
]).

type_name() ->
    rule_engine_test.

bridge_types() ->
    [rule_engine_test].

resource_callback_module() ->
    emqx_rule_engine_test_connector.

config_schema() ->
    {rule_engine_test,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_rule_engine_test_action_info, "config_connector")),
            #{
                desc => <<"Test Connector Config">>,
                required => false
            }
        )}.

schema_module() ->
    emqx_rule_engine_test_action_info.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        ?MODULE, <<"rule_engine_test">>, Method ++ "_connector"
    ).
