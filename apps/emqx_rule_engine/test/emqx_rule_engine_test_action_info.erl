%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_rule_engine_test_action_info).

-behaviour(emqx_action_info).

-export([
    bridge_v1_type_name/0,
    action_type_name/0,
    connector_type_name/0,
    schema_module/0
]).

-export([
    namespace/0,
    roots/0,
    fields/1,
    desc/1
]).

-define(CONNECTOR_TYPE, rule_engine_test).
-define(ACTION_TYPE, ?CONNECTOR_TYPE).

bridge_v1_type_name() -> ?ACTION_TYPE.

action_type_name() -> ?ACTION_TYPE.

connector_type_name() -> ?ACTION_TYPE.

schema_module() -> emqx_rule_engine_test_action_info.

%% -------------------------------------------------------------------------------------------------
%% Hocon Schema Definitions

namespace() -> "bridge_test_action_info".

roots() -> [].

fields(Field) when
    Field == "get_connector";
    Field == "put_connector";
    Field == "post_connector"
->
    Fields =
        fields(connector_fields) ++
            emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts),
    emqx_connector_schema:api_fields(Field, ?CONNECTOR_TYPE, Fields);
fields(Field) when
    Field == "get_bridge_v2";
    Field == "post_bridge_v2";
    Field == "put_bridge_v2"
->
    emqx_bridge_v2_schema:api_fields(Field, ?ACTION_TYPE, fields(rule_engine_test_action));
fields(action) ->
    {?ACTION_TYPE,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(?MODULE, rule_engine_test_action)),
            #{
                desc => <<"Test Action Config">>,
                required => false
            }
        )};
fields(rule_engine_test_action) ->
    emqx_bridge_v2_schema:make_producer_action_schema(
        hoconsc:mk(
            hoconsc:ref(?MODULE, action_parameters),
            #{
                required => true,
                desc => undefined
            }
        )
    );
fields(action_parameters) ->
    [
        {values,
            hoconsc:mk(
                typerefl:map(),
                #{desc => undefined, default => #{}}
            )}
    ];
fields("config_connector") ->
    emqx_connector_schema:common_fields() ++
        fields(connector_fields) ++
        emqx_connector_schema:resource_opts_ref(?MODULE, connector_resource_opts);
fields(connector_resource_opts) ->
    emqx_connector_schema:resource_opts_fields();
fields("config") ->
    emqx_resource_schema:fields("resource_opts") ++
        fields(connector_fields);
fields(connector_fields) ->
    [
        {values,
            hoconsc:mk(
                typerefl:map(),
                #{desc => undefined, default => #{}}
            )}
    ].
desc(_) ->
    undefined.
