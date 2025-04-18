%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_tdengine_connector_info).

-behaviour(emqx_connector_info).

-export([
    type_name/0,
    bridge_types/0,
    resource_callback_module/0,
    config_transform_module/0,
    config_schema/0,
    schema_module/0,
    api_schema/1,
    validate_config/1
]).

type_name() ->
    tdengine.

bridge_types() ->
    [tdengine].

resource_callback_module() ->
    emqx_bridge_tdengine_connector.

config_transform_module() ->
    emqx_bridge_tdengine_connector.

config_schema() ->
    {tdengine,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_tdengine_connector, "config_connector")),
            #{
                desc => <<"TDengine Connector Config">>,
                validator => fun ?MODULE:validate_config/1,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_tdengine_connector.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_tdengine_connector, <<"tdengine">>, Method
    ).

%% Value level validation
validate_config(#{<<"enable">> := _, <<"description">> := _} = Config) ->
    Username = maps:get(<<"username">>, Config, undefined),
    Password = maps:get(<<"password">>, Config, undefined),
    Token = maps:get(<<"token">>, Config, undefined),
    case {Username, Password, Token} of
        {undefined, undefined, undefined} ->
            {error, <<"The username/password or token field is required">>};
        {_Username, undefined, undefined} ->
            {error, <<"The password field is required when username is provided">>};
        {undefined, _Password, undefined} ->
            {error, <<"The username field is required when password is provided">>};
        {undefined, undefined, _Token} ->
            ok;
        {_Username, _Password, undefined} ->
            ok;
        {_Username, _Password, _Token} ->
            {error, <<"Do not provide both username/password and token">>}
    end;
%% Map level validation
validate_config(_Config) ->
    ok.
