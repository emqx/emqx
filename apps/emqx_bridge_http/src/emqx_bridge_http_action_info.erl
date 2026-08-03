%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_http_action_info).

-behaviour(emqx_action_info).

-export([
    action_type_name/0,
    connector_type_name/0,
    schema_module/0,
    action_convert_from_connector/2
]).

action_type_name() -> http.

connector_type_name() -> http.

schema_module() -> emqx_bridge_http_schema.

%% Validate fields whose compatibility depends on the referenced connector
%% before the action configuration is persisted.
action_convert_from_connector(ConnectorConfig, ActionConfig) ->
    Parameters = conf_get(parameters, ActionConfig, #{}),
    Headers = conf_get(headers, Parameters, #{}),
    Oauth2 = normalize_oauth2(conf_get(oauth2, ConnectorConfig, undefined)),
    case emqx_connector_oauth2_schema:validate(Headers, Oauth2) of
        ok ->
            ActionConfig;
        {error, #{message := Message}} ->
            throw(#{
                kind => validation_error,
                reason => Message
            })
    end.

normalize_oauth2(undefined) ->
    undefined;
normalize_oauth2(Oauth2) ->
    #{enable => conf_get(enable, Oauth2, false)}.

conf_get(Key, Config, Default) ->
    maps:get(Key, Config, maps:get(atom_to_binary(Key), Config, Default)).
