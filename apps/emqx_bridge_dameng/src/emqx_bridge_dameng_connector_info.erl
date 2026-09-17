%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_dameng_connector_info).

-behaviour(emqx_connector_info).

-export([
    type_name/0,
    bridge_types/0,
    resource_callback_module/0,
    config_schema/0,
    schema_module/0,
    api_schema/1
]).

-export([validate_dsn_or_server/1]).

type_name() ->
    dameng.

bridge_types() ->
    [dameng].

resource_callback_module() ->
    emqx_bridge_dameng_connector.

config_schema() ->
    {dameng,
        hoconsc:mk(
            hoconsc:map(name, hoconsc:ref(emqx_bridge_dameng, "config_connector")),
            #{
                desc => <<"Dameng DM8 Connector Config">>,
                validator => fun ?MODULE:validate_dsn_or_server/1,
                required => false
            }
        )}.

schema_module() ->
    emqx_bridge_dameng.

api_schema(Method) ->
    emqx_connector_schema:api_ref(
        emqx_bridge_dameng, <<"dameng">>, Method ++ "_connector"
    ).

%% @doc Enforce that a Dameng connector is configured with at least one of
%% `dsn' and `server'.
%%
%% `server' has no default and `dsn' defaults to the empty string, so neither
%% being set is always a config error. Both may be set: `dsn' takes precedence
%% and the other connection fields are ignored (see
%% `emqx_odbc:build_conn_string/1').
%%
%% `hocon' calls this validator twice: once with the outer `#{Name => Config}'
%% map of the `connectors.dameng' field, and once per connector config.
%% Atom-keyed configs (probe/runtime paths) are normalized first.
validate_dsn_or_server(Config) when is_map(Config) ->
    case is_named_connector_map(Config) of
        true -> validate_each(maps:values(Config));
        false -> validate_either(Config)
    end;
validate_dsn_or_server(_Config) ->
    ok.

validate_each(Configs) ->
    case lists:search(fun(Config) -> validate_either(Config) =/= ok end, Configs) of
        false -> ok;
        {value, BadConfig} -> validate_either(BadConfig)
    end.

validate_either(Config0) ->
    Config = emqx_utils_maps:binary_key_map(Config0),
    case is_configured(<<"dsn">>, Config) orelse is_configured(<<"server">>, Config) of
        true ->
            ok;
        false ->
            {error, <<"either 'dsn' or 'server' must be configured">>}
    end.

%% The outer `connectors.dameng' map has connector configs as all of its values
%% (an empty map means "no connectors"), whereas a single connector config
%% always carries non-map fields (e.g. `enable').
is_named_connector_map(Config) ->
    lists:all(fun is_map/1, maps:values(Config)).

is_configured(Key, Config) ->
    case maps:get(Key, Config, undefined) of
        undefined -> false;
        null -> false;
        <<>> -> false;
        "" -> false;
        _ -> true
    end.
