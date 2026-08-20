%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_maptabs_app).

-behaviour(application).

-emqx_plugin(?MODULE).

%% Application callbacks
-export([
    start/2,
    stop/1
]).

%% EMQX Plugin callbacks
-export([
    on_config_changed/2,
    on_health_check/1
]).

start(_StartType, _StartArgs) ->
    {ok, Sup} = emqx_maptabs_sup:start_link(),
    ok = emqx_rule_engine:register_external_functions(emqx_maptabs_rule_funcs),
    ok = emqx_maptabs_cli:load(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_maptabs_cli:unload(),
    ok = emqx_rule_engine:unregister_external_functions(emqx_maptabs_rule_funcs),
    ok.

-doc """
Validate a config update. Every limit must be a positive integer.
A missing field is valid: the schema default applies.
Also called on nodes an accepted config propagates to, where the result
is ignored, so it must stay free of side effects.
""".
on_config_changed(_OldConf, NewConf) ->
    validate_limits(
        [
            <<"max_tables">>,
            <<"max_rows_per_table">>,
            <<"max_table_file_bytes">>
        ],
        NewConf
    ).

validate_limits(_Fields, NewConf) when not is_map(NewConf) ->
    {error, #{reason => config_not_a_map, config => NewConf}};
validate_limits([], _NewConf) ->
    ok;
validate_limits([Field | Rest], NewConf) ->
    case NewConf of
        #{Field := Value} when is_integer(Value), Value > 0 ->
            validate_limits(Rest, NewConf);
        #{Field := Value} ->
            {error, #{
                reason => invalid_config_value,
                field => Field,
                value => Value,
                expected => <<"a positive integer">>
            }};
        #{} ->
            validate_limits(Rest, NewConf)
    end.

on_health_check(_Options) ->
    emqx_maptabs:health_check().
