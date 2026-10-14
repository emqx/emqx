%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_registry_config).

-behaviour(emqx_config_handler).

-export([post_config_update/5, propagated_post_config_update/5]).

%% API
-export([
    maybe_register_hooks/0,
    is_enabled/0,
    is_schema_validation_enabled/0
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc "Install the hooks when A2A is first enabled. Keep them until the application stops.".
maybe_register_hooks() ->
    maybe_register_hooks(is_enabled()).

post_config_update([a2a_registry, enable], _Request, Enabled, _OldConf, _AppEnvs) ->
    maybe_register_hooks(Enabled).

propagated_post_config_update(Path, Request, Enabled, OldConf, AppEnvs) ->
    post_config_update(Path, Request, Enabled, OldConf, AppEnvs).

maybe_register_hooks(true) ->
    emqx_a2a_registry_hookcb:register_hooks();
maybe_register_hooks(_Disabled) ->
    ok.

is_enabled() ->
    emqx_config:get([a2a_registry, enable], false).

is_schema_validation_enabled() ->
    emqx_config:get([a2a_registry, validate_schema], true).

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------
