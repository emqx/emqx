%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_config_backup).

-export([config_dependencies/1]).

-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%% todo fixme: `emqx_utils_maps:config_key()` is misleading here; currently, the import
%% process only understands atoms....
-type ok_result() :: #{
    root_key => emqx_utils_maps:config_key(),
    changed => [emqx_utils_maps:config_key_path()]
}.

%% todo fixme: `emqx_utils_maps:config_key()` is misleading here; currently, the import
%% process only understands atoms....
-type error_result() :: #{root_key => emqx_utils_maps:config_key(), reason => term()}.

-type config_dependency_ret() :: #{
    root_keys := [binary(), ...],
    dependencies := [module()]
}.

-callback import_config(emqx_config:maybe_namespace(), RawConf :: map()) ->
    {ok, ok_result()}
    | {error, error_result()}
    | {results, {[ok_result()], [error_result()]}}.

-doc """
Declares any dependencies the implementing module has when multiple configuration root
keys are imported at once.
""".
-callback config_dependencies() -> config_dependency_ret().

-optional_callbacks([
    config_dependencies/0
]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec config_dependencies(module()) ->
    undefined | config_dependency_ret().
config_dependencies(Mod) ->
    case erlang:function_exported(Mod, config_dependencies, 0) of
        true ->
            Mod:config_dependencies();
        false ->
            undefined
    end.
