%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_config_backup).

-export([]).

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

-callback import_config(emqx_config:maybe_namespace(), RawConf :: map()) ->
    {ok, ok_result()}
    | {error, error_result()}
    | {results, {[ok_result()], [error_result()]}}.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------
