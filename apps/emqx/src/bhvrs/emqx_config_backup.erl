%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_config_backup).

-type ok_result() :: #{
    root_key => emqx_utils_maps:config_key(),
    changed => [emqx_utils_maps:config_key_path()]
}.

-type error_result() :: #{root_key => emqx_utils_maps:config_key(), reason => term()}.

-callback import_config(RawConf :: map()) ->
    {ok, ok_result()}
    | {error, error_result()}
    | {results, {[ok_result()], [error_result()]}}.
