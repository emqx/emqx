%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_local_app).

-behaviour(application).

%% behavior callbacks:
-export([start/2, stop/1]).

%%================================================================================
%% behavior callbacks
%%================================================================================

start(_StartType, _StartArgs) ->
    ok = emqx_dsch:register_backend(builtin_local, emqx_ds_builtin_local),
    emqx_ds_builtin_local_sup:start_top().

stop(_) ->
    ok.
