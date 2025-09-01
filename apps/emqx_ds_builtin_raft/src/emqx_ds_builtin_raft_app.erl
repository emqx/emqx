%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_builtin_raft_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    ok = emqx_dsch:register_backend(builtin_raft, emqx_ds_builtin_raft),
    emqx_ds_builtin_raft_sup:start_top().

stop(_) ->
    ok.
