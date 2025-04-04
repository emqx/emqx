%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_app).

-export([start/2]).

start(_Type, _Args) ->
    emqx_ds_sup:start_link().
