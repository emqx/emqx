%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_replay_app).

-export([start/2]).

start(_Type, _Args) ->
    emqx_replay_sup:start_link().
