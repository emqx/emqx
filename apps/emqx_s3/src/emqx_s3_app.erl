%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_s3_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    {ok, Sup} = emqx_s3_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok.
