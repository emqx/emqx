%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% @doc EMQX License Management Application.
%%--------------------------------------------------------------------

-module(emqx_license_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_Type, _Args) ->
    ok = emqx_license:load(),
    {ok, Sup} = emqx_license_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_license:unload(),
    ok.
