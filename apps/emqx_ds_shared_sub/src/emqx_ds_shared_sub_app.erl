%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_app).

-behaviour(application).

%% application behaviour callbacks
-export([start/2, stop/1]).

%%------------------------------------------------------------------------------
%% application behaviour callbacks
%%------------------------------------------------------------------------------

-spec start(application:start_type(), term()) -> {ok, pid()}.
start(_Type, _Args) ->
    {ok, Sup} = emqx_ds_shared_sub_sup:start_link(),
    ok = emqx_ds_shared_sub_config:load(),
    case emqx_ds_shared_sub_config:enabled() of
        true ->
            ok = emqx_ds_shared_sub_sup:on_enable();
        false ->
            ok
    end,
    {ok, Sup}.

-spec stop(term()) -> ok.
stop(_State) ->
    ok = emqx_ds_shared_sub_sup:on_disable(),
    ok = emqx_ds_shared_sub_config:unload(),
    ok.
