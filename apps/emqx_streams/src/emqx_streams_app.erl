%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_app).

-behaviour(application).

-export([start/2, stop/1]).

%% Behaviour callbacks

start(_StartType, _StartArgs) ->
    emqx_conf:add_handler(emqx_streams_schema:roots(), emqx_streams_config),
    ok = mria:wait_for_tables(emqx_streams_registry:create_tables()),
    %% The rest of the startup is orchestrated by `emqx_streams_controller` running under the main supervisor.
    {ok, Sup} = emqx_streams_sup:start_link(),
    {ok, Sup}.

stop(_State) ->
    ok = emqx_conf:remove_handler(emqx_streams_schema:roots()),
    ok.
