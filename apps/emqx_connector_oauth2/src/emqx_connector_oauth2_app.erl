%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQX Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_oauth2_app).

-behaviour(application).

%% `application' API
-export([start/2, stop/1]).

%%------------------------------------------------------------------------------
%% `application' API
%%------------------------------------------------------------------------------

start(_StartType, _StartArgs) ->
    emqx_connector_oauth2_sup:start_link().

stop(_State) ->
    ok.
