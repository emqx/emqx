%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_s3_app).

-behaviour(application).
-export([start/2, stop/1]).

%%

start(_StartType, _StartArgs) ->
    emqx_bridge_s3_sup:start_link().

stop(_State) ->
    ok.
