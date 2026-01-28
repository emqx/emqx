%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_setopts_app).

-behaviour(application).

-export([start/2, stop/1]).

-doc """
Start the setopts application.
""".
start(_StartType, _StartArgs) ->
    emqx_setopts_sup:start_link().

-doc """
Stop the setopts application.
""".
stop(_State) ->
    ok.
