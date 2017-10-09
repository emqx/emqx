%%
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. All Rights Reserved.
%%
%% @doc EMQ X Distributed RPC.
%%

-module(emqx_rpc).

-author("Feng Lee <feng@emqtt.io>").

-export([cast/4]).

%% @doc Wraps gen_rpc first.
cast(Node, Mod, Fun, Args) ->
    emqx_metrics:inc('messages/forward'),
    gen_rpc:cast({Node, erlang:system_info(scheduler_id)}, Mod, Fun, Args).

