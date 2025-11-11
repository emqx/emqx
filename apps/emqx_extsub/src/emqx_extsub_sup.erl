%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_sup).

-behaviour(supervisor).

-export([
    start_link/0
]).

-export([init/1]).

-define(ROOT_SUP, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?ROOT_SUP}, ?MODULE, ?ROOT_SUP).

%% Internals

init(?ROOT_SUP) ->
    ok = emqx_extsub_handler_registry:init(),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [
        emqx_extsub_metrics:child_spec()
    ],
    {ok, {SupFlags, ChildSpecs}}.
