%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ft_assembler_sup).

-export([start_link/0]).
-export([ensure_child/4]).

-behaviour(supervisor).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

ensure_child(Storage, Transfer, Size, Opts) ->
    Childspec = #{
        id => Transfer,
        start => {emqx_ft_assembler, start_link, [Storage, Transfer, Size, Opts]},
        restart => temporary
    },
    case supervisor:start_child(?MODULE, Childspec) of
        {ok, Pid} ->
            {ok, Pid};
        {error, {already_started, Pid}} ->
            {ok, Pid}
    end.

init(_) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1000
    },
    {ok, {SupFlags, []}}.
