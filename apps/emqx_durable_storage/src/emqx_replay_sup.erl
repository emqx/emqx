%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_replay_sup).

-behaviour(supervisor).

%% API:
-export([start_link/0]).

%% behaviour callbacks:
-export([init/1]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(SUP, ?MODULE).

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?SUP}, ?MODULE, []).

%%================================================================================
%% behaviour callbacks
%%================================================================================

init([]) ->
    Children = [shard_sup()],
    SupFlags = #{
        strategy => one_for_all,
        intensity => 0,
        period => 1
    },
    {ok, {SupFlags, Children}}.

%%================================================================================
%% Internal functions
%%================================================================================

shard_sup() ->
    #{
        id => local_store_shard_sup,
        start => {emqx_replay_local_store_sup, start_link, []},
        restart => permanent,
        type => supervisor,
        shutdown => infinity
    }.
