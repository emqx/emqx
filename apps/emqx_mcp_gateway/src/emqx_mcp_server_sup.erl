%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mcp_server_sup).
-include("emqx_mcp_gateway.hrl").

-behaviour(supervisor).
%% API
-export([start_link/0, start_child/1]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Opts = [public, named_table, set, {read_concurrency, true}],
    ets:new(?TAB_MCP_ESTAB_SERVERS, Opts),
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 100,
        period => 10
    },
    {ok, {SupFlags, [worker_spec()]}}.

-spec start_child(emqx_mcp_server:config()) -> supervisor:startchild_ret().
start_child(Conf) ->
    supervisor:start_child(?MODULE, [Conf]).

worker_spec() ->
    #{
        id => emqx_mcp_server,
        start => {emqx_mcp_server, start_link, []},
        restart => temporary,
        shutdown => 3_000,
        type => worker
    }.
