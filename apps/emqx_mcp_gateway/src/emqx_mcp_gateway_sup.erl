%%%-------------------------------------------------------------------
%% @doc emqx_mcp_gateway top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(emqx_mcp_gateway_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    _ = mria:wait_for_tables(emqx_mcp_server_name_manager:create_tables()),
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 3,
        period => 10
    },
    ServerNameManager = #{
        id => emqx_mcp_server_name_manager,
        start => {emqx_mcp_server_name_manager, start_link, []},
        restart => permanent,
        shutdown => 1000,
        type => worker
    },
    ChildSpecs = [ServerNameManager],
    {ok, {SupFlags, ChildSpecs}}.
