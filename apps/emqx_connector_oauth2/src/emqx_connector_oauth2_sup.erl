%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQX Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_oauth2_sup).

-behaviour(supervisor).

-include("emqx_connector_oauth2_tables.hrl").

-export([
    start_link/0
]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ok = ensure_token_table(),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 5,
        auto_shutdown => never
    },
    ChildSpecs = [
        #{
            id => emqx_connector_oauth2,
            start => {emqx_connector_oauth2, start_link, []},
            restart => permanent,
            type => worker,
            significant => false,
            shutdown => 5_000,
            modules => [emqx_connector_oauth2]
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.

-spec ensure_token_table() -> ok.
ensure_token_table() ->
    _ = emqx_utils_ets:new(?OAUTH2_TOKEN_TAB, [ordered_set, public, {read_concurrency, true}]),
    ok.
