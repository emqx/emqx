%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_cluster_link_bookkeeper).

%% API
-export([
    start_link/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-ifdef(TEST).
%% ms
-define(TALLY_ROUTES_INTERVAL, 300).
-else.
%% ms
-define(TALLY_ROUTES_INTERVAL, 15_000).
-endif.

%% call/cast/info events
-record(tally_routes, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, _InitOpts = #{}, _Opts = []).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    State = #{},
    {ok, State, {continue, #tally_routes{}}}.

handle_continue(#tally_routes{}, State) ->
    handle_tally_routes(),
    {noreply, State}.

handle_call(_Call, _From, State) ->
    {reply, {error, bad_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#tally_routes{}, State) ->
    handle_tally_routes(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

cluster_names() ->
    Links = emqx_cluster_link_config:links(),
    lists:map(fun(#{name := Name}) -> Name end, Links).

ensure_timer(Event, Timeout) ->
    _ = erlang:send_after(Timeout, self(), Event),
    ok.

handle_tally_routes() ->
    ClusterNames = cluster_names(),
    tally_routes(ClusterNames),
    ensure_timer(#tally_routes{}, ?TALLY_ROUTES_INTERVAL),
    ok.

tally_routes([ClusterName | ClusterNames]) ->
    NumRoutes = emqx_cluster_link_extrouter:count(ClusterName),
    emqx_cluster_link_metrics:routes_set(ClusterName, NumRoutes),
    tally_routes(ClusterNames);
tally_routes([]) ->
    ok.
