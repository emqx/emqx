%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_machine_replicant_health_probe).

%% @doc This process performs periodic checks to assess the health of a replicant node.
%% It's not started on core nodes.
%% This process must start before we call `emqx_machine_boot:post_boot' because
%% applications might hang while waiting for tables.

%% API
-export([
    start_link/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include_lib("emqx/include/logger.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(COUNT_CORE_INTERVAL, 30_000).
-define(INITIAL_COUNT_CORE_INTERVAL, 10_000).

%% calls/casts/infos
-record(check_core_node_count, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    State = #{},
    _ = start_core_node_count_timeout(?INITIAL_COUNT_CORE_INTERVAL),
    {ok, State}.

handle_call(_Call, _From, State) ->
    {reply, ignored, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(#check_core_node_count{}, State) ->
    _ = handle_check_core_node_count(),
    _ = start_core_node_count_timeout(),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

handle_check_core_node_count() ->
    case valid_upstream_cores() of
        [] ->
            Hint = iolist_to_binary([
                <<"There are no core EMQX nodes running the same version as this node.">>,
                <<" If you are performing a rolling upgrade, please keep different core node">>,
                <<" versions (both old and new) around until all replicant nodes are migrated.">>
            ]),
            ?SLOG(warning, #{
                msg => "no_available_core_node_at_same_version",
                hint => Hint,
                known_cores => classy:nodes(core)
            }),
            ok;
        [_ | _] ->
            %% Note: there's no need to explicitly check if the local
            %% node is a replicant. On cores `node()' always appears
            %% in the list.
            ok
    end.

start_core_node_count_timeout() ->
    start_core_node_count_timeout(?COUNT_CORE_INTERVAL).

start_core_node_count_timeout(Timeout) ->
    _ = erlang:send_after(Timeout, self(), #check_core_node_count{}),
    ok.

%% Return the list of nodes that are 1. core 2. connected 3. run the
%% same EMQX version
valid_upstream_cores() ->
    ordsets:intersection(
        [
            classy:nodes(emqx_same_vsn),
            classy:nodes(core),
            classy:nodes(connected)
        ]
    ).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------
