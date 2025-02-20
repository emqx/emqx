%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_purge).

-include("emqx_node_rebalance.hrl").

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    start/1,
    status/0,
    stop/0
]).

-export([start_link/0]).

-behaviour(gen_statem).

-export([
    init/1,
    callback_mode/0,
    handle_event/4,
    code_change/4
]).

-export_type([
    start_opts/0,
    start_error/0,
    stop_error/0
]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-define(DEFAULT_PURGE_RATE, 500).
-define(ENABLE_KIND, purge).

%% gen_statem states
-define(disabled, disabled).
-define(purging, purging).
-define(cleaning_data, cleaning_data).

-type start_opts() :: #{
    purge_rate => pos_integer()
}.
-type start_error() :: already_started.
-type stop_error() :: not_started.
-type stats() :: #{
    initial_sessions := non_neg_integer(),
    current_sessions := non_neg_integer(),
    purge_rate := pos_integer()
}.
-type status() :: {enabled, stats()} | disabled.

-spec start(start_opts()) -> ok_or_error(start_error()).
start(StartOpts) ->
    Opts = maps:merge(default_opts(), StartOpts),
    gen_statem:call(?MODULE, {start, Opts}).

-spec stop() -> ok_or_error(not_started).
stop() ->
    gen_statem:call(?MODULE, stop).

-spec status() -> status().
status() ->
    gen_statem:call(?MODULE, status).

-spec start_link() -> startlink_ret().
start_link() ->
    gen_statem:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_statem callbacks
%%--------------------------------------------------------------------

callback_mode() -> handle_event_function.

%% states: disabled, purging, cleaning_data

init([]) ->
    {ok, disabled, #{}}.

%% start
handle_event({call, From}, {start, Opts}, ?disabled, #{} = Data) ->
    ok = enable_purge(),
    ?SLOG(warning, #{
        msg => "cluster_purge_started",
        opts => Opts
    }),
    NewData = init_data(Data, Opts),
    {next_state, ?purging, NewData, [
        {state_timeout, 0, purge},
        {reply, From, ok}
    ]};
handle_event({call, From}, {start, _Opts}, _State, #{}) ->
    {keep_state_and_data, [{reply, From, {error, already_started}}]};
%% stop
handle_event({call, From}, stop, ?disabled, #{}) ->
    {keep_state_and_data, [{reply, From, {error, not_started}}]};
handle_event({call, From}, stop, _State, Data) ->
    ok = disable_purge(),
    ?SLOG(warning, #{msg => "cluster_purge_stopped"}),
    {next_state, disabled, deinit(Data), [{reply, From, ok}]};
%% status
handle_event({call, From}, status, ?disabled, #{}) ->
    {keep_state_and_data, [{reply, From, disabled}]};
handle_event({call, From}, status, State, Data) ->
    Stats = maps:with(
        [
            initial_sessions,
            current_sessions,
            purge_rate
        ],
        Data
    ),
    {keep_state_and_data, [
        {reply, From, {enabled, Stats#{state => State}}}
    ]};
%% session purge
handle_event(
    state_timeout,
    purge,
    ?purging,
    #{
        purge_rate := PurgeRate
    } = Data
) ->
    case emqx_eviction_agent:all_channels_count() of
        InMemSessions when InMemSessions > 0 ->
            DSCount = emqx_eviction_agent:durable_session_count(),
            Sessions = InMemSessions + DSCount,
            ok = purge_sessions(PurgeRate),
            ?tp(
                warning,
                "cluster_purge_evict_sessions",
                #{
                    count => Sessions,
                    purge_rate => PurgeRate
                }
            ),
            NewData = Data#{current_sessions => Sessions},
            {keep_state, NewData, [{state_timeout, ?EVICT_INTERVAL, purge}]};
        _Sessions = 0 ->
            NewData = Data#{current_conns => 0},
            {keep_state, NewData, [
                {state_timeout, 0, purge_ds}
            ]}
    end;
%% durable session purge
handle_event(
    state_timeout,
    purge_ds,
    ?purging,
    #{
        purge_rate := PurgeRate
    } = Data
) ->
    case purge_durable_sessions(PurgeRate) of
        ok ->
            %% Count is updated asynchronously; better rely on deletion results to known
            %% when to stop.
            Sessions = emqx_eviction_agent:durable_session_count(),
            ?tp(
                warning,
                "cluster_purge_evict_sessions",
                #{
                    count => Sessions,
                    purge_rate => PurgeRate
                }
            ),
            NewData = Data#{current_sessions => Sessions},
            {keep_state, NewData, [{state_timeout, ?EVICT_INTERVAL, purge_ds}]};
        done ->
            ?SLOG(warning, #{msg => "cluster_purge_evict_sessions_done"}),
            {next_state, ?cleaning_data, Data, [
                {state_timeout, 0, clean_retained_messages}
            ]}
    end;
%% retained message purge
handle_event(
    state_timeout,
    clean_retained_messages,
    ?cleaning_data,
    Data
) ->
    ?SLOG(warning, #{msg => "cluster_purge_cleaning_data"}),
    ok = emqx_retainer:clean(),
    ok = emqx_delayed:clear_all(),
    ?tp(warning, "cluster_purge_done", #{}),
    ok = disable_purge(),
    ?tp(warning, "cluster_purge_finished_successfully", #{}),
    {next_state, ?disabled, deinit(Data)};
handle_event({call, From}, Msg, State, Data) ->
    ?SLOG(warning, #{msg => "unknown_call", call => Msg, state => State, data => Data}),
    {keep_state_and_data, [{reply, From, ignored}]};
handle_event(info, Msg, State, Data) ->
    ?SLOG(warning, #{msg => "unknown_info", info => Msg, state => State, data => Data}),
    keep_state_and_data;
handle_event(cast, Msg, State, Data) ->
    ?SLOG(warning, #{msg => "unknown_cast", cast => Msg, state => State, data => Data}),
    keep_state_and_data.

code_change(_Vsn, State, Data, _Extra) ->
    {ok, State, Data}.

%%--------------------------------------------------------------------
%% internal funs
%%--------------------------------------------------------------------

default_opts() ->
    #{
        purge_rate => ?DEFAULT_PURGE_RATE
    }.

init_data(Data0, Opts) ->
    Data1 = maps:merge(Data0, Opts),
    SessCount = emqx_eviction_agent:session_count(),
    Data1#{
        initial_sessions => SessCount,
        current_sessions => SessCount
    }.

deinit(Data) ->
    Keys =
        [
            initial_sessions,
            current_sessions
            | maps:keys(default_opts())
        ],
    maps:without(Keys, Data).

wrap_multicall(Result, Nodes) ->
    case Result of
        {Results, []} ->
            case lists:partition(fun is_ok/1, lists:zip(Nodes, Results)) of
                {_OkResults, []} ->
                    ok;
                {_, BadResults} ->
                    %% we crash on errors so that the coordinator death is signalled to
                    %% the eviction agents in the cluster.
                    error({bad_nodes, BadResults})
            end;
        {_, [_BadNode | _] = BadNodes} ->
            error({bad_nodes, BadNodes})
    end.

is_ok({_Node, {ok, _}}) -> true;
is_ok({_Node, ok}) -> true;
is_ok(_) -> false.

enable_purge() ->
    Nodes = emqx:running_nodes(),
    _ = wrap_multicall(
        emqx_node_rebalance_proto_v4:enable_rebalance_agent(Nodes, self(), ?ENABLE_KIND), Nodes
    ),
    ok.

disable_purge() ->
    Nodes = emqx:running_nodes(),
    _ = wrap_multicall(
        emqx_node_rebalance_proto_v4:disable_rebalance_agent(Nodes, self(), ?ENABLE_KIND), Nodes
    ),
    ok.

purge_sessions(PurgeRate) ->
    Nodes = emqx:running_nodes(),
    %% Currently, purge is not configurable, so we use the inherited infinity value as the timeout.
    Timeout = infinity,
    _ = wrap_multicall(
        emqx_node_rebalance_proto_v4:purge_sessions(Nodes, PurgeRate, Timeout), Nodes
    ),
    ok.

purge_durable_sessions(PurgeRate) ->
    emqx_eviction_agent:purge_durable_sessions(PurgeRate).
