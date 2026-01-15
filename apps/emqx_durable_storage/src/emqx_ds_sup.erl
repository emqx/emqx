%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_sup).

-behaviour(supervisor).

%% API:
-export([ensure_new_stream_watch/1]).

%% behaviour callbacks:
-export([init/1]).

%% Internal exports
-export([start_link/0, start_link_watch_sup/0]).

%%================================================================================
%% Type declarations
%%================================================================================

-define(TOP, ?MODULE).

-define(WATCH_SUP, emqx_ds_new_streams_watch_sup).

%%================================================================================
%% API functions
%%================================================================================

-spec ensure_new_stream_watch(emqx_ds:db()) -> ok | {error, _}.
ensure_new_stream_watch(DB) ->
    %% Currently children of this supervisor are never stopped. This
    %% is done intentionally: since clients don't monitor (or link to)
    %% the `emqx_ds_new_streams' server, stopping it would leave them
    %% with disfunctional subscriptions. To avoid this, the new stream
    %% subscription servers for each DB should run indefinitely.
    case supervisor:start_child(?WATCH_SUP, [DB]) of
        {ok, _} ->
            ok;
        {error, {already_started, _}} ->
            ok;
        Err ->
            Err
    end.

%%================================================================================
%% behaviour callbacks
%%================================================================================

init(top) ->
    Children = [
        #{
            id => pending_tasks,
            start => {emqx_ds_pending_task_sup, start_link, []},
            type => supervisor,
            restart => permanent,
            shutdown => infinity
        },
        #{
            id => schema,
            start => {emqx_dsch, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 5_000
        },
        #{
            id => db_manager,
            start => {emqx_ds, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 5_000
        },
        emqx_ds_builtin_metrics:child_spec(),
        #{
            id => new_streams_watch_sup,
            start => {?MODULE, start_link_watch_sup, []},
            type => supervisor,
            restart => permanent,
            shutdown => infinity
        }
    ],
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 1
    },
    {ok, {SupFlags, Children}};
init(new_streams_watch_sup) ->
    Flags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 100
    },
    ChildSpec = #{
        id => worker,
        start => {emqx_ds_new_streams, start_link, []},
        restart => transient,
        type => worker
    },
    {ok, {Flags, [ChildSpec]}}.

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    supervisor:start_link({local, ?TOP}, ?MODULE, top).

-spec start_link_watch_sup() -> {ok, pid()}.
start_link_watch_sup() ->
    supervisor:start_link({local, ?WATCH_SUP}, ?MODULE, new_streams_watch_sup).

%%================================================================================
%% Internal functions
%%================================================================================
