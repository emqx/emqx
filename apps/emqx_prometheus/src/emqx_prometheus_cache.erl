%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_prometheus_cache).

-behaviour(gen_server).

%% API
-export([
    start_link/1,

    get_mria_shard_lag/1
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-include_lib("emqx/include/logger.hrl").

-define(TAB, emqx_prometheus_cache).
-define(VAL_POS, 2).
-define(DEFAULT_MRIA_SHARD_LAG, 0).

-define(MRIA_SHARD_LAG_KEY(SHARD), {mria_shard_lag, SHARD}).

-define(mria_refresh_shard_lag_timer, mria_refresh_shard_lag_timer).
-define(undefined, undefined).

%% Calls/casts/infos/continues
-record(refresh_mria_shard_lag, {}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(Opts) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Opts, []).

get_mria_shard_lag(Shard) ->
    try
        ets:lookup_element(?TAB, ?MRIA_SHARD_LAG_KEY(Shard), ?VAL_POS, ?DEFAULT_MRIA_SHARD_LAG)
    catch
        error:badarg ->
            ?DEFAULT_MRIA_SHARD_LAG
    end.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    _ = ets:new(?TAB, [named_table, public, ordered_set, {read_concurrency, true}]),
    State = #{?mria_refresh_shard_lag_timer => ?undefined},
    {ok, State, {continue, #refresh_mria_shard_lag{}}}.

terminate(_Reason, _State) ->
    ok.

handle_continue(#refresh_mria_shard_lag{}, State0) ->
    State = handle_refresh_mria_shard_lag(State0),
    {noreply, State}.

handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(
    {timeout, TRef, #refresh_mria_shard_lag{}},
    #{?mria_refresh_shard_lag_timer := TRef} = State0
) ->
    State = handle_refresh_mria_shard_lag(State0),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

handle_refresh_mria_shard_lag(State0) ->
    Shards = [S || S <- mria_schema:shards(), S =/= undefined],
    lists:foreach(fun do_refresh_mria_shard_lag/1, Shards),
    ensure_refresh_mria_shard_lag_timer(State0).

do_refresh_mria_shard_lag(Shard) ->
    try mria_status:get_shard_lag(Shard) of
        Val ->
            true = ets:insert(?TAB, {?MRIA_SHARD_LAG_KEY(Shard), Val}),
            ok
    catch
        Kind:Reason:Stacktrace ->
            %% Likely an `erpc` timeout
            ?SLOG(warning, #{
                msg => "prometheus_mria_shard_lag_refresh_exception",
                shard => Shard,
                kind => Kind,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            ok
    end.

ensure_refresh_mria_shard_lag_timer(State0) ->
    #{?mria_refresh_shard_lag_timer := TRef0} = State0,
    emqx_utils:cancel_timer(TRef0),
    Interval = refresh_mria_shard_lag_interval(),
    TRef = emqx_utils:start_timer(Interval, self(), #refresh_mria_shard_lag{}),
    State0#{?mria_refresh_shard_lag_timer := TRef}.

refresh_mria_shard_lag_interval() ->
    emqx_prometheus_config:mria_lag_refresh_interval().
