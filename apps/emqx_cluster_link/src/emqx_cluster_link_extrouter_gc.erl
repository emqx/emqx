%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_extrouter_gc).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([start_link/0]).

-export([run/0, force/1]).

-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-define(SERVER, ?MODULE).

-ifndef(TEST).
-define(REPEAT_GC_INTERVAL, 5_000).
-else.
-define(REPEAT_GC_INTERVAL, 1_000).
-endif.

%%

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

run() ->
    gen_server:call(?SERVER, run).

force(Timestamp) ->
    case emqx_cluster_link_extrouter:actor_gc(#{timestamp => Timestamp}) of
        1 ->
            force(Timestamp);
        0 ->
            ok
    end.

%%

-record(st, {
    gc_timer :: undefined | reference()
}).

init(_) ->
    {ok, schedule_gc(#st{})}.

handle_call(run, _From, St) ->
    Result = run_gc(),
    Timeout = choose_timeout(Result),
    {reply, Result, reschedule_gc(Timeout, St)};
handle_call(_Call, _From, St) ->
    {reply, ignored, St}.

handle_cast(Cast, State) ->
    ?SLOG(warning, #{msg => "unexpected_cast", cast => Cast}),
    {noreply, State}.

handle_info({timeout, TRef, _GC}, St = #st{gc_timer = TRef}) ->
    Result = run_gc_exclusive(),
    ?tp("clink_extrouter_gc_ran", #{result => Result}),
    Timeout = choose_timeout(Result),
    {noreply, schedule_gc(Timeout, St#st{gc_timer = undefined})};
handle_info(Info, St) ->
    ?SLOG(warning, #{msg => "unexpected_info", info => Info}),
    {noreply, St}.

%%

run_gc_exclusive() ->
    case is_responsible() of
        true -> run_gc();
        false -> 0
    end.

is_responsible() ->
    Nodes = lists:sort(mria_membership:running_core_nodelist()),
    Nodes =/= [] andalso hd(Nodes) == node().

-spec run_gc() -> _NumCleaned :: non_neg_integer().
run_gc() ->
    Env = #{timestamp => erlang:system_time(millisecond)},
    emqx_cluster_link_extrouter:actor_gc(Env).

choose_timeout(_NumCleaned = 0) ->
    emqx_cluster_link_config:actor_gc_interval();
choose_timeout(_NumCleaned) ->
    %% NOTE: There could likely be more outdated actors.
    ?REPEAT_GC_INTERVAL.

schedule_gc(St) ->
    schedule_gc(emqx_cluster_link_config:actor_gc_interval(), St).

schedule_gc(Timeout, St = #st{gc_timer = undefined}) ->
    TRef = erlang:start_timer(Timeout, self(), gc),
    St#st{gc_timer = TRef}.

reschedule_gc(Timeout, St = #st{gc_timer = undefined}) ->
    schedule_gc(Timeout, St);
reschedule_gc(Timeout, St = #st{gc_timer = TRef}) ->
    ok = emqx_utils:cancel_timer(TRef),
    schedule_gc(Timeout, St#st{gc_timer = undefined}).
