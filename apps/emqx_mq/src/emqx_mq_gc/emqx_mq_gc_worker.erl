%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_gc_worker).

-moduledoc "The module is responsible for garbage collection of Message "
"Queue data.".

-behaviour(gen_server).

-include("../emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/0, child_spec/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_continue/2, terminate/2]).

-define(CONSUME_BATCH_SIZE, 100).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(gc, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    gen_server:start_link(?MODULE, [], []).

child_spec() ->
    #{
        id => ?MODULE,
        start => {?MODULE, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [?MODULE]
    }.

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    erlang:process_flag(trap_exit, true),
    ?tp_debug(mq_gc_worker_started, #{}),
    {ok, #{}, {continue, gc_regular_queues}}.

handle_continue(gc_regular_queues, #{}) ->
    ok = gc_regular_queues(),
    ?tp(info, mq_gc_regular_done, #{}),
    {noreply, start_gc_compacted_queues()}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(#gc{}, State) ->
    gc_next_compacted_batch(State).

terminate(_Reason, _State) ->
    ?tp_debug(mq_gc_worker_terminated, #{reason => _Reason}),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%
%% Compact Queues GC
%%

start_gc_compacted_queues() ->
    ?tp_debug(mq_gc_compacted_queues_started, #{}),
    erlang:send_after(0, self(), #gc{}),
    #{stream => compacted_mq_stream()}.

gc_next_compacted_batch(#{stream := Stream0} = State) ->
    case emqx_utils_stream:consume(?CONSUME_BATCH_SIZE, Stream0) of
        {MQs, Stream} ->
            ok = gc_compacted_queues(MQs),
            erlang:send_after(0, self(), #gc{}),
            {noreply, State#{stream => Stream}};
        MQs when is_list(MQs) ->
            ok = gc_compacted_queues(MQs),
            ?tp(warning, mq_gc_done, #{}),
            {stop, normal, State}
    end.

gc_compacted_queues(MQs) ->
    NowMS = now_ms(),
    emqx_mq_message_db:delete_compacted_data(MQs, NowMS).

compacted_mq_stream() ->
    emqx_utils_stream:filter(
        fun
            (#{is_compacted := true}) ->
                true;
            (_) ->
                false
        end,
        emqx_mq_registry:list()
    ).

%%
%% Regular Queues GC
%%

gc_regular_queues() ->
    ?tp_debug(mq_gc_regular_queues_started, #{}),
    SlabInfo = emqx_mq_message_db:regular_db_slab_info(),
    NowMS = now_ms(),
    RetentionPeriod = emqx_config:get([mq, regular_queue_retention_period]),
    TimeThreshold = NowMS - RetentionPeriod,
    maybe_create_new_generation(SlabInfo, TimeThreshold),
    ExpiredSlabInfo =
        lists:filtermap(
            fun({Slab, #{until := Until}}) ->
                case is_number(Until) andalso Until =< TimeThreshold of
                    true ->
                        {true, {Slab, #{finished_ago => NowMS - Until}}};
                    false ->
                        false
                end
            end,
            maps:to_list(SlabInfo)
        ),
    ?tp(warning, mq_gc_regular, #{expired_slabs => ExpiredSlabInfo}),
    {ExpiredSlabs, _} = lists:unzip(ExpiredSlabInfo),
    lists:foreach(
        fun(Slab) ->
            ok = emqx_mq_message_db:drop_regular_db_slab(Slab),
            ?tp(mq_message_gc_regular_db_slab_dropped, #{slab => Slab})
        end,
        ExpiredSlabs
    ).

maybe_create_new_generation(SlabInfo, TimeThreshold) ->
    NeedNewGen =
        lists:all(
            fun({_SlabId, #{created_at := CreatedAt}}) -> CreatedAt =< TimeThreshold end,
            maps:to_list(SlabInfo)
        ),
    case NeedNewGen of
        false ->
            ok;
        true ->
            ok = emqx_mq_message_db:add_regular_db_generation()
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

now_ms() ->
    erlang:system_time(millisecond).
