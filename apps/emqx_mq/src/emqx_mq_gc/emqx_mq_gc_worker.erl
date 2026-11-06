%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_gc_worker).

-moduledoc """
The module is responsible for garbage collection of Message Queue data (expired messages).

This worker is periodically installed into MQ GC supervisor, runs one time GC,
and exits.

The logic of GC is different for different types of queues.

- For regular queues, we use the same logic as the regular message database, i.e.
regularly delete the expired slabs.
- Each last-value slab has a linked worker that traverses the registry lazily and deletes
expired data in asynchronous batches. A failed slab does not stop other slab workers.
""".

-behaviour(gen_server).

-include("../emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([start_link/0, child_spec/0]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, handle_continue/2, terminate/2]).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(gc_done, {}).

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
    ?tp(info, mq_gc_worker_started, #{}),
    {ok, #{}, {continue, gc_regular_queues}}.

handle_continue(gc_regular_queues, #{}) ->
    ok = gc_regular_queues(),
    ?tp_debug(mq_gc_regular_done, #{}),
    {noreply, start_gc_lastvalue_queues()}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info({'EXIT', Pid, _Reason}, #{workers := Workers0} = State) when
    is_map_key(Pid, Workers0)
->
    Workers = maps:remove(Pid, Workers0),
    maybe_done(Workers),
    {noreply, State#{workers := Workers}};
handle_info(#gc_done{}, State) ->
    ?tp(info, mq_gc_done, #{}),
    {stop, normal, State}.

terminate(_Reason, _State) ->
    ?tp_debug(mq_gc_worker_terminated, #{reason => _Reason}),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%
%% Non Append-Only Queue GC
%%

start_gc_lastvalue_queues() ->
    ?tp_debug(mq_gc_lastvalue_queues_started, #{}),
    {Slabs, Errors} = emqx_ds:list_slabs(?MQ_MESSAGE_LASTVALUE_DB, #{}),
    %% Shards that fail to report their slabs are skipped in this run.
    case Errors of
        [] -> ok;
        _ -> ?tp(warning, mq_gc_lastvalue_list_slabs_failed, #{errors => Errors})
    end,
    Workers = maps:from_list([
        begin
            {ok, Pid} = emqx_mq_gc_slab_worker:start_link(Slab),
            {Pid, true}
        end
     || Slab <- maps:keys(Slabs)
    ]),
    maybe_done(Workers),
    #{workers => Workers}.

maybe_done(Workers) when map_size(Workers) =:= 0 ->
    self() ! #gc_done{},
    ok;
maybe_done(_) ->
    ok.

%%
%% Append-Only Queue GC
%%

gc_regular_queues() ->
    ?tp_debug(mq_gc_regular_queues_started, #{}),
    case emqx_mq_message_db:regular_db_slab_info() of
        {ok, SlabInfo} ->
            gc_regular_queues(SlabInfo);
        {error, Errors} ->
            ?tp(warning, mq_gc_regular_queues_skipped, #{
                reason => list_slabs_failed,
                errors => Errors
            }),
            ok
    end.

gc_regular_queues(SlabInfo) ->
    NowMS = now_ms(),
    RetentionPeriod = emqx_config:get([mq, regular_queue_retention_period]),
    TimeThreshold = NowMS - RetentionPeriod,
    maybe_create_new_generation(SlabInfo, TimeThreshold),
    ExpiredSlabInfo =
        lists:filtermap(
            fun({Slab, #{until := Until}}) ->
                case is_number(Until) andalso slab_time_to_ms(Until) =< TimeThreshold of
                    true ->
                        {true, {Slab, #{finished_ago => NowMS - slab_time_to_ms(Until)}}};
                    false ->
                        false
                end
            end,
            maps:to_list(SlabInfo)
        ),
    ?tp(info, mq_gc_regular, #{expired_slabs => ExpiredSlabInfo}),
    {ExpiredSlabs, _} = lists:unzip(ExpiredSlabInfo),
    lists:foreach(
        fun(Slab) ->
            ok = emqx_mq_message_db:drop_regular_db_slab(Slab),
            ?tp(info, mq_message_gc_regular_db_slab_dropped, #{slab => Slab})
        end,
        ExpiredSlabs
    ).

maybe_create_new_generation(SlabInfo, TimeThreshold) ->
    NeedNewGen =
        lists:all(
            fun({_SlabId, #{since := Since}}) ->
                slab_time_to_ms(Since) =< TimeThreshold
            end,
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

slab_time_to_ms(CreatedAt) ->
    erlang:convert_time_unit(CreatedAt, microsecond, millisecond).
