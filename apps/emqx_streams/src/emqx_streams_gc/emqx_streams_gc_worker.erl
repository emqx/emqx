%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_gc_worker).

-moduledoc """
The module is responsible for garbage collection of Stream data (expired messages).

This worker is periodically installed into Stream GC supervisor, runs one time GC,
and exits.

The logic of GC is different for different types of streams.

- For regular unlimited streams, we use the same logic as the regular message database, i.e.
regularly delete the expired slabs.
- Then, for lastvalue or/and limited streams, we iterate over all the streams and delete the expired data
via ranged `tx_del_topic`.

NOTE: We call `emqx_utils_stream` objects `iterators' here to avoid confusion.
""".

-behaviour(gen_server).

-include("../emqx_streams_internal.hrl").
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
    ?tp(info, streams_gc_worker_started, #{}),
    {ok, #{}, {continue, gc_regular_queues}}.

handle_continue(gc_regular_queues, #{}) ->
    ok = gc_regular_streams(),
    ?tp_debug(streams_gc_regular_done, #{}),
    {noreply, start_gc_lastvalue_streams()}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(_Request, State) ->
    {noreply, State}.

handle_info(#gc{}, State) ->
    gc_next_lastvalue_batch(State).

terminate(_Reason, _State) ->
    ?tp_debug(streams_gc_worker_terminated, #{reason => _Reason}),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%
%% Non Append-Only Stream GC
%%

start_gc_lastvalue_streams() ->
    ?tp_debug(streams_gc_lastvalue_streams_started, #{}),
    erlang:send_after(0, self(), #gc{}),
    #{iterator => lastvalue_streams_iterator()}.

gc_next_lastvalue_batch(#{iterator := Iterator0} = State) ->
    case emqx_utils_stream:consume(?CONSUME_BATCH_SIZE, Iterator0) of
        {Streams, Iterator} ->
            ok = gc_lastvalue_streams(Streams),
            erlang:send_after(0, self(), #gc{}),
            {noreply, State#{iterator => Iterator}};
        Streams when is_list(Streams) ->
            ok = gc_lastvalue_streams(Streams),
            ?tp(info, streams_gc_done, #{}),
            {stop, normal, State}
    end.

gc_lastvalue_streams(Streams) ->
    emqx_streams_message_db:delete_expired_data(Streams).

lastvalue_streams_iterator() ->
    emqx_utils_stream:filter(
        fun(Stream) ->
            not emqx_streams_prop:is_append_only(Stream)
        end,
        emqx_streams_registry:list()
    ).

%%
%% Append-Only Queue GC
%%

gc_regular_streams() ->
    ?tp_debug(streams_gc_regular_streams_started, #{}),
    SlabInfo = emqx_streams_message_db:regular_db_slab_info(),
    NowMS = now_ms(),
    RetentionPeriod = emqx_config:get([streams, regular_stream_retention_period]),
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
            ok = emqx_streams_message_db:drop_regular_db_slab(Slab),
            ?tp(info, streams_message_gc_regular_db_slab_dropped, #{slab => Slab})
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
            ok = emqx_streams_message_db:add_regular_db_generation()
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

now_ms() ->
    erlang:system_time(millisecond).

slab_time_to_ms(CreatedAt) ->
    erlang:convert_time_unit(CreatedAt, microsecond, millisecond).
