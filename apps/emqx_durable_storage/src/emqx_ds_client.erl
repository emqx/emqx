%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_client).
-moduledoc """
A helper module that simplifies the task of subscribing to a topic via DS.
It takes care of monitoring DS streams, creating iterators, advancing the generations
and dealing with transient failures.
It acts as a supervisor for individual DS stream subscriptions.

This module can manage multiple topic subscriptions.
It doesn't spawn new processes, and is designed to be embedded in
a process (host) that handles the business logic.

Host must pass all unknown received messages into `dispatch_message/3` function.

NOTE: The client does NOT ack DS batches.
Host MUST call `emqx_ds:suback` function when it's done processing the batch.
""".

%% API:
-export([
    new/2, destroy/2, subscribe/3, unsubscribe/3, dispatch_message/3, complete_stream/3, inspect/1
]).

-export_type([sub_id/0, t/0, sub_options/0]).

-include("emqx_ds.hrl").
-include("emqx_ds_client_internals.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-ifdef(TEST).
-compile(nowarn_export_all).
-compile(export_all).
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-type sub() :: #sub{}.
-type subs() :: #{sub_id() => sub()}.

-type watches() :: #{emqx_ds_new_streams:watch() => sub_id()}.

-type ds_sub() :: #ds_sub{
    id :: sub_id(),
    slab :: emqx_ds:slab(),
    db :: emqx_ds:db(),
    stream :: emqx_ds:stream(),
    handle :: emqx_ds:subscription_handle(),
    %% SeqNo, Stuck, Lagging:
    vars :: atomics:atomics_ref()
}.

-type ds_subs() :: #{emqx_ds:sub_ref() => ds_sub()}.

-type streams() :: #{{sub_id(), emqx_ds:shard()} => stream_cache()}.

-type stream_cache() :: #stream_cache{
    current_gen :: emqx_ds:generation(),
    %% Streams that belong to the current generation:
    %%    Streams that are known, but currently lack the iterator:
    pending_iterator :: [emqx_ds:stream()],
    %%    Streams that have the iterator:
    active :: #{emqx_ds:stream() => {emqx_ds:iterator(), emqx_ds:sub_ref() | undefined}},
    %%    Streams that have been fully replayed:
    replayed :: #{emqx_ds:stream() => true},
    %% Streams that belong to the future generations (sorted by generation):
    future :: gb_sets:set({emqx_ds:generation(), emqx_ds:stream()})
}.

-doc """
Global state of the client. It encapsulates states of all active subscriptions.
""".
-type t() :: #cs{
    ref :: reference(),
    cbm :: module(),
    options :: #{retry_interval := non_neg_integer()},
    %% Retry:
    retry_tref :: reference() | undefined,
    %% "Logical" subs that hold data for each subscription created by `subscribe' API:
    subs :: subs(),
    streams :: streams(),
    new_streams_watches :: watches(),
    ds_subs :: ds_subs(),
    plan :: effects(),
    retry :: effects()
}.

%% Effects:
-record(eff_renew_streams, {
    sub_id :: sub_id(),
    db :: emqx_ds:db(),
    shard :: emqx_ds:shard(),
    topic :: emqx_ds:topic_filter(),
    start_time :: emqx_ds:time(),
    current_generation :: emqx_ds:generation()
}).

-record(eff_watch_streams, {
    sub_id :: sub_id(),
    db :: emqx_ds:db(),
    topic :: emqx_ds:topic_filter()
}).

-record(eff_unwatch_streams, {
    db :: emqx_ds:db(),
    watch :: emqx_ds_new_streams:watch()
}).

-record(eff_make_iterator, {
    sub_id :: sub_id(),
    db :: emqx_ds:db(),
    slab :: emqx_ds:slab(),
    stream :: emqx_ds:stream(),
    topic :: emqx_ds:topic_filter(),
    start_time :: emqx_ds:time()
}).

-record(eff_ds_sub, {
    sub_id :: sub_id(),
    db :: emqx_ds:db(),
    slab :: emqx_ds:slab(),
    stream :: emqx_ds:stream(),
    iterator :: emqx_ds:iterator(),
    sub_options :: emqx_ds:sub_opts()
}).

-record(eff_ds_unsub, {
    ref :: reference(),
    db :: emqx_ds:db(),
    handle :: emqx_ds:subscription_handle()
}).

-doc "Global options for the client.".
-type client_opts() :: #{
    retry_interval => non_neg_integer()
}.

-doc "Unique identifier of the subscription.".
-type sub_id() :: term().

-type sub_options() :: #{
    id := sub_id(),
    db := emqx_ds:db(),
    topic := emqx_ds:topic_filter(),
    start_time => emqx_ds:time(),
    ds_sub_opts => emqx_ds:sub_opts()
}.

-type effect() ::
    #eff_renew_streams{}
    | #eff_watch_streams{}
    | #eff_unwatch_streams{}
    | #eff_make_iterator{}
    | #eff_ds_sub{}
    | #eff_ds_unsub{}.
-type effects() :: [effect()].

-type effect_handler() :: fun((effect()) -> _Result).
-type result_handler() :: fun((effect(), t(), Acc, _Result) -> {t(), Acc}).

%%================================================================================
%% Callbacks
%%================================================================================

-doc """
Query the host about current generation for the shard.
Host should return 0 by default.
""".
-callback get_current_generation(sub_id(), emqx_ds:shard(), _HostState) -> emqx_ds:generation().

-doc """
Notify the host that all streams in the current generation have been replayed.
Client advances to the next generation.

The value of generation passed to this function should be returned by the next `get_current_generation` call.
""".
-callback on_advance_generation(sub_id(), emqx_ds:shard(), emqx_ds:generation(), HostState) ->
    HostState.

-doc """
Query the host about the stream replay position.

## Return values:
- `undefined`: There is no recorded replay position for the stream.
  The client should create a new iterator.

- `{subscribe, Iterator}`: Host has record of the previous replay position.
  The client should create a new subscription with the iterator.

- `{ok, Iterator}`: Iterator exists, but client should not subscribe automatically.
""".
-callback get_iterator(sub_id(), emqx_ds:slab(), emqx_ds:stream(), _HostState) ->
    {ok, emqx_ds:iterator() | end_of_stream}
    | {subscribe, emqx_ds:iterator()}
    | undefined.

-doc """
Notify the host about creation of a new iterator.
""".
-callback on_new_iterator(
    sub_id(), emqx_ds:slab(), emqx_ds:stream(), emqx_ds:iterator(), HostState
) ->
    {subscribe | ignore, HostState}.

-doc """
Notify the host that operation with the stream (creation of iterator or subscription) failed unrecoverably.
Client will make no further attempts to interact with the stream.
""".
-callback on_unrecoverable_error(sub_id(), emqx_ds:slab(), emqx_ds:stream(), _Error, HostState) ->
    HostState.

-doc """
Notify the host about a transient error with a DS subscription.

After execution of this function the host should expect that the client will restart replay from the position
specified by `get_iterator` callback.

The purpose of this callback is to prevent duplication of messages.
For example, if the host is caching stream messages, it should drop the cache.
""".
-callback on_subscription_down(sub_id(), emqx_ds:slab(), emqx_ds:stream(), HostState) -> HostState.

%%================================================================================
%% API functions
%%================================================================================

-doc """
Create the client.
""".
-spec new(module(), client_opts()) -> t().
new(CBM, UserOpts) ->
    Options = maps:merge(
        #{
            retry_interval => 5_000
        },
        UserOpts
    ),
    #cs{cbm = CBM, options = Options}.

-doc """
Destroy the client and all its subscriptions.
""".
-spec destroy(t(), HostState) -> HostState.
destroy(CS, HostState0) ->
    {_, HostState} = execute(destroy_(CS), HostState0),
    HostState.

-doc """
Subscribe to a DS topic.

Mandatory options:

- `db`: DS DB
- `id`: Unique identifier of the subscription.
  It can be an arbitrary term, except atom `undefined`.
  All messages received by the client will be tagged with this ID.
- `topic`: DS topic.

Optional:
- `start_time`: Consume messages older than this time.
- `ds_sub_opts`: Flow control options for the DS subscriptions.
""".
-spec subscribe(t(), sub_options(), HostState) ->
    {ok, t(), HostState} | {error, badarg | already_exists}.
subscribe(CS0, UserOpts = #{id := _, db := _, topic := _}, HostState0) ->
    case subscribe_(CS0, UserOpts, HostState0) of
        {ok, CS1} ->
            {CS, HostState} = execute(CS1, HostState0),
            {ok, CS, HostState};
        Error ->
            Error
    end.

-doc """
Remove a subscription with the given ID.
""".
-spec unsubscribe(t(), sub_id(), HostState) -> {ok, t(), HostState} | {error, not_found}.
unsubscribe(CS0, SubId, HostState0) ->
    case unsubscribe_(CS0, SubId, HostState0) of
        {ok, CS1, HostState1} ->
            {CS, HostState} = execute(CS1, HostState1),
            {ok, CS, HostState};
        Error ->
            Error
    end.

-doc """
Generally, all messages received by the process should be passed into this function.

If atom `ignore` is returned, the message was not addressed to the client and should be processed elsewhere.
""".
-spec dispatch_message(term(), t(), HostState) ->
    ignore | {t(), HostState} | {data, sub_id(), emqx_ds:stream(), #ds_sub_reply{}}.
dispatch_message(Message, CS0, HS0) ->
    case do_dispatch_message(Message, CS0, HS0) of
        ignore ->
            ignore;
        {data, SubId, Stream, Reply} ->
            {data, SubId, Stream, Reply};
        {Field, CS, HS} ->
            execute(Field, CS, HS)
    end.

-spec complete_stream(t(), emqx_ds:sub_ref(), HostState) -> {t(), HostState}.
complete_stream(CS0, SRef, HS0) ->
    {CS, HS} = complete_stream_(CS0, SRef, HS0),
    execute(#cs.plan, CS, HS).

-doc """
Pretty-print state of the client.
""".
-spec inspect(t()) -> map().
inspect(CS = #cs{streams = Streams, ds_subs = DSSubs}) ->
    ?record_to_map(cs, CS#cs{
        streams = maps:map(fun(_, V) -> ?record_to_map(stream_cache, V) end, Streams),
        ds_subs = maps:map(fun(_, V) -> ?record_to_map(ds_sub, V) end, DSSubs)
    }).

%%================================================================================
%% Internal functions
%%================================================================================

-spec complete_stream_(t(), emqx_ds:sub_ref(), HostState) -> {t(), HostState}.
complete_stream_(CS0 = #cs{ds_subs = DSSubs}, SRef, HS) ->
    case DSSubs of
        #{SRef := DSSub} ->
            #ds_sub{id = SubId, db = DB, handle = Handle, slab = {Shard, _}, stream = Stream} =
                DSSub,
            with_stream_cache(
                SubId,
                Shard,
                CS0,
                HS,
                fun(Cache0 = #stream_cache{active = Active, replayed = Replayed}) ->
                    %% 1. Unsubscribe
                    CS1 = plan(#eff_ds_unsub{ref = SRef, db = DB, handle = Handle}, CS0),
                    %% 2. Remove stream from active and move it to replayed:
                    Cache = Cache0#stream_cache{
                        replayed = Replayed#{Stream => true},
                        active = maps:remove(Stream, Active)
                    },
                    maybe_advance_generation(SubId, Shard, Cache, CS1, HS)
                end
            );
        #{} ->
            {CS0, HS}
    end.

-spec do_dispatch_message(_Message, t(), HostState) ->
    {integer(), t(), HostState}
    | ignore
    | {data, sub_id(), emqx_ds:stream(), #ds_sub_reply{}}.
do_dispatch_message(
    {'DOWN', MRef, _, _, _} = Msg,
    CS0 = #cs{ds_subs = DSSubs},
    HS
) ->
    case DSSubs of
        #{MRef := DSSub} ->
            handle_ds_sub_message_(Msg, CS0, MRef, DSSub, HS);
        #{} ->
            ignore
    end;
do_dispatch_message(#ds_sub_reply{ref = SRef} = Msg, CS0 = #cs{ds_subs = DSSubs}, HS) ->
    case DSSubs of
        #{SRef := DSSub} ->
            handle_ds_sub_message_(Msg, CS0, SRef, DSSub, HS);
        #{} ->
            ignore
    end;
do_dispatch_message(
    #emqx_ds_client_retry{ref = Ref},
    CS = #cs{ref = Ref, retry_tref = TRef},
    HS
) when is_reference(TRef) ->
    {#cs.retry, CS, HS};
do_dispatch_message(
    #new_stream_event{subref = Watch}, CS0 = #cs{new_streams_watches = Watches, subs = Subs}, HS
) ->
    case Watches of
        #{Watch := SubId} ->
            #{SubId := Sub} = Subs,
            CS = renew_streams_(CS0, SubId, Sub, HS),
            {#cs.plan, CS, HS};
        #{} ->
            ignore
    end;
do_dispatch_message(_, _, _) ->
    ignore.

handle_ds_sub_message_(Reason = {'DOWN', _, _, _, _}, CS, SRef, DSSub, HS) ->
    handle_ds_sub_recoverable_error_(Reason, CS, SRef, DSSub, HS);
handle_ds_sub_message_(#ds_sub_reply{payload = ?err_rec(Reason)}, CS, SRef, DSSub, HS) ->
    handle_ds_sub_recoverable_error_(Reason, CS, SRef, DSSub, HS);
handle_ds_sub_message_(#ds_sub_reply{payload = ?err_unrec(Reason)}, CS, SRef, DSSub, HS) ->
    handle_ds_sub_unrecoverable_error_(Reason, CS, SRef, DSSub, HS);
handle_ds_sub_message_(
    Data = #ds_sub_reply{
        seqno = SeqNo, size = Size, stuck = Stuck, lagging = Lagging
    },
    CS,
    SRef,
    DSSub = #ds_sub{id = SubId, vars = Vars, stream = Stream},
    HS
) ->
    %% Verify sequence numbers:
    case atomics:add_get(Vars, ?ds_sub_a_seqno, Size) of
        SeqNo ->
            %% Match:
            atomics:put(Vars, ?ds_sub_a_stuck, sub_reply_flag_to_int(Stuck)),
            atomics:put(Vars, ?ds_sub_a_lagging, sub_reply_flag_to_int(Lagging)),
            %% Note: even if the payload contains `end_of_stream' we
            %% cannot advance generation just yet. This will be done
            %% after client's ack.
            {data, SubId, Stream, Data};
        WrongSeqNo ->
            %% Mismatch:
            handle_ds_sub_recoverable_error_(
                {seqno_mismatch, SeqNo, WrongSeqNo}, CS, SRef, DSSub, HS
            )
    end.

handle_ds_sub_recoverable_error_(Reason, CS0, SRef, DSSub, HS0) ->
    #cs{cbm = CBM} = CS0,
    #ds_sub{id = SubId, db = DB, handle = Handle, stream = Stream, slab = Slab = {Shard, _Gen}} =
        DSSub,
    ?tp(
        info,
        emqx_ds_client_subscription_down,
        #{
            reason => Reason,
            sub_id => SubId,
            stream => Stream
        }
    ),
    %% Notify host about subscription down:
    HS1 = on_subscription_down(CBM, SubId, Slab, Stream, HS0),
    %% Unsubscribe. Even if the subscription is already dead, it'll
    %% remove it from the registry.
    CS1 = plan(#eff_ds_unsub{ref = SRef, db = DB, handle = Handle}, CS0),
    %% Re-add stream:
    CS2 = forget_stream(CS1, SubId, Slab, Stream),
    {CS, HS} = update_streams(CS2, SubId, Shard, [{Slab, Stream}], HS1),
    {#cs.plan, CS, HS}.

handle_ds_sub_unrecoverable_error_(Reason, CS0, SRef, DSSub, HS) ->
    #cs{cbm = CBM} = CS0,
    #ds_sub{id = SubId, db = DB, handle = Handle, stream = Stream, slab = Slab} =
        DSSub,
    ?tp(info, emqx_ds_client_read_failure, #{
        unrecoverable => Reason,
        sub_id => SubId,
        db => DB,
        slab => Slab,
        stream => Stream
    }),
    CS = plan(#eff_ds_unsub{ref = SRef, db = DB, handle = Handle}, CS0),
    {
        #cs.plan,
        forget_stream(CS, SubId, Slab, Stream),
        on_unrecoverable_error(CBM, SubId, Slab, Stream, Reason, HS)
    }.

-spec destroy_(t()) -> t().
destroy_(CS0 = #cs{retry_tref = TRef}) ->
    %% Cancel retry timer:
    _ = is_reference(TRef) andalso erlang:cancel_timer(TRef),
    %% Drop all pending effects:
    CS = CS0#cs{
        retry_tref = undefined,
        plan = [],
        retry = []
    },
    %% Free all subscriptions:
    release_objects_(undefined, CS).

%% Schedule release of subscriptions and watches owned by `RelSubId'
%% or release everything when `RelSubId' = `undefined':
-spec release_objects_(sub_id() | undefined, t()) -> t().
release_objects_(RelSubId, CS0 = #cs{subs = Subs, new_streams_watches = Watches, ds_subs = DSSubs}) ->
    %% Remove watches:
    CS = maps:fold(
        fun
            (Watch, SubId, Acc) when
                SubId =:= RelSubId; RelSubId =:= undefined
            ->
                #{SubId := #sub{db = DB}} = Subs,
                plan(#eff_unwatch_streams{db = DB, watch = Watch}, Acc);
            (_, _, Acc) ->
                Acc
        end,
        CS0,
        Watches
    ),
    %% Remove subscriptions:
    maps:fold(
        fun
            (SubRef, #ds_sub{id = SubId, handle = Handle}, Acc) when
                SubId =:= RelSubId; RelSubId =:= undefined
            ->
                #{SubId := #sub{db = DB}} = Subs,
                plan(#eff_ds_unsub{ref = SubRef, db = DB, handle = Handle}, Acc);
            (_, _, Acc) ->
                Acc
        end,
        CS,
        DSSubs
    ).

-spec subscribe_(t(), sub_options(), _HostState) -> {ok, t()} | {error, badarg | already_exists}.
subscribe_(_, #{id := undefined}, _) ->
    {error, badarg};
subscribe_(CS0 = #cs{subs = Subs0}, UserOpts = #{id := SubId, db := DB, topic := Topic}, HostState) ->
    %% Check uniqueness of the subscription id:
    case Subs0 of
        #{SubId := _} ->
            {error, already_exists};
        _ ->
            %% Derive optional subscription options:
            StartTime = maps:get(start_time, UserOpts, 0),
            DSSubOpts = maps:get(ds_sub_opts, UserOpts, #{max_unacked => 1000}),
            %% Register new subscription:
            Sub = #sub{
                db = DB,
                topic = Topic,
                start_time = StartTime,
                ds_sub_opts = DSSubOpts
            },
            CS1 = CS0#cs{subs = Subs0#{SubId => Sub}},
            CS = watch_streams_(CS1, SubId, Sub, HostState),
            {ok, CS}
    end.

-spec unsubscribe_(t(), sub_id(), HostState) -> {ok, t(), HostState} | {error, not_found}.
unsubscribe_(
    CS0 = #cs{subs = Subs0}, SubId, HostState
) ->
    case maps:take(SubId, Subs0) of
        {Sub = #sub{}, Subs} ->
            %% 1. Remove all previously scheduled non-destructive
            %% events previously scheduled by the subscription:
            CS1 = filter_effects(
                fun(Eff) ->
                    eff_subid(Eff) =/= SubId orelse eff_destructive(Eff)
                end,
                CS0
            ),
            %% 2. Remove new stream watch if exists:
            CS2 = unwatch_streams_(CS1, SubId, Sub),
            %% 3. Remove all streams:
            CS3 = streams_destroy_(CS2, SubId),
            %% 4. Remove subscription from the registered set:
            CS = CS3#cs{subs = Subs},
            {ok, CS, HostState};
        error ->
            {error, not_found}
    end.

-spec renew_streams_(t(), sub_id(), sub(), _HostState) -> t().
renew_streams_(
    CS = #cs{cbm = CBM},
    SubId,
    #sub{
        db = DB,
        topic = Topic,
        start_time = StartTime
    },
    HostState
) ->
    lists:foldl(
        fun(Shard, Acc) ->
            Gen = get_current_generation(CBM, SubId, Shard, HostState),
            plan(
                #eff_renew_streams{
                    sub_id = SubId,
                    db = DB,
                    shard = Shard,
                    topic = Topic,
                    start_time = StartTime,
                    current_generation = Gen
                },
                Acc
            )
        end,
        CS,
        emqx_ds:list_shards(DB)
    ).

%%------------------------------------------------------------------------------
%% Effect handler
%%------------------------------------------------------------------------------

-spec result_handler(effect(), t(), _Result, HostState) -> {t(), HostState}.
result_handler(
    #eff_watch_streams{sub_id = SubId},
    CS0 = #cs{new_streams_watches = Watches},
    HostState,
    Watch
) ->
    CS = CS0#cs{new_streams_watches = Watches#{Watch => SubId}},
    {CS, HostState};
result_handler(
    #eff_unwatch_streams{watch = Watch},
    CS0 = #cs{new_streams_watches = Watches},
    HostState,
    _Result
) ->
    CS = CS0#cs{new_streams_watches = maps:remove(Watch, Watches)},
    {CS, HostState};
result_handler(
    Eff = #eff_renew_streams{sub_id = SubId, shard = Shard},
    CS,
    HostState,
    {Streams, Errors}
) ->
    case Errors of
        [] ->
            update_streams(CS, SubId, Shard, Streams, HostState);
        _ ->
            {
                retry(Errors, Eff, CS),
                HostState
            }
    end;
result_handler(Eff = #eff_make_iterator{}, CS0, HostState0, Result) ->
    case Result of
        {ok, It} ->
            handle_add_iterator(Eff, CS0, HostState0, It);
        ?err_rec(Err) ->
            {
                retry(Err, Eff, CS0),
                HostState0
            };
        ?err_unrec(Err) ->
            handle_make_iterator_fail(Eff, CS0, HostState0, Err)
    end;
result_handler(Eff = #eff_ds_sub{}, CS, HostState, Result) ->
    case Result of
        {ok, Handle, SubRef} ->
            {
                handle_new_ds_sub(Eff, CS, Handle, SubRef),
                HostState
            };
        ?err_rec(Err) ->
            {
                retry(Err, Eff, CS),
                HostState
            }
    end;
result_handler(#eff_ds_unsub{ref = Ref}, CS, HostState, _Result) ->
    #cs{ds_subs = DSSubs} = CS,
    {
        CS#cs{ds_subs = maps:remove(Ref, DSSubs)},
        HostState
    }.

handle_new_ds_sub(Eff, CS0, Handle, SubRef) ->
    #eff_ds_sub{sub_id = SubId, db = DB, slab = Slab, stream = Stream, iterator = It} = Eff,
    CS = #cs{ds_subs = DSSubs} = activate_stream(CS0, SubId, Slab, Stream, It, SubRef),
    DSSub = #ds_sub{
        id = SubId,
        handle = Handle,
        db = DB,
        slab = Slab,
        stream = Stream
    },
    CS#cs{
        ds_subs = DSSubs#{SubRef => DSSub}
    }.

handle_add_iterator(Eff, CS0, HostState0, It) ->
    #cs{cbm = CBM, subs = Subs} = CS0,
    #eff_make_iterator{sub_id = SubId, db = DB, slab = Slab, stream = Stream} = Eff,
    CS = activate_stream(CS0, SubId, Slab, Stream, It, undefined),
    case on_new_iterator(CBM, SubId, Slab, Stream, It, HostState0) of
        {subscribe, HostState} ->
            #{SubId := #sub{ds_sub_opts = SubOpts}} = Subs,
            {
                plan(
                    #eff_ds_sub{
                        sub_id = SubId,
                        db = DB,
                        slab = Slab,
                        stream = Stream,
                        iterator = It,
                        sub_options = SubOpts
                    },
                    CS
                ),
                HostState
            };
        {ignore, HostState} ->
            {
                CS,
                HostState
            }
    end.

-doc """
Handle unrecoverable errors that happen during creation of iterators.
""".
handle_make_iterator_fail(Eff, CS = #cs{cbm = CBM}, HostState, Err) ->
    #eff_make_iterator{
        sub_id = SubId, db = DB, slab = Slab, stream = Stream, topic = Topic, start_time = StartTime
    } = Eff,
    ?tp(info, emqx_ds_client_make_iterator_fail, #{
        unrecoverable => Err,
        sub_id => SubId,
        db => DB,
        slab => Slab,
        topic => Topic,
        stream => Stream,
        start_time => StartTime
    }),
    {
        forget_stream(CS, SubId, Slab, Stream),
        on_unrecoverable_error(CBM, SubId, Slab, Stream, Err, HostState)
    }.

-spec effect_handler(effect()) -> _Result.
effect_handler(#eff_watch_streams{db = DB, topic = Topic}) ->
    {ok, Watch} = emqx_ds_new_streams:watch(DB, Topic),
    Watch;
effect_handler(#eff_unwatch_streams{db = DB, watch = Watch}) ->
    emqx_ds_new_streams:unwatch(DB, Watch);
effect_handler(
    #eff_renew_streams{
        db = DB,
        shard = Shard,
        topic = Topic,
        start_time = StartTime,
        current_generation = Gen
    }
) ->
    emqx_ds:get_streams(DB, Topic, StartTime, #{shard => Shard, generation_min => Gen});
effect_handler(#eff_make_iterator{db = DB, stream = Stream, topic = TF, start_time = StartTime}) ->
    emqx_ds:make_iterator(DB, Stream, TF, StartTime);
effect_handler(#eff_ds_sub{db = DB, iterator = It, sub_options = Opts}) ->
    emqx_ds:subscribe(DB, It, Opts);
effect_handler(#eff_ds_unsub{db = DB, handle = Handle}) ->
    emqx_ds:unsubscribe(DB, Handle).

%%------------------------------------------------------------------------------
%% Stream management
%%------------------------------------------------------------------------------

-spec activate_stream(
    t(),
    sub_id(),
    emqx_ds:slab(),
    emqx_ds:stream(),
    emqx_ds:iterator(),
    emqx_ds:sub_ref() | undefined
) ->
    t().
activate_stream(CS, SubId, {Shard, _Gen}, Stream, Iterator, MaybeSubRef) ->
    with_stream_cache(
        SubId,
        Shard,
        CS,
        fun(#stream_cache{pending_iterator = Pending, active = Active} = Cache) ->
            Cache#stream_cache{
                pending_iterator = Pending -- [Stream],
                active = Active#{Stream => {Iterator, MaybeSubRef}}
            }
        end
    ).

-spec forget_stream(t(), sub_id(), emqx_ds:slab(), emqx_ds:stream()) -> t().
forget_stream(CS, SubId, {Shard, Gen}, Stream) ->
    with_stream_cache(
        SubId,
        Shard,
        CS,
        fun
            (undefined) ->
                undefined;
            (
                #stream_cache{
                    pending_iterator = Pending,
                    active = Active,
                    replayed = Replayed,
                    future = Future
                } = Cache
            ) ->
                Cache#stream_cache{
                    pending_iterator = Pending -- [Stream],
                    active = maps:remove(Stream, Active),
                    replayed = maps:remove(Stream, Replayed),
                    future = gb_sets:delete_any({Gen, Stream}, Future)
                }
        end
    ).

-spec update_streams(
    t(), sub_id(), emqx_ds:shard(), [{emqx_ds:slab(), emqx_ds:stream()}], HostState
) ->
    {t(), HostState}.
update_streams(CS0, SubId, Shard, Streams, HostState0) ->
    with_stream_cache(
        SubId,
        Shard,
        CS0,
        HostState0,
        fun(Cache0) ->
            {CS, Cache} = do_update_streams(CS0, HostState0, SubId, Shard, Cache0, Streams),
            maybe_advance_generation(SubId, Shard, Cache, CS, HostState0)
        end
    ).

-spec do_update_streams(t(), _HostState, sub_id(), emqx_ds:shard(), stream_cache(), [
    {emqx_ds:slab(), emqx_ds:stream()}
]) ->
    {t(), stream_cache()}.
do_update_streams(CS0, HostState, SubId, Shard, Cache0, Streams) ->
    lists:foldl(
        fun({{_Shard, Generation}, Stream}, {AccCS, AccCache}) ->
            add_stream_to_cache(AccCS, HostState, SubId, Shard, AccCache, Generation, Stream)
        end,
        {CS0, Cache0},
        Streams
    ).

add_stream_to_cache(
    CS,
    _HostState,
    _SubId,
    _Shard,
    Cache = #stream_cache{current_gen = Current},
    Generation,
    _Stream
) when Generation < Current ->
    %% Should not happen:
    {
        CS,
        Cache
    };
add_stream_to_cache(
    CS,
    _HostState,
    _SubId,
    _Shard,
    Cache = #stream_cache{current_gen = Current, future = Future0},
    Generation,
    Stream
) when Generation > Current ->
    %% This is a stream we'll replay in the future:
    Future = gb_sets:add_element({Generation, Stream}, Future0),
    {
        CS,
        Cache#stream_cache{future = Future}
    };
add_stream_to_cache(
    CS0, HostState, SubId, Shard, Cache0 = #stream_cache{current_gen = Current}, Generation, Stream
) when Generation =:= Current ->
    #stream_cache{
        pending_iterator = Pending,
        active = Active,
        replayed = Replayed
    } = Cache0,
    %% First handle the most likely case when the stream is already
    %% active, then check if the stream is already replayed, then
    %% check if the stream is pending for creation of the iterator:
    case
        maps:is_key(Stream, Active) orelse maps:is_key(Stream, Replayed) orelse
            lists:member(Stream, Pending)
    of
        true ->
            %% This stream is already known:
            {CS0, Cache0};
        false ->
            %% This stream is new (for the client)
            ?tp(debug, emqx_ds_client_new_stream, #{sub => SubId, shard => Shard, stream => Stream}),
            %% Does the host already have the iterator?
            case get_iterator(CS0#cs.cbm, SubId, {Shard, Generation}, Stream, HostState) of
                {Action, end_of_stream} when Action =:= ok; Action =:= subscribe ->
                    %% This is a known replayed stream.
                    Cache = Cache0#stream_cache{replayed = Replayed#{Stream => true}},
                    {CS0, Cache};
                undefined ->
                    %% This stream is new to the host. Schedule
                    %% creation of the iterator:
                    #sub{db = DB, topic = Topic, start_time = StartTime} = maps:get(
                        SubId, CS0#cs.subs
                    ),
                    CS = plan(
                        #eff_make_iterator{
                            sub_id = SubId,
                            db = DB,
                            slab = {Shard, Generation},
                            stream = Stream,
                            topic = Topic,
                            start_time = StartTime
                        },
                        CS0
                    ),
                    Cache = Cache0#stream_cache{pending_iterator = [Stream | Pending]},
                    {CS, Cache};
                {Action, It} when Action =:= ok; Action =:= subscribe ->
                    %% This is a known in-progress stream:
                    #sub{db = DB, ds_sub_opts = DSSubOpts} = maps:get(
                        SubId, CS0#cs.subs
                    ),
                    Cache = Cache0#stream_cache{active = Active#{Stream => {It, undefined}}},
                    CS =
                        case Action of
                            ok ->
                                %% Host doesn't want to subscribe:
                                CS0;
                            subscribe ->
                                plan(
                                    #eff_ds_sub{
                                        sub_id = SubId,
                                        db = DB,
                                        slab = {Shard, Generation},
                                        stream = Stream,
                                        iterator = It,
                                        sub_options = DSSubOpts
                                    },
                                    CS0
                                )
                        end,
                    {CS, Cache}
            end
    end.

maybe_advance_generation(
    SubId, Shard, Cache0, CS0 = #cs{cbm = CBM, subs = Subs}, HostState0
) ->
    case is_fully_replayed(Cache0) of
        false ->
            %% Generation is not fully replayed:
            {Cache0, CS0, HostState0};
        {true, NextGen, StreamsOfNextGen, Future} ->
            ?tp(debug, emqx_ds_client_advance_generation, #{next_gen => NextGen}),
            %% Advance generation:
            #{SubId := #sub{db = DB, topic = Topic, start_time = StartTime}} = Subs,
            %% Here we don't ask the host if it has the iterator: it
            %% should not, otherwise replay order would be violated.
            Cache = #stream_cache{
                current_gen = NextGen,
                pending_iterator = StreamsOfNextGen,
                future = Future
            },
            %% Schedule creation of iterators for the new streams:
            CS = lists:foldl(
                fun(Stream, CS1) ->
                    plan(
                        #eff_make_iterator{
                            sub_id = SubId,
                            db = DB,
                            slab = {Shard, NextGen},
                            stream = Stream,
                            topic = Topic,
                            start_time = StartTime
                        },
                        CS1
                    )
                end,
                CS0,
                StreamsOfNextGen
            ),
            HostState = on_advance_generation(CBM, SubId, Shard, NextGen, HostState0),
            {Cache, CS, HostState}
    end.

-spec is_fully_replayed(stream_cache()) ->
    {true, emqx_ds:generation(), [emqx_ds:stream(), ...],
        gb_sets:set({emqx_ds:generation(), emqx_ds:stream()})}
    | false.
is_fully_replayed(#stream_cache{
    current_gen = Current, pending_iterator = Pending, active = Active, future = Future0
}) ->
    maybe
        [] ?= Pending,
        0 ?= maps:size(Active),
        {NextGen, Streams, Future} ?= pop_future_streams(Current, undefined, Future0, []),
        {true, NextGen, Streams, Future}
    else
        _ ->
            false
    end.

pop_future_streams(Current, NextGen, Future0, Acc) ->
    case gb_sets:is_empty(Future0) of
        true when NextGen =:= undefined ->
            %% There are no cached streams with greater generation than `Current'
            undefined;
        true ->
            %% Next generation is also the last known one. We've
            %% consumed all of its streams:
            {NextGen, Acc, Future0};
        false ->
            {{Gen, Stream}, Future} = gb_sets:take_smallest(Future0),
            case is_integer(NextGen) of
                false when Gen > Current ->
                    %% Found the next generation to replay:
                    pop_future_streams(Current, Gen, Future, [Stream]);
                true when Gen =:= NextGen ->
                    pop_future_streams(Current, Gen, Future, [Stream | Acc]);
                true when Gen > NextGen ->
                    %% Reached the end of NextGen:
                    {NextGen, Acc, Future0}
            end
    end.

%%------------------------------------------------------------------------------
%% Functions for manipulating the plan:
%%------------------------------------------------------------------------------

-spec plan(effect(), t()) -> t().
plan(Effect, CS = #cs{plan = Plan}) ->
    CS#cs{plan = [Effect | Plan]}.

-doc """
A wrapper of `retry/2` that prints a message before adding effect to the retry queue.
""".
-spec retry(_Reason, effect(), t()) -> t().
retry(Reason, Effect, CS) ->
    ?tp(info, emqx_ds_client_retry, #{action => Effect, reason => Reason}),
    retry(Effect, CS).

-spec retry(effect(), t()) -> t().
retry(Effect, CS0 = #cs{retry = Retry, ref = Ref, options = #{retry_interval := RetryInterval}}) ->
    CS = CS0#cs{retry = [Effect | Retry]},
    %% Start timer if not running:
    case CS0#cs.retry_tref of
        undefined ->
            TRef = erlang:send_after(RetryInterval, self(), #emqx_ds_client_retry{ref = Ref}),
            CS#cs{retry_tref = TRef};
        TRef when is_reference(TRef) ->
            CS
    end.

-spec eff_subid(effect()) -> sub_id() | undefined.
eff_subid(#eff_watch_streams{sub_id = SubId}) -> SubId;
eff_subid(#eff_unwatch_streams{}) -> undefined;
eff_subid(#eff_renew_streams{sub_id = SubId}) -> SubId;
eff_subid(#eff_make_iterator{sub_id = SubId}) -> SubId;
eff_subid(#eff_ds_sub{sub_id = SubId}) -> SubId;
eff_subid(#eff_ds_unsub{}) -> undefined.

-doc "Return true if the effect destroys an entity.".
-spec eff_destructive(effect()) -> boolean().
eff_destructive(#eff_unwatch_streams{}) -> true;
eff_destructive(#eff_ds_unsub{}) -> true;
eff_destructive(_) -> false.

-spec filter_effects(fun((effect()) -> boolean()), t()) -> t().
filter_effects(Pred, CS = #cs{plan = Plan, retry = Retry}) ->
    CS#cs{plan = lists:filter(Pred, Plan), retry = lists:filter(Pred, Retry)}.

%%------------------------------------------------------------------------------
%% The interpreter:
%%------------------------------------------------------------------------------

-doc """

This module is architected as following:

- It's assumed that all APIs are executed in a process called host.
  Host keeps the state of the client, and it has its own state that is unknown to us.

- Host interacts with the client via API calls, such as `subscribe`, `unsubscribe`, `ack`, etc.

- Client interacts with the host via callbacks defined in this module.
  Callbacks can query and mutate state of the host.
  Host state (or relevant parts of thereof) is threaded through the client logic.

The logic is split up between three types of functions:

- Planners
- Effect handler
- Result handler

Planners are triggered by the API calls (`subscribe`, `unsubscribe`, `destroy`, etc.).
Planners create sequences of effects that are fed into the interpreter (`execute`).

The interpreter feeds the effects into the effect handler, which evaluates them,
returns the result and updates its own state (`EffHandlerState`).
The results are then fed into the result handler,
which can further mutate client and host states, and schedule more effects if needed.

Effects are stored in the client state in two queues: `plan` and `retry`.
Effects in the `plan` queue are executed immediately,
while `retry` effects are executed on retry timeout.

This architecture isolates all interactions with the real world in the effect handler,
which is a trivial wrapper of DS API,
and allows to implement all complex logic as pure functions in the planners and result handler.

It also allows to create a fake effect handler for property-based testing.

On the flip side, we now have to deal with three separate states:

- Host state
- Client state
- Effect handler state

The good news is that the last one is only used for testing.
`real_world` effect handler ignores its state.

""".
-spec execute(t(), HostState) -> {t(), HostState}.
execute(CS, HostState) ->
    execute(#cs.plan, CS, HostState).

-spec execute(integer(), t(), HostState) -> {t(), HostState}.
execute(Field, CS0, HS0) ->
    execute(Field, fun effect_handler/1, fun result_handler/4, CS0, HS0).

-spec execute(integer(), effect_handler(), result_handler(), t(), HostState) -> {t(), HostState}.
execute(Field, EffectHandler, ResultHandler, CS0, HS0) ->
    ?tp_ignore_side_effects_in_prod(emqx_ds_client_exec_loop, #{field => Field}),
    case element(Field, CS0) of
        [] ->
            %% No planned effects:
            ?tp_ignore_side_effects_in_prod(emqx_ds_client_exec_done, #{
                cs => inspect(CS0), hs => HS0
            }),
            {CS0, HS0};
        Effects ->
            %% Clear effects in the state:
            CS1 = erlang:setelement(Field, CS0, []),
            {CS, HS} = do_execute(
                EffectHandler, ResultHandler, lists:reverse(Effects), CS1, HS0
            ),
            %% Note: from this point on we always continue on the
            %% `#cs.plan', even if the original `Field' was
            %% `#cs.retry'. Effects can fail again, we should retry
            %% them later:
            execute(#cs.plan, EffectHandler, ResultHandler, CS, HS)
    end.

-spec do_execute(effect_handler(), result_handler(), [effect()], t(), HostState) ->
    {t(), HostState}.
do_execute(_, _, [], CS, Acc) ->
    {CS, Acc};
do_execute(EffectHandler, ResultHandler, [Effect | Effects], CS0, Acc0) ->
    Result = EffectHandler(Effect),
    ?tp_ignore_side_effects_in_prod(emqx_ds_client_exec, #{eff => Effect, res => Result}),
    {CS, Acc} = ResultHandler(Effect, CS0, Acc0, Result),
    do_execute(EffectHandler, ResultHandler, Effects, CS, Acc).

%%------------------------------------------------------------------------------
%% Callback module wrappers:
%%------------------------------------------------------------------------------

-spec get_current_generation(module(), sub_id(), emqx_ds:shard(), _HostState) ->
    emqx_ds:generation().
get_current_generation(CBM, SubId, Shard, HostState) ->
    CBM:get_current_generation(SubId, Shard, HostState).

-spec get_iterator(module(), sub_id(), emqx_ds:slab(), emqx_ds:stream(), _HostState) ->
    {ok | subscribe, emqx_ds:iterator() | end_of_stream}
    | undefined.
get_iterator(CBM, SubId, Slab, Stream, HostState) ->
    CBM:get_iterator(SubId, Slab, Stream, HostState).

-spec on_new_iterator(
    module(), sub_id(), emqx_ds:slab(), emqx_ds:stream(), emqx_ds:iterator(), HostState
) ->
    {subscribe | ignore, HostState}.
on_new_iterator(CBM, SubId, Slab, Stream, Iterator, HostState) ->
    CBM:on_new_iterator(SubId, Slab, Stream, Iterator, HostState).

-spec on_unrecoverable_error(
    module(), sub_id(), emqx_ds:slab(), emqx_ds:stream(), _Error, HostState
) ->
    HostState.
on_unrecoverable_error(CBM, SubId, Slab, Stream, Error, HostState) ->
    CBM:on_unrecoverable_error(SubId, Slab, Stream, Error, HostState).

-spec on_advance_generation(module(), sub_id(), emqx_ds:shard(), emqx_ds:generation(), HostState) ->
    HostState.
on_advance_generation(CBM, SubId, Shard, NewCurrentGeneration, HostState) ->
    CBM:on_advance_generation(SubId, Shard, NewCurrentGeneration, HostState).

-spec on_subscription_down(module(), sub_id(), emqx_ds:slab(), emqx_ds:stream(), HostState) ->
    HostState.
on_subscription_down(CBM, SubId, Slab, Stream, HostState) ->
    CBM:on_subscription_down(SubId, Slab, Stream, HostState).

%%------------------------------------------------------------------------------
%% Misc.
%%------------------------------------------------------------------------------

-doc "Destroy all streams that belong to `SubId`.".
-spec streams_destroy_(t(), sub_id()) -> t().
streams_destroy_(CS0 = #cs{streams = Streams}, SubId) ->
    maps:fold(
        fun
            ({SId, Shard}, _, Acc) when SId =:= SubId ->
                stream_cache_destroy_(Acc, SubId, Shard);
            (_, _, Acc) ->
                Acc
        end,
        CS0,
        Streams
    ).

-doc "Remove stream cache from the state and schedule clean up of all active subscriptions.".
-spec stream_cache_destroy_(t(), sub_id(), emqx_ds:shard()) -> t().
stream_cache_destroy_(CS0 = #cs{streams = Streams0}, SubId, Shard) ->
    {#stream_cache{active = Active}, Streams} = maps:take({SubId, Shard}, Streams0),
    CS = CS0#cs{streams = Streams},
    maps:fold(
        fun
            (_Stream, {_Iterator, undefined}, Acc) ->
                Acc;
            (_Stream, {_Iterator, SRef}, Acc) ->
                ds_unsub_(Acc, SRef)
        end,
        CS,
        Active
    ).

-doc "Schedule DS unsubscribe event and remove DS subscription from the state.".
-spec ds_unsub_(t(), emqx_ds:sub_ref()) -> t().
ds_unsub_(CS0 = #cs{ds_subs = DSSubs0}, SRef) ->
    {#ds_sub{handle = Handle, db = DB}, DSSubs} = maps:take(SRef, DSSubs0),
    CS = CS0#cs{ds_subs = DSSubs},
    plan(#eff_ds_unsub{ref = SRef, db = DB, handle = Handle}, CS).

-doc "Plan subscription to the stream events followed by stream renewal.".
-spec watch_streams_(t(), sub_id(), sub(), _HostState) -> t().
watch_streams_(CS0, SubId, Sub = #sub{db = DB, topic = Topic}, HostState) ->
    CS1 = plan(#eff_watch_streams{sub_id = SubId, db = DB, topic = Topic}, CS0),
    renew_streams_(CS1, SubId, Sub, HostState).

-doc "Schedule unwatch effect and remove watch from the state.".
-spec unwatch_streams_(t(), sub_id(), sub()) -> t().
unwatch_streams_(CS0 = #cs{new_streams_watches = Watches0}, SubId, #sub{db = DB}) ->
    case emqx_utils_maps:find_key(SubId, Watches0) of
        {ok, Watch} ->
            CS1 = CS0#cs{new_streams_watches = maps:remove(Watch, Watches0)},
            plan(#eff_unwatch_streams{db = DB, watch = Watch}, CS1);
        undefined ->
            CS0
    end.

-doc """
Run a function in the context of stream cache belonging to one shard of a subscription.
Insert the updated stream cache record into the #cs record.

- This function allows mutation of the host state.
- If stream cache doesn't exist, it will create a fresh record and initialize it
  with the host's current generation.
""".
-spec with_stream_cache(sub_id(), emqx_ds:shard(), t(), HostState, fun(
    (stream_cache()) -> {stream_cache(), t(), HostState}
)) ->
    {t(), HostState}.
with_stream_cache(SubId, Shard, #cs{cbm = CBM, streams = Streams}, HostState0, Fun) ->
    Key = {SubId, Shard},
    %% Get the existing cache or create new empty one:
    case Streams of
        #{Key := Cache0} ->
            ok;
        #{} ->
            Cache0 = #stream_cache{
                current_gen = get_current_generation(CBM, SubId, Shard, HostState0)
            }
    end,
    {Cache, CS1, HostState} = Fun(Cache0),
    CS = CS1#cs{streams = Streams#{Key => Cache}},
    {CS, HostState}.

-doc """
Run a function in the context of stream cache belonging to one shard of a subscription.
Insert the updated stream cache record into the #cs record.

This function does not allow mutation of the host and client states.
""".
-spec with_stream_cache(sub_id(), emqx_ds:shard(), t(), fun(
    (stream_cache() | undefined) -> stream_cache() | undefined
)) ->
    t().
with_stream_cache(SubId, Shard, CS0 = #cs{streams = Streams}, Fun) ->
    Key = {SubId, Shard},
    Cache0 = maps:get(Key, Streams, undefined),
    case Fun(Cache0) of
        #stream_cache{} = Cache ->
            CS0#cs{streams = Streams#{Key => Cache}};
        undefined ->
            CS0
    end.

sub_reply_flag_to_int(true) -> 1;
sub_reply_flag_to_int(_) -> 0.
