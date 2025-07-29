%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_fake).

%% API:
-export([
    load/0,
    unload/1,

    new/1,
    destroy/0,

    add_generation/2,
    del_generation/3,
    add_stream/1,
    publish_payloads/3,

    seterr/2,
    fixerr/2,
    fixall/0,

    ds_sub_recoverable_error/1,

    ls_subs/0,
    ls_watches/0,
    inspect/0
]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([]).

-include("emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_ds_client_internals.hrl").
-include("emqx_ds_client_tests.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-define(fake_ds, emqx_ds_fake_state).

-type injected_errors() :: #{{emqx_ds:shard(), atom()} => true}.

-record(test_ds_sub, {
    sref :: ?sub_ref(_),
    handle :: ?sub_handle(_),
    it :: #fake_iter{},
    seqno = 0 :: integer()
}).

-record(fake_ds, {
    shards :: [emqx_ds:shard()],
    %% Generations:
    gens = #{} :: #{emqx_ds:shard() => emqx_ds:generation()},
    %% New streams process:
    watches = [] :: [reference()],
    %% Counters for making fake references:
    sub_ref_ctr = 0 :: integer(),
    %% Active subscriptions:
    ds_subs = [] :: [#test_ds_sub{}],
    %% Streams:
    streams = [],
    %% Injected errors:
    errors = #{} :: injected_errors()
}).

%%================================================================================
%% API functions
%%================================================================================

load() ->
    meck:new(emqx_ds_new_streams, [no_history, no_link]),
    meck:expect(emqx_ds_new_streams, watch, fun watch_streams/2),
    meck:expect(emqx_ds_new_streams, unwatch, fun unwatch_streams/2),

    meck:new(emqx_ds, [no_history, no_link]),
    meck:expect(emqx_ds, list_shards, fun list_shards/1),
    meck:expect(emqx_ds, get_streams, fun get_streams/4),
    meck:expect(emqx_ds, make_iterator, fun make_iterator/4),
    meck:expect(emqx_ds, subscribe, fun subscribe/3),
    meck:expect(emqx_ds, unsubscribe, fun unsubscribe/2),
    {ok, Started} = application:ensure_all_started(gproc),
    Started.

unload(Started) ->
    meck:unload(emqx_ds),
    meck:unload(emqx_ds_new_streams),
    [application:stop(I) || I <- lists:reverse(Started)].

new(Shards) ->
    put(?fake_ds, #fake_ds{
        shards = Shards
    }).

destroy() ->
    ok.

seterr(Shard, Kind) ->
    with(#fake_ds.errors, fun(Err) ->
        {ok, Err#{{Shard, Kind} => true}}
    end).

fixerr(Shard, Kind) ->
    with(#fake_ds.errors, fun(Err) ->
        {ok, maps:remove({Shard, Kind}, Err)}
    end),
    publish_payloads(1, 0, true).

fixall() ->
    with(#fake_ds.errors, fun(_) ->
        {ok, #{}}
    end),
    publish_payloads(1, 0, true).

add_stream(Stream) ->
    with(#fake_ds.streams, fun(Streams) ->
        {
            notify_new_streams(),
            [Stream | Streams]
        }
    end).

notify_new_streams() ->
    WW = (get(?fake_ds))#fake_ds.watches,
    [self() ! #new_stream_event{subref = Ref} || Ref <- WW].

add_generation(Shard, Generation) ->
    with(#fake_ds.gens, fun(Gens) ->
        {ok, Gens#{Shard => Generation}}
    end),
    publish_payloads(1, 0, true).

del_generation(Shard, Generation, Graceful) ->
    %% 1. Delete streams:
    with(#fake_ds.streams, fun(Streams) ->
        {ok,
            lists:filter(
                fun(#fake_stream{shard = S, gen = G}) ->
                    S =/= Shard orelse G > Generation
                end,
                Streams
            )}
    end),
    %% 2. Delete subscriptions:
    publish_payloads(1, 0, Graceful).

ds_sub_recoverable_error(Reason) ->
    %% Emulate all DS subscriptions going down:
    with(#fake_ds.ds_subs, fun(DSSubs) ->
        lists:foreach(
            fun(#test_ds_sub{sref = SRef}) ->
                destroy_ds_sub(SRef, Reason)
            end,
            DSSubs
        ),
        {ok, []}
    end).

ls_subs() ->
    #fake_ds{ds_subs = Subs} = get(?fake_ds),
    [SRef || #test_ds_sub{sref = SRef} <- Subs].

ls_watches() ->
    (get(?fake_ds))#fake_ds.watches.

inspect() ->
    ?record_to_map(fake_ds, get(?fake_ds)).

%%================================================================================
%% behavior callbacks
%%================================================================================

watch_streams(_DB, _Topic) ->
    with(#fake_ds.watches, fun(Watches) ->
        Ref = make_ref(),
        {{ok, Ref}, [Ref | Watches]}
    end).

unwatch_streams(_DB, Ref) ->
    with(#fake_ds.watches, fun(Watches) ->
        {
            ok,
            Watches -- [Ref]
        }
    end).

list_shards(_DB) ->
    (get(?fake_ds))#fake_ds.shards.

get_streams(_DB, _TopicFilter, _StartTime, #{shard := Shard, generation_min := Gen}) ->
    #fake_ds{errors = Err, streams = Streams} = get(?fake_ds),
    case maps:is_key({Shard, ?err_get_streams}, Err) of
        false ->
            Filtered = [
                {{S, G}, Stream}
             || Stream = #fake_stream{shard = S, gen = G} <- Streams,
                S =:= Shard,
                G >= Gen
            ],
            {Filtered, []};
        true ->
            {[], [?err_rec(simulated)]}
    end.

make_iterator(_DB, Stream = #fake_stream{shard = Shard}, _TopicFilter, StartTime) ->
    #fake_ds{errors = Err, streams = Streams} = get(?fake_ds),
    case maps:is_key({Shard, ?err_make_iterator}, Err) of
        false ->
            case lists:member(Stream, Streams) of
                true ->
                    {ok, #fake_iter{stream = Stream, time = StartTime}};
                false ->
                    ?err_unrec(no_such_stream)
            end;
        true ->
            ?err_rec(simulated)
    end.

subscribe(_DB, It, _SubOpts) ->
    #fake_iter{stream = #fake_stream{shard = Shard}} = It,
    %% Add subscription:
    Result = with(
        fun(DS) ->
            #fake_ds{errors = Err, ds_subs = DSSubs, sub_ref_ctr = Ctr} = DS,
            case maps:is_key({Shard, ?err_subscribe}, Err) of
                false ->
                    Handle = ?sub_handle(Ctr),
                    SubRef = ?sub_ref(Handle),
                    DSSub = #test_ds_sub{
                        sref = SubRef,
                        handle = Handle,
                        it = It
                    },
                    {
                        {ok, Handle, SubRef},
                        DS#fake_ds{
                            sub_ref_ctr = Ctr + 1,
                            ds_subs = [DSSub | DSSubs]
                        }
                    };
                true ->
                    {
                        ?err_rec(simulated),
                        DS
                    }
            end
        end
    ),
    %% Publish payload for subscription:
    maybe
        {ok, _Handle, SRef} ?= Result,
        #fake_ds{ds_subs = Subs, errors = Err} = get(?fake_ds),
        false ?= maps:is_key({Shard, ?err_publish}, Err),
        {value, SState} = lists:keysearch(SRef, #test_ds_sub.sref, Subs),
        publish_payload(1, 0, true, SState)
    end,
    Result.

unsubscribe(_DB, Handle) ->
    with(#fake_ds.ds_subs, fun(Subs) ->
        {
            ok,
            lists:keydelete(Handle, #test_ds_sub.handle, Subs)
        }
    end).

publish_payloads(BatchSize, SeqNoError, Graceful) ->
    #fake_ds{ds_subs = DSSubs0, errors = Err} = get(?fake_ds),
    lists:foreach(
        fun(DSSub) ->
            maybe_publish_payload(BatchSize, SeqNoError, Graceful, Err, DSSub)
        end,
        DSSubs0
    ).

%%================================================================================
%% Internal functions
%%================================================================================

maybe_publish_payload(BatchSize, SeqNoError, Graceful, Err, DSSub = #test_ds_sub{it = It}) ->
    #fake_iter{stream = #fake_stream{shard = Shard}} = It,
    maps:is_key({Shard, ?err_publish}, Err) orelse
        publish_payload(BatchSize, SeqNoError, Graceful, DSSub).

publish_payload(BatchSize, SeqNoError, Graceful, DSSub = #test_ds_sub{sref = SRef, it = It}) ->
    #fake_ds{gens = Gens, streams = Streams} = get(?fake_ds),
    #fake_iter{stream = Stream} = It,
    case lists:member(Stream, Streams) of
        true ->
            %% Stream exists:
            do_publish_payload(BatchSize, SeqNoError, DSSub);
        false ->
            %% Stream is gone:
            Reason =
                case Graceful of
                    true -> ?err_unrec(generation_is_gone);
                    false -> 'DOWN'
                end,
            destroy_ds_sub(SRef, Reason)
    end.

do_publish_payload(
    BatchSize, SeqNoError, DSSub = #test_ds_sub{sref = SRef, it = It0, seqno = SeqNo0}
) ->
    #fake_ds{gens = Gens, streams = Streams} = get(?fake_ds),
    #fake_iter{stream = #fake_stream{shard = Shard, gen = Gen}} = It0,
    Msg =
        case maps:get(Shard, Gens, 0) > Gen of
            true ->
                SeqNo = SeqNo0 + 1 + SeqNoError,
                #ds_sub_reply{
                    ref = SRef,
                    payload = {ok, end_of_stream},
                    size = 1,
                    seqno = SeqNo
                };
            false ->
                SeqNo = SeqNo0 + BatchSize + SeqNoError,
                %% TODO: make data more realistic:
                It = It0,
                TTVs = [],
                #ds_sub_reply{
                    ref = SRef,
                    payload = {ok, It, TTVs},
                    size = BatchSize,
                    seqno = SeqNo
                }
        end,
    ?tp(test_publish_message_to_sub, #{message => Msg}),
    self() ! Msg,
    update_seqno(DSSub, SeqNo).

update_seqno(DSSub = #test_ds_sub{sref = SRef}, SeqNo) ->
    with(#fake_ds.ds_subs, fun(Subs) ->
        {ok,
            lists:keyreplace(SRef, #test_ds_sub.sref, Subs, DSSub#test_ds_sub{
                seqno = SeqNo
            })}
    end).

destroy_ds_sub(SRef, Reason) ->
    Msg =
        case Reason of
            'DOWN' ->
                {'DOWN', SRef, process, self(), simulated};
            {error, _, _} ->
                %% Note: we send error with invalid seqno. The
                %% client should never check seqno for errors
                %% anyway, else it would end up with dangling
                %% subscriptions:
                #ds_sub_reply{
                    ref = SRef,
                    payload = Reason,
                    size = -1,
                    seqno = -1
                }
        end,
    self() ! Msg,
    with(#fake_ds.ds_subs, fun(Subs) ->
        {ok, lists:keydelete(SRef, #test_ds_sub.sref, Subs)}
    end).

with(Fun) ->
    {Ret, DS} = Fun(get(?fake_ds)),
    put(?fake_ds, DS),
    Ret.

with(Field, Fun) ->
    with(
        fun(World) ->
            {Ret, NewVal} = Fun(element(Field, World)),
            {Ret, setelement(Field, World, NewVal)}
        end
    ).
