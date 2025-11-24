%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_state_db).

-export([
    open/0,
    close/0,
    wait_readiness/1
]).

-export([
    lease_shard_async/5,
    progress_shard_async/5,
    release_shard_async/5,
    progress_shard_tx_result/3,
    announce_consumer/4,
    reannounce_consumer/4,
    deannounce_consumer/3,
    shard_progress_dirty/1,
    shard_progress_dirty/2,
    shard_leases_dirty/1
]).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx_utils/include/emqx_ds_dbs.hrl").

-define(DB, ?STREAMS_STATE_DB).

-define(topic_shard(SGROUP, TAIL), [<<"sdisp">>, SGROUP | TAIL]).
-define(topic_shard_lease(SGROUP, SHARD), ?topic_shard(SGROUP, [<<"ls">>, SHARD])).
-define(topic_shard_hbeat(SGROUP, SHARD), ?topic_shard(SGROUP, [<<"hb">>, SHARD])).
-define(topic_consumer_announce(SGROUP), ?topic_shard(SGROUP, [<<"ann">>])).

-define(topic_shard_progress(SGROUP, SHARD), [<<"sdisp:pr">>, SGROUP, SHARD]).

%%

open() ->
    Config = emqx_ds_schema:db_config_streams_states(),
    ok = emqx_ds:open_db(?DB, Config#{
        storage => {emqx_ds_storage_skipstream_lts_v2, #{}}
    }).

close() ->
    emqx_ds:close_db(?DB).

-spec wait_readiness(timeout()) -> ok | timeout.
wait_readiness(Timeout) ->
    emqx_ds:wait_db(?DB, all, Timeout).

%%

-define(offset_ahead(O1, O2), (is_integer(O1) andalso O1 > O2)).

lease_shard_async(SGroup, Shard, Consumer, OffsetNext, Heartbeat) ->
    async_tx(SGroup, fun() ->
        TopicLease = ?topic_shard_lease(SGroup, Shard),
        TopicProgress = ?topic_shard_progress(SGroup, Shard),
        emqx_ds:tx_ttv_assert_absent(TopicLease, 0),
        case ttv_extract_offset(emqx_ds:tx_read(#{end_time => 1}, TopicProgress)) of
            Offset when not ?offset_ahead(Offset, OffsetNext) ->
                emqx_ds:tx_write({TopicLease, 0, Consumer}),
                emqx_ds:tx_write(mk_ttv_heartbeat(SGroup, Shard, Heartbeat)),
                emqx_ds:tx_write({TopicProgress, 0, enc_offset(OffsetNext)});
            Offset ->
                {invalid, {offset_ahead, Offset}}
        end
    end).

progress_shard_async(SGroup, Shard, Consumer, OffsetNext, Heartbeat) ->
    async_tx(SGroup, fun() ->
        TopicLease = ?topic_shard_lease(SGroup, Shard),
        TopicProgress = ?topic_shard_progress(SGroup, Shard),
        emqx_ds:tx_ttv_assert_present(TopicLease, 0, Consumer),
        case ttv_extract_offset(emqx_ds:tx_read(#{end_time => 1}, TopicProgress)) of
            Offset when not ?offset_ahead(Offset, OffsetNext) ->
                has_offset_changed(Offset, OffsetNext) andalso
                    emqx_ds:tx_write({TopicProgress, 0, enc_offset(OffsetNext)}),
                emqx_ds:tx_write(mk_ttv_heartbeat(SGroup, Shard, Heartbeat));
            Offset ->
                {invalid, {offset_ahead, Offset}}
        end
    end).

release_shard_async(SGroup, Shard, Consumer, OffsetNext, LastHeartbeat) ->
    async_tx(SGroup, fun() ->
        TopicLease = ?topic_shard_lease(SGroup, Shard),
        TopicHeartbeat = ?topic_shard_hbeat(SGroup, Shard),
        TopicProgress = ?topic_shard_progress(SGroup, Shard),
        emqx_ds:tx_ttv_assert_present(TopicLease, 0, Consumer),
        emqx_ds:tx_ttv_assert_present(TopicHeartbeat, 0, enc_timestamp(LastHeartbeat)),
        case ttv_extract_offset(emqx_ds:tx_read(#{end_time => 1}, TopicProgress)) of
            Offset when not ?offset_ahead(Offset, OffsetNext) ->
                emqx_ds:tx_del_topic(TopicLease, 0, 1),
                emqx_ds:tx_write({TopicProgress, 0, enc_offset(OffsetNext)});
            Offset ->
                {invalid, {offset_ahead, Offset}}
        end
    end).

has_offset_changed(undefined, _) ->
    true;
has_offset_changed(Offset1, Offset2) ->
    Offset2 > Offset1.

ttv_extract_offset([{_Topic, 0, V}]) ->
    dec_offset(V);
ttv_extract_offset([]) ->
    undefined.

mk_ttv_heartbeat(SGroup, Shard, Ts) ->
    {?topic_shard_hbeat(SGroup, Shard), 0, enc_timestamp(Ts)}.

progress_shard_tx_result(ok, Ref, Reply) ->
    case emqx_ds:tx_commit_outcome(?DB, Ref, Reply) of
        {ok, _} ->
            ok;
        ?err_unrec({precondition_failed, [How | _]}) ->
            case How of
                #{topic := ?topic_shard_lease(_, _), unexpected := DifferentConsumer} ->
                    {invalid, {leased, DifferentConsumer}};
                #{topic := ?topic_shard_lease(_, _), got := DifferentConsumer} ->
                    {invalid, {leased, DifferentConsumer}};
                #{topic := ?topic_shard_hbeat(_, _), got := Different} ->
                    DifferentOffset = emqx_maybe:apply(fun dec_offset/1, Different),
                    {invalid, {offset_mismatch, DifferentOffset}}
            end;
        ?err_rec({read_conflict, _}) ->
            {invalid, conflict};
        Error ->
            Error
    end;
progress_shard_tx_result(Invalid, Ref, Reply) ->
    _ = emqx_ds:tx_commit_outcome(?DB, Ref, Reply),
    Invalid.

-define(announce_hbits, 16).
-define(time_announce(HEARTBEAT, HBITS), max(0, (HEARTBEAT) bsl ?announce_hbits + (HBITS))).

announce_consumer(SGroup, Consumer, Heartbeat, Lifetime) ->
    <<HBits:?announce_hbits, _/bits>> = erlang:md5(Consumer),
    sync_tx(SGroup, fun() ->
        TopicAnnounce = ?topic_consumer_announce(SGroup),
        emqx_ds:tx_del_topic(TopicAnnounce, 0, ?time_announce(Heartbeat - Lifetime, 0)),
        emqx_ds:tx_write({TopicAnnounce, ?time_announce(Heartbeat, HBits), Consumer})
    end).

reannounce_consumer(SGroup, Consumer, Heartbeat, HeartbeatPrev) ->
    <<HBits:?announce_hbits, _/bits>> = erlang:md5(Consumer),
    sync_tx(SGroup, fun() ->
        TopicAnnounce = ?topic_consumer_announce(SGroup),
        Time = ?time_announce(Heartbeat, HBits),
        TimePrev = ?time_announce(HeartbeatPrev, HBits),
        emqx_ds:tx_del_topic(TopicAnnounce, TimePrev, TimePrev + 1),
        emqx_ds:tx_write({TopicAnnounce, Time, Consumer})
    end).

deannounce_consumer(SGroup, Consumer, Heartbeat) ->
    <<HBits:?announce_hbits, _/bits>> = erlang:md5(Consumer),
    Ret = sync_tx(SGroup, fun() ->
        TopicAnnounce = ?topic_consumer_announce(SGroup),
        Time = ?time_announce(Heartbeat, HBits),
        emqx_ds:tx_ttv_assert_present(TopicAnnounce, Time, Consumer),
        emqx_ds:tx_del_topic(TopicAnnounce, Time, Time + 1)
    end),
    case Ret of
        ok ->
            ok;
        ?err_unrec({precondition_failed, [#{topic := ?topic_consumer_announce(_)} | _]}) ->
            {invalid, undefined};
        Error ->
            Error
    end.

shard_progress_dirty(SGroup) ->
    TTVs = emqx_ds:dirty_read(
        #{
            db => ?DB,
            shard => emqx_ds:shard_of(?DB, SGroup),
            generation => 1,
            end_time => 1
        },
        ?topic_shard_progress(SGroup, '+')
    ),
    lists:foldl(
        fun({?topic_shard_progress(_, Shard), _, V}, Acc) ->
            Acc#{Shard => dec_offset(V)}
        end,
        #{},
        TTVs
    ).

shard_progress_dirty(SGroup, Shard) ->
    ttv_extract_offset(
        emqx_ds:dirty_read(
            #{
                db => ?DB,
                shard => emqx_ds:shard_of(?DB, SGroup),
                generation => 1,
                end_time => 1
            },
            ?topic_shard_progress(SGroup, Shard)
        )
    ).

shard_leases_dirty(SGroup) ->
    TTVs = emqx_ds:dirty_read(
        #{
            db => ?DB,
            shard => emqx_ds:shard_of(?DB, SGroup),
            generation => 1
            % end_time => 1
        },
        ?topic_shard(SGroup, ['#'])
    ),
    Leases = lists:foldl(
        fun
            ({?topic_shard_lease(_, Shard), _, Consumer}, Acc) ->
                Acc#{Shard => Consumer};
            (_TTV, Acc) ->
                Acc
        end,
        #{},
        TTVs
    ),
    Consumers = lists:foldl(
        fun
            ({?topic_consumer_announce(_), Time, Consumer}, Acc) ->
                Heartbeat = Time bsr ?announce_hbits,
                HeartbeatShard = maps:get(Consumer, Acc, 0),
                Acc#{Consumer => max(Heartbeat, HeartbeatShard)};
            ({?topic_shard_hbeat(_, Shard), _, V}, Acc) ->
                Heartbeat = dec_timestamp(V),
                case maps:get(Shard, Leases, undefined) of
                    undefined ->
                        Acc;
                    Consumer ->
                        Acc#{Consumer => max(Heartbeat, maps:get(Consumer, Acc, 0))}
                end;
            (_TTV, Acc) ->
                Acc
        end,
        #{},
        TTVs
    ),
    {Consumers, Leases}.

async_tx(SGroup, Fun) ->
    TxRet = emqx_ds:trans(
        #{
            db => ?DB,
            shard => {auto, SGroup},
            generation => 1,
            sync => false,
            retries => 0
        },
        Fun
    ),
    case TxRet of
        {async, _Ref, _Ret} = TxAsync ->
            TxAsync;
        {nop, Ret} ->
            Ret;
        Error ->
            Error
    end.

sync_tx(SGroup, Fun) ->
    TxRet = emqx_ds:trans(
        #{
            db => ?DB,
            shard => {auto, SGroup},
            generation => 1,
            sync => true,
            retries => 0
        },
        Fun
    ),
    case TxRet of
        {atomic, _Serial, Ret} ->
            Ret;
        {nop, Ret} ->
            Ret;
        Error ->
            Error
    end.

enc_offset(Offset) ->
    integer_to_binary(Offset).

enc_timestamp(Offset) ->
    integer_to_binary(Offset).

dec_offset(Value) ->
    binary_to_integer(Value).

dec_timestamp(Value) ->
    binary_to_integer(Value).
