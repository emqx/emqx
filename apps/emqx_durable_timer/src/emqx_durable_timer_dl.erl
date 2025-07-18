%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_durable_timer_dl).
-moduledoc """
Data layer for the durable timer queue.

## Topic structure

### Heartbeats topic:

```
"h"/Node/Epoch
```

Sharding by epoch.
Time is always = 0.
Value: <<TimeOfLastHeartbeatMS:64, IsUp:8>>

IsUp: 1 or 0.

### Regular timers

```
"s"/Type/Epoch/Key
```

Sharding by key.
Time: wall time when the timer is due to fire.
Value = arbitrary binary that is passed to the callback.

### Dead hand timers

```
"d"/Type/Epoch/Key
```

Sharding by key.
Time: offset from the last heartbeat for the epoch when the timer is due to fire.
Value = arbitrary binary that is passed to the callback.

### Epochs

```
"e"/"s"/Epoch
"e"/"d"/Epoch
```

Sharding: copy in every shard.
Time: shard monotonic
Value: empty.
""".

%% API:
-export([
    lts_threshold_cb/2,
    shards/0,

    update_epoch/4,
    last_heartbeat/1,
    delete_epoch_if_empty/1,

    dirty_ls_epochs/0,
    dirty_ls_epochs/1,

    insert_epoch_marker/2,

    insert_started_async/5,
    insert_dead_hand/5,
    cancel/2,

    clean_replayed_async/3,
    clean_replayed/3,
    started_topic/3,
    dead_hand_topic/3
]).

-export_type([]).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("internals.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%%================================================================================
%% API functions
%%================================================================================

lts_threshold_cb(0, _) ->
    infinity;
lts_threshold_cb(N, Root) when Root =:= ?top_deadhand; Root =:= ?top_started ->
    %% [<<"d">>, Type, Epoch, Key]
    if
        N =:= 1 -> infinity;
        true -> 0
    end;
lts_threshold_cb(_, _) ->
    0.

%%--------------------------------------------------------------------------------
%% Heartbeats
%%--------------------------------------------------------------------------------

-spec dirty_ls_epochs() ->
    [{node(), emqx_durable_timer:epoch(), IsUp, LastHeartbeat}]
when
    IsUp :: boolean(),
    LastHeartbeat :: integer().
dirty_ls_epochs() ->
    L = emqx_ds:fold_topic(
        fun(_, _, ?hb(NodeBin, Epoch, Time, IUp), Acc) ->
            [{binary_to_atom(NodeBin), Epoch, IUp =:= 1, Time} | Acc]
        end,
        [],
        ?hb_topic('+', '+'),
        #{db => ?DB_GLOB, errors => ignore}
    ),
    lists:keysort(4, L).

-spec dirty_ls_epochs(node()) ->
    [{emqx_durable_timer:epoch(), IsUp, LastHeartbeat}]
when
    IsUp :: boolean(),
    LastHeartbeat :: integer().
dirty_ls_epochs(Node) ->
    L = emqx_ds:fold_topic(
        fun(_, _, ?hb(_, Epoch, Time, IUp), Acc) ->
            [{Epoch, IUp =:= 1, Time} | Acc]
        end,
        [],
        ?hb_topic(atom_to_binary(Node), '+'),
        #{db => ?DB_GLOB, errors => ignore}
    ),
    lists:keysort(3, L).

-spec shards() -> [emqx_ds:shard()].
shards() ->
    %% TODO: it's not the nicest way to get shards. Add an API to DS?
    NShards = emqx_config:get([durable_storage, timers, n_shards]),
    [integer_to_binary(I) || I <- lists:seq(0, NShards - 1)].

-spec last_heartbeat(emqx_durable_timer:epoch()) -> {ok, integer()} | undefined.
last_heartbeat(Epoch) ->
    emqx_ds:fold_topic(
        fun(_, _, ?hb(_, _, Time, _), _) ->
            {ok, Time}
        end,
        undefined,
        ?hb_topic('+', Epoch),
        #{
            db => ?DB_GLOB,
            generation => generation(),
            shard => emqx_ds:shard_of(?DB_GLOB, Epoch)
        }
    ).

-spec update_epoch(node(), emqx_durable_timer:epoch(), integer(), boolean()) ->
    ok | emqx_ds:error(_).
update_epoch(Node, Epoch, LastHeartbeat, IsUp) ->
    NodeBin = atom_to_binary(Node),
    Result = emqx_ds:trans(
        #{
            db => ?DB_GLOB,
            generation => generation(),
            shard => {auto, Epoch},
            retries => 10,
            retry_interval => 100
        },
        fun() ->
            IUp =
                case IsUp of
                    true -> 1;
                    false -> 0
                end,
            ?ds_tx_on_success(
                ?tp(debug, ?tp_update_epoch, #{
                    node => Node, epoch => Epoch, up => IsUp, hb => LastHeartbeat
                })
            ),
            emqx_ds:tx_write(?hb(NodeBin, Epoch, LastHeartbeat, IUp))
        end
    ),
    case Result of
        {atomic, _, _} ->
            ok;
        Err ->
            Err
    end.

-spec delete_epoch_if_empty(emqx_durable_timer:epoch()) -> boolean().
delete_epoch_if_empty(Epoch) ->
    HBShard = emqx_ds:shard_of(?DB_GLOB, Epoch),
    maybe
        {[?hb(NodeBin, _, _, 0)], []} ?=
            emqx_ds:dirty_read(
                #{db => ?DB_GLOB, shard => HBShard, generation => generation(), errors => report},
                ?hb_topic('+', Epoch)
            ),
        false ?=
            lists:any(
                fun(Shard) ->
                    has_data(Shard, [?top_started, '+', Epoch, '+']) orelse
                        has_data(Shard, [?top_deadhand, '+', Epoch, '+'])
                end,
                shards()
            ),
        %% Delete epoch markers:
        lists:foreach(
            fun(Shard) ->
                emqx_ds:trans(
                    epoch_tx_opts(Shard, #{}),
                    fun() ->
                        emqx_ds:tx_del_topic([?top_epoch, Epoch])
                    end
                )
            end,
            shards()
        ),
        %% Delete heartbeat:
        _ = emqx_ds:trans(
            #{
                db => ?DB_GLOB,
                generation => generation(),
                shard => HBShard,
                retries => 10,
                retry_interval => 100
            },
            fun() ->
                emqx_ds:tx_del_topic(?hb_topic(NodeBin, Epoch))
            end
        ),
        true
    else
        _ -> false
    end.

%%--------------------------------------------------------------------------------
%% Epoch data
%%--------------------------------------------------------------------------------

-spec insert_dead_hand(
    emqx_durable_timer:type(),
    emqx_durable_timer:epoch(),
    emqx_durable_timer:key(),
    emqx_durable_timer:value(),
    emqx_durable_timer:delay()
) -> ok.
insert_dead_hand(Type, Epoch, Key, Val, Delay) when
    ?is_valid_timer(Type, Key, Val, Delay) andalso is_binary(Epoch)
->
    Result = emqx_ds:trans(
        epoch_tx_opts({auto, Key}, #{}),
        fun() ->
            tx_del_dead_hand(Type, Key),
            tx_del_started(Type, Key),
            emqx_ds:tx_write({
                dead_hand_topic(Type, Epoch, Key),
                Delay,
                Val
            })
        end
    ),
    case Result of
        {atomic, _, _} ->
            ok;
        Err ->
            Err
    end.

-spec cancel(emqx_durable_timer:type(), emqx_durable_timer:key()) -> ok.
cancel(Type, Key) ->
    Result = emqx_ds:trans(
        epoch_tx_opts({auto, Key}, #{}),
        fun() ->
            tx_del_dead_hand(Type, Key),
            tx_del_started(Type, Key)
        end
    ),
    case Result of
        {atomic, _, _} ->
            ok;
        Err ->
            Err
    end.

-spec insert_started_async(
    emqx_durable_timer:type(),
    emqx_durable_timer:epoch(),
    emqx_durable_timer:key(),
    emqx_durable_timer:value(),
    emqx_durable_timer:delay()
) -> reference().
insert_started_async(Type, Epoch, Key, Val, NotEarlierThan) when
    ?is_valid_timer(Type, Key, Val, NotEarlierThan) andalso is_binary(Epoch)
->
    {async, Ref, _Ret} =
        emqx_ds:trans(
            epoch_tx_opts({auto, Key}, #{sync => false}),
            fun() ->
                %% Clear previous data:
                tx_del_dead_hand(Type, Key),
                tx_del_started(Type, Key),
                %% Insert the new data:
                emqx_ds:tx_write({started_topic(Type, Epoch, Key), NotEarlierThan, Val})
            end
        ),
    Ref.

-spec insert_epoch_marker(emqx_ds:shard(), emqx_durable_timer:epoch()) -> ok.
insert_epoch_marker(Shard, Epoch) ->
    {atomic, _, _} = emqx_ds:trans(
        epoch_tx_opts(Shard, #{}),
        fun() ->
            emqx_ds:tx_write({[?top_epoch, Epoch], ?ds_tx_ts_monotonic, <<>>})
        end
    ),
    ok.

-spec clean_replayed_async(emqx_ds:shard(), emqx_ds:topic_filter(), emqx_ds:time()) -> reference().
clean_replayed_async(Shard, Topic, Time) ->
    {async, Ref, _} =
        emqx_ds:trans(
            epoch_tx_opts(Shard, #{sync => false}),
            fun() ->
                emqx_ds:tx_del_topic(Topic, 0, Time + 1)
            end
        ),
    Ref.

-spec clean_replayed(emqx_ds:shard(), emqx_ds:topic_filter(), emqx_ds:time()) ->
    ok | emqx_ds:error(_).
clean_replayed(Shard, Topic, Time) ->
    Result =
        emqx_ds:trans(
            epoch_tx_opts(Shard, #{}),
            fun() ->
                emqx_ds:tx_del_topic(Topic, 0, Time + 1)
            end
        ),
    case Result of
        {atomic, _, _} ->
            ok;
        Other ->
            Other
    end.

dead_hand_topic(Type, NodeEpochId, Key) ->
    [?top_deadhand, <<Type:?type_bits>>, NodeEpochId, Key].

started_topic(Type, NodeEpochId, Key) ->
    [?top_started, <<Type:?type_bits>>, NodeEpochId, Key].

%%================================================================================
%% Internal functions
%%================================================================================

generation() ->
    1.

%%--------------------------------------------------------------------------------
%% Epoch data
%%--------------------------------------------------------------------------------

tx_del_dead_hand(Type, Key) ->
    [emqx_ds:tx_del_topic(dead_hand_topic(Type, Epoch, Key)) || Epoch <- tx_ls_epochs()],
    ok.

tx_del_started(Type, Key) ->
    [emqx_ds:tx_del_topic(started_topic(Type, Epoch, Key)) || Epoch <- tx_ls_epochs()],
    ok.

tx_ls_epochs() ->
    emqx_ds:tx_fold_topic(
        fun(_, _, {[?top_epoch, E], _, _}, Acc) ->
            [E | Acc]
        end,
        [],
        [?top_epoch, '+']
    ).

epoch_tx_opts(Shard, Other) ->
    Other#{
        db => ?DB_GLOB,
        generation => generation(),
        shard => Shard,
        retries => 10,
        retry_interval => 1000
    }.

has_data(Shard, Topic) ->
    case emqx_ds:get_streams(?DB_GLOB, Topic, 0, #{shard => Shard}) of
        {[], []} ->
            false;
        {Streams, []} ->
            lists:any(
                fun({_Slab, Stream}) ->
                    case emqx_ds:make_iterator(?DB_GLOB, Stream, Topic, 0) of
                        {ok, It} ->
                            case emqx_ds:next(?DB_GLOB, It, 1) of
                                {ok, _, []} ->
                                    false;
                                {ok, end_of_stream} ->
                                    false;
                                _ ->
                                    true
                            end;
                        _ ->
                            %% Assume yes on errors:
                            true
                    end
                end,
                Streams
            );
        _ ->
            %% Assume yes when error happens:
            true
    end.
