-module(replayq).

-export([open/1, close/1]).
-export([append/2, pop/2, ack/2, ack_sync/2, peek/1, overflow/1]).
-export([count/1, bytes/1, is_empty/1, is_mem_only/1]).
%% exported for troubleshooting
-export([do_read_items/2]).

%% internal exports for beam reload
-export([committer_loop/2, default_sizer/1, default_marshaller/1]).

-export_type([config/0, q/0, ack_ref/0, sizer/0, marshaller/0]).

-define(NOTHING_TO_ACK, nothing_to_ack).
-define(PENDING_ACKS(Ref), {replayq_pending_acks, Ref}).

-type segno() :: pos_integer().
-type item() :: term().
-type count() :: non_neg_integer().
-type id() :: count().
-type bytes() :: non_neg_integer().
-type filename() :: file:filename_all().
-type dir() :: filename().
-type ack_ref() :: ?NOTHING_TO_ACK | {segno(), ID :: pos_integer()}.
-type sizer() :: fun((item()) -> bytes()).
-type marshaller() :: fun((item()) -> binary()).

-type config() :: #{
    dir => dir(),
    seg_bytes => bytes(),
    mem_only => boolean(),
    max_total_bytes => bytes(),
    offload => boolean(),
    sizer => sizer(),
    marshaller => marshaller()
}.
%% writer cursor
-define(NO_FD, no_fd).
-type w_cur() :: #{
    segno := segno(),
    bytes := bytes(),
    count := count(),
    fd := ?NO_FD | file:fd()
}.

-type stats() :: #{
    bytes := bytes(),
    count := count()
}.

-opaque q() :: #{
    config := mem_only | config(),
    stats := stats(),
    in_mem := queue:queue(in_mem_item()),
    w_cur => w_cur(),
    committer => pid(),
    head_segno => segno(),
    sizer := sizer(),
    marshaller => marshaller(),
    max_total_bytes := bytes()
}.

-define(LAYOUT_VSN_0, 0).
-define(LAYOUT_VSN_1, 1).
-define(MAGIC, 841265288).
-define(SUFFIX, "replaylog").
-define(DEFAULT_POP_BYTES_LIMIT, 2000000).
-define(DEFAULT_POP_COUNT_LIMIT, 1000).
-define(DEFAULT_REPLAYQ_LIMIT, 2000000000).
-define(COMMIT(SEGNO, ID, From), {commit, SEGNO, ID, From}).
-define(NO_COMMIT_HIST, no_commit_hist).
-define(FIRST_SEGNO, 1).
-define(NEXT_SEGNO(N), (N + 1)).
-define(STOP, stop).
-define(MEM_ONLY_ITEM(Bytes, Item), {Bytes, Item}).
-define(DISK_CP_ITEM(Id, Bytes, Item), {Id, Bytes, Item}).

-type in_mem_item() ::
    ?MEM_ONLY_ITEM(bytes(), item())
    | ?DISK_CP_ITEM(id(), bytes(), item()).

-spec open(config()) -> q().
open(#{mem_only := true} = C) ->
    #{
        stats => #{bytes => 0, count => 0},
        in_mem => queue:new(),
        sizer => get_sizer(C),
        config => mem_only,
        max_total_bytes => maps:get(max_total_bytes, C, ?DEFAULT_REPLAYQ_LIMIT)
    };
open(#{dir := Dir, seg_bytes := _} = Config) ->
    ok = filelib:ensure_dir(filename:join(Dir, "foo")),
    Sizer = get_sizer(Config),
    Marshaller = get_marshaller(Config),
    IsOffload = is_offload_mode(Config),
    Q =
        case delete_consumed_and_list_rest(Dir) of
            [] ->
                %% no old segments, start over from zero
                #{
                    stats => #{bytes => 0, count => 0},
                    w_cur => init_writer(Dir, empty, IsOffload),
                    committer => spawn_committer(?FIRST_SEGNO, Dir),
                    head_segno => ?FIRST_SEGNO,
                    in_mem => queue:new()
                };
            Segs ->
                LastSegno = lists:last(Segs),
                CommitHist = get_commit_hist(Dir),
                Reader = fun(Seg, Ch) -> read_items(Dir, Seg, Ch, Sizer, Marshaller) end,
                HeadItems = Reader(hd(Segs), CommitHist),
                #{
                    stats => collect_stats(HeadItems, tl(Segs), Reader),
                    w_cur => init_writer(Dir, LastSegno, IsOffload),
                    committer => spawn_committer(hd(Segs), Dir),
                    head_segno => hd(Segs),
                    in_mem => queue:from_list(HeadItems)
                }
        end,
    Q#{
        sizer => Sizer,
        marshaller => Marshaller,
        config => maps:without([sizer, marshaller], Config),
        max_total_bytes => maps:get(max_total_bytes, Config, ?DEFAULT_REPLAYQ_LIMIT)
    }.

-spec close(q() | w_cur()) -> ok | {error, any()}.
close(#{config := mem_only}) ->
    ok;
close(#{w_cur := W_Cur, committer := Pid} = Q) ->
    MRef = erlang:monitor(process, Pid),
    Pid ! ?STOP,
    unlink(Pid),
    receive
        {'DOWN', MRef, process, Pid, _Reason} ->
            ok
    end,
    ok = maybe_dump_back_to_disk(Q),
    do_close(W_Cur).

do_close(#{fd := ?NO_FD}) -> ok;
do_close(#{fd := Fd}) -> file:close(Fd).

%% In case of offload mode, dump the unacked (and un-popped) on disk
%% before close. this serves as a best-effort data loss protection
maybe_dump_back_to_disk(#{config := Config} = Q) ->
    case is_offload_mode(Config) of
        true -> dump_back_to_disk(Q);
        false -> ok
    end.

dump_back_to_disk(#{
    config := #{dir := Dir},
    head_segno := ReaderSegno,
    in_mem := InMem,
    marshaller := Marshaller
}) ->
    IoData0 = get_unacked(process_info(self(), dictionary), ReaderSegno, Marshaller),
    Items1 = queue:to_list(InMem),
    IoData1 = lists:map(fun(?DISK_CP_ITEM(_, _, I)) -> make_iodata(I, Marshaller) end, Items1),
    %% ensure old segment file is deleted
    ok = ensure_deleted(filename(Dir, ReaderSegno)),
    %% rewrite the segment with what's currently in memory
    IoData = [IoData0, IoData1],
    case iolist_size(IoData) > 0 of
        true ->
            #{fd := Fd} = open_segment(Dir, ReaderSegno),
            ok = file:write(Fd, [IoData0, IoData1]),
            ok = file:close(Fd);
        false ->
            %% nothing to write
            ok
    end.

get_unacked({dictionary, Dict}, ReaderSegno, Marshaller) ->
    F = fun
        ({?PENDING_ACKS(AckRef), Items}) ->
            erase(?PENDING_ACKS(AckRef)),
            {Segno, Id} = AckRef,
            Segno =:= ReaderSegno andalso
                {true, {Id, Items}};
        (_) ->
            false
    end,
    Pendings0 = lists:filtermap(F, Dict),
    Pendings = lists:keysort(1, Pendings0),
    do_get_unacked(Pendings, Marshaller).

do_get_unacked([], _Marshaller) ->
    [];
do_get_unacked([{_, Items} | Rest], Marshaller) ->
    [
        [make_iodata(I, Marshaller) || I <- Items]
        | do_get_unacked(Rest, Marshaller)
    ].

-spec append(q(), [item()]) -> q().
append(Q, []) ->
    Q;
append(
    #{
        config := mem_only,
        in_mem := InMem,
        stats := #{bytes := Bytes0, count := Count0},
        sizer := Sizer
    } = Q,
    Items0
) ->
    {CountDiff, BytesDiff, Items} = transform(false, Items0, Sizer),

    Stats = #{count => Count0 + CountDiff, bytes => Bytes0 + BytesDiff},
    Q#{
        stats := Stats,
        in_mem := append_in_mem(Items, InMem)
    };
append(
    #{
        config := #{seg_bytes := BytesLimit, dir := Dir} = Config,
        stats := #{bytes := Bytes0, count := Count0},
        w_cur := #{count := CountInSeg, segno := WriterSegno} = W_Cur0,
        head_segno := ReaderSegno,
        sizer := Sizer,
        marshaller := Marshaller,
        in_mem := HeadItems0
    } = Q,
    Items0
) ->
    IoData = lists:map(fun(I) -> make_iodata(I, Marshaller) end, Items0),
    {CountDiff, BytesDiff, Items} = transform(CountInSeg + 1, Items0, Sizer),
    TotalBytes = Bytes0 + BytesDiff,
    Stats = #{count => Count0 + CountDiff, bytes => TotalBytes},
    IsOffload = is_offload_mode(Config),
    W_Cur1 = do_append(W_Cur0, CountDiff, BytesDiff, IoData),
    W_Cur =
        case is_segment_full(W_Cur1, TotalBytes, BytesLimit, ReaderSegno, IsOffload) of
            true ->
                ok = do_close(W_Cur1),
                %% get ready for the next append
                open_segment(Dir, ?NEXT_SEGNO(WriterSegno));
            false ->
                W_Cur1
        end,
    HeadItems =
        case ReaderSegno =:= WriterSegno of
            true -> append_in_mem(Items, HeadItems0);
            false -> HeadItems0
        end,
    Q#{
        stats := Stats,
        w_cur := W_Cur,
        in_mem := HeadItems
    }.

%% @doc pop out at least one item from the queue.
%% volume limited by `bytes_limit' and `count_limit'.
-spec pop(q(), #{bytes_limit => bytes(), count_limit => count()}) ->
    {q(), ack_ref(), [item()]}.
pop(Q, Opts) ->
    Bytes = maps:get(bytes_limit, Opts, ?DEFAULT_POP_BYTES_LIMIT),
    Count = maps:get(count_limit, Opts, ?DEFAULT_POP_COUNT_LIMIT),
    true = (Count > 0),
    pop(Q, Bytes, Count, ?NOTHING_TO_ACK, []).

%% @doc peek the queue front item.
-spec peek(q()) -> empty | item().
peek(#{in_mem := HeadItems}) ->
    case queue:peek(HeadItems) of
        empty -> empty;
        {value, ?MEM_ONLY_ITEM(_, Item)} -> Item;
        {value, ?DISK_CP_ITEM(_, _, Item)} -> Item
    end.

%% @doc Asynch-ly write the consumed item Segment number + ID to a file.
-spec ack(q(), ack_ref()) -> ok.
ack(_, ?NOTHING_TO_ACK) ->
    ok;
ack(#{committer := Pid}, {Segno, Id} = AckRef) ->
    _ = erlang:erase(?PENDING_ACKS(AckRef)),
    Pid ! ?COMMIT(Segno, Id, false),
    ok.

%% @hidden Synced ack, for deterministic tests only
-spec ack_sync(q(), ack_ref()) -> ok.
ack_sync(_, ?NOTHING_TO_ACK) ->
    ok;
ack_sync(#{committer := Pid}, {Segno, Id} = AckRef) ->
    _ = erlang:erase(?PENDING_ACKS(AckRef)),
    Ref = make_ref(),
    Pid ! ?COMMIT(Segno, Id, {self(), Ref}),
    receive
        {Ref, ok} -> ok
    end.

-spec count(q()) -> count().
count(#{stats := #{count := Count}}) -> Count.

-spec bytes(q()) -> bytes().
bytes(#{stats := #{bytes := Bytes}}) -> Bytes.

is_empty(#{config := mem_only, in_mem := All}) ->
    queue:is_empty(All);
is_empty(
    #{
        w_cur := #{segno := WriterSegno},
        head_segno := ReaderSegno,
        in_mem := HeadItems
    } = Q
) ->
    Result = ((WriterSegno =:= ReaderSegno) andalso queue:is_empty(HeadItems)),
    %% assert
    Result = (count(Q) =:= 0).

%% @doc Returns number of bytes the size of the queue has exceeded
%% total bytes limit. Result is negative when it is not overflow.
-spec overflow(q()) -> integer().
overflow(#{
    max_total_bytes := MaxTotalBytes,
    stats := #{bytes := Bytes}
}) ->
    Bytes - MaxTotalBytes.

-spec is_mem_only(q()) -> boolean().
is_mem_only(#{config := mem_only}) ->
    true;
is_mem_only(_) ->
    false.

%% internals =========================================================

transform(Id, Items, Sizer) ->
    transform(Id, Items, Sizer, 0, 0, []).

transform(_Id, [], _Sizer, Count, Bytes, Acc) ->
    {Count, Bytes, lists:reverse(Acc)};
transform(Id, [Item0 | Rest], Sizer, Count, Bytes, Acc) ->
    Size = Sizer(Item0),
    {NextId, Item} =
        case Id of
            false -> {false, ?MEM_ONLY_ITEM(Size, Item0)};
            N -> {N + 1, ?DISK_CP_ITEM(Id, Size, Item0)}
        end,
    transform(NextId, Rest, Sizer, Count + 1, Bytes + Size, [Item | Acc]).

append_in_mem([], Q) -> Q;
append_in_mem([Item | Rest], Q) -> append_in_mem(Rest, queue:in(Item, Q)).

pop(Q, _Bytes, 0, AckRef, Acc) ->
    Result = lists:reverse(Acc),
    ok = maybe_save_pending_acks(AckRef, Q, Result),
    {Q, AckRef, Result};
pop(#{config := Cfg} = Q, Bytes, Count, AckRef, Acc) ->
    case is_empty(Q) of
        true ->
            {Q, AckRef, lists:reverse(Acc)};
        false when Cfg =:= mem_only ->
            pop_mem(Q, Bytes, Count, Acc);
        false ->
            pop2(Q, Bytes, Count, AckRef, Acc)
    end.

pop_mem(
    #{
        in_mem := InMem,
        stats := #{count := TotalCount, bytes := TotalBytes} = Stats
    } = Q,
    Bytes,
    Count,
    Acc
) ->
    case queue:out(InMem) of
        {{value, ?MEM_ONLY_ITEM(Sz, _Item)}, _} when Sz > Bytes andalso Acc =/= [] ->
            {Q, ?NOTHING_TO_ACK, lists:reverse(Acc)};
        {{value, ?MEM_ONLY_ITEM(Sz, Item)}, Rest} ->
            NewQ = Q#{
                in_mem := Rest,
                stats := Stats#{
                    count := TotalCount - 1,
                    bytes := TotalBytes - Sz
                }
            },
            pop(NewQ, Bytes - Sz, Count - 1, ?NOTHING_TO_ACK, [Item | Acc])
    end.

pop2(
    #{
        head_segno := ReaderSegno,
        in_mem := HeadItems,
        stats := #{count := TotalCount, bytes := TotalBytes} = Stats,
        w_cur := #{segno := WriterSegno}
    } = Q,
    Bytes,
    Count,
    AckRef,
    Acc
) ->
    case queue:out(HeadItems) of
        {{value, ?DISK_CP_ITEM(_, Sz, _Item)}, _} when Sz > Bytes andalso Acc =/= [] ->
            %% taking the head item would cause exceeding size limit
            {Q, AckRef, lists:reverse(Acc)};
        {{value, ?DISK_CP_ITEM(Id, Sz, Item)}, Rest} ->
            Q1 = Q#{
                in_mem := Rest,
                stats := Stats#{
                    count := TotalCount - 1,
                    bytes := TotalBytes - Sz
                }
            },
            %% read the next segment in case current is drained
            NewQ =
                case queue:is_empty(Rest) andalso ReaderSegno < WriterSegno of
                    true -> read_next_seg(Q1);
                    false -> Q1
                end,
            NewAckRef = {ReaderSegno, Id},
            pop(NewQ, Bytes - Sz, Count - 1, NewAckRef, [Item | Acc])
    end.

%% due to backward compatibility reasons for the ack api
%% we ca nnot track pending acks in q() -- reason to use process dictionary
maybe_save_pending_acks(?NOTHING_TO_ACK, _, _) ->
    ok;
maybe_save_pending_acks(AckRef, #{config := Config}, Items) ->
    case is_offload_mode(Config) of
        true ->
            _ = erlang:put(?PENDING_ACKS(AckRef), Items),
            ok;
        false ->
            ok
    end.

read_next_seg(
    #{
        config := #{dir := Dir} = Config,
        head_segno := ReaderSegno,
        w_cur := #{segno := WriterSegno, fd := Fd} = WCur0,
        sizer := Sizer,
        marshaller := Marshaller
    } = Q
) ->
    NextSegno = ReaderSegno + 1,
    %% reader has caught up to latest segment
    case NextSegno =:= WriterSegno of
        true ->
            %% force flush to disk so the next read can get all bytes
            ok = file:sync(Fd);
        false ->
            ok
    end,
    IsOffload = is_offload_mode(Config),
    WCur =
        case IsOffload andalso NextSegno =:= WriterSegno of
            true ->
                %% reader has caught up to latest segment in offload mode,
                %% close the writer's fd. Continue in mem-only mode for the head segment
                ok = do_close(WCur0),
                WCur0#{fd := ?NO_FD};
            false ->
                WCur0
        end,
    NextSegItems = read_items(Dir, NextSegno, ?NO_COMMIT_HIST, Sizer, Marshaller),
    Q#{
        head_segno := NextSegno,
        in_mem := queue:from_list(NextSegItems),
        w_cur := WCur
    }.

delete_consumed_and_list_rest(Dir0) ->
    Dir = unicode:characters_to_list(Dir0),
    Segnos0 = lists:sort([parse_segno(N) || N <- filelib:wildcard("*." ?SUFFIX, Dir)]),
    {SegnosToDelete, Segnos} = find_segnos_to_delete(Dir, Segnos0),
    ok = lists:foreach(fun(Segno) -> ensure_deleted(filename(Dir, Segno)) end, SegnosToDelete),
    case Segnos of
        [] ->
            %% delete commit file in case there is no segments left
            %% segment number will start from 0 again.
            ensure_deleted(commit_filename(Dir)),
            [];
        X ->
            X
    end.

find_segnos_to_delete(Dir, Segnos) ->
    CommitHist = get_commit_hist(Dir),
    do_find_segnos_to_delete(Dir, Segnos, CommitHist).

do_find_segnos_to_delete(_Dir, Segnos, ?NO_COMMIT_HIST) ->
    {[], Segnos};
do_find_segnos_to_delete(Dir, Segnos0, {CommittedSegno, CommittedId}) ->
    {SegnosToDelete, Segnos} = lists:partition(fun(N) -> N < CommittedSegno end, Segnos0),
    case
        Segnos =/= [] andalso
            hd(Segnos) =:= CommittedSegno andalso
            is_all_consumed(Dir, CommittedSegno, CommittedId)
    of
        true ->
            %% assert
            CommittedSegno = hd(Segnos),
            %% all items in the oldest segment have been consumed,
            %% no need to keep this segment
            {[CommittedSegno | SegnosToDelete], tl(Segnos)};
        _ ->
            {SegnosToDelete, Segnos}
    end.

%% ALL items are consumed if the committed item ID is no-less than the number
%% of items in this segment
is_all_consumed(Dir, CommittedSegno, CommittedId) ->
    CommittedId >= erlang:length(do_read_items(Dir, CommittedSegno)).

ensure_deleted(Filename) ->
    case file:delete(Filename) of
        ok -> ok;
        {error, enoent} -> ok
    end.

%% The committer writes consumer's acked segmeng number + item ID
%% to a file. The file is only read at start/restart.
spawn_committer(ReaderSegno, Dir) ->
    Name = iolist_to_binary(filename:join([Dir, committer])),
    %% register a name to avoid having two committers spawned for the same dir
    RegName = binary_to_atom(Name, utf8),
    Pid = erlang:spawn_link(fun() -> committer_loop(ReaderSegno, Dir) end),
    true = erlang:register(RegName, Pid),
    Pid.

committer_loop(ReaderSegno, Dir) ->
    receive
        ?COMMIT(Segno0, Id0, false) ->
            {Segno, Id} = collect_async_commits(Segno0, Id0),
            ok = handle_commit(ReaderSegno, Dir, Segno, Id, false),
            ?MODULE:committer_loop(Segno, Dir);
        ?COMMIT(Segno, Id, From) ->
            ok = handle_commit(ReaderSegno, Dir, Segno, Id, From),
            ?MODULE:committer_loop(Segno, Dir);
        ?STOP ->
            ok;
        Msg ->
            exit({replayq_committer_unkown_msg, Msg})
    after 200 ->
        ?MODULE:committer_loop(ReaderSegno, Dir)
    end.

handle_commit(ReaderSegno, Dir, Segno, Id, From) ->
    IoData = io_lib:format("~p.\n", [#{segno => Segno, id => Id}]),
    ok = do_commit(Dir, IoData),
    case Segno > ReaderSegno of
        true ->
            SegnosToDelete = lists:seq(ReaderSegno, Segno - 1),
            lists:foreach(fun(N) -> ok = ensure_deleted(filename(Dir, N)) end, SegnosToDelete);
        false ->
            ok
    end,
    ok = reply_ack_ok(From).

%% Collect async acks which are already sent in the mailbox,
%% and keep only the last one for the current segment.
collect_async_commits(Segno, Id) ->
    receive
        ?COMMIT(Segno, AnotherId, false) ->
            collect_async_commits(Segno, AnotherId)
    after 0 ->
        {Segno, Id}
    end.

reply_ack_ok({Pid, Ref}) ->
    Pid ! {Ref, ok},
    ok;
reply_ack_ok(_) ->
    ok.

get_commit_hist(Dir) ->
    CommitFile = commit_filename(Dir),
    case filelib:is_regular(CommitFile) of
        true ->
            {ok, [#{segno := Segno, id := Id}]} = file:consult(CommitFile),
            {Segno, Id};
        false ->
            ?NO_COMMIT_HIST
    end.

do_commit(Dir, IoData) ->
    TmpName = commit_filename(Dir, "COMMIT.tmp"),
    Name = commit_filename(Dir),
    ok = file:write_file(TmpName, IoData),
    ok = file:rename(TmpName, Name).

commit_filename(Dir) ->
    commit_filename(Dir, "COMMIT").

commit_filename(Dir, Name) ->
    filename:join([Dir, Name]).

do_append(
    #{fd := ?NO_FD, bytes := Bytes0, count := Count0} = Cur,
    Count,
    Bytes,
    _IoData
) ->
    %% offload mode, fd is not initialized yet
    Cur#{
        bytes => Bytes0 + Bytes,
        count => Count0 + Count
    };
do_append(
    #{fd := Fd, bytes := Bytes0, count := Count0} = Cur,
    Count,
    Bytes,
    IoData
) ->
    ok = file:write(Fd, IoData),
    Cur#{
        bytes => Bytes0 + Bytes,
        count => Count0 + Count
    }.

read_items(Dir, Segno, CommitHist, Sizer, Marshaller) ->
    Items0 = do_read_items(Dir, Segno),
    Items =
        case CommitHist of
            ?NO_COMMIT_HIST ->
                %% no commit hist, return all
                Items0;
            {CommitedSegno, _} when CommitedSegno < Segno ->
                %% committed at an older segment
                Items0;
            {Segno, CommittedId} ->
                %% committed at current segment keep only the tail
                {_, R} = lists:splitwith(fun({I, _}) -> I =< CommittedId end, Items0),
                R
        end,
    lists:map(
        fun({Id, Bin}) ->
            Item = Marshaller(Bin),
            Size = Sizer(Item),
            ?DISK_CP_ITEM(Id, Size, Item)
        end,
        Items
    ).

do_read_items(Dir, Segno) ->
    Filename = filename(Dir, Segno),
    {ok, Bin} = file:read_file(Filename),
    case parse_items(Bin, 1, []) of
        {Items, <<>>} ->
            Items;
        {Items, Corrupted} ->
            error_logger:error_msg(
                "corrupted replayq log: ~s, skipped ~p bytes",
                [Filename, size(Corrupted)]
            ),
            Items
    end.

parse_items(<<>>, _Id, Acc) ->
    {lists:reverse(Acc), <<>>};
parse_items(
    <<?LAYOUT_VSN_1:8, ?MAGIC:32/unsigned-integer, CRC:32/unsigned-integer,
        Size:32/unsigned-integer, Item:Size/binary, Rest/binary>> = All,
    Id,
    Acc
) ->
    case CRC =:= erlang:crc32(Item) of
        true -> parse_items(Rest, Id + 1, [{Id, Item} | Acc]);
        false -> {lists:reverse(Acc), All}
    end;
parse_items(
    <<?LAYOUT_VSN_0:8, CRC:32/unsigned-integer, Size:32/unsigned-integer, Item:Size/binary,
        Rest/binary>> = All,
    Id,
    Acc
) ->
    case CRC =:= erlang:crc32(Item) andalso Item =/= <<>> of
        true -> parse_items(Rest, Id + 1, [{Id, Item} | Acc]);
        false -> {lists:reverse(Acc), All}
    end;
parse_items(Corrupted, _Id, Acc) ->
    {lists:reverse(Acc), Corrupted}.

make_iodata(Item0, Marshaller) ->
    Item = Marshaller(Item0),
    Size = size(Item),
    CRC = erlang:crc32(Item),
    [
        <<?LAYOUT_VSN_1:8, ?MAGIC:32/unsigned-integer, CRC:32/unsigned-integer,
            Size:32/unsigned-integer>>,
        Item
    ].

collect_stats(HeadItems, SegsOnDisk, Reader) ->
    ItemF = fun(?DISK_CP_ITEM(_Id, Sz, _Item), {B, C}) ->
        {B + Sz, C + 1}
    end,
    Acc0 = lists:foldl(ItemF, {0, 0}, HeadItems),
    {Bytes, Count} =
        lists:foldl(
            fun(Segno, Acc) ->
                Items = Reader(Segno, ?NO_COMMIT_HIST),
                lists:foldl(ItemF, Acc, Items)
            end,
            Acc0,
            SegsOnDisk
        ),
    #{bytes => Bytes, count => Count}.

parse_segno(Filename) ->
    [Segno, ?SUFFIX] = string:tokens(Filename, "."),
    list_to_integer(Segno).

filename(Dir, Segno) ->
    Name = lists:flatten(io_lib:format("~10.10.0w." ?SUFFIX, [Segno])),
    filename:join(Dir, Name).

%% open the current segment for write if it is empty
%% otherwise rollout to the next segment
-spec init_writer(dir(), empty | segno(), boolean()) -> w_cur().
init_writer(_Dir, empty, true) ->
    %% clean start for offload mode
    #{fd => ?NO_FD, segno => ?FIRST_SEGNO, bytes => 0, count => 0};
init_writer(Dir, empty, false) ->
    open_segment(Dir, ?FIRST_SEGNO);
init_writer(Dir, Segno, _IsOffload) when is_number(Segno) ->
    Filename = filename(Dir, Segno),
    case filelib:file_size(Filename) of
        0 -> open_segment(Dir, Segno);
        _ -> open_segment(Dir, ?NEXT_SEGNO(Segno))
    end.

-spec open_segment(dir(), segno()) -> w_cur().
open_segment(Dir, Segno) ->
    Filename = filename(Dir, Segno),
    %% raw so there is no need to go through the single gen_server file_server
    {ok, Fd} = file:open(Filename, [raw, read, write, binary, delayed_write]),
    #{fd => Fd, segno => Segno, bytes => 0, count => 0}.

get_sizer(C) ->
    maps:get(sizer, C, fun ?MODULE:default_sizer/1).

get_marshaller(C) ->
    maps:get(marshaller, C, fun ?MODULE:default_marshaller/1).

is_offload_mode(Config) when is_map(Config) ->
    maps:get(offload, Config, false).

default_sizer(I) when is_binary(I) -> erlang:size(I).

default_marshaller(I) when is_binary(I) -> I.

is_segment_full(
    #{segno := WriterSegno, bytes := SegmentBytes},
    TotalBytes,
    SegmentBytesLimit,
    ReaderSegno,
    true
) ->
    %% in offload mode, when reader is lagging behind, we try
    %% writer rolls to a new segment when file size limit is reached
    %% when reader is reading off from the same segment as writer
    %% i.e. the in memory queue, only start writing to segment file
    %% when total bytes (in memory) is reached segment limit
    %%
    %% NOTE: we never shrink segment bytes, even when popping out
    %% from the in-memory queue.
    case ReaderSegno < WriterSegno of
        true -> SegmentBytes >= SegmentBytesLimit;
        false -> TotalBytes >= SegmentBytesLimit
    end;
is_segment_full(
    #{bytes := SegmentBytes},
    _TotalBytes,
    SegmentBytesLimit,
    _ReaderSegno,
    false
) ->
    %% here we check if segment size is greater than segment size limit
    %% after append based on the assumption that the items are usually
    %% very small in size comparing to segment size.
    %% We can change implementation to split items list to avoid
    %% segment overflow if really necessary
    SegmentBytes >= SegmentBytesLimit.
