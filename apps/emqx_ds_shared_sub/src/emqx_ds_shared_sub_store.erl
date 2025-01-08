%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_store).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-export([
    open/0,
    close/0
]).

%% Leadership API
-export([
    %% Leadership claims
    claim_leadership/3,
    renew_leadership/3,
    disown_leadership/2,
    %% Accessors
    leader_id/1,
    alive_until/1,
    heartbeat_interval/1
]).

%% Store API
-export([
    %% Lifecycle
    init/1,
    open/1,
    exists/1,
    id/1,
    dirty/1,
    create/1,
    destroy/1,
    commit_dirty/2,
    commit_renew/3,
    %% Managing records
    get/3,
    get/4,
    fold/4,
    size/2,
    put/4,
    get/2,
    set/3,
    delete/3
]).

-export([
    select/1,
    select/2,
    select_next/2,
    select_preserve/1
]).

%% Messing with IDs
-export([
    mk_id/1,
    mk_id/2
]).

-export_type([
    t/0,
    id/0,
    leader_claim/1
]).

-type id() :: binary().
-type leader_claim(Leader) :: {Leader, _Heartbeat :: emqx_message:timestamp()}.

-define(DS_DB, dqleader).

-define(LEADER_TTL, 30_000).
-define(LEADER_HEARTBEAT_INTERVAL, 10_000).

-define(LEADER_TOPIC_PREFIX, <<"$leader">>).
-define(LEADER_HEADER_HEARTBEAT, <<"$leader.ts">>).

-define(STORE_TOPIC_PREFIX, <<"$s">>).

-define(STORE_SK(SPACE, KEY), [SPACE | KEY]).
-define(STORE_STAGE_ENTRY(SEQNUM, VALUE), {SEQNUM, VALUE}).
-define(STORE_TOMBSTONE, '$tombstone').
-define(STORE_PAYLOAD(ID, VALUE), [ID, VALUE]).
-define(STORE_HEADER_CHANGESEQNUM, '$store.seqnum').

-define(STORE_BATCH_SIZE, 500).
-define(STORE_SLURP_RETRIES, 2).
-define(STORE_SLURP_RETRY_TIMEOUT, 1000).

-ifdef(TEST).
-undef(LEADER_TTL).
-undef(LEADER_HEARTBEAT_INTERVAL).
-define(LEADER_TTL, 3_000).
-define(LEADER_HEARTBEAT_INTERVAL, 1_000).
-endif.

%%

open() ->
    emqx_ds:open_db(?DS_DB, db_config()).

close() ->
    emqx_ds:close_db(?DS_DB).

db_config() ->
    Config = emqx_ds_schema:db_config([durable_storage, queues]),
    tune_db_config(Config).

tune_db_config(Config0 = #{backend := Backend}) ->
    Config = Config0#{
        %% We need total control over timestamp assignment.
        append_only => false
    },
    case Backend of
        B when B == builtin_raft; B == builtin_local ->
            Storage =
                {emqx_ds_storage_bitfield_lts, #{
                    %% Should be enough, topic structure is pretty simple.
                    topic_index_bytes => 4,
                    bits_per_wildcard_level => 64,
                    %% Enables single-epoch storage.
                    epoch_bits => 64,
                    lts_threshold_spec => {simple, {inf, 0, inf, 0}}
                }},
            Config#{storage => Storage};
        _ ->
            Config
    end.

%%

-spec mk_id(emqx_types:share()) -> id().
mk_id(#share{group = ShareGroup, topic = Topic}) ->
    mk_id(ShareGroup, Topic).

-spec mk_id(_GroupName :: binary(), emqx_types:topic()) -> id().
mk_id(Group, Topic) ->
    %% NOTE: Should not contain `/`s.
    %% TODO: More observable encoding.
    iolist_to_binary([Group, $:, binary:encode_hex(Topic)]).

%%

-spec claim_leadership(id(), Leader, emqx_message:timestamp()) ->
    {ok | exists, leader_claim(Leader)} | emqx_ds:error(_).
claim_leadership(ID, Leader, TS) ->
    LeaderClaim = {Leader, TS},
    case try_replace_leader(ID, LeaderClaim, undefined) of
        ok ->
            {ok, LeaderClaim};
        {exists, ExistingClaim = {_, LastHeartbeat}} when LastHeartbeat > TS - ?LEADER_TTL ->
            {exists, ExistingClaim};
        {exists, ExistingClaim = {_LeaderDead, _}} ->
            case try_replace_leader(ID, LeaderClaim, ExistingClaim) of
                ok ->
                    {ok, LeaderClaim};
                {exists, ConcurrentClaim} ->
                    {exists, ConcurrentClaim};
                Error ->
                    Error
            end;
        Error ->
            Error
    end.

-spec renew_leadership(id(), leader_claim(Leader), emqx_message:timestamp()) ->
    {ok | exists, leader_claim(Leader)} | emqx_ds:error(_).
renew_leadership(ID, LeaderClaim, TS) ->
    RenewedClaim = renew_claim(LeaderClaim, TS),
    case RenewedClaim =/= false andalso try_replace_leader(ID, RenewedClaim, LeaderClaim) of
        ok ->
            {ok, RenewedClaim};
        {exists, NewestClaim} ->
            {exists, NewestClaim};
        false ->
            {error, unrecoverable, leader_claim_outdated};
        Error ->
            Error
    end.

-spec renew_claim(leader_claim(ID), emqx_message:timestamp()) -> leader_claim(ID) | false.
renew_claim({LeaderID, LastHeartbeat}, TS) ->
    RenewedClaim = {LeaderID, TS},
    IsRenewable = (LastHeartbeat > TS - ?LEADER_TTL),
    IsRenewable andalso RenewedClaim.

-spec disown_leadership(id(), leader_claim(_)) ->
    ok | emqx_ds:error(_).
disown_leadership(ID, LeaderClaim) ->
    try_delete_leader(ID, LeaderClaim).

-spec leader_id(leader_claim(ID)) ->
    ID.
leader_id({LeaderID, _}) ->
    LeaderID.

-spec alive_until(leader_claim(_)) ->
    emqx_message:timestamp().
alive_until({_LeaderID, LastHeartbeatTS}) ->
    LastHeartbeatTS + ?LEADER_TTL.

-spec heartbeat_interval(leader_claim(_)) ->
    _Milliseconds :: pos_integer().
heartbeat_interval(_) ->
    ?LEADER_HEARTBEAT_INTERVAL.

try_replace_leader(ID, LeaderClaim, ExistingClaim) ->
    Batch = #dsbatch{
        preconditions = [mk_leader_precondition(ID, ExistingClaim)],
        operations = [encode_leader_claim(ID, LeaderClaim)]
    },
    case emqx_ds:store_batch(?DS_DB, Batch, #{sync => true}) of
        ok ->
            ok;
        {error, unrecoverable, {precondition_failed, Mismatch}} ->
            {exists, decode_leader_msg(Mismatch)};
        Error ->
            Error
    end.

try_delete_leader(ID, LeaderClaim) ->
    {_Cond, Matcher} = mk_leader_precondition(ID, LeaderClaim),
    emqx_ds:store_batch(?DS_DB, #dsbatch{operations = [{delete, Matcher}]}, #{sync => false}).

mk_leader_precondition(ID, undefined) ->
    {unless_exists, #message_matcher{
        from = ID,
        topic = mk_leader_topic(ID),
        timestamp = 0,
        payload = '_'
    }};
mk_leader_precondition(ID, {Leader, HeartbeatTS}) ->
    {if_exists, #message_matcher{
        from = ID,
        topic = mk_leader_topic(ID),
        timestamp = 0,
        payload = encode_leader(Leader),
        headers = #{?LEADER_HEADER_HEARTBEAT => HeartbeatTS}
    }}.

encode_leader_claim(ID, {Leader, HeartbeatTS}) ->
    #message{
        id = <<>>,
        qos = 0,
        from = ID,
        topic = mk_leader_topic(ID),
        timestamp = 0,
        payload = encode_leader(Leader),
        headers = #{?LEADER_HEADER_HEARTBEAT => HeartbeatTS}
    }.

decode_leader_msg(#message{from = _ID, payload = Payload, headers = Headers}) ->
    Leader = decode_leader(Payload),
    Heartbeat = maps:get(?LEADER_HEADER_HEARTBEAT, Headers, 0),
    {Leader, Heartbeat}.

encode_leader(Leader) ->
    %% NOTE: Lists are compact but easy to extend later.
    term_to_binary([Leader]).

decode_leader(Payload) ->
    [Leader | _Extra] = binary_to_term(Payload),
    Leader.

mk_leader_topic(ID) ->
    emqx_topic:join([?LEADER_TOPIC_PREFIX, ID]).

%%

-type space_name() :: stream.
-type var_name() :: properties | rank_progress.
-type space_key() :: nonempty_improper_list(space_name(), _Key).

%% NOTE
%% Instances of `emqx_ds:stream()` type are persisted in durable storage.
%% Given that streams are opaque and identity of a stream is stream itself (i.e.
%% if S1 =:= S2 then both are the same stream), it's critical to keep the "shape"
%% of the term intact between releases. Otherwise, if it changes then we will
%% need an additional API to deal with that (e.g. `emqx_ds:term_to_stream/2`).
%% Instances of `emqx_ds:iterator()` are also persisted in durable storage,
%% but those already has similar requirement because in some backends they travel
%% in RPCs between different nodes of potentially different releases.
-type t() :: #{
    %% General.
    id := id(),
    %% Spaces and variables: most up-to-date in-memory state.
    properties := properties(),
    stream := #{emqx_ds:stream() => stream_state()},
    rank_progress => _RankProgress,
    %% Internal _sequence numbers_ that tracks every change.
    seqnum := integer(),
    %% Mapping between complex keys and seqnums.
    seqmap := #{space_key() => _SeqNum :: integer()},
    %% Last committed sequence number.
    committed := integer(),
    %% Stage: uncommitted changes.
    stage := #{space_key() | var_name() => _Value}
}.

-type properties() :: #{
    %% TODO: Efficient encoding.
    group => emqx_types:group(),
    topic => emqx_types:topic(),
    start_time => emqx_message:timestamp(),
    created_at => emqx_message:timestamp()
}.

-type stream_state() :: #{
    progress => emqx_persistent_session_ds_shared_subs:progress(),
    rank => emqx_ds:stream_rank()
}.

-spec init(id()) -> t().
init(ID) ->
    %% NOTE: Empty store is impicitly dirty because rootset needs to be persisted.
    mk_store(ID).

-spec open(id()) -> {ok, t()} | false | emqx_ds:error(_).
open(ID) ->
    case open_rootset(ID) of
        Rootset = #{} ->
            slurp_store(Rootset, mk_store(ID));
        Otherwise ->
            Otherwise
    end.

-spec exists(id()) -> boolean().
exists(ID) ->
    open_rootset(ID) =/= false.

mk_store(ID) ->
    #{
        id => ID,
        stream => #{},
        properties => #{},
        seqnum => 0,
        seqmap => #{},
        committed => 0,
        stage => #{}
    }.

open_rootset(ID) ->
    ReadRootset = mk_read_root_batch(ID),
    case emqx_ds:store_batch(?DS_DB, ReadRootset, #{sync => true}) of
        ok ->
            false;
        {error, unrecoverable, {precondition_failed, RootMessage}} ->
            open_root_message(RootMessage)
    end.

slurp_store(Rootset, Acc) ->
    slurp_store(Rootset, #{}, ?STORE_SLURP_RETRIES, ?STORE_SLURP_RETRY_TIMEOUT, Acc).

slurp_store(Rootset, StreamIts0, Retries, RetryTimeout, Acc = #{id := ID}) ->
    TopicFilter = mk_store_wildcard(ID),
    StreamIts1 = ds_refresh_streams(TopicFilter, _StartTime = 0, StreamIts0),
    {StreamIts, Store} = ds_streams_fold(
        fun(Message, StoreAcc) ->
            lists:foldl(fun slurp_record/2, StoreAcc, open_message(Message))
        end,
        Acc,
        StreamIts1
    ),
    case map_get(seqnum, Store) of
        %% NOTE
        %% Comparison is non-strict. Seqnum going ahead of what is in the rootset is
        %% concerning, because this suggests there were concurrent writes that slipped
        %% past the leadership claim guards, yet we can still make progress.
        SeqNum when SeqNum >= map_get(seqnum, Rootset) ->
            {ok, reset_dirty(maps:merge(Store, Rootset))};
        _Mismatch when Retries > 0 ->
            ok = timer:sleep(RetryTimeout),
            slurp_store(Rootset, StreamIts, Retries - 1, RetryTimeout, Store);
        _Mismatch ->
            {error, unrecoverable, {leader_store_inconsistent, Store, Rootset}}
    end.

slurp_record({_ID, Record, ChangeSeqNum}, Store = #{seqnum := SeqNum}) ->
    open_record(Record, Store#{seqnum := max(ChangeSeqNum, SeqNum)}).

-spec get(space_name(), _ID, t()) -> _Value.
get(SpaceName, ID, Store) ->
    Space = maps:get(SpaceName, Store),
    maps:get(ID, Space).

-spec get(space_name(), _ID, Default, t()) -> _Value | Default.
get(SpaceName, ID, Default, Store) ->
    Space = maps:get(SpaceName, Store),
    maps:get(ID, Space, Default).

-spec fold(space_name(), fun((_ID, _Value, Acc) -> Acc), Acc, t()) -> Acc.
fold(SpaceName, Fun, Acc, Store) ->
    Space = maps:get(SpaceName, Store),
    maps:fold(Fun, Acc, Space).

-spec size(space_name(), t()) -> non_neg_integer().
size(SpaceName, Store) ->
    map_size(maps:get(SpaceName, Store)).

-spec put(space_name(), _ID, _Value, t()) -> t().
put(SpaceName, ID, Value, Store0 = #{stage := Stage, seqnum := SeqNum0, seqmap := SeqMap}) ->
    Space0 = maps:get(SpaceName, Store0),
    Space1 = maps:put(ID, Value, Space0),
    SeqNum = SeqNum0 + 1,
    SK = ?STORE_SK(SpaceName, ID),
    Store = Store0#{
        SpaceName := Space1,
        seqnum := SeqNum,
        stage := Stage#{SK => ?STORE_STAGE_ENTRY(SeqNum, Value)}
    },
    case map_size(Space1) of
        S when S > map_size(Space0) ->
            Store#{seqmap := maps:put(SK, SeqNum, SeqMap)};
        _ ->
            Store
    end.

get_seqnum(?STORE_SK(_SpaceName, _) = SK, SeqMap) ->
    maps:get(SK, SeqMap);
get_seqnum(_VarName, _SeqMap) ->
    0.

-spec get(var_name(), t()) -> _Value.
get(VarName, Store) ->
    maps:get(VarName, Store).

-spec set(var_name(), _Value, t()) -> t().
set(VarName, Value, Store = #{stage := Stage, seqnum := SeqNum0}) ->
    SeqNum = SeqNum0 + 1,
    Store#{
        VarName => Value,
        seqnum := SeqNum,
        stage := Stage#{VarName => ?STORE_STAGE_ENTRY(SeqNum, Value)}
    }.

-spec delete(space_name(), _ID, t()) -> t().
delete(SpaceName, ID, Store = #{stage := Stage, seqmap := SeqMap}) ->
    Space0 = maps:get(SpaceName, Store),
    Space1 = maps:remove(ID, Space0),
    case map_size(Space1) of
        S when S < map_size(Space0) ->
            %% NOTE
            %% We do not bump seqnum on deletions because tracking them does
            %% not make a lot of sense, assuming batches are atomic.
            SK = ?STORE_SK(SpaceName, ID),
            Store#{
                SpaceName := Space1,
                stage := Stage#{SK => ?STORE_TOMBSTONE},
                seqmap := maps:remove(SK, SeqMap)
            };
        _ ->
            Store
    end.

-spec id(t()) -> id().
id(#{id := ID}) ->
    ID.

-spec dirty(t()) -> boolean().
dirty(#{stage := Stage}) ->
    map_size(Stage) > 0.

-spec create(t()) -> {ok, t()} | exists | emqx_ds:error(_).
create(Store) ->
    Batch = mk_store_create_batch(Store),
    case emqx_ds:store_batch(?DS_DB, Batch, #{sync => true}) of
        ok ->
            {ok, reset_dirty(Store)};
        {error, unrecoverable, {precondition_failed, _Mismatch}} ->
            exists;
        Error ->
            Error
    end.

-spec destroy(t()) -> ok | conflict | emqx_ds:error(_).
destroy(Store) ->
    Batch = mk_store_delete_batch(Store),
    case emqx_ds:store_batch(?DS_DB, Batch, #{sync => true}) of
        ok ->
            ok;
        {error, unrecoverable, {precondition_failed, not_found}} ->
            %% Probably was deleted concurrently.
            ok;
        {error, unrecoverable, {precondition_failed, #message{}}} ->
            %% Probably was updated concurrently.
            conflict;
        Error ->
            Error
    end.

%% @doc Commit staged changes to the storage.
%% Does nothing if there are no staged changes.
-spec commit_dirty(leader_claim(_), t()) ->
    {ok, t()} | destroyed | emqx_ds:error(_).
commit_dirty(LeaderClaim, Store = #{stage := Stage}) when map_size(Stage) > 0 ->
    Batch = mk_store_leader_batch(Store, LeaderClaim),
    case emqx_ds:store_batch(?DS_DB, Batch, #{sync => true}) of
        ok ->
            {ok, reset_dirty(Store)};
        {error, unrecoverable, {precondition_failed, Mismatch}} ->
            map_commit_precondition_failure(Mismatch);
        Error ->
            Error
    end;
commit_dirty(_LeaderClaim, Store) ->
    Store.

%% @doc Commit staged changes and renew leadership at the same time.
%% Goes to the storage even if there are no staged changes.
-spec commit_renew(leader_claim(ID), emqx_message:timestamp(), t()) ->
    {ok, leader_claim(ID), t()} | destroyed | emqx_ds:error(_).
commit_renew(LeaderClaim, TS, Store) ->
    case renew_claim(LeaderClaim, TS) of
        RenewedClaim when RenewedClaim =/= false ->
            Batch = mk_store_leader_batch(Store, LeaderClaim, RenewedClaim),
            case emqx_ds:store_batch(?DS_DB, Batch, #{sync => true}) of
                ok ->
                    {ok, RenewedClaim, reset_dirty(Store)};
                {error, unrecoverable, {precondition_failed, Mismatch}} ->
                    map_commit_precondition_failure(Mismatch);
                Error ->
                    Error
            end;
        false ->
            {error, unrecoverable, leader_claim_outdated}
    end.

map_commit_precondition_failure(not_found) ->
    %% Assuming store was destroyed concurrently.
    destroyed;
map_commit_precondition_failure(Msg = #message{topic = Topic}) ->
    case emqx_topic:tokens(Topic) of
        [?LEADER_TOPIC_PREFIX | _] ->
            {error, unrecoverable, {leadership_lost, decode_leader_msg(Msg)}};
        [?STORE_TOPIC_PREFIX | _] ->
            Rootset = open_root_message(Msg),
            {error, unrecoverable, {concurrent_update, Rootset}}
    end.

reset_dirty(Stage = #{seqnum := SeqNum}) ->
    Stage#{stage := #{}, committed := SeqNum}.

mk_store_leader_batch(Store = #{id := ID}, LeaderClaim) ->
    RootPrecondition = {if_exists, mk_store_root_matcher(Store)},
    LeaderPrecondition = mk_leader_precondition(ID, LeaderClaim),
    #dsbatch{
        preconditions = [LeaderPrecondition, RootPrecondition],
        operations = mk_store_operations(Store)
    }.

mk_store_leader_batch(Store = #{id := ID}, ExistingClaim, RenewedClaim) ->
    Batch = #dsbatch{operations = Operations} = mk_store_leader_batch(Store, ExistingClaim),
    Batch#dsbatch{
        operations = [encode_leader_claim(ID, RenewedClaim) | Operations]
    }.

mk_store_create_batch(Store = #{id := ID}) ->
    #dsbatch{
        preconditions = [mk_nonexist_precondition(ID)],
        operations = mk_store_operations(Store)
    }.

mk_nonexist_precondition(ID) ->
    {unless_exists, #message_matcher{
        from = ID,
        topic = mk_store_root_topic(ID),
        timestamp = 0,
        payload = '_'
    }}.

mk_store_delete_batch(Store) ->
    RootMatcher = mk_store_root_matcher(Store),
    #dsbatch{
        preconditions = [{if_exists, RootMatcher}],
        operations = [{delete, RootMatcher} | mk_store_deletes(Store)]
    }.

mk_store_deletes(Store = #{id := ID, seqmap := SeqMap}) ->
    Acc0 = [],
    Acc1 = fold(
        stream,
        fun(Stream, _, Acc) ->
            [mk_store_operation(ID, ?STORE_SK(stream, Stream), ?STORE_TOMBSTONE, SeqMap) | Acc]
        end,
        Acc0,
        Store
    ),
    Acc2 = [mk_store_operation(ID, properties, ?STORE_TOMBSTONE, SeqMap) | Acc1],
    Acc3 = [mk_store_operation(ID, rank_progress, ?STORE_TOMBSTONE, SeqMap) | Acc2],
    Acc3.

mk_store_operations(Store = #{id := ID, stage := Stage, seqmap := SeqMap}) ->
    %% NOTE: Always persist rootset.
    RootOperation = mk_store_root(Store),
    maps:fold(
        fun(SK, Value, Acc) ->
            [mk_store_operation(ID, SK, Value, SeqMap) | Acc]
        end,
        [RootOperation],
        Stage
    ).

mk_store_root(#{id := ID, seqnum := SeqNum}) ->
    Payload = #{seqnum => SeqNum},
    #message{
        id = <<>>,
        qos = 0,
        from = ID,
        topic = mk_store_root_topic(ID),
        payload = term_to_binary(Payload),
        timestamp = 0
    }.

mk_store_root_matcher(#{id := ID, committed := Committed}) ->
    Payload = #{seqnum => Committed},
    #message_matcher{
        from = ID,
        topic = mk_store_root_topic(ID),
        payload = term_to_binary(Payload),
        timestamp = 0
    }.

mk_read_root_batch(ID) ->
    %% NOTE
    %% Construct batch that essentially does nothing but reads rootset in a consistent
    %% manner.
    Matcher = #message_matcher{
        from = ID,
        topic = mk_store_root_topic(ID),
        payload = '_',
        timestamp = 0
    },
    #dsbatch{
        preconditions = [{unless_exists, Matcher}],
        operations = [{delete, Matcher#message_matcher{payload = <<>>}}]
    }.

mk_store_operation(ID, SK, ?STORE_TOMBSTONE, SeqMap) ->
    {delete, #message_matcher{
        from = ID,
        topic = mk_store_topic(ID, SK, SeqMap),
        payload = '_',
        timestamp = get_seqnum(SK, SeqMap)
    }};
mk_store_operation(ID, SK, ?STORE_STAGE_ENTRY(ChangeSeqNum, Value), SeqMap) ->
    %% NOTE
    %% Using `SeqNum` as timestamp to further disambiguate one record (message) from
    %% another in the DS DB keyspace. As an example, Skipstream-LTS storage layout
    %% _requires_ messages in the same stream to have unique timestamps.
    %% TODO
    %% Do we need to have wall-clock timestamp here?
    Payload = mk_store_payload(SK, Value),
    #message{
        id = <<>>,
        qos = 0,
        from = ID,
        topic = mk_store_topic(ID, SK, SeqMap),
        payload = term_to_binary(Payload),
        timestamp = get_seqnum(SK, SeqMap),
        %% NOTE: Preserving the seqnum when this change has happened.
        headers = #{?STORE_HEADER_CHANGESEQNUM => ChangeSeqNum}
    }.

open_root_message(#message{payload = Payload, timestamp = 0}) ->
    #{} = binary_to_term(Payload).

open_message(
    Msg = #message{topic = Topic, payload = Payload, timestamp = SeqNum, headers = Headers}
) ->
    try
        case emqx_topic:tokens(Topic) of
            [_Prefix, SpaceID, SpaceTok, _SeqTok] ->
                SpaceName = token_to_space(SpaceTok),
                ?STORE_PAYLOAD(ID, Value) = binary_to_term(Payload),
                %% TODO: Records.
                Record = {SpaceName, ID, Value, SeqNum};
            [_Prefix, SpaceID, VarTok] ->
                VarName = token_to_varname(VarTok),
                Value = binary_to_term(Payload),
                Record = {VarName, Value}
        end,
        ChangeSeqNum = maps:get(?STORE_HEADER_CHANGESEQNUM, Headers),
        [{SpaceID, Record, ChangeSeqNum}]
    catch
        error:_ ->
            ?tp(warning, "dssub_leader_store_unrecognized_message", #{message => Msg}),
            []
    end.

open_record({SpaceName, ID, Value, SeqNum}, Store = #{seqmap := SeqMap}) ->
    Space0 = maps:get(SpaceName, Store),
    Space1 = maps:put(ID, Value, Space0),
    SK = ?STORE_SK(SpaceName, ID),
    Store#{
        SpaceName := Space1,
        seqmap := SeqMap#{SK => SeqNum}
    };
open_record({VarName, Value}, Store) ->
    Store#{VarName => Value}.

mk_store_payload(?STORE_SK(_SpaceName, ID), Value) ->
    ?STORE_PAYLOAD(ID, Value);
mk_store_payload(_VarName, Value) ->
    Value.

%%

-record(select, {tf, start, it, streams}).

-type select() :: #select{}.

-spec select(var_name()) -> select().
select(VarName) ->
    select_jump(select_new(VarName)).

-spec select(var_name(), _Cursor) -> select().
select(VarName, Cursor) ->
    select_restore(Cursor, select_new(VarName)).

select_new(VarName) ->
    TopicFilter = mk_store_varname_wildcard(VarName),
    StartTime = 0,
    RankedStreams = emqx_ds:get_streams(?DS_DB, TopicFilter, StartTime),
    Streams = [S || {_Rank, S} <- lists:sort(RankedStreams)],
    #select{tf = TopicFilter, start = StartTime, streams = Streams}.

select_jump(It = #select{tf = TopicFilter, start = StartTime, streams = [Stream | Rest]}) ->
    DSIt = ds_make_iterator(Stream, TopicFilter, StartTime),
    It#select{it = DSIt, streams = Rest};
select_jump(It = #select{streams = []}) ->
    It#select{it = undefined}.

-spec select_next(select(), _N) -> {[{id(), _Var}], select() | end_of_iterator}.
select_next(ItSelect, N) ->
    select_fold(
        ItSelect,
        N,
        fun(Message, Acc) ->
            [{ID, Var} || {ID, {_VarName, Var}, _} <- open_message(Message)] ++ Acc
        end,
        []
    ).

select_fold(#select{it = undefined}, _, _Fun, Acc) ->
    {Acc, end_of_iterator};
select_fold(It = #select{it = DSIt0}, N, Fun, Acc0) ->
    case emqx_ds:next(?DS_DB, DSIt0, N) of
        {ok, DSIt, Messages} ->
            Acc = lists:foldl(fun({_Key, Msg}, Acc) -> Fun(Msg, Acc) end, Acc0, Messages),
            case length(Messages) of
                N ->
                    {Acc, It#select{it = DSIt}};
                NLess when NLess < N ->
                    select_fold(select_jump(It), N - NLess, Fun, Acc)
            end;
        {ok, end_of_stream} ->
            select_fold(select_jump(It), N, Fun, Acc0)
    end.

-spec select_preserve(select()) -> _Cursor.
select_preserve(#select{it = It, streams = Streams}) ->
    case Streams of
        [StreamNext | _Rest] ->
            %% Preserve only the subsequent stream.
            [It | StreamNext];
        [] ->
            %% Iterating over last stream, preserve only iterator.
            [It]
    end.

select_restore([It], Select) ->
    Select#select{it = It, streams = []};
select_restore([It | StreamNext], Select = #select{streams = Streams}) ->
    StreamsRest = lists:dropwhile(fun(S) -> S =/= StreamNext end, Streams),
    Select#select{it = It, streams = StreamsRest}.

mk_store_root_topic(ID) ->
    emqx_topic:join([?STORE_TOPIC_PREFIX, ID]).

mk_store_topic(ID, ?STORE_SK(SpaceName, _) = SK, SeqMap) ->
    SeqNum = get_seqnum(SK, SeqMap),
    SeqTok = integer_to_binary(SeqNum),
    emqx_topic:join([?STORE_TOPIC_PREFIX, ID, space_to_token(SpaceName), SeqTok]);
mk_store_topic(ID, VarName, _SeqMap) ->
    emqx_topic:join([?STORE_TOPIC_PREFIX, ID, varname_to_token(VarName)]).

mk_store_wildcard(ID) ->
    [?STORE_TOPIC_PREFIX, ID, '+', '#'].

mk_store_varname_wildcard(VarName) ->
    [?STORE_TOPIC_PREFIX, '+', varname_to_token(VarName)].

ds_refresh_streams(TopicFilter, StartTime, StreamIts) ->
    Streams = emqx_ds:get_streams(?DS_DB, TopicFilter, StartTime),
    lists:foldl(
        fun({_Rank, Stream}, Acc) ->
            case StreamIts of
                #{Stream := _It} ->
                    Acc;
                #{} ->
                    Acc#{Stream => ds_make_iterator(Stream, TopicFilter, StartTime)}
            end
        end,
        StreamIts,
        Streams
    ).

ds_make_iterator(Stream, TopicFilter, StartTime) ->
    %% TODO: Gracefully handle `emqx_ds:error(_)`?
    {ok, It} = emqx_ds:make_iterator(?DS_DB, Stream, TopicFilter, StartTime),
    It.

ds_streams_fold(Fun, AccIn, StreamItsIn) ->
    maps:fold(
        fun(Stream, It0, {StreamIts, Acc0}) ->
            {It, Acc} = ds_stream_fold(Fun, Acc0, It0),
            {StreamIts#{Stream := It}, Acc}
        end,
        {StreamItsIn, AccIn},
        StreamItsIn
    ).

ds_stream_fold(_Fun, Acc0, end_of_stream) ->
    %% NOTE: Assuming atom `end_of_stream` is not a valid `emqx_ds:iterator()`.
    {end_of_stream, Acc0};
ds_stream_fold(Fun, Acc0, It0) ->
    %% TODO: Gracefully handle `emqx_ds:error(_)`?
    case emqx_ds:next(?DS_DB, It0, ?STORE_BATCH_SIZE) of
        {ok, It, Messages = [_ | _]} ->
            Acc1 = lists:foldl(fun({_Key, Msg}, Acc) -> Fun(Msg, Acc) end, Acc0, Messages),
            ds_stream_fold(Fun, Acc1, It);
        {ok, It, []} ->
            {It, Acc0};
        {ok, end_of_stream} ->
            {end_of_stream, Acc0}
    end.

%%

space_to_token(stream) -> <<"s">>.

token_to_space(<<"s">>) -> stream.

varname_to_token(rank_progress) -> <<"rankp">>;
varname_to_token(properties) -> <<"props">>.

token_to_varname(<<"rankp">>) -> rank_progress;
token_to_varname(<<"props">>) -> properties.
