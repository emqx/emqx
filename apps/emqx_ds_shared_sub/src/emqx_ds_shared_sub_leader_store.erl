%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_shared_sub_leader_store).

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
    %% TODO
    %% destroy/1,
    %% Managing records
    get/3,
    get/4,
    fold/4,
    size/2,
    put/4,
    get/2,
    set/3,
    delete/3,
    dirty/1,
    commit_dirty/2,
    commit_renew/3
]).

-export_type([
    t/0,
    leader_claim/1
]).

-type group() :: binary().
-type leader_claim(ID) :: {ID, _Heartbeat :: emqx_message:timestamp()}.

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

-define(STORE_IS_ROOTSET(VAR), (VAR == seqnum)).

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
        force_monotonic_timestamps => false
    },
    case Backend of
        B when B == builtin_raft; B == builtin_local ->
            tune_db_storage_layout(Config);
        _ ->
            Config
    end.

tune_db_storage_layout(Config = #{storage := {Layout, Opts0}}) when
    Layout == emqx_ds_storage_skipstream_lts;
    Layout == emqx_ds_storage_bitfield_lts
->
    Opts = Opts0#{
        %% Since these layouts impose somewhat strict requirements on message
        %% timestamp uniqueness, we need to additionally ensure that LTS always
        %% keeps different groups under separate indices.
        lts_threshold_spec => {simple, {inf, inf, inf, 0}}
    },
    Config#{storage := {Layout, Opts}};
tune_db_storage_layout(Config = #{storage := _}) ->
    Config.

%%

-spec claim_leadership(group(), ID, emqx_message:timestamp()) ->
    {ok | exists, leader_claim(ID)} | emqx_ds:error(_).
claim_leadership(Group, LeaderID, TS) ->
    LeaderClaim = {LeaderID, TS},
    case try_replace_leader(Group, LeaderClaim, undefined) of
        ok ->
            {ok, LeaderClaim};
        {exists, ExistingClaim = {_, LastHeartbeat}} when LastHeartbeat > TS - ?LEADER_TTL ->
            {exists, ExistingClaim};
        {exists, ExistingClaim = {_LeaderDead, _}} ->
            case try_replace_leader(Group, LeaderClaim, ExistingClaim) of
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

-spec renew_leadership(group(), leader_claim(ID), emqx_message:timestamp()) ->
    {ok | exists, leader_claim(ID)} | emqx_ds:error(_).
renew_leadership(Group, LeaderClaim, TS) ->
    RenewedClaim = renew_claim(LeaderClaim, TS),
    case RenewedClaim =/= false andalso try_replace_leader(Group, RenewedClaim, LeaderClaim) of
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

-spec disown_leadership(group(), leader_claim(_ID)) ->
    ok | emqx_ds:error(_).
disown_leadership(Group, LeaderClaim) ->
    try_delete_leader(Group, LeaderClaim).

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
try_replace_leader(Group, LeaderClaim, ExistingClaim) ->
    Batch = #dsbatch{
        preconditions = [mk_precondition(Group, ExistingClaim)],
        operations = [encode_leader_claim(Group, LeaderClaim)]
    },
    case emqx_ds:store_batch(?DS_DB, Batch, #{sync => true}) of
        ok ->
            ok;
        {error, unrecoverable, {precondition_failed, Mismatch}} ->
            {exists, decode_leader_msg(Mismatch)};
        Error ->
            Error
    end.

try_delete_leader(Group, LeaderClaim) ->
    {_Cond, Matcher} = mk_precondition(Group, LeaderClaim),
    emqx_ds:store_batch(?DS_DB, #dsbatch{operations = [{delete, Matcher}]}, #{sync => false}).

mk_precondition(Group, undefined) ->
    {unless_exists, #message_matcher{
        from = Group,
        topic = mk_leader_topic(Group),
        timestamp = 0,
        payload = '_'
    }};
mk_precondition(Group, {Leader, HeartbeatTS}) ->
    {if_exists, #message_matcher{
        from = Group,
        topic = mk_leader_topic(Group),
        timestamp = 0,
        payload = encode_leader(Leader),
        headers = #{?LEADER_HEADER_HEARTBEAT => HeartbeatTS}
    }}.

encode_leader_claim(Group, {Leader, HeartbeatTS}) ->
    #message{
        id = <<>>,
        qos = 0,
        from = Group,
        topic = mk_leader_topic(Group),
        timestamp = 0,
        payload = encode_leader(Leader),
        headers = #{?LEADER_HEADER_HEARTBEAT => HeartbeatTS}
    }.

decode_leader_msg(#message{from = _Group, payload = Payload, headers = Headers}) ->
    Leader = decode_leader(Payload),
    Heartbeat = maps:get(?LEADER_HEADER_HEARTBEAT, Headers, 0),
    {Leader, Heartbeat}.

encode_leader(Leader) ->
    %% NOTE: Lists are compact but easy to extend later.
    term_to_binary([Leader]).

decode_leader(Payload) ->
    [Leader | _Extra] = binary_to_term(Payload),
    Leader.

mk_leader_topic(GroupName) ->
    emqx_topic:join([?LEADER_TOPIC_PREFIX, GroupName]).

%%

-type space_name() :: stream.
-type var_name() :: start_time | rank_progress.
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
    group := group(),
    %% Spaces and variables: most up-to-date in-memory state.
    stream := #{emqx_ds:stream() => stream_state()},
    start_time => _SubsriptionStartTime :: emqx_message:timestamp(),
    rank_progress => _RankProgress,
    %% Internal _sequence number_ that tracks every change.
    seqnum := integer(),
    %% Mapping between complex keys and seqnums.
    seqmap := #{space_key() => _SeqNum :: integer()},
    %% Stage: uncommitted changes.
    stage := #{space_key() | var_name() => _Value},
    dirty => true
}.

-type stream_state() :: #{
    progress => emqx_persistent_session_ds_shared_subs:progress(),
    rank => emqx_ds:stream_rank()
}.

-spec init(group()) -> t().
init(Group) ->
    %% NOTE: Empty store is dirty because rootset needs to be persisted.
    mark_dirty(mk_store(Group)).

-spec open(group()) -> t() | false.
open(Group) ->
    open_store(mk_store(Group)).

mk_store(Group) ->
    #{
        group => Group,
        stream => #{},
        seqnum => 0,
        seqmap => #{},
        stage => #{}
    }.

open_store(Store = #{group := Group}) ->
    ReadRootset = mk_read_root_batch(Group),
    case emqx_ds:store_batch(?DS_DB, ReadRootset, #{sync => true}) of
        ok ->
            false;
        {error, unrecoverable, {precondition_failed, RootMessage}} ->
            Rootset = open_root_message(RootMessage),
            slurp_store(Rootset, Store)
    end.

slurp_store(Rootset, Acc) ->
    slurp_store(Rootset, #{}, ?STORE_SLURP_RETRIES, ?STORE_SLURP_RETRY_TIMEOUT, Acc).

slurp_store(Rootset, StreamIts0, Retries, RetryTimeout, Acc = #{group := Group}) ->
    TopicFilter = mk_store_wildcard(Group),
    StreamIts1 = ds_refresh_streams(TopicFilter, _StartTime = 0, StreamIts0),
    {StreamIts, Store} = ds_streams_fold(
        fun(Message, StoreAcc) -> open_message(Message, StoreAcc) end,
        Acc,
        StreamIts1
    ),
    case map_get(seqnum, Store) of
        SeqNum when SeqNum >= map_get(seqnum, Rootset) ->
            maps:merge(Store, Rootset);
        _Mismatch when Retries > 0 ->
            ok = timer:sleep(RetryTimeout),
            slurp_store(Rootset, StreamIts, Retries - 1, RetryTimeout, Store);
        _Mismatch ->
            {error, unrecoverable, {leader_store_inconsistent, Store, Rootset}}
    end.

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

mark_dirty(Store) ->
    Store#{dirty => true}.

mark_clean(Store) ->
    maps:remove(dirty, Store).

-spec dirty(t()) -> boolean().
dirty(#{dirty := Dirty}) ->
    Dirty;
dirty(#{stage := Stage}) ->
    map_size(Stage) > 0.

%% @doc Commit staged changes to the storage.
%% Does nothing if there are no staged changes.
-spec commit_dirty(leader_claim(_), t()) ->
    {ok, t()} | emqx_ds:error(_).
commit_dirty(LeaderClaim, Store = #{dirty := true}) ->
    commit(LeaderClaim, Store);
commit_dirty(LeaderClaim, Store = #{stage := Stage}) when map_size(Stage) > 0 ->
    commit(LeaderClaim, Store).

commit(LeaderClaim, Store = #{group := Group}) ->
    Operations = mk_store_operations(Store),
    Batch = mk_store_batch(Group, LeaderClaim, Operations),
    case emqx_ds:store_batch(?DS_DB, Batch, #{sync => true}) of
        ok ->
            {ok, mark_clean(Store#{stage := #{}})};
        {error, unrecoverable, {precondition_failed, Mismatch}} ->
            {error, unrecoverable, {leadership_lost, decode_leader_msg(Mismatch)}};
        Error ->
            Error
    end.

%% @doc Commit staged changes and renew leadership at the same time.
%% Goes to the storage even if there are no staged changes.
-spec commit_renew(leader_claim(ID), emqx_message:timestamp(), t()) ->
    {ok, leader_claim(ID), t()} | emqx_ds:error(_).
commit_renew(LeaderClaim, TS, Store = #{group := Group}) ->
    case renew_claim(LeaderClaim, TS) of
        RenewedClaim when RenewedClaim =/= false ->
            Operations = mk_store_operations(Store),
            Batch = mk_store_batch(Group, LeaderClaim, RenewedClaim, Operations),
            case emqx_ds:store_batch(?DS_DB, Batch, #{sync => true}) of
                ok ->
                    {ok, RenewedClaim, mark_clean(Store#{stage := #{}})};
                {error, unrecoverable, {precondition_failed, Mismatch}} ->
                    {error, unrecoverable, {leadership_lost, decode_leader_msg(Mismatch)}};
                Error ->
                    Error
            end;
        false ->
            {error, unrecoverable, leader_claim_outdated}
    end.

mk_store_batch(Group, LeaderClaim, Operations) ->
    #dsbatch{
        preconditions = [mk_precondition(Group, LeaderClaim)],
        operations = Operations
    }.

mk_store_batch(Group, ExistingClaim, RenewedClaim, Operations) ->
    #dsbatch{
        preconditions = [mk_precondition(Group, ExistingClaim)],
        operations = [encode_leader_claim(Group, RenewedClaim) | Operations]
    }.

mk_store_operations(Store = #{group := Group, stage := Stage, seqmap := SeqMap}) ->
    %% NOTE: Always persist rootset.
    RootOperation = mk_store_root(Store),
    maps:fold(
        fun(SK, Value, Acc) ->
            [mk_store_operation(Group, SK, Value, SeqMap) | Acc]
        end,
        [RootOperation],
        Stage
    ).

mk_store_root(Store = #{group := Group}) ->
    Payload = maps:filter(fun(V, _) -> ?STORE_IS_ROOTSET(V) end, Store),
    #message{
        id = <<>>,
        qos = 0,
        from = Group,
        topic = mk_store_root_topic(Group),
        payload = term_to_binary(Payload),
        timestamp = 0
    }.

mk_store_operation(Group, SK, ?STORE_TOMBSTONE, SeqMap) ->
    {delete, #message_matcher{
        from = Group,
        topic = mk_store_topic(Group, SK, SeqMap),
        payload = '_',
        timestamp = get_seqnum(SK, SeqMap)
    }};
mk_store_operation(Group, SK, ?STORE_STAGE_ENTRY(ChangeSeqNum, Value), SeqMap) ->
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
        from = Group,
        topic = mk_store_topic(Group, SK, SeqMap),
        payload = term_to_binary(Payload),
        timestamp = get_seqnum(SK, SeqMap),
        %% NOTE: Preserving the seqnum when this change has happened.
        headers = #{?STORE_HEADER_CHANGESEQNUM => ChangeSeqNum}
    }.

open_root_message(#message{payload = Payload, timestamp = 0}) ->
    #{} = binary_to_term(Payload).

open_message(
    Msg = #message{topic = Topic, payload = Payload, timestamp = SeqNum, headers = Headers}, Store
) ->
    Entry =
        try
            ChangeSeqNum = maps:get(?STORE_HEADER_CHANGESEQNUM, Headers),
            case emqx_topic:tokens(Topic) of
                [_Prefix, _Group, SpaceTok, _SeqTok] ->
                    SpaceName = token_to_space(SpaceTok),
                    ?STORE_PAYLOAD(ID, Value) = binary_to_term(Payload),
                    %% TODO: Records.
                    Record = {SpaceName, ID, Value, SeqNum};
                [_Prefix, _Group, VarTok] ->
                    VarName = token_to_varname(VarTok),
                    Value = binary_to_term(Payload),
                    Record = {VarName, Value}
            end,
            {ChangeSeqNum, Record}
        catch
            error:_ ->
                ?tp(warning, "dssubs_leader_store_unrecognized_message", #{
                    group => maps:get(group, Store),
                    message => Msg
                }),
                unrecognized
        end,
    open_entry(Entry, Store).

open_entry({ChangeSeqNum, Record}, Store = #{seqnum := SeqNum}) ->
    open_record(Record, Store#{seqnum := max(ChangeSeqNum, SeqNum)}).

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

mk_store_root_topic(GroupName) ->
    emqx_topic:join([?STORE_TOPIC_PREFIX, GroupName]).

mk_store_topic(GroupName, ?STORE_SK(SpaceName, _) = SK, SeqMap) ->
    SeqNum = get_seqnum(SK, SeqMap),
    SeqTok = integer_to_binary(SeqNum),
    emqx_topic:join([?STORE_TOPIC_PREFIX, GroupName, space_to_token(SpaceName), SeqTok]);
mk_store_topic(GroupName, VarName, _SeqMap) ->
    emqx_topic:join([?STORE_TOPIC_PREFIX, GroupName, varname_to_token(VarName)]).

mk_store_wildcard(GroupName) ->
    [?STORE_TOPIC_PREFIX, GroupName, '+', '#'].

mk_read_root_batch(Group) ->
    %% NOTE
    %% Construct batch that essentially does nothing but reads rootset in a consistent
    %% manner.
    Matcher = #message_matcher{
        from = Group,
        topic = mk_store_root_topic(Group),
        payload = '_',
        timestamp = 0
    },
    #dsbatch{
        preconditions = [{unless_exists, Matcher}],
        operations = [{delete, Matcher#message_matcher{payload = <<>>}}]
    }.

ds_refresh_streams(TopicFilter, StartTime, StreamIts) ->
    Streams = emqx_ds:get_streams(?DS_DB, TopicFilter, StartTime),
    lists:foldl(
        fun({_Rank, Stream}, Acc) ->
            case StreamIts of
                #{Stream := _It} ->
                    Acc;
                #{} ->
                    %% TODO: Gracefully handle `emqx_ds:error(_)`?
                    {ok, It} = emqx_ds:make_iterator(?DS_DB, Stream, TopicFilter, StartTime),
                    Acc#{Stream => It}
            end
        end,
        StreamIts,
        Streams
    ).

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

space_to_token(stream) -> <<"s">>;
space_to_token(progress) -> <<"prog">>;
space_to_token(sequence) -> <<"seq">>.

token_to_space(<<"s">>) -> stream;
token_to_space(<<"prog">>) -> progress;
token_to_space(<<"seq">>) -> sequence.

varname_to_token(rank_progress) -> <<"rankp">>;
varname_to_token(start_time) -> <<"stime">>.

token_to_varname(<<"rankp">>) -> rank_progress;
token_to_varname(<<"stime">>) -> start_time.
