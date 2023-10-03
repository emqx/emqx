%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_storage_layer).

-behaviour(gen_server).

%% API:
-export([start_link/2]).
-export([create_generation/3]).

-export([get_streams/3]).
-export([message_store/3]).
-export([delete/4]).

-export([make_iterator/2, next/1, next/2]).

-export([
    preserve_iterator/2,
    restore_iterator/2,
    discard_iterator/2,
    ensure_iterator/3,
    discard_iterator_prefix/2,
    list_iterator_prefix/2,
    foldl_iterator_prefix/4
]).

%% behaviour callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([stream/0, cf_refs/0, gen_id/0, options/0, state/0, iterator/0]).
-export_type([db_options/0, db_write_options/0, db_read_options/0]).

-compile({inline, [meta_lookup/2]}).

-include_lib("emqx/include/emqx.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type stream() :: term(). %% Opaque term returned by the generation callback module

-type options() :: #{
    dir => file:filename()
}.

%% see rocksdb:db_options()
-type db_options() :: proplists:proplist().
%% see rocksdb:write_options()
-type db_write_options() :: proplists:proplist().
%% see rocksdb:read_options()
-type db_read_options() :: proplists:proplist().

-type cf_refs() :: [{string(), rocksdb:cf_handle()}].

%% Message storage generation
%% Keep in mind that instances of this type are persisted in long-term storage.
-type generation() :: #{
    %% Module that handles data for the generation
    module := module(),
    %% Module-specific data defined at generation creation time
    data := term(),
    %% When should this generation become active?
    %% This generation should only contain messages timestamped no earlier than that.
    %% The very first generation will have `since` equal 0.
    since := emqx_ds:time()
}.

-record(s, {
    shard :: emqx_ds:shard(),
    keyspace :: emqx_ds_conf:keyspace(),
    db :: rocksdb:db_handle(),
    cf_iterator :: rocksdb:cf_handle(),
    cf_generations :: cf_refs()
}).

-record(it, {
    shard :: emqx_ds:shard(),
    gen :: gen_id(),
    replay :: emqx_ds:replay(),
    module :: module(),
    data :: term()
}).

-type gen_id() :: 0..16#ffff.

-opaque state() :: #s{}.
-opaque iterator() :: #it{}.

%% Contents of the default column family:
%%
%% [{<<"genNN">>, #generation{}}, ...,
%%  {<<"current">>, GenID}]

-define(DEFAULT_CF, "default").
-define(DEFAULT_CF_OPTS, []).

-define(ITERATOR_CF, "$iterators").

%% TODO
%% 1. CuckooTable might be of use here / `OptimizeForPointLookup(...)`.
%% 2. Supposedly might be compressed _very_ effectively.
%% 3. `inplace_update_support`?
-define(ITERATOR_CF_OPTS, []).

-define(REF(ShardId), {via, gproc, {n, l, {?MODULE, ShardId}}}).

%%================================================================================
%% Callbacks
%%================================================================================

-callback create_new(rocksdb:db_handle(), gen_id(), _Options :: term()) ->
    {_Schema, cf_refs()}.

-callback open(
    emqx_ds:shard(),
    rocksdb:db_handle(),
    gen_id(),
    cf_refs(),
    _Schema
) ->
    _DB.

-callback store(
    _DB,
    _MessageID :: binary(),
    emqx_ds:time(),
    emqx_ds:topic(),
    _Payload :: binary()
) ->
    ok | {error, _}.

-callback delete(_DB, _MessageID :: binary(), emqx_ds:time(), emqx_ds:topic()) ->
    ok | {error, _}.

-callback get_streams(_DB, emqx_ds:topic_filter(), emqx_ds:time()) ->
    [_Stream].

-callback make_iterator(_DB, emqx_ds:replay()) ->
    {ok, _It} | {error, _}.

-callback restore_iterator(_DB, _Serialized :: binary()) -> {ok, _It} | {error, _}.

-callback preserve_iterator(_It) -> term().

-callback next(It) -> {value, binary(), It} | none | {error, closed}.

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link(emqx_ds:shard(), emqx_ds_storage_layer:options()) ->
    {ok, pid()}.
start_link(Shard, Options) ->
    gen_server:start_link(?REF(Shard), ?MODULE, {Shard, Options}, []).

-spec get_streams(emqx_ds:shard_id(), emqx_ds:topic_filter(), emqx_ds:time()) -> [_Stream].
get_streams(_ShardId, _TopicFilter, _StartTime) ->
    [].


-spec create_generation(
    emqx_ds:shard(), emqx_ds:time(), emqx_ds_conf:backend_config()
) ->
    {ok, gen_id()} | {error, nonmonotonic}.
create_generation(ShardId, Since, Config = {_Module, _Options}) ->
    gen_server:call(?REF(ShardId), {create_generation, Since, Config}).

-spec message_store(emqx_ds:shard(), [emqx_types:message()], emqx_ds:message_store_opts()) ->
                           {ok, _MessageId} | {error, _}.
message_store(Shard, Msgs, _Opts) ->
    {ok, lists:map(
           fun(Msg) ->
                   GUID = emqx_message:id(Msg),
                   Timestamp = Msg#message.timestamp,
                   {_GenId, #{module := Mod, data := ModState}} = meta_lookup_gen(Shard, Timestamp),
                   Topic = emqx_topic:words(emqx_message:topic(Msg)),
                   Payload = serialize(Msg),
                   Mod:store(ModState, GUID, Timestamp, Topic, Payload),
                   GUID
           end,
           Msgs)}.

-spec delete(emqx_ds:shard(), emqx_guid:guid(), emqx_ds:time(), emqx_ds:topic()) ->
    ok | {error, _}.
delete(Shard, GUID, Time, Topic) ->
    {_GenId, #{module := Mod, data := Data}} = meta_lookup_gen(Shard, Time),
    Mod:delete(Data, GUID, Time, Topic).

-spec make_iterator(emqx_ds:shard(), emqx_ds:replay()) ->
    {ok, iterator()} | {error, _TODO}.
make_iterator(Shard, Replay = {_, StartTime}) ->
    {GenId, Gen} = meta_lookup_gen(Shard, StartTime),
    open_iterator(Gen, #it{
        shard = Shard,
        gen = GenId,
        replay = Replay
    }).

-spec next(iterator()) -> {ok, iterator(), [binary()]} | end_of_stream.
next(It = #it{}) ->
    next(It, _BatchSize = 1).

-spec next(iterator(), pos_integer()) -> {ok, iterator(), [binary()]} | end_of_stream.
next(#it{data = {?MODULE, end_of_stream}}, _BatchSize) ->
    end_of_stream;
next(
    It = #it{shard = Shard, module = Mod, gen = Gen, data = {?MODULE, retry, Serialized}}, BatchSize
) ->
    #{data := DBData} = meta_get_gen(Shard, Gen),
    {ok, ItData} = Mod:restore_iterator(DBData, Serialized),
    next(It#it{data = ItData}, BatchSize);
next(It = #it{}, BatchSize) ->
    do_next(It, BatchSize, _Acc = []).

-spec do_next(iterator(), non_neg_integer(), [binary()]) ->
    {ok, iterator(), [binary()]} | end_of_stream.
do_next(It, N, Acc) when N =< 0 ->
    {ok, It, lists:reverse(Acc)};
do_next(It = #it{module = Mod, data = ItData}, N, Acc) ->
    case Mod:next(ItData) of
        {value, Bin, ItDataNext} ->
            Val = deserialize(Bin),
            do_next(It#it{data = ItDataNext}, N - 1, [Val | Acc]);
        {error, _} = _Error ->
            %% todo: log?
            %% iterator might be invalid now; will need to re-open it.
            Serialized = Mod:preserve_iterator(ItData),
            {ok, It#it{data = {?MODULE, retry, Serialized}}, lists:reverse(Acc)};
        none ->
            case open_next_iterator(It) of
                {ok, ItNext} ->
                    do_next(ItNext, N, Acc);
                {error, _} = _Error ->
                    %% todo: log?
                    %% fixme: only bad options may lead to this?
                    %% return an "empty" iterator to be re-opened when retrying?
                    Serialized = Mod:preserve_iterator(ItData),
                    {ok, It#it{data = {?MODULE, retry, Serialized}}, lists:reverse(Acc)};
                none ->
                    case Acc of
                        [] ->
                            end_of_stream;
                        _ ->
                            {ok, It#it{data = {?MODULE, end_of_stream}}, lists:reverse(Acc)}
                    end
            end
    end.

-spec preserve_iterator(iterator(), emqx_ds:iterator_id()) ->
    ok | {error, _TODO}.
preserve_iterator(It = #it{}, IteratorID) ->
    iterator_put_state(IteratorID, It).

-spec restore_iterator(emqx_ds:shard(), emqx_ds:replay_id()) ->
    {ok, iterator()} | {error, _TODO}.
restore_iterator(Shard, ReplayID) ->
    case iterator_get_state(Shard, ReplayID) of
        {ok, Serial} ->
            restore_iterator_state(Shard, Serial);
        not_found ->
            {error, not_found};
        {error, _Reason} = Error ->
            Error
    end.

-spec ensure_iterator(emqx_ds:shard(), emqx_ds:iterator_id(), emqx_ds:replay()) ->
    {ok, iterator()} | {error, _TODO}.
ensure_iterator(Shard, IteratorID, Replay = {_TopicFilter, _StartMS}) ->
    case restore_iterator(Shard, IteratorID) of
        {ok, It} ->
            {ok, It};
        {error, not_found} ->
            {ok, It} = make_iterator(Shard, Replay),
            ok = emqx_ds_storage_layer:preserve_iterator(It, IteratorID),
            {ok, It};
        Error ->
            Error
    end.

-spec discard_iterator(emqx_ds:shard(), emqx_ds:replay_id()) ->
    ok | {error, _TODO}.
discard_iterator(Shard, ReplayID) ->
    iterator_delete(Shard, ReplayID).

-spec discard_iterator_prefix(emqx_ds:shard(), binary()) ->
    ok | {error, _TODO}.
discard_iterator_prefix(Shard, KeyPrefix) ->
    case do_discard_iterator_prefix(Shard, KeyPrefix) of
        {ok, _} -> ok;
        Error -> Error
    end.

-spec list_iterator_prefix(
    emqx_ds:shard(),
    binary()
) -> {ok, [emqx_ds:iterator_id()]} | {error, _TODO}.
list_iterator_prefix(Shard, KeyPrefix) ->
    do_list_iterator_prefix(Shard, KeyPrefix).

-spec foldl_iterator_prefix(
    emqx_ds:shard(),
    binary(),
    fun((_Key :: binary(), _Value :: binary(), Acc) -> Acc),
    Acc
) -> {ok, Acc} | {error, _TODO} when
    Acc :: term().
foldl_iterator_prefix(Shard, KeyPrefix, Fn, Acc) ->
    do_foldl_iterator_prefix(Shard, KeyPrefix, Fn, Acc).

%%================================================================================
%% behaviour callbacks
%%================================================================================

init({Shard, Options}) ->
    process_flag(trap_exit, true),
    {ok, S0} = open_db(Shard, Options),
    S = ensure_current_generation(S0),
    ok = populate_metadata(S),
    {ok, S}.

handle_call({create_generation, Since, Config}, _From, S) ->
    case create_new_gen(Since, Config, S) of
        {ok, GenId, NS} ->
            {reply, {ok, GenId}, NS};
        {error, _} = Error ->
            {reply, Error, S}
    end;
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, #s{db = DB, shard = Shard}) ->
    meta_erase(Shard),
    ok = rocksdb:close(DB).

%%================================================================================
%% Internal functions
%%================================================================================

-record(db, {handle :: rocksdb:db_handle(), cf_iterator :: rocksdb:cf_handle()}).

-spec populate_metadata(state()) -> ok.
populate_metadata(S = #s{shard = Shard, db = DBHandle, cf_iterator = CFIterator}) ->
    ok = meta_put(Shard, db, #db{handle = DBHandle, cf_iterator = CFIterator}),
    Current = schema_get_current(DBHandle),
    lists:foreach(fun(GenId) -> populate_metadata(GenId, S) end, lists:seq(0, Current)).

-spec populate_metadata(gen_id(), state()) -> ok.
populate_metadata(GenId, S = #s{shard = Shard, db = DBHandle}) ->
    Gen = open_gen(GenId, schema_get_gen(DBHandle, GenId), S),
    meta_register_gen(Shard, GenId, Gen).

-spec ensure_current_generation(state()) -> state().
ensure_current_generation(S = #s{shard = _Shard, keyspace = Keyspace, db = DBHandle}) ->
    case schema_get_current(DBHandle) of
        undefined ->
            Config = emqx_ds_conf:keyspace_config(Keyspace),
            {ok, _, NS} = create_new_gen(0, Config, S),
            NS;
        _GenId ->
            S
    end.

-spec create_new_gen(emqx_ds:time(), emqx_ds_conf:backend_config(), state()) ->
    {ok, gen_id(), state()} | {error, nonmonotonic}.
create_new_gen(Since, Config, S = #s{shard = Shard, db = DBHandle}) ->
    GenId = get_next_id(meta_get_current(Shard)),
    GenId = get_next_id(schema_get_current(DBHandle)),
    case is_gen_valid(Shard, GenId, Since) of
        ok ->
            {ok, Gen, NS} = create_gen(GenId, Since, Config, S),
            %% TODO: Transaction? Column family creation can't be transactional, anyway.
            ok = schema_put_gen(DBHandle, GenId, Gen),
            ok = schema_put_current(DBHandle, GenId),
            ok = meta_register_gen(Shard, GenId, open_gen(GenId, Gen, NS)),
            {ok, GenId, NS};
        {error, _} = Error ->
            Error
    end.

-spec create_gen(gen_id(), emqx_ds:time(), emqx_ds_conf:backend_config(), state()) ->
    {ok, generation(), state()}.
create_gen(GenId, Since, {Module, Options}, S = #s{db = DBHandle, cf_generations = CFs}) ->
    % TODO: Backend implementation should ensure idempotency.
    {Schema, NewCFs} = Module:create_new(DBHandle, GenId, Options),
    Gen = #{
        module => Module,
        data => Schema,
        since => Since
    },
    {ok, Gen, S#s{cf_generations = NewCFs ++ CFs}}.

-spec open_db(emqx_ds:shard(), options()) -> {ok, state()} | {error, _TODO}.
open_db(Shard, Options) ->
    DefaultDir = binary_to_list(Shard),
    DBDir = unicode:characters_to_list(maps:get(dir, Options, DefaultDir)),
    %% TODO: properly forward keyspace
    Keyspace = maps:get(keyspace, Options, default_keyspace),
    DBOptions = [
        {create_if_missing, true},
        {create_missing_column_families, true}
        | emqx_ds_conf:db_options(Keyspace)
    ],
    _ = filelib:ensure_dir(DBDir),
    ExistingCFs =
        case rocksdb:list_column_families(DBDir, DBOptions) of
            {ok, CFs} ->
                [{Name, []} || Name <- CFs, Name /= ?DEFAULT_CF, Name /= ?ITERATOR_CF];
            % DB is not present. First start
            {error, {db_open, _}} ->
                []
        end,
    ColumnFamilies = [
        {?DEFAULT_CF, ?DEFAULT_CF_OPTS},
        {?ITERATOR_CF, ?ITERATOR_CF_OPTS}
        | ExistingCFs
    ],
    case rocksdb:open(DBDir, DBOptions, ColumnFamilies) of
        {ok, DBHandle, [_CFDefault, CFIterator | CFRefs]} ->
            {CFNames, _} = lists:unzip(ExistingCFs),
            {ok, #s{
                shard = Shard,
                keyspace = Keyspace,
                db = DBHandle,
                cf_iterator = CFIterator,
                cf_generations = lists:zip(CFNames, CFRefs)
            }};
        Error ->
            Error
    end.

-spec open_gen(gen_id(), generation(), state()) -> generation().
open_gen(
    GenId,
    Gen = #{module := Mod, data := Data},
    #s{shard = Shard, db = DBHandle, cf_generations = CFs}
) ->
    DB = Mod:open(Shard, DBHandle, GenId, CFs, Data),
    Gen#{data := DB}.

-spec open_next_iterator(iterator()) -> {ok, iterator()} | {error, _Reason} | none.
open_next_iterator(It = #it{shard = Shard, gen = GenId}) ->
    open_next_iterator(meta_get_gen(Shard, GenId + 1), It#it{gen = GenId + 1}).

open_next_iterator(undefined, _It) ->
    none;
open_next_iterator(Gen = #{}, It) ->
    open_iterator(Gen, It).

-spec open_iterator(generation(), iterator()) -> {ok, iterator()} | {error, _Reason}.
open_iterator(#{module := Mod, data := Data}, It = #it{}) ->
    Options = #{}, % TODO: passthrough options
    case Mod:make_iterator(Data, It#it.replay, Options) of
        {ok, ItData} ->
            {ok, It#it{module = Mod, data = ItData}};
        Err ->
            Err
    end.

-spec open_restore_iterator(generation(), iterator(), binary()) ->
    {ok, iterator()} | {error, _Reason}.
open_restore_iterator(#{module := Mod, data := Data}, It = #it{}, Serial) ->
    case Mod:restore_iterator(Data, Serial) of
        {ok, ItData} ->
            {ok, It#it{module = Mod, data = ItData}};
        Err ->
            Err
    end.

%%

-define(KEY_REPLAY_STATE(IteratorId), <<(IteratorId)/binary, "rs">>).
-define(KEY_REPLAY_STATE_PAT(KeyReplayState), begin
    <<IteratorId:(size(KeyReplayState) - 2)/binary, "rs">> = (KeyReplayState),
    IteratorId
end).

-define(ITERATION_WRITE_OPTS, []).
-define(ITERATION_READ_OPTS, []).

iterator_get_state(Shard, ReplayID) ->
    #db{handle = Handle, cf_iterator = CF} = meta_lookup(Shard, db),
    rocksdb:get(Handle, CF, ?KEY_REPLAY_STATE(ReplayID), ?ITERATION_READ_OPTS).

iterator_put_state(ID, It = #it{shard = Shard}) ->
    #db{handle = Handle, cf_iterator = CF} = meta_lookup(Shard, db),
    Serial = preserve_iterator_state(It),
    rocksdb:put(Handle, CF, ?KEY_REPLAY_STATE(ID), Serial, ?ITERATION_WRITE_OPTS).

iterator_delete(Shard, ID) ->
    #db{handle = Handle, cf_iterator = CF} = meta_lookup(Shard, db),
    rocksdb:delete(Handle, CF, ?KEY_REPLAY_STATE(ID), ?ITERATION_WRITE_OPTS).

preserve_iterator_state(#it{
    gen = Gen,
    replay = {TopicFilter, StartTime},
    module = Mod,
    data = ItData
}) ->
    term_to_binary(#{
        v => 1,
        gen => Gen,
        filter => TopicFilter,
        start => StartTime,
        st => Mod:preserve_iterator(ItData)
    }).

restore_iterator_state(Shard, Serial) when is_binary(Serial) ->
    restore_iterator_state(Shard, binary_to_term(Serial));
restore_iterator_state(
    Shard,
    #{
        v := 1,
        gen := Gen,
        filter := TopicFilter,
        start := StartTime,
        st := State
    }
) ->
    It = #it{shard = Shard, gen = Gen, replay = {TopicFilter, StartTime}},
    open_restore_iterator(meta_get_gen(Shard, Gen), It, State).

do_list_iterator_prefix(Shard, KeyPrefix) ->
    Fn = fun(K0, _V, Acc) ->
        K = ?KEY_REPLAY_STATE_PAT(K0),
        [K | Acc]
    end,
    do_foldl_iterator_prefix(Shard, KeyPrefix, Fn, []).

do_discard_iterator_prefix(Shard, KeyPrefix) ->
    #db{handle = DBHandle, cf_iterator = CF} = meta_lookup(Shard, db),
    Fn = fun(K, _V, _Acc) -> ok = rocksdb:delete(DBHandle, CF, K, ?ITERATION_WRITE_OPTS) end,
    do_foldl_iterator_prefix(Shard, KeyPrefix, Fn, ok).

do_foldl_iterator_prefix(Shard, KeyPrefix, Fn, Acc) ->
    #db{handle = Handle, cf_iterator = CF} = meta_lookup(Shard, db),
    case rocksdb:iterator(Handle, CF, ?ITERATION_READ_OPTS) of
        {ok, It} ->
            NextAction = {seek, KeyPrefix},
            do_foldl_iterator_prefix(Handle, CF, It, KeyPrefix, NextAction, Fn, Acc);
        Error ->
            Error
    end.

do_foldl_iterator_prefix(DBHandle, CF, It, KeyPrefix, NextAction, Fn, Acc) ->
    case rocksdb:iterator_move(It, NextAction) of
        {ok, K = <<KeyPrefix:(size(KeyPrefix))/binary, _/binary>>, V} ->
            NewAcc = Fn(K, V, Acc),
            do_foldl_iterator_prefix(DBHandle, CF, It, KeyPrefix, next, Fn, NewAcc);
        {ok, _K, _V} ->
            ok = rocksdb:iterator_close(It),
            {ok, Acc};
        {error, invalid_iterator} ->
            ok = rocksdb:iterator_close(It),
            {ok, Acc};
        Error ->
            ok = rocksdb:iterator_close(It),
            Error
    end.

%% Functions for dealing with the metadata stored persistently in rocksdb

-define(CURRENT_GEN, <<"current">>).
-define(SCHEMA_WRITE_OPTS, []).
-define(SCHEMA_READ_OPTS, []).

-spec schema_get_gen(rocksdb:db_handle(), gen_id()) -> generation().
schema_get_gen(DBHandle, GenId) ->
    {ok, Bin} = rocksdb:get(DBHandle, schema_gen_key(GenId), ?SCHEMA_READ_OPTS),
    binary_to_term(Bin).

-spec schema_put_gen(rocksdb:db_handle(), gen_id(), generation()) -> ok | {error, _}.
schema_put_gen(DBHandle, GenId, Gen) ->
    rocksdb:put(DBHandle, schema_gen_key(GenId), term_to_binary(Gen), ?SCHEMA_WRITE_OPTS).

-spec schema_get_current(rocksdb:db_handle()) -> gen_id() | undefined.
schema_get_current(DBHandle) ->
    case rocksdb:get(DBHandle, ?CURRENT_GEN, ?SCHEMA_READ_OPTS) of
        {ok, Bin} ->
            binary_to_integer(Bin);
        not_found ->
            undefined
    end.

-spec schema_put_current(rocksdb:db_handle(), gen_id()) -> ok | {error, _}.
schema_put_current(DBHandle, GenId) ->
    rocksdb:put(DBHandle, ?CURRENT_GEN, integer_to_binary(GenId), ?SCHEMA_WRITE_OPTS).

-spec schema_gen_key(integer()) -> binary().
schema_gen_key(N) ->
    <<"gen", N:32>>.

-undef(CURRENT_GEN).
-undef(SCHEMA_WRITE_OPTS).
-undef(SCHEMA_READ_OPTS).

%% Functions for dealing with the runtime shard metadata:

-define(PERSISTENT_TERM(SHARD, GEN), {?MODULE, SHARD, GEN}).

-spec meta_register_gen(emqx_ds:shard(), gen_id(), generation()) -> ok.
meta_register_gen(Shard, GenId, Gen) ->
    Gs =
        case GenId > 0 of
            true -> meta_lookup(Shard, GenId - 1);
            false -> []
        end,
    ok = meta_put(Shard, GenId, [Gen | Gs]),
    ok = meta_put(Shard, current, GenId).

-spec meta_lookup_gen(emqx_ds:shard(), emqx_ds:time()) -> {gen_id(), generation()}.
meta_lookup_gen(Shard, Time) ->
    %% TODO
    %% Is cheaper persistent term GC on update here worth extra lookup? I'm leaning
    %% towards a "no".
    Current = meta_lookup(Shard, current),
    Gens = meta_lookup(Shard, Current),
    find_gen(Time, Current, Gens).

find_gen(Time, GenId, [Gen = #{since := Since} | _]) when Time >= Since ->
    {GenId, Gen};
find_gen(Time, GenId, [_Gen | Rest]) ->
    find_gen(Time, GenId - 1, Rest).

-spec meta_get_gen(emqx_ds:shard(), gen_id()) -> generation() | undefined.
meta_get_gen(Shard, GenId) ->
    case meta_lookup(Shard, GenId, []) of
        [Gen | _Older] -> Gen;
        [] -> undefined
    end.

-spec meta_get_current(emqx_ds:shard()) -> gen_id() | undefined.
meta_get_current(Shard) ->
    meta_lookup(Shard, current, undefined).

-spec meta_lookup(emqx_ds:shard(), _K) -> _V.
meta_lookup(Shard, K) ->
    persistent_term:get(?PERSISTENT_TERM(Shard, K)).

-spec meta_lookup(emqx_ds:shard(), _K, Default) -> _V | Default.
meta_lookup(Shard, K, Default) ->
    persistent_term:get(?PERSISTENT_TERM(Shard, K), Default).

-spec meta_put(emqx_ds:shard(), _K, _V) -> ok.
meta_put(Shard, K, V) ->
    persistent_term:put(?PERSISTENT_TERM(Shard, K), V).

-spec meta_erase(emqx_ds:shard()) -> ok.
meta_erase(Shard) ->
    [
        persistent_term:erase(K)
     || {K = ?PERSISTENT_TERM(Z, _), _} <- persistent_term:get(), Z =:= Shard
    ],
    ok.

-undef(PERSISTENT_TERM).

get_next_id(undefined) -> 0;
get_next_id(GenId) -> GenId + 1.

is_gen_valid(Shard, GenId, Since) when GenId > 0 ->
    [GenPrev | _] = meta_lookup(Shard, GenId - 1),
    case GenPrev of
        #{since := SincePrev} when Since > SincePrev ->
            ok;
        #{} ->
            {error, nonmonotonic}
    end;
is_gen_valid(_Shard, 0, 0) ->
    ok.

serialize(Msg) ->
    %% TODO: remove topic, GUID, etc. from the stored
    %% message. Reconstruct it from the metadata.
    term_to_binary(emqx_message:to_map(Msg)).

deserialize(Bin) ->
    emqx_message:from_map(binary_to_term(Bin)).


%% -spec store_cfs(rocksdb:db_handle(), [{string(), rocksdb:cf_handle()}]) -> ok.
%% store_cfs(DBHandle, CFRefs) ->
%%     lists:foreach(
%%       fun({CFName, CFRef}) ->
%%               persistent_term:put({self(), CFName}, {DBHandle, CFRef})
%%       end,
%%       CFRefs).
