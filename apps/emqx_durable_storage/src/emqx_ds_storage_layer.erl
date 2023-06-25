%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_storage_layer).

-behaviour(gen_server).

%% API:
-export([start_link/1]).
-export([create_generation/3]).

-export([store/5]).

-export([make_iterator/2, next/1]).

-export([preserve_iterator/2, restore_iterator/2, discard_iterator/2]).

%% behaviour callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-export_type([cf_refs/0, gen_id/0, db_write_options/0, state/0, iterator/0]).

-compile({inline, [meta_lookup/2]}).

%%================================================================================
%% Type declarations
%%================================================================================

%% see rocksdb:db_options()
% -type options() :: proplists:proplist().

-type db_write_options() :: proplists:proplist().

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

-define(REF(Shard), {via, gproc, {n, l, {?MODULE, Shard}}}).

%%================================================================================
%% Callbacks
%%================================================================================

-callback create_new(rocksdb:db_handle(), gen_id(), _Options :: term()) ->
    {_Schema, cf_refs()}.

-callback open(emqx_ds:shard(), rocksdb:db_handle(), gen_id(), cf_refs(), _Schema) ->
    term().

-callback store(_Schema, binary(), emqx_ds:time(), emqx_ds:topic(), binary()) ->
    ok | {error, _}.

-callback make_iterator(_Schema, emqx_ds:replay()) ->
    {ok, _It} | {error, _}.

-callback restore_iterator(_Schema, emqx_ds:replay(), binary()) -> {ok, _It} | {error, _}.

-callback preserve_iterator(_Schema, _It) -> term().

-callback next(It) -> {value, binary(), It} | none | {error, closed}.

%%================================================================================
%% API funcions
%%================================================================================

-spec start_link(emqx_ds:shard()) -> {ok, pid()}.
start_link(Shard) ->
    gen_server:start_link(?REF(Shard), ?MODULE, [Shard], []).

-spec create_generation(emqx_ds:shard(), emqx_ds:time(), emqx_ds_conf:backend_config()) ->
    {ok, gen_id()} | {error, nonmonotonic}.
create_generation(Shard, Since, Config = {_Module, _Options}) ->
    gen_server:call(?REF(Shard), {create_generation, Since, Config}).

-spec store(
    emqx_ds:shard(), emqx_guid:guid(), emqx_ds:time(), emqx_ds:topic(), binary()
) ->
    ok | {error, _}.
store(Shard, GUID, Time, Topic, Msg) ->
    {_GenId, #{module := Mod, data := Data}} = meta_lookup_gen(Shard, Time),
    Mod:store(Data, GUID, Time, Topic, Msg).

-spec make_iterator(emqx_ds:shard(), emqx_ds:replay()) ->
    {ok, iterator()} | {error, _TODO}.
make_iterator(Shard, Replay = {_, StartTime}) ->
    {GenId, Gen} = meta_lookup_gen(Shard, StartTime),
    open_iterator(Gen, #it{
        shard = Shard,
        gen = GenId,
        replay = Replay
    }).

-spec next(iterator()) -> {value, binary(), iterator()} | none | {error, closed}.
next(It = #it{module = Mod, data = ItData}) ->
    case Mod:next(ItData) of
        {value, Val, ItDataNext} ->
            {value, Val, It#it{data = ItDataNext}};
        {error, _} = Error ->
            Error;
        none ->
            case open_next_iterator(It) of
                {ok, ItNext} ->
                    next(ItNext);
                {error, _} = Error ->
                    Error;
                none ->
                    none
            end
    end.

-spec preserve_iterator(iterator(), emqx_ds:replay_id()) ->
    ok | {error, _TODO}.
preserve_iterator(It = #it{}, ReplayID) ->
    iterator_put_state(ReplayID, It).

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

-spec discard_iterator(emqx_ds:shard(), emqx_ds:replay_id()) ->
    ok | {error, _TODO}.
discard_iterator(Shard, ReplayID) ->
    iterator_delete(Shard, ReplayID).

%%================================================================================
%% behaviour callbacks
%%================================================================================

init([Shard]) ->
    process_flag(trap_exit, true),
    {ok, S0} = open_db(Shard),
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
ensure_current_generation(S = #s{shard = Shard, db = DBHandle}) ->
    case schema_get_current(DBHandle) of
        undefined ->
            Config = emqx_ds_conf:shard_config(Shard),
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

-spec open_db(emqx_ds:shard()) -> {ok, state()} | {error, _TODO}.
open_db(Shard) ->
    Filename = binary_to_list(Shard),
    DBOptions = [
        {create_if_missing, true},
        {create_missing_column_families, true}
        | emqx_ds_conf:db_options()
    ],
    ExistingCFs =
        case rocksdb:list_column_families(Filename, DBOptions) of
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
    case rocksdb:open(Filename, DBOptions, ColumnFamilies) of
        {ok, DBHandle, [_CFDefault, CFIterator | CFRefs]} ->
            {CFNames, _} = lists:unzip(ExistingCFs),
            {ok, #s{
                shard = Shard,
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
    case Mod:make_iterator(Data, It#it.replay) of
        {ok, ItData} ->
            {ok, It#it{module = Mod, data = ItData}};
        Err ->
            Err
    end.

-spec open_restore_iterator(generation(), iterator(), binary()) ->
    {ok, iterator()} | {error, _Reason}.
open_restore_iterator(#{module := Mod, data := Data}, It = #it{replay = Replay}, Serial) ->
    case Mod:restore_iterator(Data, Replay, Serial) of
        {ok, ItData} ->
            {ok, It#it{module = Mod, data = ItData}};
        Err ->
            Err
    end.

%%

-define(KEY_REPLAY_STATE(ReplayID), <<(ReplayID)/binary, "rs">>).

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
    % TODO
    % Is cheaper persistent term GC on update here worth extra lookup? I'm leaning
    % towards a "no".
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

%% -spec store_cfs(rocksdb:db_handle(), [{string(), rocksdb:cf_handle()}]) -> ok.
%% store_cfs(DBHandle, CFRefs) ->
%%     lists:foreach(
%%       fun({CFName, CFRef}) ->
%%               persistent_term:put({self(), CFName}, {DBHandle, CFRef})
%%       end,
%%       CFRefs).
