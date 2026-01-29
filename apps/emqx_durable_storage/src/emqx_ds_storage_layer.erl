%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_storage_layer).

-behaviour(gen_server).

%% Replication layer API:
-export([
    create_db_group/2,
    update_db_group/3,
    destroy_db_group/2,
    check_soft_quota/1,

    %% Lifecycle
    start_link/2,
    start_link_no_schema/2,
    drop_shard/1,
    shard_info/2,
    ensure_schema/2,

    %% Generations
    update_config_v0/3,
    add_generation/2,
    add_generation/3,
    list_slabs/1,
    drop_slab/2,
    find_generation/2,

    %% Global
    store_global/3,
    fetch_global/2,

    %% Snapshotting
    flush/1,
    take_snapshot/1,
    accept_snapshot/1,

    %% Custom events
    handle_event/3
]).

%% gen_server
-export([
    init/1,
    format_status/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% internal exports:
-export([
    db_dir/1,
    base_dir/0,
    generation_get/2,
    generation_current/1,
    generations_since/2,
    ls_shards/1,
    get_stats/1,
    db_group_stats/2,
    print_state/1,
    has_schema/1
]).

-export_type([
    rocksdb_options/0,
    batch_prepare_opts/0,
    batch_store_opts/0,
    gen_id/0,
    generation/0,
    generation_data/0,
    cf_refs/0,
    stream/0,
    iterator/0,
    dbshard/0,
    options/0,
    prototype/0,
    cooked_batch/0,
    event_dispatch_f/0,
    db_group/0
]).

-include("emqx_ds.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../gen_src/DSBuiltinMetadata.hrl").
-include_lib("emqx_utils/include/emqx_record_to_map.hrl").

-elvis([
    {elvis_style, atom_naming_convention, disable},
    {elvis_style, invalid_dynamic_call, disable}
]).

-define(REF(ShardId), {via, gproc, {n, l, {?MODULE, ShardId}}}).

%% Wrappers for the storage events:
-define(storage_event(GEN_ID, PAYLOAD), #{0 := 3333, 1 := GEN_ID, 2 := PAYLOAD}).
-define(mk_storage_event(GEN_ID, PAYLOAD), #{0 => 3333, 1 => GEN_ID, 2 => PAYLOAD}).

%%================================================================================
%% Type declarations
%%================================================================================

-define(APP, emqx_durable_storage).

-define(MB, 1024 * 1024).

-type rocksdb_options() ::
    #{
        cache_size => non_neg_integer(),
        write_buffer_size => non_neg_integer(),
        max_open_files => non_neg_integer(),
        allow_fallocate => boolean(),
        misc_options => [{atom(), _}]
    }.

-type batch_prepare_opts() :: #{}.

%% Options affecting how batches should be stored.
%% See also: `emqx_ds:message_store_opts()'.
-type batch_store_opts() ::
    #{
        %% Should the storage make sure that the batch is written durably? Non-durable
        %% writes are in general unsafe but require much less resources, i.e. with RocksDB
        %% non-durable (WAL-less) writes do not usually involve _any_ disk I/O.
        %% Default: `true'.
        durable => boolean()
    }.

%% # "Record" integer keys.  We use maps with integer keys to avoid persisting and sending
%% records over the wire.
%% tags:
-define(COOKED_BATCH, 4).

%% keys:
-define(tag, 1).
-define(generation, 2).
-define(enc, 3).

-type prototype() ::
    {emqx_ds_storage_skipstream_lts_v2, emqx_ds_storage_skipstream_lts_v2:schema()}.

-type dbshard() :: {emqx_ds:db(), binary()}.

-type cf_ref() :: {string(), rocksdb:cf_handle()}.
-type cf_refs() :: [cf_ref()].

-type gen_id() :: 0..16#ffff.
-type slab_info() :: #{
    created_at := emqx_ds:time(),
    since := emqx_ds:time(),
    until := undefined | emqx_ds:time(),
    _ => _
}.

%% Note: this might be stored permanently on a remote node.
-opaque stream() :: #'Stream'{}.

%% Note: this might be stored permanently on a remote node.
-type iterator() :: #'Iterator'{}.

-opaque cooked_batch() ::
    #{
        ?tag := ?COOKED_BATCH,
        ?generation := gen_id(),
        ?enc := term()
    }.

-type event_dispatch_f() :: fun(([stream()]) -> ok).

%%%% Generation:

-define(GEN_KEY(GEN_ID), {generation, GEN_ID}).

-type generation(Data) :: #{
    %% Module that handles data for the generation:
    module := module(),
    %% Module-specific data defined at generation creation time:
    data := Data,
    %% Column families used by this generation
    cf_names := [string()],
    %% Time at which this was created.  Might differ from `since', in particular for the
    %% first generation.
    created_at := emqx_ds:time(),
    %% When should this generation become active?
    %% This generation should only contain messages timestamped no earlier than that.
    %% The very first generation will have `since` equal 0.
    since := emqx_ds:time(),
    ptrans := emqx_ds_payload_transform:schema(),
    until := emqx_ds:time() | undefined
}.

%% Module-specific runtime data, as instantiated by `Mod:open/5` callback function.
-type generation_data() :: term().

%% Schema for a generation. Persistent term.
-type generation_schema() :: generation(term()).

%% Runtime view of generation:
-type generation() :: generation(generation_data()).

%%%% Shard:

-type shard(GenData) :: #{
    %% ID of the current generation (where the new data is written):
    current_generation := gen_id(),
    %% This data WAS used to create new generation in raft
    %% machine ver.0:
    prototype := prototype(),
    ptrans := emqx_ds_payload_transform:schema(),
    %% Generations:
    ?GEN_KEY(gen_id()) => GenData,
    %% DB handle (runtime only).
    db => rocksdb:db_handle()
}.

%% Shard schema (persistent):
-type shard_schema() :: shard(generation_schema()).

%% Shard (runtime):
-type shard() :: shard(generation()).

%% Which DB options to provide to the storage layout module:
%% See `emqx_ds:db_opts()`.
-define(STORAGE_LAYOUT_DB_OPTS, []).

-define(GLOBAL(K), <<"G/", K/binary>>).

-type options() :: #{db_group := emqx_ds:db_group(), _ => _}.

-record(db_group, {
    env :: rocksdb:env_handle(),
    sst_file_mgr :: rocksdb:sst_file_manager(),
    write_buffer_mgr :: rocksdb:write_buffer_manager(),
    conf :: map()
}).

-type create_db_group_opts() :: #{
    backend := _,
    env_type => default | memenv,
    storage_quota => infinity | pos_integer(),
    write_buffer_size => pos_integer(),
    rocksdb_nthreads_low => pos_integer(),
    rocksdb_nthreads_high => pos_integer()
}.

-opaque db_group() :: #db_group{}.

-record(call_ensure_schema, {options :: emqx_ds:create_db_opts()}).
-record(call_has_schema, {}).

%%================================================================================
%% API for the replication layer
%%================================================================================

-spec has_schema(dbshard()) -> boolean().
has_schema(DBShard) ->
    gen_server:call(?REF(DBShard), #call_has_schema{}).

-doc """
Create shard schema and the first generation.
NOP if schema already exists.
""".
-spec ensure_schema(dbshard(), emqx_ds:create_db_opts()) -> ok | {error, _}.
ensure_schema(ShardId, Options) ->
    gen_server:call(?REF(ShardId), #call_ensure_schema{options = Options}, infinity).

-spec create_db_group(emqx_ds:db_group(), create_db_group_opts()) -> {ok, db_group()} | {error, _}.
create_db_group(_GroupId, UserOpts) ->
    maybe
        {ok, Opts} ?= verify_db_group_config(UserOpts),
        EnvType = maps:get(env_type, Opts, default),
        %% Create SST file manager:
        {ok, Env} ?= rocksdb:new_env(EnvType),
        {ok, SSTManager} = rocksdb:new_sst_file_manager(Env),
        %% Create write buffer manager:
        case maps:get(write_buffer_size, Opts) of
            infinity ->
                WriteBufferSize = 0;
            WriteBufferSize ->
                ok
        end,
        {ok, WriteBufferManager} = rocksdb:new_write_buffer_manager(WriteBufferSize),
        %% Result:
        {ok,
            update_rocksdb_group_runtime(#db_group{
                env = Env,
                sst_file_mgr = SSTManager,
                write_buffer_mgr = WriteBufferManager,
                conf = Opts
            })}
    end.

-spec update_db_group(emqx_ds:db_group(), create_db_group_opts(), db_group()) ->
    {ok, db_group()} | {error, _}.
update_db_group(_Id, UserOpts, Grp = #db_group{}) ->
    maybe
        {ok, Opts} ?= verify_db_group_config(UserOpts),
        {ok, update_rocksdb_group_runtime(Grp#db_group{conf = Opts})}
    end.

-spec update_rocksdb_group_runtime(db_group()) -> db_group().
update_rocksdb_group_runtime(#db_group{env = Env, conf = Conf} = Grp) ->
    #{rocksdb_nthreads_high := BTHP, rocksdb_nthreads_low := BTLP} = Conf,
    ok = rocksdb:set_env_background_threads(Env, BTHP, priority_high),
    ok = rocksdb:set_env_background_threads(Env, BTLP, priority_low),
    Grp.

-spec destroy_db_group(emqx_ds:db_group(), db_group()) -> ok | {error, _}.
destroy_db_group(_GroupId, #db_group{env = Env, sst_file_mgr = SSTManager, write_buffer_mgr = WBM}) ->
    ok = rocksdb:release_write_buffer_manager(WBM),
    ok = rocksdb:release_sst_file_manager(SSTManager),
    ok = rocksdb:destroy_env(Env),
    ok.

-spec check_soft_quota(db_group()) -> boolean().
check_soft_quota(#db_group{conf = #{storage_quota := infinity}}) ->
    true;
check_soft_quota(#db_group{sst_file_mgr = SSTFM, conf = #{storage_quota := Quota}}) when
    is_integer(Quota)
->
    Usage = rocksdb:sst_file_manager_info(SSTFM, total_size),
    true = is_integer(Usage),
    Usage < Quota.

%% Note: we specify gen_server requests as records to make use of Dialyzer:
-record(call_add_generation, {since :: emqx_ds:time(), prototype :: prototype() | undefined}).
-record(call_update_config_v0, {options :: emqx_ds:create_db_opts(), since :: emqx_ds:time()}).
-record(call_list_generations_with_lifetimes, {}).
-record(call_drop_generation, {gen_id :: gen_id()}).
-record(call_flush, {}).
-record(call_take_snapshot, {}).
-record(call_get_stats, {}).

-spec drop_shard(dbshard()) -> ok.
drop_shard(Shard) ->
    ok = rocksdb:destroy(db_dir(Shard), []).

%% @doc Persist a bunch of key/value pairs in the storage globally, in the "outside
%% of specific generation" sense. Once persisted, values can be read back by calling
%% `fetch_global/2`.
%%
%% Adding or dropping generations won't affect persisted key/value pairs, hence the
%% purpose: keep state that needs to be tied to the shard itself and outlive any
%% generation.
%%
%% This operation is idempotent, previous values associated with existing keys are
%% overwritten. While atomicity of the operation can be specifically requested through
%% `atomic` option, it is atomic by default: either all of pairs are persisted, or none
%% at all. Writes are durable by default, but this is optional, see `batch_store_opts()`
%% for details.
-spec store_global(dbshard(), _KVs :: #{binary() => binary()}, #{durable => boolean()}) ->
    ok | emqx_ds:error(_).
store_global(ShardId, KVs, Options) ->
    maybe
        #{db := DB} ?= get_schema_runtime(ShardId),
        {ok, Batch} = rocksdb:batch(),
        try
            ok = maps:foreach(fun(K, V) -> rocksdb:batch_put(Batch, ?GLOBAL(K), V) end, KVs),
            WriteOpts = [{disable_wal, not maps:get(durable, Options, true)}],
            Result = rocksdb:write_batch(DB, Batch, WriteOpts),
            case Result of
                ok ->
                    ok;
                {error, {error, Reason}} ->
                    {error, unrecoverable, {rocksdb, Reason}}
            end
        after
            rocksdb:release_batch(Batch)
        end
    end.

%% @doc Retrieve a value for a single key from the storage written there previously by
%% `store_global/3`.
-spec fetch_global(dbshard(), _Key :: binary()) ->
    {ok, _Value :: binary()} | not_found | emqx_ds:error(_).
fetch_global(ShardId, K) ->
    maybe
        #{db := DB} ?= get_schema_runtime(ShardId),
        Result = rocksdb:get(DB, ?GLOBAL(K), _ReadOpts = []),
        case Result of
            {ok, _} ->
                Result;
            not_found ->
                Result;
            {error, Reason} ->
                {error, unrecoverable, {rocksdb, Reason}}
        end
    end.

%% Deprecated. Kept for compatibility with ra machine version 0.
-spec update_config_v0(dbshard(), emqx_ds:time(), emqx_ds:create_db_opts()) ->
    ok
    | {error, overlaps_existing_generations | {no_schema, _}}.
update_config_v0(ShardId, Since, Options) ->
    Call = #call_update_config_v0{since = Since, options = Options},
    gen_server:call(?REF(ShardId), Call, infinity).

-spec add_generation(dbshard(), emqx_ds:time(), prototype()) ->
    ok
    | {error, overlaps_existing_generations | {no_schema, _}}.
add_generation(ShardId, Since, {_, _} = Prototype) ->
    gen_server:call(
        ?REF(ShardId),
        #call_add_generation{since = Since, prototype = Prototype},
        infinity
    ).

-doc """
Deprecated. Use add_generation/3 instead.
""".
-spec add_generation(dbshard(), emqx_ds:time()) ->
    ok
    | {error, overlaps_existing_generations | {no_schema, _}}.
add_generation(ShardId, Since) ->
    gen_server:call(?REF(ShardId), #call_add_generation{since = Since}, infinity).

-spec list_slabs(dbshard()) ->
    #{gen_id() => slab_info()} | emqx_ds:error(_).
list_slabs(ShardId) ->
    case gen_server:call(?REF(ShardId), #call_list_generations_with_lifetimes{}, infinity) of
        {error, {no_schema, _} = Err} ->
            ?err_rec(Err);
        Other ->
            Other
    end.

-spec drop_slab(dbshard(), gen_id()) -> ok | {error, _}.
drop_slab(ShardId, GenId) ->
    gen_server:call(?REF(ShardId), #call_drop_generation{gen_id = GenId}, infinity).

-spec find_generation(dbshard(), current | _At :: emqx_ds:time()) ->
    {gen_id(), slab_info()} | not_found.
find_generation(ShardId, current) ->
    GenId = generation_current(ShardId),
    GenData = #{} = generation_get(ShardId, GenId),
    {GenId, GenData};
find_generation(ShardId, AtTime) ->
    generation_at(ShardId, AtTime).

-spec shard_info(dbshard(), status) -> running | down.
shard_info(ShardId, status) ->
    case get_schema_runtime(ShardId) of
        #{} -> running;
        ?err_rec(_) -> down
    end.

-spec flush(dbshard() | emqx_ds:db()) -> ok | {error, _}.
flush(DB) when is_atom(DB) ->
    lists:foreach(
        fun(Shard) ->
            flush({DB, Shard})
        end,
        ls_shards(DB)
    );
flush(ShardId) ->
    gen_server:call(?REF(ShardId), #call_flush{}, infinity).

-spec take_snapshot(dbshard()) -> {ok, emqx_ds_storage_snapshot:reader()} | {error, _Reason}.
take_snapshot(ShardId) ->
    case gen_server:call(?REF(ShardId), #call_take_snapshot{}, infinity) of
        {ok, Dir} ->
            emqx_ds_storage_snapshot:new_reader(Dir);
        Error ->
            Error
    end.

-spec accept_snapshot(dbshard()) -> {ok, emqx_ds_storage_snapshot:writer()} | {error, _Reason}.
accept_snapshot(ShardId) ->
    ok = drop_shard(ShardId),
    handle_accept_snapshot(ShardId).

-doc "List shards of the DB".
-spec ls_shards(emqx_ds:db()) -> [emqx_ds:shard()].
ls_shards(DB) ->
    ShardMS = {n, l, {?MODULE, {DB, '$1'}}},
    MS = {{ShardMS, '_', '_'}, [], ['$1']},
    gproc:select({local, names}, [MS]).

%%================================================================================
%% gen_server for the shard
%%================================================================================

-doc """
Start the stroage server and ensure schema.
""".
-spec start_link(dbshard(), emqx_ds:create_db_opts()) ->
    {ok, pid()}.
start_link(ShardId, Options) ->
    Ret = {ok, _} = start_link_no_schema(ShardId, Options),
    ok = ensure_schema(ShardId, Options),
    Ret.

-doc """
Start the storage server.
""".
-spec start_link_no_schema(dbshard(), emqx_ds:create_db_opts()) -> {ok, pid()}.
start_link_no_schema(ShardId, Options = #{db_group := _}) ->
    gen_server:start_link(?REF(ShardId), ?MODULE, {ShardId, Options}, []).

-record(s_no_schema, {
    shard_id :: dbshard(),
    db :: rocksdb:db_handle(),
    cf_refs :: cf_refs()
}).

-record(s, {
    shard_id :: dbshard(),
    db :: rocksdb:db_handle(),
    db_opts :: emqx_ds:db_opts(),
    cf_refs :: cf_refs(),
    cf_need_flush :: gen_id(),
    schema :: shard_schema(),
    shard :: shard()
}).

-type server_state() :: #s{} | #s_no_schema{}.

-define(DEFAULT_CF, "default").
-define(DEFAULT_CF_OPTS, []).

init({ShardId, Options}) ->
    process_flag(trap_exit, true),
    ?tp(info, ds_storage_init, #{shard => ShardId}),
    logger:set_process_metadata(#{shard_id => ShardId, domain => [ds, storage_layer, shard]}),
    erase_schema_runtime(ShardId),
    clear_all_checkpoints(ShardId),
    {ok, DB, CFRefs} = rocksdb_open(ShardId, Options),
    case get_schema_persistent(DB) of
        not_found ->
            S = #s_no_schema{
                shard_id = ShardId,
                db = DB,
                cf_refs = CFRefs
            },
            {ok, S};
        Schema ->
            Shard = open_shard(ShardId, DB, CFRefs, Schema),
            CurrentGenId = maps:get(current_generation, Schema),
            S = #s{
                shard_id = ShardId,
                db = DB,
                db_opts = filter_layout_db_opts(Options),
                cf_refs = CFRefs,
                cf_need_flush = CurrentGenId,
                schema = Schema,
                shard = Shard
            },
            commit_metadata(S),
            ?tp(debug, ds_storage_init_state, #{shard => ShardId, s => S}),
            {ok, S, {continue, clean_orphans}}
    end.

handle_continue(
    clean_orphans,
    S = #s{shard_id = ShardId, db = DB, cf_refs = CFRefs, schema = Schema}
) ->
    %% Add / drop generation are not transactional.
    %% This means that the storage may contain "orphaned" column families, i.e.
    %% column families that do not belong to a live generation. We need to clean
    %% them, because an attempt to create existing column family is an error,
    %% therefore `add_generation/2` is not idempotent. Cleaning seems to be safe:
    %% either it's unfinished `handle_add_generation/2` meaning CFs are empty, or
    %% it's unfinished `handle_drop_generation/2` meaning CFs was meant to be
    %% dropped anyway.
    CFNames = maps:fold(
        fun
            (?GEN_KEY(_), #{cf_names := GenCFNames}, Acc) ->
                GenCFNames ++ Acc;
            (_Prop, _, Acc) ->
                Acc
        end,
        [],
        Schema
    ),
    OrphanedCFRefs = lists:foldl(fun proplists:delete/2, CFRefs, CFNames),
    case OrphanedCFRefs of
        [] ->
            {noreply, S};
        [_ | _] ->
            lists:foreach(
                fun({CFName, CFHandle}) ->
                    Result = rocksdb:drop_column_family(DB, CFHandle),
                    ?tp(
                        warning,
                        ds_storage_layer_dropped_orphaned_column_family,
                        #{
                            shard => ShardId,
                            orphan => CFName,
                            result => Result,
                            s => format_state(S)
                        }
                    )
                end,
                OrphanedCFRefs
            ),
            {noreply, S#s{cf_refs = CFRefs -- OrphanedCFRefs}}
    end.

format_status(Status) ->
    maps:map(
        fun
            (state, State) ->
                format_state(State);
            (_, Val) ->
                Val
        end,
        Status
    ).

handle_call(#call_has_schema{}, _From, S) ->
    Reply =
        case S of
            #s{} -> true;
            #s_no_schema{} -> false
        end,
    {reply, Reply, S};
handle_call(#call_ensure_schema{options = Opts}, _From, S0) ->
    case S0 of
        #s{} ->
            %% Already exists:
            {reply, ok, S0};
        #s_no_schema{shard_id = ShardId, db = DB, cf_refs = CFRefs} ->
            case handle_create_schema(ShardId, DB, CFRefs, Opts) of
                {ok, S} ->
                    {reply, ok, S};
                {error, _} = Err ->
                    {reply, Err, S0}
            end
    end;
handle_call(_Call, _From, S = #s_no_schema{shard_id = Shard}) ->
    Err = {error, {no_schema, Shard}},
    {reply, Err, S};
handle_call(#call_update_config_v0{since = Since, options = Options}, _From, S0) ->
    case handle_update_config_v0(S0, Since, Options) of
        S = #s{} ->
            commit_metadata(S),
            {reply, ok, S};
        Error = {error, _} ->
            {reply, Error, S0}
    end;
handle_call(#call_add_generation{since = Since, prototype = MaybePrototype}, _From, S0) ->
    case handle_add_generation(S0, Since, MaybePrototype) of
        S = #s{} ->
            commit_metadata(S),
            {reply, ok, S};
        Error = {error, _} ->
            {reply, Error, S0}
    end;
handle_call(#call_list_generations_with_lifetimes{}, _From, S) ->
    Generations = handle_list_generations_with_lifetimes(S),
    {reply, Generations, S};
handle_call(#call_drop_generation{gen_id = GenId}, _From, S0) ->
    {Reply, S} = handle_drop_generation(S0, GenId),
    {reply, Reply, S};
handle_call(#call_flush{}, _From, S0) ->
    {Reply, S} = handle_flush(S0),
    {reply, Reply, S};
handle_call(#call_take_snapshot{}, _From, S) ->
    Snapshot = handle_take_snapshot(S),
    {reply, Snapshot, S};
handle_call(#call_get_stats{}, _From, S) ->
    Reply =
        try
            handle_get_stats(S)
        catch
            EC:Err:Stack ->
                {EC, Err, Stack}
        end,
    {reply, Reply, S};
handle_call(_Call, _From, S) ->
    {reply, {error, unknown_call}, S}.

handle_cast(_Cast, S) ->
    {noreply, S}.

handle_info(_Info, S) ->
    {noreply, S}.

terminate(_Reason, S) ->
    case S of
        #s{db = DB, shard_id = ShardId} -> ok;
        #s_no_schema{db = DB, shard_id = ShardId} -> ok
    end,
    erase_schema_runtime(ShardId),
    ok = rocksdb:close(DB).

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

-spec clear_all_checkpoints(dbshard()) -> ok.
clear_all_checkpoints(ShardId) ->
    CheckpointBaseDir = checkpoints_dir(ShardId),
    ok = filelib:ensure_path(CheckpointBaseDir),
    {ok, AllFiles} = file:list_dir(CheckpointBaseDir),
    CheckpointDirs = [Dir || Dir <- AllFiles, filelib:is_dir(Dir)],
    lists:foreach(
        fun(Dir) ->
            logger:debug(#{
                msg => "ds_storage_deleting_previous_checkpoint",
                dir => Dir
            }),
            ok = file:del_dir_r(Dir)
        end,
        CheckpointDirs
    ).

-spec open_shard(dbshard(), rocksdb:db_handle(), cf_refs(), shard_schema() | #{}) ->
    shard().
open_shard(ShardId, DB, CFRefs, ShardSchema) ->
    %% Transform generation schemas to generation runtime data:
    Shard = maps:map(
        fun
            (?GEN_KEY(GenId), GenSchema) ->
                open_generation(ShardId, DB, CFRefs, GenId, GenSchema);
            (_K, Val) ->
                Val
        end,
        ShardSchema
    ),
    Shard#{db => DB}.

-spec handle_add_generation(server_state(), emqx_ds:time(), prototype() | undefined) ->
    server_state() | {error, overlaps_existing_generations}.
handle_add_generation(
    S0 = #s{
        shard_id = ShardId,
        db = DB,
        db_opts = DBOpts,
        schema = Schema0,
        shard = Shard0,
        cf_refs = CFRefs0
    },
    Since,
    MaybePrototype
) ->
    Schema1 = update_last_until(Schema0, Since),
    Shard1 = update_last_until(Shard0, Since),
    case Schema1 of
        _Updated = #{} ->
            {GenId, Schema, NewCFRefs} =
                new_generation(ShardId, DB, Schema1, MaybePrototype, Shard0, Since, DBOpts),
            CFRefs = NewCFRefs ++ CFRefs0,
            Key = ?GEN_KEY(GenId),
            Generation = open_generation(ShardId, DB, CFRefs, GenId, maps:get(Key, Schema)),
            Shard = Shard1#{current_generation := GenId, Key => Generation},
            S0#s{
                cf_refs = CFRefs,
                schema = Schema,
                shard = Shard
            };
        {error, exists} ->
            S0;
        {error, Reason} ->
            {error, Reason}
    end.

-spec handle_update_config_v0(server_state(), emqx_ds:time(), emqx_ds:create_db_opts()) ->
    server_state() | {error, overlaps_existing_generations}.
handle_update_config_v0(S0 = #s{schema = Schema}, Since, Options) ->
    Prototype = maps:get(storage, Options),
    S = S0#s{schema = Schema#{prototype := Prototype}},
    handle_add_generation(S, Since, undefined).

-spec handle_list_generations_with_lifetimes(server_state()) -> #{gen_id() => map()}.
handle_list_generations_with_lifetimes(#s{schema = ShardSchema}) ->
    maps:fold(
        fun
            (?GEN_KEY(GenId), GenSchema, Acc) ->
                Acc#{GenId => export_generation(GenSchema)};
            (_Key, _Value, Acc) ->
                Acc
        end,
        #{},
        ShardSchema
    ).

-spec export_generation(generation_schema()) -> map().
export_generation(GenSchema) ->
    maps:with([created_at, since, until], GenSchema).

-spec handle_drop_generation(server_state(), gen_id()) ->
    {ok | {error, current_generation}, server_state()}.
handle_drop_generation(#s{schema = #{current_generation := GenId}} = S0, GenId) ->
    {{error, current_generation}, S0};
handle_drop_generation(#s{schema = Schema} = S0, GenId) when
    not is_map_key(?GEN_KEY(GenId), Schema)
->
    {{error, not_found}, S0};
handle_drop_generation(S0, GenId) ->
    #s{
        shard_id = ShardId,
        db = DB,
        schema = #{?GEN_KEY(GenId) := GenSchema} = Schema0,
        shard = #{?GEN_KEY(GenId) := #{data := RuntimeData}} = Shard0,
        cf_refs = CFRefs0
    } = S0,
    %% 1. Commit the metadata first, so other functions are less
    %% likely to see stale data, and replicas don't end up
    %% inconsistent state, where generation's column families are
    %% absent, but its metadata is still present.
    %%
    %% Note: in theory, this operation may be interrupted in the
    %% middle. This will leave column families hanging.
    Shard = maps:remove(?GEN_KEY(GenId), Shard0),
    Schema = maps:remove(?GEN_KEY(GenId), Schema0),
    S1 = S0#s{
        shard = Shard,
        schema = Schema
    },
    commit_metadata(S1),
    %% 2. Now, actually drop the data from RocksDB:
    #{module := Mod, cf_names := GenCFNames} = GenSchema,
    GenCFRefs = [cf_ref(Name, CFRefs0) || Name <- GenCFNames],
    try
        Mod:drop(ShardId, DB, GenId, GenCFRefs, RuntimeData)
    catch
        EC:Err:Stack ->
            ?tp(
                error,
                ds_storage_layer_failed_to_drop_generation,
                #{
                    shard => ShardId,
                    EC => Err,
                    stacktrace => Stack,
                    generation => GenId,
                    s => format_state(S0)
                }
            )
    end,
    CFRefs = CFRefs0 -- GenCFRefs,
    S = S1#s{cf_refs = CFRefs},
    {ok, S}.

-spec open_generation(dbshard(), rocksdb:db_handle(), cf_refs(), gen_id(), generation_schema()) ->
    generation().
open_generation(ShardId, DB, CFRefs, GenId, GenSchema) ->
    ?tp(debug, ds_open_generation, #{gen_id => GenId, schema => GenSchema}),
    #{module := Mod, data := Schema} = GenSchema,
    RuntimeData = Mod:open(ShardId, DB, GenId, CFRefs, Schema),
    GenSchema#{data => RuntimeData}.

-spec handle_create_schema(
    dbshard(), rocksdb:db_handle(), cf_refs(), emqx_ds:create_db_opts()
) ->
    {ok, server_state()} | {error, _}.
handle_create_schema(ShardId, DB, CFRefs0, Options) ->
    maybe
        {ok, Schema, CFRefs} ?= create_new_shard_schema(ShardId, DB, CFRefs0, Options),
        CurrentGenId = maps:get(current_generation, Schema),
        Shard = open_shard(ShardId, DB, CFRefs, Schema),
        S = #s{
            shard_id = ShardId,
            db = DB,
            db_opts = filter_layout_db_opts(Options),
            cf_refs = CFRefs,
            cf_need_flush = CurrentGenId,
            schema = Schema,
            shard = Shard
        },
        commit_metadata(S),
        {ok, S}
    end.

-spec create_new_shard_schema(dbshard(), rocksdb:db_handle(), cf_refs(), emqx_ds:create_db_opts()) ->
    {ok, shard_schema(), cf_refs()} | {error, {bad_options, _}}.
create_new_shard_schema(
    ShardId, DB, CFRefs, Options = #{storage := Prototype, payload_type := PType}
) ->
    ?tp(notice, ds_create_new_shard_schema, #{
        shard => ShardId, prototype => Prototype, payload_transform => PType
    }),
    %% TODO: allow customization of the ptrans schema
    Schema0 = #{
        current_generation => 0,
        prototype => Prototype,
        ptrans => emqx_ds_payload_transform:default_schema(PType)
    },
    DBOpts = filter_layout_db_opts(Options),
    {_NewGenId, Schema, NewCFRefs} =
        new_generation(ShardId, DB, Schema0, Prototype, undefined, _Since = 0, DBOpts),
    {ok, Schema, NewCFRefs ++ CFRefs};
create_new_shard_schema(_ShardId, _DB, _CFRefs, Options) ->
    {error, {bad_options, Options}}.

-spec new_generation(
    dbshard(),
    rocksdb:db_handle(),
    shard_schema(),
    prototype() | undefined,
    shard() | undefined,
    emqx_ds:time(),
    emqx_ds:db_opts()
) ->
    {gen_id(), shard_schema(), cf_refs()}.
new_generation(ShardId, DB, Schema0, MaybePrototype, Shard0, Since, DBOpts) ->
    #{current_generation := PrevGenId, prototype := OldPrototype} = Schema0,
    {Mod, ModConf} =
        case MaybePrototype of
            undefined ->
                %% This behavior is kept for compatibility with
                %% version 0 of raft state machine:
                OldPrototype;
            {_, _} ->
                MaybePrototype
        end,
    PTrans = maps:get(ptrans, Schema0, ?ds_pt_ttv),
    case Shard0 of
        #{?GEN_KEY(PrevGenId) := #{module := Mod} = PrevGen} ->
            %% When the new generation's module is the same as the last one, we might want
            %% to perform actions like inheriting some of the previous (meta)data.
            PrevRuntimeData = maps:get(data, PrevGen);
        _ ->
            PrevRuntimeData = undefined
    end,
    %% Provide a small subset of DB options to the storage layout module.
    GenId = next_generation_id(PrevGenId),
    {GenData, NewCFRefs} = Mod:create(ShardId, DB, GenId, ModConf, PrevRuntimeData, DBOpts),
    GenSchema = #{
        module => Mod,
        data => GenData,
        cf_names => cf_names(NewCFRefs),
        created_at => Since,
        ptrans => PTrans,
        since => Since,
        until => undefined
    },
    Schema = Schema0#{
        current_generation => GenId,
        ?GEN_KEY(GenId) => GenSchema
    },
    {GenId, Schema, NewCFRefs}.

-spec next_generation_id(gen_id()) -> gen_id().
next_generation_id(GenId) ->
    GenId + 1.

%% @doc Commit current state of the server to both rocksdb and the persistent term
-spec commit_metadata(server_state()) -> ok.
commit_metadata(#s{shard_id = ShardId, schema = Schema, shard = Runtime, db = DB}) ->
    ok = put_schema_persistent(DB, Schema),
    put_schema_runtime(ShardId, Runtime).

-spec rocksdb_open(dbshard(), options()) ->
    {ok, rocksdb:db_handle(), cf_refs()} | {error, _TODO}.
rocksdb_open(Shard, Options) ->
    Defaults = #{
        cache_size => 8 * ?MB,
        max_open_files => 100,
        allow_fallocate => false,
        misc_options => []
    },
    #{
        cache_size := CacheSize,
        max_open_files := MaxOpenFiles,
        allow_fallocate := AllowFallocate,
        misc_options := MiscOptions
    } = maps:merge(Defaults, maps:get(rocksdb, Options, #{})),
    #db_group{
        env = Env,
        sst_file_mgr = SstFileManager,
        write_buffer_mgr = WriteBufManager
    } = maps:get(db_group, Options),
    DBOptions = [
        {create_if_missing, true},
        {create_missing_column_families, true},
        %% Group settings:
        {env, Env},
        {sst_file_manager, SstFileManager},
        {write_buffer_manager, WriteBufManager},
        %% Info log file management:
        %%    Maximal log files:
        {keep_log_file_num, 10},
        %%    Create a new log file when the old one exceeds:
        %%    `max_log_file_size' (1MB):
        {max_log_file_size, 16#100000},
        %% NOTE
        %% With WAL-less writes, it's important to have CFs flushed atomically.
        %% For example, bitfield-lts backend needs data + trie CFs to be consistent.
        {atomic_flush, true},
        {enable_write_thread_adaptive_yield, false},
        {cache_size, CacheSize},
        {max_open_files, MaxOpenFiles},
        {allow_fallocate, AllowFallocate}
        | MiscOptions
    ],
    DBDir = db_dir(Shard),
    _ = filelib:ensure_dir(DBDir),
    ExistingCFs =
        case rocksdb:list_column_families(DBDir, DBOptions) of
            {ok, CFs} ->
                [{Name, []} || Name <- CFs, Name /= ?DEFAULT_CF];
            % DB is not present. First start
            {error, {db_open, _}} ->
                []
        end,
    ColumnFamilies = [
        {?DEFAULT_CF, ?DEFAULT_CF_OPTS}
        | ExistingCFs
    ],
    case rocksdb:open(DBDir, DBOptions, ColumnFamilies) of
        {ok, DBHandle, [_CFDefault | CFRefs]} ->
            {CFNames, _} = lists:unzip(ExistingCFs),
            {ok, DBHandle, lists:zip(CFNames, CFRefs)};
        Error ->
            Error
    end.

-spec base_dir() -> file:filename().
base_dir() ->
    application:get_env(?APP, db_data_dir, filename:join(emqx:data_dir(), "ds")).

-spec db_dir(dbshard()) -> file:filename().
db_dir({DB, ShardId}) ->
    filename:join([base_dir(), DB, shard_dir(ShardId)]).

-spec checkpoints_dir(dbshard()) -> file:filename().
checkpoints_dir({DB, ShardId}) ->
    filename:join([base_dir(), DB, "checkpoints", shard_dir(ShardId)]).

-spec checkpoint_dir(dbshard(), _Name :: file:name()) -> file:filename().
checkpoint_dir(ShardId, Name) ->
    filename:join([checkpoints_dir(ShardId), Name]).

shard_dir(ShardId) ->
    [$s | binary_to_list(ShardId)].

-spec update_last_until(Schema, emqx_ds:time()) ->
    Schema | {error, exists | overlaps_existing_generations}
when
    Schema :: shard_schema() | shard().
update_last_until(Schema = #{current_generation := GenId}, Until) ->
    case maps:get(?GEN_KEY(GenId), Schema) of
        GenData = #{since := CurrentSince} when CurrentSince < Until ->
            Schema#{?GEN_KEY(GenId) := GenData#{until := Until}};
        #{since := Until} ->
            {error, exists};
        #{since := CurrentSince} when CurrentSince > Until ->
            {error, overlaps_existing_generations}
    end.

handle_flush(S = #s{db = DB, cf_refs = CFRefs, cf_need_flush = NeedFlushGenId, shard = Shard}) ->
    %% NOTE
    %% There could have been few generations added since the last time `flush/1` was
    %% called. Strictly speaking, we don't need to flush them all at once as part of
    %% a single atomic flush, but the error handling is a bit easier this way.
    CurrentGenId = maps:get(current_generation, Shard),
    GenIds = lists:seq(NeedFlushGenId, CurrentGenId),
    CFHandles = lists:flatmap(
        fun(GenId) ->
            case Shard of
                #{?GEN_KEY(GenId) := #{cf_names := CFNames}} ->
                    [cf_handle(N, CFRefs) || N <- CFNames];
                #{} ->
                    %% Generation was probably dropped.
                    []
            end
        end,
        GenIds
    ),
    case rocksdb:flush(DB, CFHandles, [{wait, true}]) of
        ok ->
            %% Current generation will always need a flush.
            ?tp(ds_storage_flush_complete, #{gens => GenIds, cfs => CFHandles}),
            {ok, S#s{cf_need_flush = CurrentGenId}};
        {error, _} = Error ->
            {Error, S}
    end.

handle_take_snapshot(#s{db = DB, shard_id = ShardId}) ->
    Name = integer_to_list(erlang:system_time(millisecond)),
    Dir = checkpoint_dir(ShardId, Name),
    _ = filelib:ensure_dir(Dir),
    case rocksdb:checkpoint(DB, Dir) of
        ok ->
            {ok, Dir};
        {error, _} = Error ->
            Error
    end.

handle_accept_snapshot(ShardId) ->
    Dir = db_dir(ShardId),
    emqx_ds_storage_snapshot:new_writer(Dir).

handle_get_stats(#s{db = DB}) ->
    Unwrap = fun
        ({ok, A}) -> A;
        (A) -> A
    end,
    #{
        block_cache_usage => Unwrap(rocksdb:get_property(DB, <<"rocksdb.block-cache-usage">>)),
        cur_size_all_memtables => Unwrap(
            rocksdb:get_property(DB, <<"rocksdb.cur-size-all-mem-tables">>)
        ),
        table_readers_mem => Unwrap(
            rocksdb:get_property(DB, <<"rocksdb.estimate-table-readers-mem">>)
        ),
        block_cache_pinned_usage => Unwrap(
            rocksdb:get_property(DB, <<"rocksdb.block-cache-pinned-usage">>)
        )
    }.

-spec handle_event(dbshard(), emqx_ds:time(), Event) -> [Event].
handle_event(Shard, Time, ?storage_event(GenId, Event)) ->
    case generation_get(Shard, GenId) of
        not_found ->
            [];
        #{module := Mod, data := GenData} ->
            case erlang:function_exported(Mod, handle_event, 4) of
                true ->
                    NewEvents = Mod:handle_event(Shard, GenData, Time, Event),
                    [?mk_storage_event(GenId, E) || E <- NewEvents];
                false ->
                    []
            end
    end;
handle_event(Shard, Time, Event) ->
    GenId = generation_current(Shard),
    handle_event(Shard, Time, ?mk_storage_event(GenId, Event)).

filter_layout_db_opts(Options) ->
    maps:with(?STORAGE_LAYOUT_DB_OPTS, Options).

%%--------------------------------------------------------------------------------

-spec cf_names(cf_refs()) -> [string()].
cf_names(CFRefs) ->
    {CFNames, _CFHandles} = lists:unzip(CFRefs),
    CFNames.

-spec cf_ref(_Name :: string(), cf_refs()) -> cf_ref().
cf_ref(Name, CFRefs) ->
    lists:keyfind(Name, 1, CFRefs).

-spec cf_handle(_Name :: string(), cf_refs()) -> rocksdb:cf_handle().
cf_handle(Name, CFRefs) ->
    element(2, cf_ref(Name, CFRefs)).

%%--------------------------------------------------------------------------------
%% Schema access
%%--------------------------------------------------------------------------------

-spec generation_current(dbshard()) -> gen_id().
generation_current(Shard) ->
    #{current_generation := Current} = get_schema_runtime(Shard),
    Current.

%% TODO: remove me
-spec generation_at(dbshard(), emqx_ds:time()) -> {gen_id(), generation()} | not_found.
generation_at(Shard, Time) ->
    Schema = #{current_generation := Current} = get_schema_runtime(Shard),
    generation_at(Time, Current, Schema).

generation_at(Time, GenId, Schema) ->
    case Schema of
        #{?GEN_KEY(GenId) := Gen} ->
            case Gen of
                #{since := Since} when Time < Since andalso GenId > 0 ->
                    generation_at(Time, GenId - 1, Schema);
                _ ->
                    {GenId, Gen}
            end;
        _ ->
            not_found
    end.

-spec generation_get(dbshard(), gen_id()) -> generation() | not_found.
generation_get(Shard, GenId) ->
    case get_schema_runtime(Shard) of
        #{?GEN_KEY(GenId) := GenData} ->
            GenData;
        #{} ->
            not_found
    end.

-spec generations_since(dbshard(), emqx_ds:time()) -> {ok, [gen_id()]} | emqx_ds:error(_).
generations_since(Shard, Since) ->
    maybe
        Schema = #{current_generation := Current} ?= get_schema_runtime(Shard),
        {ok, list_generations_since(Schema, Current, Since)}
    end.

list_generations_since(Schema, GenId, Since) ->
    case Schema of
        #{?GEN_KEY(GenId) := #{until := Until}} when Until > Since ->
            [GenId | list_generations_since(Schema, GenId - 1, Since)];
        #{} ->
            %% No more live generations.
            []
    end.

format_state(S = #s_no_schema{}) ->
    ?record_to_map(s_no_schema, S);
format_state(S = #s{shard = Shard0}) ->
    Shard = maps:map(
        fun
            (?GEN_KEY(_), _Schema) ->
                '...';
            (_K, Val) ->
                Val
        end,
        Shard0
    ),
    ?record_to_map(s, S#s{shard = Shard}).

-define(PERSISTENT_TERM(SHARD), {emqx_ds_storage_layer, SHARD}).

-spec get_schema_runtime(dbshard()) -> shard() | emqx_ds:error(_).
get_schema_runtime(Shard = {_, _}) ->
    persistent_term:get(?PERSISTENT_TERM(Shard), ?err_rec(shard_unavailable)).

-spec get_stats(emqx_ds:db()) -> map().
get_stats(DB) ->
    maps:from_list(
        [
            {Shard, gen_server:call(?REF({DB, Shard}), #call_get_stats{})}
         || Shard <- ls_shards(DB)
        ]
    ).

-spec db_group_stats(emqx_ds:db_group(), db_group()) -> {ok, emqx_ds:db_group_stats()}.
db_group_stats(_GroupId, #db_group{sst_file_mgr = SSTFM, write_buffer_mgr = WBM}) ->
    Stats1 = sstfm_info(SSTFM),
    Stats = Stats1#{write_buffer_manager => rocksdb:write_buffer_manager_info(WBM)},
    {ok, Stats}.

-doc """
Format server state for debugging and inspection.
""".
print_state(DBShard) ->
    format_state(sys:get_state(?REF(DBShard))).

-spec put_schema_runtime(dbshard(), shard()) -> ok.
put_schema_runtime(Shard = {_, _}, RuntimeSchema) ->
    persistent_term:put(?PERSISTENT_TERM(Shard), RuntimeSchema),
    ok.

-spec erase_schema_runtime(dbshard()) -> ok.
erase_schema_runtime(Shard) ->
    persistent_term:erase(?PERSISTENT_TERM(Shard)),
    ok.

-undef(PERSISTENT_TERM).

-define(ROCKSDB_SCHEMA_KEY(V), <<"schema_", V>>).

-define(ROCKSDB_SCHEMA_KEY, ?ROCKSDB_SCHEMA_KEY("v2")).
-define(ROCKSDB_SCHEMA_KEYS, [
    ?ROCKSDB_SCHEMA_KEY,
    ?ROCKSDB_SCHEMA_KEY("v1")
]).

-spec get_schema_persistent(rocksdb:db_handle()) -> shard_schema() | not_found.
get_schema_persistent(DB) ->
    get_schema_persistent(DB, ?ROCKSDB_SCHEMA_KEYS).

get_schema_persistent(DB, [Key | Rest]) ->
    case rocksdb:get(DB, Key, []) of
        {ok, Blob} ->
            deserialize_schema(Key, Blob);
        not_found ->
            get_schema_persistent(DB, Rest)
    end;
get_schema_persistent(_DB, []) ->
    not_found.

-spec put_schema_persistent(rocksdb:db_handle(), shard_schema()) -> ok.
put_schema_persistent(DB, Schema) ->
    Blob = term_to_binary(Schema),
    rocksdb:put(DB, ?ROCKSDB_SCHEMA_KEY, Blob, []).

-spec deserialize_schema(_SchemaVsn :: binary(), binary()) -> shard_schema().
deserialize_schema(SchemaVsn, Blob) ->
    %% Sanity check:
    Schema = #{current_generation := _, prototype := _} = binary_to_term(Blob),
    decode_schema(SchemaVsn, Schema).

decode_schema(?ROCKSDB_SCHEMA_KEY, Schema) ->
    Schema;
decode_schema(?ROCKSDB_SCHEMA_KEY("v1"), Schema) ->
    maps:map(fun decode_schema_v1/2, Schema).

decode_schema_v1(?GEN_KEY(_), Generation = #{}) ->
    decode_generation_schema_v1(Generation);
decode_schema_v1(_, V) ->
    V.

decode_generation_schema_v1(SchemaV1 = #{cf_refs := CFRefs}) ->
    %% Drop potentially dead CF references from the time generation was created.
    Schema = maps:remove(cf_refs, SchemaV1),
    Schema#{cf_names => cf_names(CFRefs)};
decode_generation_schema_v1(Schema = #{}) ->
    Schema.

verify_db_group_config(UserOpts) ->
    maybe
        true ?= is_map(UserOpts),
        NDIOS = erlang:system_info(dirty_io_schedulers),
        Defaults = #{
            storage_quota => infinity,
            write_buffer_size => 256 * 1024 * 1024,
            rocksdb_nthreads_high => NDIOS,
            rocksdb_nthreads_low => NDIOS
        },
        #{
            storage_quota := SQ,
            write_buffer_size := WBS,
            rocksdb_nthreads_high := BTHP,
            rocksdb_nthreads_low := BTLP
        } = Merged = maps:merge(Defaults, UserOpts),
        true ?= is_valid_max_size(storage_quota, SQ),
        true ?= is_valid_max_size(write_buffer_size, WBS),
        true ?= is_valid_n_threads(rocksdb_nthreads_high, BTHP),
        true ?= is_valid_n_threads(rocksdb_nthreads_low, BTLP),
        {ok, Merged}
    else
        {error, _} = Err ->
            Err;
        _ ->
            {error, badarg}
    end.

sstfm_info(SSTFM) ->
    {TotalSize, L} =
        lists:foldl(
            fun
                ({total_size, TS}, {_, Acc}) ->
                    {TS, Acc};
                ({Key, Val}, {TS, Acc}) ->
                    {TS, [{Key, Val} | Acc]}
            end,
            {undefined, []},
            rocksdb:sst_file_manager_info(SSTFM)
        ),
    #{
        disk_usage => TotalSize,
        sst_file_mgr => L
    }.

is_valid_max_size(_, infinity) ->
    true;
is_valid_max_size(_, Val) when is_integer(Val), Val > 0 ->
    true;
is_valid_max_size(Key, Val) ->
    {error, {badarg, Key, Val}}.

is_valid_n_threads(_, N) when is_integer(N), N > 1 ->
    true;
is_valid_n_threads(Key, N) ->
    {error, {badarg, Key, N}}.

%%--------------------------------------------------------------------------------

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

decode_schema_v1_test() ->
    SchemaV1 = #{
        current_generation => 42,
        prototype => {emqx_ds_storage_reference, #{}},
        ?GEN_KEY(41) => #{
            module => emqx_ds_storage_reference,
            data => {schema},
            cf_refs => [{"emqx_ds_storage_reference41", erlang:make_ref()}],
            created_at => 12345,
            since => 0,
            until => 123456
        },
        ?GEN_KEY(42) => #{
            module => emqx_ds_storage_reference,
            data => {schema},
            cf_refs => [{"emqx_ds_storage_reference42", erlang:make_ref()}],
            created_at => 54321,
            since => 123456,
            until => undefined
        }
    },
    ?assertEqual(
        #{
            current_generation => 42,
            prototype => {emqx_ds_storage_reference, #{}},
            ?GEN_KEY(41) => #{
                module => emqx_ds_storage_reference,
                data => {schema},
                cf_names => ["emqx_ds_storage_reference41"],
                created_at => 12345,
                since => 0,
                until => 123456
            },
            ?GEN_KEY(42) => #{
                module => emqx_ds_storage_reference,
                data => {schema},
                cf_names => ["emqx_ds_storage_reference42"],
                created_at => 54321,
                since => 123456,
                until => undefined
            }
        },
        deserialize_schema(?ROCKSDB_SCHEMA_KEY("v1"), term_to_binary(SchemaV1))
    ).

-endif.

-undef(ROCKSDB_SCHEMA_KEY).
