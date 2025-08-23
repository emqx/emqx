%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dsch).
-moduledoc """
# Durable storage schema manager

This module implements a node-local persistent storage for the database schemas,
as well as a tracking mechanism for open databases and cluster state.
It is designed an an alternative to the Mnesia schema.

Responsibilities of this module include:

- Registration of backends.

- Tracking lifetime of durable storages: creation, opening, closing and dropping

- Keeping and safely mutating schema of the durable storage databases

- Keeping runtime data (configuration, global variables)
  and disposing it when the DB is closed.

## Backends

When DS backends start they must register themselves in this module.
Registration involves passing the callback module, that will be used
as a target for dynamic dispatching of `emqx_ds` API calls towards the
backend.

## Schema, runtime config and gvars

This module separates state of the durable storage in three parts:

- *DB schema*: a permanent, immutable state.
  It's stored both on disk and is mirrored in a `persistent_term`.

- *Runtime state*: a set of configuration constants that is set when DB is opened,
  and can be modified in the runtime using `update_db_config` API.

  Runtime state includes the callback module of the backend used by
  the durable storage, and a small amount of configuration data.

  This state is not saved.

- *Gvars* (global variables): this module also creates an ETS table
  that the backend can use to store frequently changing information
  about the DB. Gvars are not saved and are erased when DB is closed.

## Implementation

`disk_log` is used as a persistence mechanism.
""".

-behavior(gen_server).

%% API:
-export([
    this_site/0,
    get_site_schema/0,
    set_cluster/1,

    register_backend/2,

    %% DB API:
    ensure_db_schema/2,
    get_db_schema/1,
    drop_db_schema/1,

    open_db/2,
    close_db/1,
    update_db_config/2,
    get_db_runtime/1,
    db_gvars/1
]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([
    start_link/0,
    schema_file/0,
    restore_from_wal/1,
    dump/2
]).

-export_type([
    wal/0,
    schema/0,
    dbshard/0,
    db_schema/0,
    db_runtime/0,
    db_runtime_config/0
]).

-include("emqx_ds.hrl").
-include("emqx_dsch.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%%--------------------------------------------------------------------------------
%% Schema
%%--------------------------------------------------------------------------------

-type site() :: binary().

-type cluster() :: binary().

-type peer_state() :: active.

-define(empty, empty).

-doc """
Schema is an immutable term associated with the DS DB.

It is unaffected by `update_config` operation.
""".
-type db_schema() :: #{backend := emqx_ds:backend(), atom() => _}.

%% %% Pending command:
%% -record(pending, {
%%     ref :: reference(),
%%     time :: integer(),
%%     target :: emqx_ds:db() | dbshard(),
%%     command :: atom(),
%%     args :: map()
%% }).
%% -type pending() :: #pending{}.

-doc """
Global schema of the site.

It encapsulates schema of all DBs and shards, as well as pending
operations and other metadata.
""".
-type schema() :: #{
    ver := 1,
    site := site(),
    cluster := cluster() | undefined,
    peers := [site()],
    dbs := #{emqx_ds:db() => db_schema()}
}.

%% Schema operations:
%%    Create a new site and schema:
-record(sop_init, {ver = 1 :: 1, id :: site()}).
%%%   Operation with the cluster
-record(sop_set_cluster, {cluster :: cluster() | undefined}).
-record(sop_set_peer, {peer :: site(), state :: peer_state()}).
-record(sop_delete_peer, {peer :: site()}).
%%    Set DB schema:
-record(sop_set_db, {db :: emqx_ds:db(), schema :: db_schema()}).
-record(sop_drop_db, {db :: emqx_ds:db()}).

-doc """
A type of WAL entries.
""".
-type schema_op() ::
    #sop_init{}
    | #sop_set_cluster{}
    | #sop_set_db{}
    | #sop_drop_db{}
    | #sop_set_peer{}
    | #sop_delete_peer{}.

%%--------------------------------------------------------------------------------
%% Server state and misc. types
%%--------------------------------------------------------------------------------

-define(schema, "ds_schema").

-doc """
Name of the disk log, as in `disk_log:open([{name, Name}, ...])`
""".
-type wal() :: term().

-type dbshard() :: {emqx_ds:db(), emqx_ds:shard()}.

-type db_runtime_config() :: #{atom() => _}.

-type db_runtime() :: #{
    cbm := module(),
    gvars := ets:tid(),
    runtime := db_runtime_config()
}.

-define(log_new, emqx_ds_dbschema_new).
-define(log_current, emqx_ds_dbschema_current).

%% Calls:
-record(call_register_backend, {alias :: atom(), cbm :: module()}).
-record(call_ensure_db_schema, {db :: emqx_ds:db(), schema :: db_schema()}).
-record(call_open_db, {db :: emqx_ds:db(), conf :: db_runtime_config()}).
-record(call_close_db, {db :: emqx_ds:db()}).
-record(call_update_db_config, {db :: emqx_ds:db(), conf :: db_runtime_config()}).

-define(SERVER, ?MODULE).

-doc """
Backend registration.

This record is ephemeral and is lost on restart.
Backends should re-register themselves on restart of DS application.
""".
-record(bs, {
    cbm :: module()
}).
-type bs() :: #bs{}.

-doc "State of an open DB used internally by the server.".
-record(dbs, {
    rtconf :: db_runtime_config(),
    gvars :: ets:tid()
}).
-type dbs() :: #dbs{}.

-doc "Server's internal state.".
-record(s, {
    sch :: schema(),
    %% Backend registrations:
    backends = #{} :: #{atom() => bs()},
    %% Transient DB and shard configuration:
    dbs = #{} :: #{emqx_ds:db() => dbs()}
}).
-type s() :: #s{}.

%%================================================================================
%% API functions
%%================================================================================

-spec this_site() -> binary().
this_site() ->
    #{site := Site} = get_site_schema(),
    Site.

-doc """
Get the entire schema of the site.
""".
-spec get_site_schema() -> schema() | undefined.
get_site_schema() ->
    persistent_term:get(?dsch_pt_schema).

-doc """
Set or erase cluster ID.

Cluster ID isn't used by this module directly, but it's stored in the
schema anyway, because most backends likely want to make sure that
they share data and communicate only with the nodes that belong to the
same cluster.
""".
-spec set_cluster(cluster() | undefined) -> ok | {error, badarg}.
set_cluster(Cluster) when is_binary(Cluster); Cluster =:= undefined ->
    gen_server:call(?SERVER, #sop_set_cluster{cluster = Cluster});
set_cluster(_) ->
    {error, badarg}.

-spec get_db_schema(emqx_ds:db()) -> db_schema() | undefined.
get_db_schema(DB) ->
    maybe
        #{dbs := DBs} ?= get_site_schema(),
        #{DB := DBSchema} ?= DBs,
        DBSchema
    else
        _ -> undefined
    end.

-spec register_backend(atom(), module()) -> ok | {error, _}.
register_backend(Alias, CBM) when is_atom(Alias), is_atom(CBM) ->
    gen_server:call(?SERVER, #call_register_backend{alias = Alias, cbm = CBM}).

-doc """
If database schema wasn't present before, create schema it (equal to the
second argument of the function).

If database schema is present and backend matches the supplied one,
return the original schema.

Return an error otherwise.
""".
-spec ensure_db_schema(emqx_ds:db(), db_schema()) -> {ok, db_schema()} | {error, _}.
ensure_db_schema(DB, Schema = #{backend := _}) ->
    gen_server:call(?SERVER, #call_ensure_db_schema{db = DB, schema = Schema}).

-spec drop_db_schema(emqx_ds:db()) -> ok | {error, _}.
drop_db_schema(DB) ->
    gen_server:call(?SERVER, #sop_drop_db{db = DB}).

-spec open_db(emqx_ds:db(), db_runtime_config()) -> ok | {error, _}.
open_db(DB, RuntimeConfig) ->
    gen_server:call(?SERVER, #call_open_db{db = DB, conf = RuntimeConfig}).

-spec close_db(emqx_ds:db()) -> ok.
close_db(DB) ->
    gen_server:call(?SERVER, #call_close_db{db = DB}).

-doc """
Update runtime configuration of an open DB.
Configurations are merged using `emqx_utils_maps:deep_merge` function.
""".
-spec update_db_config(emqx_ds:db(), db_runtime_config()) -> ok | {error, _}.
update_db_config(DB, Config) ->
    gen_server:call(?SERVER, #call_update_db_config{db = DB, conf = Config}).

-doc """
Get data about an open DB, including schema, backend callback module
and runtime config.
""".
-spec get_db_runtime(emqx_ds:db()) -> db_runtime() | undefined.
get_db_runtime(DB) ->
    persistent_term:get(?dsch_pt_db_runtime(DB), undefined).

-doc """
Get an ETS table containing global variables of the DB.
""".
-spec db_gvars(emqx_ds:db()) -> ets:tid().
db_gvars(DB) ->
    #{gvars := ETS} = persistent_term:get(?dsch_pt_db_runtime(DB)),
    ETS.

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-doc """
Re-create the schema state by reading the WAL.

WAL should be opened using `disk_log:open(...)`.
""".
-spec restore_from_wal(wal()) -> schema() | ?empty.
restore_from_wal(Log) ->
    replay_wal(Log, ?empty, start).

-doc """
Return location of the node's schema file.
(For debugging and troubleshooting).
""".
-spec schema_file() -> file:filename().
schema_file() ->
    filename:join(emqx_ds_storage_layer:base_dir(), ?schema).

-doc """
Dump schema to a WAL.

The WAL should be opened and empty.
""".
-spec dump(schema(), wal()) -> ok | {error, _}.
dump(#{ver := Ver, site := Site, cluster := Cluster, dbs := DBs}, WAL) ->
    Ops =
        [#sop_init{ver = Ver, id = Site}] ++
            [#sop_set_cluster{cluster = Cluster} || is_binary(Cluster)] ++
            [#sop_set_db{db = DB, schema = DBSchema} || {DB, DBSchema} <- maps:to_list(DBs)],
    case safe_add_l(WAL, Ops, ?empty) of
        {ok, _} ->
            ok;
        {error, _} = Err ->
            Err
    end.

%%================================================================================
%% behavior callbacks
%%================================================================================

init(_) ->
    process_flag(trap_exit, true),
    Schema = #{} = restore_or_init_schema(),
    persistent_term:put(?dsch_pt_schema, Schema),
    S = #s{sch = Schema},
    {ok, S}.

handle_call(#call_open_db{db = DB, conf = RuntimeConf}, _From, S0) ->
    case do_open_db(DB, RuntimeConf, S0) of
        {ok, S} -> {reply, ok, S};
        {error, _} = Err -> {reply, Err, S0}
    end;
handle_call(#call_update_db_config{db = DB, conf = NewConf}, _From, S0) ->
    case do_update_db_config(DB, NewConf, S0) of
        {ok, S} -> {reply, ok, S};
        {error, _} = Err -> {reply, Err, S0}
    end;
handle_call(#call_close_db{db = DB}, _From, S0) ->
    {reply, ok, do_close_db(DB, S0)};
handle_call(#call_ensure_db_schema{db = DB, schema = NewDBSchema}, _From, S) ->
    do_ensure_db_schema(DB, NewDBSchema, S);
handle_call(#sop_drop_db{db = DB}, _From, S) ->
    do_drop_db(DB, S);
handle_call(#call_register_backend{alias = Alias, cbm = CBM}, _From, S) ->
    do_register_backend(Alias, CBM, S);
handle_call(#sop_set_cluster{cluster = Cluster}, _From, S) ->
    do_set_cluster(Cluster, S);
handle_call(Call, From, S) ->
    ?tp(error, emqx_dsch_unkown_call, #{from => From, call => Call, state => S}),
    {reply, {error, unknown_call}, S}.

handle_cast(Cast, S) ->
    ?tp(error, emqx_dsch_unkown_cast, #{call => Cast, state => S}),
    {noreply, S}.

handle_info({'EXIT', _, shutdown}, S) ->
    {stop, shutdown, S};
handle_info(_Info, S) ->
    {noreply, S}.

terminate(Reason, S0 = #s{dbs = DBs}) ->
    %% Close all DBs:
    _ = maps:fold(
        fun(DB, _, S) ->
            do_close_db(DB, S)
        end,
        S0,
        DBs
    ),
    terminate(Reason, undefined);
terminate(_Reason, undefined) ->
    persistent_term:erase(?dsch_pt_schema),
    _ = disk_log:close(?log_current),
    _ = disk_log:close(?log_new),
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

-spec do_open_db(emqx_ds:db(), db_runtime_config(), s()) -> {ok, s()} | {error, _}.
do_open_db(DB, RuntimeConf, S0 = #s{dbs = DBs}) ->
    maybe
        false ?= maps:is_key(DB, DBs) andalso
            {error, already_open},
        {ok, DBSchema} ?= lookup_db_schema(DB, S0),
        #{backend := Backend} = DBSchema,
        {ok, CBM} ?= lookup_backend_cbm(Backend, S0),
        GVars = ets:new(db_gvars, [
            set, public, {read_concurrency, true}, {write_concurrency, false}
        ]),
        S = S0#s{
            dbs = DBs#{
                DB => #dbs{
                    rtconf = RuntimeConf,
                    gvars = GVars
                }
            }
        },
        set_db_runtime(DB, CBM, GVars, RuntimeConf),
        {ok, S}
    end.

-spec do_update_db_config(emqx_ds:db(), db_runtime_config(), s()) -> {ok, s()} | {error, _}.
do_update_db_config(DB, NewConf, S0 = #s{dbs = DBs}) ->
    maybe
        #{DB := DBstate0 = #dbs{rtconf = OldConf, gvars = GVars}} ?= DBs,
        {ok, DBSchema} ?= lookup_db_schema(DB, S0),
        #{backend := Backend} = DBSchema,
        {ok, CBM} ?= lookup_backend_cbm(Backend, S0),
        MergedConf = emqx_utils_maps:deep_merge(OldConf, NewConf),
        DBstate = DBstate0#dbs{rtconf = MergedConf},
        S = S0#s{
            dbs = DBs#{DB := DBstate}
        },
        set_db_runtime(DB, CBM, GVars, MergedConf),
        {ok, S}
    else
        #{} ->
            {error, {database_is_not_open, DB}}
    end.

-spec do_close_db(emqx_ds:db(), s()) -> s().
do_close_db(DB, S = #s{dbs = DBs}) ->
    case DBs of
        #{DB := #dbs{gvars = GVars}} ->
            erase_db_consts(DB),
            ets:delete(GVars),
            S#s{
                dbs = maps:remove(DB, DBs)
            };
        #{} ->
            S
    end.

-spec do_register_backend(emqx_ds:backend(), module(), s()) -> {reply, ok | {error, _}, s()}.
do_register_backend(Alias, CBM, S = #s{backends = B0}) ->
    case B0 of
        #{Alias := #bs{cbm = CBM}} ->
            {reply, ok, S};
        #{Alias := Other} ->
            Err = {error, {conflict, Other}},
            {reply, Err, S};
        #{} ->
            BS = #bs{cbm = CBM},
            {reply, ok, S#s{backends = B0#{Alias => BS}}}
    end.

-spec do_ensure_db_schema(emqx_ds:db(), db_schema(), s()) ->
    {reply, {ok, db_schema()} | {error, _}, s()}.
do_ensure_db_schema(DB, NewDBSchema, S0 = #s{sch = Schema0}) ->
    #{backend := Backend} = NewDBSchema,
    #{dbs := DBs} = Schema0,
    case DBs of
        #{DB := OldDBSchema = #{backend := Backend}} ->
            %% Backend matches. Return the original schema:
            Reply = {ok, OldDBSchema},
            {reply, Reply, S0};
        #{DB := #{backend := OldBackend}} when OldBackend =/= Backend ->
            Reply = {error, {backend_mismatch, OldBackend, Backend}},
            {reply, Reply, S0};
        #{} ->
            {ok, Schema} = modify_schema([#sop_set_db{db = DB, schema = NewDBSchema}], Schema0),
            S = S0#s{sch = Schema},
            Reply = {ok, NewDBSchema},
            {reply, Reply, S}
    end.

-spec do_drop_db(emqx_ds:db(), s()) -> {reply, ok | {error, _}, s()}.
do_drop_db(DB, S0 = #s{sch = Schema0, dbs = OpenDBs}) ->
    #{dbs := DBSchemas} = Schema0,
    IsOpen = maps:is_key(DB, OpenDBs),
    case DBSchemas of
        #{DB := _} when not IsOpen ->
            {ok, Schema} = modify_schema([#sop_drop_db{db = DB}], Schema0),
            S = S0#s{sch = Schema},
            {reply, ok, S};
        #{} when IsOpen ->
            {reply, {error, database_is_open}, S0};
        #{} ->
            {reply, {error, no_db_schema}, S0}
    end.

-spec do_set_cluster(cluster() | undefined, s()) -> {reply, ok, s()}.
do_set_cluster(MaybeCluster, S = #s{sch = Schema0}) ->
    {ok, Schema} = modify_schema([#sop_set_cluster{cluster = MaybeCluster}], Schema0),
    {reply, ok, S#s{sch = Schema}}.

-doc """
Open `current` log and apply all entries contained there to the empty
state, thus re-creating the state before shutdown of the node.

If the log is empty, then initialize the schema by creating a new
random site ID.
""".
-spec restore_or_init_schema() -> schema().
restore_or_init_schema() ->
    File = schema_file(),
    _ = filelib:ensure_dir(File),
    case
        disk_log:open([
            {name, ?log_current},
            {file, File},
            {repair, true},
            {type, halt}
        ])
    of
        {error, Reason} ->
            ?tp(critical, "Failed to read durable storage schema", #{
                reason => Reason, file => File
            }),
            exit(badschema);
        Ok ->
            case Ok of
                {ok, _} -> ok;
                {repaired, _, _, _} -> ok
            end,
            ensure_site_schema(restore_from_wal(?log_current))
    end.

-spec new_empty_schema(1, site()) -> schema().
new_empty_schema(Ver, Site) ->
    #{
        ver => Ver,
        site => Site,
        cluster => undefined,
        peers => [],
        dbs => #{}
    }.

-doc """
A pure function that mutates the state record accoring to a command.
""".
-spec pure_mutate(schema_op(), schema() | ?empty) -> {ok, schema() | ?empty} | {error, _}.
pure_mutate(Command, ?empty) ->
    %% Only one command is allowed in the empty state:
    case Command of
        #sop_init{ver = Ver, id = Site} ->
            {ok, new_empty_schema(Ver, Site)};
        _ ->
            {error, site_schema_is_not_initialized}
    end;
pure_mutate(#sop_set_db{db = DB, schema = DBSchema}, Schema = #{dbs := DBs}) ->
    {ok, Schema#{
        dbs := DBs#{DB => DBSchema}
    }};
pure_mutate(#sop_drop_db{db = DB}, Schema = #{dbs := DBs}) ->
    {ok, Schema#{
        dbs := maps:remove(DB, DBs)
    }};
pure_mutate(#sop_set_cluster{cluster = Cluster}, Schema) ->
    {ok, Schema#{
        cluster := Cluster
    }};
pure_mutate(Cmd, _S) ->
    {error, {unknown_comand, Cmd}}.

-spec pure_mutate_l([schema_op()], schema() | ?empty) -> {ok, schema() | ?empty} | {error, _}.
pure_mutate_l([], S) ->
    {ok, S};
pure_mutate_l([Command | L], S0) ->
    case pure_mutate(Command, S0) of
        {ok, S} -> pure_mutate_l(L, S);
        {error, _} = Err -> Err
    end.

-spec replay_wal(wal(), schema() | ?empty, disk_log:continuation() | start) -> schema() | ?empty.
replay_wal(Log, Schema0, Cont0) ->
    case disk_log:chunk(Log, Cont0, 1000) of
        {Cont, Cmds} ->
            case pure_mutate_l(Cmds, Schema0) of
                {ok, Schema} ->
                    replay_wal(Log, Schema, Cont);
                {error, Err} ->
                    ?tp(
                        critical,
                        "Failed to restore schema. Database schema has been created by a later version of EMQX?",
                        #{error => Err}
                    ),
                    exit(cannot_process_schema_command)
            end;
        eof ->
            Schema0
    end.

-spec ensure_site_schema(schema() | ?empty) -> schema().
ensure_site_schema(?empty) ->
    Site = binary:encode_hex(crypto:strong_rand_bytes(8)),
    ?tp(notice, "Initializing durable storage for the first time", #{site => binary_to_list(Site)}),
    {ok, Schema} = safe_add_l(?log_current, [#sop_init{id = Site}], ?empty),
    Schema;
ensure_site_schema(Schema = #{site := _}) ->
    Schema.

-doc """
A wrapper over `safe_add_l` that automatically puts the updated schema
to the persistent term.
""".
-spec modify_schema([schema_op()], schema()) -> {ok, schema()} | {error, _}.
modify_schema(Ops, Schema0) ->
    maybe
        {ok, Schema} ?= safe_add_l(?log_current, Ops, Schema0),
        persistent_term:put(?dsch_pt_schema, Schema),
        {ok, Schema}
    end.

-doc """
A safe way to permanently change the schema.

This function does it in three steps:

1. Verify that sequence of commands is valid.
2. Append the commands to the WAL.
3. Return the mutated schema.
""".
-spec safe_add_l(wal(), [schema_op()], schema() | ?empty) -> {ok, schema() | ?empty} | {error, _}.
safe_add_l(WAL, Commands, Schema0) ->
    try pure_mutate_l(Commands, Schema0) of
        {ok, Schema = #{site := _}} ->
            ok = disk_log:log_terms(WAL, Commands),
            ok = disk_log:sync(WAL),
            {ok, Schema};
        {error, _} = Err ->
            Err
    catch
        EC:Err:Stack ->
            ?tp(warning, ds_schema_command_crash, #{
                EC => Err, stacktrace => Stack, commands => Commands, s => Schema0
            }),
            {error, unknown}
    end.

-spec wal_backup(file:filename()) -> ok.
wal_backup(Filename) ->
    NewFilename = binary_to_list(
        iolist_to_binary(
            io_lib:format("~s.BAK.~p", [Filename, os:system_time(millisecond)])
        )
    ),
    file:rename(Filename, NewFilename).

-spec set_db_runtime(emqx_ds:db(), module(), ets:tid(), db_runtime_config()) -> ok.
set_db_runtime(DB, CBM, GVars, RuntimeConf) ->
    persistent_term:put(
        ?dsch_pt_db_runtime(DB),
        #{
            cbm => CBM,
            gvars => GVars,
            runtime => RuntimeConf
        }
    ).

-spec erase_db_consts(emqx_ds:db()) -> ok.
erase_db_consts(DB) ->
    persistent_term:erase(?dsch_pt_db_runtime(DB)),
    ok.

-spec lookup_db_schema(emqx_ds:db(), s()) -> {ok, db_schema()} | {error, no_db_schema}.
lookup_db_schema(DB, #s{sch = #{dbs := DBs}}) ->
    case DBs of
        #{DB := DBSchema} ->
            {ok, DBSchema};
        #{} ->
            {error, no_db_schema}
    end.

-spec lookup_backend_cbm(emqx_ds:backend(), s()) -> {ok, module()} | {error, _}.
lookup_backend_cbm(Backend, #s{backends = Backends}) ->
    case Backends of
        #{Backend := #bs{cbm = CBM}} ->
            {ok, CBM};
        #{} ->
            {error, {no_such_backend, Backend}}
    end.
