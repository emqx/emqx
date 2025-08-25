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

- *Runtime state*: a set of configuration constants that is set when
  DB is opened, and can be modified in the runtime using
  `update_db_config` API.

  Runtime state includes the callback module of the backend used by
  the durable storage, and a small amount of configuration data.

  This state is not saved, and it's recreated on every start of the
  DB.

- *Gvars* (global variables): this module also creates an ETS table
  that the backend can use to store frequently changing information
  about the DB. Gvars are also not saved and are erased when DB is
  closed.

## Cluster tracking

All functionality related to cluster and peer tracking is optional,
and it's designed to stay dormant until some backend requests uses it.

It's activated using `need_cluster(Nnodes)` API.
If cluster ID wasn't previously created, it is initialized from
`emqx_durable_storage.cluster_id` application environment variable.

## Implementation

OTP's `disk_log` is used as the persistence mechanism.
Contents of the schema are mirrored in a persistent term.
This makes scheama very cheap to read and hard to update.
So don't update it often.

All operations that mutate the scheama are synchronously written to
the WAL. For simplicity, WAL is not truncated or compressed while the
application is running. It is happens when the server starts.

When the server starts, it first completely replays the WAL to get to
the latest schema state, then this state is potentially migrated, and
dumped to another WAL. The latter is read again to initialize the
server state.
""".

-behaviour(gen_server).

%% API:
-export([
    register_backend/2,
    get_backend_cbm/1,

    this_site/0,
    get_site_schema/0,
    get_site_schema/1,
    get_site_schema/2,

    %% Cluster API:
    whereis_site/1,

    %% Pending action API:
    add_pending/3,
    list_pending/0,
    list_pending/1,
    del_pending/1,

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
    dump/2,

    %% Low-level cluster API:
    set_cluster/1,
    set_peer/2,
    delete_peer/1,

    safe_call_backend/4
]).

-export_type([
    site/0,
    cluster/0,
    peer_state/0,
    schema/0,
    db_schema/0,

    db_runtime/0,
    db_runtime_config/0,

    pending_id/0,
    pending_scope/0,
    pending/0,

    wal/0,
    dbshard/0,
    human_readable/0
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

-type peer_state() :: atom().

-doc """
Schema is an immutable term associated with the DS DB.

It is unaffected by `update_config` operation.
""".
-type db_schema() :: #{backend := emqx_ds:backend(), atom() => _}.

-type pending_scope() :: site | {db, emqx_ds:db()}.

%% Pending command:
-type pending_id() :: pos_integer().

-type pending() :: #{
    start_time := integer(),
    scope := pending_scope(),
    command := atom(),
    atom() => _
}.

-doc """
Global schema of the site (visible to the world).

It encapsulates schema of all DBs and shards, as well as pending
operations and other metadata.
""".
-type schema() :: #{
    site := site(),
    cluster := cluster() | singleton,
    peers := #{site() => peer_state()},
    dbs := #{emqx_ds:db() => db_schema()}
}.

-doc """
Persistent state of the node including some private data.
""".
-type pstate() :: #{
    ver := 1,
    pending_ctr := pending_id(),
    pending := #{pending_id() => pending()},
    schema := schema()
}.

%% Schema operations:
%%    Create a new site and schema:
-record(sop_init, {ver = 1 :: 1, id :: site()}).
%%%   Operation with the cluster
-record(sop_set_cluster, {cluster :: cluster() | singleton}).
-record(sop_set_peer, {peer :: site(), state :: peer_state()}).
-record(sop_delete_peer, {peer :: site()}).
%%    Set DB schema:
-record(sop_set_db, {db :: emqx_ds:db(), schema :: db_schema()}).
-record(sop_drop_db, {db :: emqx_ds:db()}).
%%    Pending actions:
-record(sop_add_pending, {id :: pending_id(), p :: pending()}).
-record(sop_del_pending, {id :: pending_id()}).

-doc """
A type of WAL entries.
""".
-type schema_op() ::
    #sop_init{}
    | #sop_set_cluster{}
    | #sop_set_db{}
    | #sop_drop_db{}
    | #sop_set_peer{}
    | #sop_delete_peer{}
    | #sop_add_pending{}
    | #sop_del_pending{}.

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
-define(log_old, emqx_ds_dbschema_old).
-define(log_current, emqx_ds_dbschema_current).

%% Calls:
-record(call_register_backend, {alias :: atom(), cbm :: module()}).
-record(call_ensure_db_schema, {
    db :: emqx_ds:db(), backend :: emqx_ds:backend(), schema :: db_schema()
}).
-record(call_add_pending, {scope :: pending_scope(), cmd :: atom(), data :: #{atom() => _}}).
-record(call_list_pending, {scope :: pending_scope() | all}).
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
    sch :: pstate(),
    %% Backend registrations:
    backends = #{} :: #{atom() => bs()},
    %% Transient DB and shard configuration:
    dbs = #{} :: #{emqx_ds:db() => dbs()}
}).
-type s() :: #s{}.

-ifdef(TEST).
-define(replay_chunk_size, 2).
-else.
-define(replay_chunk_size, 1000).
-endif.

-type human_readable() :: string().

%%--------------------------------------------------------------------------------
%% Backend callbacks
%%--------------------------------------------------------------------------------

-doc """
Return human-readable information about the DB useful for the operator.
""".
-callback db_info(emqx_ds:db()) -> {ok, human_readable()} | undefined.

-doc """
Validate configuration of a DB and split it into variable and permanent (schema) parts.
""".
-callback validate_db_opts(emqx_ds:create_db_opts()) ->
    {ok, db_schema(), db_runtime_config()}
    | {error, _}.

-doc """
Used to politely ask the backend about the consequences of removing site from the cluster.
""".
-callback verify_peer_leave(emqx_ds:db(), site()) -> ok | {warning, human_readable()}.

-doc """
Notify the backend that a site has been removed from the cluster.

Note: avoid long-running computation in this callback.
If some actions should be taken to handle the situation they should be implemented
using "pending" mechanism.
""".
-callback on_peer_leave(emqx_ds:db(), site()) -> {ok, [pending()]}.

-doc """
Notify the backend that a new site has been added to the cluster.

Note: avoid long-running computation in this callback.
If some actions should be taken to handle the situation they should be implemented
using "pending" mechanism.
""".
-callback on_peer_join(emqx_ds:db(), site()) -> {ok, [pending()]}.

-optional_callbacks([verify_peer_leave/2, on_peer_join/2, on_peer_leave/2]).

%%================================================================================
%% API functions
%%================================================================================

-spec this_site() -> binary().
this_site() ->
    #{site := Site} = get_site_schema(),
    Site.

-spec whereis_site(site()) -> node() | undefined.
whereis_site(Site) ->
    case global:whereis_name(?global_name(Site)) of
        undefined ->
            undefined;
        Pid ->
            node(Pid)
    end.

-doc """
Get the entire schema of the site.
""".
-spec get_site_schema() -> schema() | ?empty_schema.
get_site_schema() ->
    persistent_term:get(?dsch_pt_schema, ?empty_schema).

-doc """
Equivalent to `get_site_schema(NodeOrSite, 5_000)`
""".
-spec get_site_schema(node() | site()) -> {ok, schema() | ?empty_schema} | {error, _}.
get_site_schema(NodeOrSite) ->
    %% Note: this is an RPC target.
    get_site_schema(NodeOrSite, 5_000).

-doc """
Get schema of a remote site.
""".
-spec get_site_schema(node() | site(), timeout()) -> {ok, schema() | ?empty_schema} | {error, _}.
get_site_schema(Site, Timeout) when is_binary(Site) ->
    case whereis_site(Site) of
        undefined ->
            {error, down};
        Node ->
            get_site_schema(Node, Timeout)
    end;
get_site_schema(Node, Timeout) when is_atom(Node) ->
    case emqx_dsch_proto_v1:get_site_schemas([Node], Timeout) of
        [{ok, _} = Ret] ->
            Ret;
        [Other] ->
            %% TODO: better error reason
            {error, Other}
    end.

-doc """
Update cluster ID.

Setting cluster to a special value `singleton` prevents peers from
joining.

Cluster ID isn't used by this module directly, but it's stored in the
schema anyway, because most backends likely want to make sure that
they share data and communicate only with the nodes that belong to the
same cluster.

Cluster cannot be set to `singleton` when there are peers, they should
be removed first.
""".
-spec set_cluster(cluster() | singleton) -> ok | {error, badarg}.
set_cluster(Cluster) when is_binary(Cluster); Cluster =:= singleton ->
    gen_server:call(?SERVER, #sop_set_cluster{cluster = Cluster});
set_cluster(_) ->
    {error, badarg}.

-doc """
Unconditionally add peer site to the cluster.

WARNING: This function doesn't check if the peer belongs to the same
cluster and therefore it's unsafe for general use.
""".
-spec set_peer(site(), peer_state()) -> ok | {error, _}.
set_peer(Site, State) when is_binary(Site), is_atom(State) ->
    gen_server:call(?SERVER, #sop_set_peer{peer = Site, state = State}).

-spec delete_peer(site()) -> ok | {error, _}.
delete_peer(Site) when is_binary(Site) ->
    gen_server:call(?SERVER, #sop_delete_peer{peer = Site}).

-spec get_db_schema(emqx_ds:db()) -> db_schema() | undefined.
get_db_schema(DB) ->
    maybe
        #{dbs := DBs} ?= get_site_schema(),
        #{DB := DBSchema} ?= DBs,
        DBSchema
    else
        _ -> undefined
    end.

-spec register_backend(emqx_ds:backend(), module()) -> ok | {error, _}.
register_backend(Alias, CBM) when is_atom(Alias), is_atom(CBM) ->
    gen_server:call(?SERVER, #call_register_backend{alias = Alias, cbm = CBM}).

-spec get_backend_cbm(emqx_ds:backend()) -> {ok, module()} | {error, _}.
get_backend_cbm(Backend) ->
    case persistent_term:get(?dsch_pt_backends, #{}) of
        #{Backend := #bs{cbm = Mod}} ->
            {ok, Mod};
        #{} ->
            {error, {no_such_backend, Backend}}
    end.

-doc """
Add a pending action.
""".
-spec add_pending(pending_scope(), Command, Data) -> ok | {error, _} when
    Command :: atom(), Data :: #{atom() => _}.
add_pending(Scope, Command, Data) ->
    gen_server:call(?SERVER, #call_add_pending{scope = Scope, cmd = Command, data = Data}).

-spec list_pending() -> #{pending_id() => pending()}.
list_pending() ->
    list_pending(all).

-spec list_pending(pending_scope() | all) -> #{pending_id() => pending()}.
list_pending(Scope) ->
    gen_server:call(?SERVER, #call_list_pending{scope = Scope}).

-doc """
Delete pending operation with the given ID.
""".
-spec del_pending(pending_id()) -> ok.
del_pending(Id) ->
    gen_server:call(?SERVER, #sop_del_pending{id = Id}).

-doc """
If database schema wasn't present before, create schema it (equal to the
second argument of the function).

If database schema is present and backend matches the supplied one,
return the original schema.

Return an error otherwise.
""".
-spec ensure_db_schema(emqx_ds:db(), db_schema()) -> {ok, IsNew, db_schema()} | {error, _} when
    IsNew :: boolean().
ensure_db_schema(DB, Schema = #{backend := Backend}) ->
    gen_server:call(?SERVER, #call_ensure_db_schema{db = DB, backend = Backend, schema = Schema}).

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
-spec restore_from_wal(wal()) -> pstate() | ?empty_schema.
restore_from_wal(Log) ->
    replay_wal(Log, ?empty_schema, start).

-doc """
Return location of the node's schema file.
(For debugging and troubleshooting).
""".
-spec schema_file() -> file:filename().
schema_file() ->
    filename:join(emqx_ds_storage_layer:base_dir(), ?schema).

-doc """
Dump schema to a WAL.
The WAL should be open; all data previously stored there is discarded.
""".
-spec dump(pstate(), wal()) -> ok | {error, _}.
dump(Pstate, WAL) ->
    ok = disk_log:truncate(WAL),
    #{ver := Ver, schema := Schema, pending_ctr := PendingCtr, pending := Pending} = Pstate,
    #{site := Site, cluster := Cluster, dbs := DBs, peers := Peers} = Schema,
    Ops =
        [
            #sop_init{ver = Ver, id = Site},
            #sop_set_cluster{cluster = Cluster}
        ] ++
            [#sop_set_db{db = DB, schema = DBSchema} || {DB, DBSchema} <- maps:to_list(DBs)] ++
            [
                #sop_set_peer{peer = Peer, state = PeerState}
             || {Peer, PeerState} <- maps:to_list(Peers)
            ] ++
            dump_pending(PendingCtr, Pending),
    case safe_add_l(WAL, Ops, ?empty_schema) of
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
    Pstate = #{} = restore_or_init_pstate(),
    persistent_term:put(?dsch_pt_schema, maps:get(schema, Pstate)),
    S = #s{sch = Pstate},
    #{schema := #{site := Site}} = Pstate,
    global:register_name(?global_name(Site), self(), fun global:random_notify_name/3),
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
handle_call(#call_ensure_db_schema{db = DB, backend = Backend, schema = NewDBSchema}, _From, S) ->
    do_ensure_db_schema(DB, Backend, NewDBSchema, S);
handle_call(#sop_drop_db{db = DB}, _From, S) ->
    do_drop_db(DB, S);
handle_call(#call_register_backend{alias = Alias, cbm = CBM}, _From, S) ->
    do_register_backend(Alias, CBM, S);
handle_call(#call_add_pending{scope = Scope, cmd = Command, data = Data}, _From, S0) ->
    case do_add_pending(Scope, Command, Data, S0) of
        {ok, S} -> {reply, ok, S};
        {error, _} = Err -> {reply, Err, S0}
    end;
handle_call(#call_list_pending{scope = Scope}, _From, S) ->
    {reply, do_list_pending(Scope, S), S};
handle_call(#sop_del_pending{id = Id}, _From, S) ->
    {reply, ok, do_del_pending(Id, S)};
handle_call(SchemaOp, _From, S0) when
    is_record(SchemaOp, sop_set_peer);
    is_record(SchemaOp, sop_delete_peer);
    is_record(SchemaOp, sop_set_cluster)
->
    case do_update_cluster(SchemaOp, S0) of
        {ok, S} ->
            {reply, ok, S};
        {error, _} = Err ->
            {reply, Err, S0}
    end;
handle_call(Call, From, S) ->
    ?tp(error, emqx_dsch_unkown_call, #{from => From, call => Call, state => S}),
    {reply, {error, unknown_call}, S}.

handle_cast(Cast, S) ->
    ?tp(error, emqx_dsch_unkown_cast, #{call => Cast, state => S}),
    {noreply, S}.

handle_info({global_name_conflict, ?global_name(Site)}, S = #s{sch = Pstate}) ->
    #{schema := #{site := MySite}} = Pstate,
    case Site =:= MySite of
        true ->
            Expl = "Another node claimed site ID. All sites must be unique",
            ?tp(error, global_site_conflict, #{site => Site, exlanation => Expl}),
            {stop, site_conflict, S};
        false ->
            {noreply, S}
    end;
handle_info({'EXIT', From, Reason}, S) ->
    case Reason of
        normal ->
            {noreply, S};
        _ ->
            ?tp(debug, emqx_dsch_graceful_shutdown, #{from => From, reason => Reason}),
            {stop, shutdown, S}
    end;
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
    persistent_term:erase(?dsch_pt_backends),
    _ = disk_log:close(?log_current),
    _ = disk_log:close(?log_new),
    _ = disk_log:close(?log_old),
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
do_register_backend(Alias, CBM, S = #s{backends = Backends0}) ->
    case Backends0 of
        #{Alias := #bs{cbm = CBM}} ->
            {reply, ok, S};
        #{Alias := #bs{cbm = Other}} ->
            Err = {error, {conflict, Other}},
            {reply, Err, S};
        #{} ->
            Backends = Backends0#{Alias => #bs{cbm = CBM}},
            set_backend_cbms_pt(Backends),
            {reply, ok, S#s{backends = Backends}}
    end.

set_backend_cbms_pt(Backends) ->
    persistent_term:put(?dsch_pt_backends, Backends).

do_list_pending(Scope, #s{sch = #{pending := Pending}}) ->
    maps:filter(
        fun(_Id, #{scope := Sc}) ->
            Scope =:= all orelse Sc =:= Scope
        end,
        Pending
    ).

do_add_pending(Scope, Command, Data, S0) ->
    maybe
        {ok, Wrapper} ?= verify_pending(Scope, Command, Data),
        #s{sch = #{pending_ctr := LastId}} = S0,
        modify_schema([#sop_add_pending{id = LastId, p = Wrapper}], S0)
    end.

-spec dump_pending(pending_id(), #{pending_id() => pending()}) -> [schema_op()].
dump_pending(PendingCtr, Pending) ->
    Nop = #sop_add_pending{id = PendingCtr, p = #{start_time => 0, scope => site, command => nop}},
    maps:fold(
        fun(Id, Val, Acc) ->
            [#sop_add_pending{id = Id, p = Val} | Acc]
        end,
        [Nop],
        Pending
    ).

verify_pending(site, Command, Data) when is_atom(Command), is_map(Data) ->
    case Command of
        _ ->
            {error, unknown_site_command}
    end;
verify_pending(Scope = {db, DB}, Command, Data) when is_atom(Command), is_map(Data) ->
    %% Note: this function runs before command is persisted, and while
    %% server is running, so it's safe to use persistent term:
    maybe
        %% DB should exist, other than that we don't run additional
        %% checks: backend should cancel actions it doesn't
        %% understand.
        #{} ?= get_db_schema(DB),
        {ok, Data#{
            start_time => os:system_time(millisecond),
            scope => Scope,
            command => Command
        }}
    else
        _ -> {error, no_db}
    end;
verify_pending(_, _, _) ->
    {error, badarg}.

do_del_pending(Id, S0 = #s{sch = Pdata}) ->
    #{pending := Pend0} = Pdata,
    case Pend0 of
        #{Id := _} ->
            {ok, S} = modify_schema([#sop_del_pending{id = Id}], S0),
            S;
        #{} ->
            S0
    end.

-spec do_ensure_db_schema(emqx_ds:db(), emqx_ds:backend(), db_schema(), s()) ->
    {reply, {ok, boolean(), db_schema()} | {error, _}, s()}.
do_ensure_db_schema(DB, Backend, NewDBSchema, S0) ->
    maybe
        %% Handle creation path:
        {error, no_db_schema} ?= lookup_db_schema(DB, S0),
        {ok, _} ?= lookup_backend_cbm(Backend, S0),
        {ok, S} ?= modify_schema([#sop_set_db{db = DB, schema = NewDBSchema}], S0),
        Reply = {ok, true, NewDBSchema},
        {reply, Reply, S}
    else
        {ok, OldDBSchema = #{backend := Backend}} ->
            %% Schema with the same backend already exists, return old schema:
            Reply1 = {ok, false, OldDBSchema},
            {reply, Reply1, S0};
        {ok, #{backend := OldBackend}} ->
            Reply1 = {error, {backend_mismatch, OldBackend, Backend}},
            {reply, Reply1, S0};
        {error, _} = Err ->
            {reply, Err, S0}
    end.

-spec do_drop_db(emqx_ds:db(), s()) -> {reply, ok | {error, _}, s()}.
do_drop_db(DB, S0 = #s{dbs = OpenDBs}) ->
    maybe
        {ok, _} ?= lookup_db_schema(DB, S0),
        false ?= maps:is_key(DB, OpenDBs),
        {ok, S} ?= modify_schema([#sop_drop_db{db = DB}], S0),
        {reply, ok, S}
    else
        true ->
            {reply, {error, database_is_currently_open}, S0};
        {error, _} = Err ->
            {reply, Err, S0}
    end.

-spec do_update_cluster(schema_op(), s()) -> {ok, s()} | {error, _}.
do_update_cluster(Op, S0 = #s{sch = Pstate0}) ->
    #{schema := #{cluster := OldCID, peers := Peers0}} = Pstate0,
    HasPeers = maps:size(Peers0) > 0,
    case Op of
        #sop_set_cluster{cluster = NewCID} when is_binary(OldCID), NewCID =:= singleton, HasPeers ->
            {error, has_peers};
        #sop_set_cluster{cluster = NewCID} when
            is_binary(NewCID), is_binary(OldCID), NewCID =/= OldCID
        ->
            %% TODO: ask backends if they're ok with the change
            %% instead of simply rejecting?
            {error, cannot_change_cluster_existing_id};
        #sop_set_peer{} when OldCID =:= singleton ->
            {error, cannot_add_peers_while_in_singleton_mode};
        _ ->
            modify_schema([Op], S0)
    end.

-doc """
A wrapper over `safe_add_l` that automatically puts the updated schema
to the persistent term.
""".
-spec modify_schema([schema_op()], s()) -> {ok, s()} | {error, _}.
modify_schema(Ops, S = #s{sch = Pstate0}) ->
    maybe
        {ok, Pstate} ?= safe_add_l(?log_current, Ops, Pstate0),
        persistent_term:put(?dsch_pt_schema, maps:get(schema, Pstate)),
        {ok, S#s{sch = Pstate}}
    end.

-doc """
Open `current` log and apply all entries contained there to the empty
state, thus re-creating the state before shutdown of the node.

If the log is empty, then initialize the schema by creating a new
random site ID.
""".
-spec restore_or_init_pstate() -> pstate().
restore_or_init_pstate() ->
    ok = compress_and_migrate_pstate(1),
    File = schema_file(),
    _ = filelib:ensure_dir(File),
    case open_log(read_write, ?log_current, File) of
        ok ->
            ensure_site_schema(restore_from_wal(?log_current));
        {error, Reason} ->
            ?tp(critical, "Failed to read durable storage schema", #{
                reason => Reason, file => File
            }),
            exit(badschema)
    end.

-spec new_empty_pstate(1, site()) -> pstate().
new_empty_pstate(Ver, Site) ->
    #{
        ver => Ver,
        pending_ctr => 1,
        pending => #{},
        schema => #{
            site => Site,
            cluster => singleton,
            peers => #{},
            dbs => #{}
        }
    }.

-doc """
A pure function that mutates the state record accoring to a command.

Note: this function has to maintain backward-compatibility.
""".
-spec pure_mutate(schema_op(), pstate() | ?empty_schema) ->
    {ok, pstate() | ?empty_schema} | {error, _}.
pure_mutate(Command, ?empty_schema) ->
    %% Only one command is allowed in the empty state:
    case Command of
        #sop_init{ver = Ver, id = Site} ->
            {ok, new_empty_pstate(Ver, Site)};
        _ ->
            {error, site_schema_is_not_initialized}
    end;
pure_mutate(#sop_set_db{db = DB, schema = DBSchema}, S) ->
    with_schema(
        dbs,
        S,
        fun(DBs) ->
            DBs#{DB => DBSchema}
        end
    );
pure_mutate(#sop_drop_db{db = DB}, S) ->
    with_schema(
        dbs,
        S,
        fun(DBs) ->
            maps:remove(DB, DBs)
        end
    );
pure_mutate(#sop_set_cluster{cluster = Cluster}, S) ->
    with_schema(
        cluster,
        S,
        fun(_) ->
            Cluster
        end
    );
pure_mutate(#sop_set_peer{peer = Site, state = State}, S) ->
    with_schema(
        peers,
        S,
        fun(Peers) ->
            Peers#{Site => State}
        end
    );
pure_mutate(#sop_delete_peer{peer = Site}, S) ->
    with_schema(
        peers,
        S,
        fun(Peers) ->
            maps:remove(Site, Peers)
        end
    );
pure_mutate(#sop_add_pending{id = Id, p = Pending}, S0 = #{pending := Pend0}) ->
    S =
        case Pending of
            #{scope := site, command := nop} ->
                S0#{pending_ctr := Id};
            _ ->
                S0#{pending := Pend0#{Id => Pending}, pending_ctr := Id + 1}
        end,
    {ok, S};
pure_mutate(#sop_del_pending{id = Id}, S0 = #{pending := Pend0}) ->
    S = S0#{pending := maps:remove(Id, Pend0)},
    {ok, S};
pure_mutate(Cmd, _S) ->
    {error, {unknown_comand, Cmd}}.

-spec with_schema(atom(), pstate(), Fun) -> {ok, pstate()} | {error, Err} when
    Fun :: fun((A) -> {ok, A} | {error, Err}).
with_schema(Key, Pstate = #{schema := Schema}, Fun) ->
    #{Key := Val0} = Schema,
    Val = Fun(Val0),
    {ok, Pstate#{schema := Schema#{Key := Val}}}.

-spec pure_mutate_l([schema_op()], pstate() | ?empty_schema) ->
    {ok, pstate() | ?empty_schema} | {error, _}.
pure_mutate_l([], S) ->
    {ok, S};
pure_mutate_l([Command | L], S0) ->
    case pure_mutate(Command, S0) of
        {ok, S} -> pure_mutate_l(L, S);
        {error, _} = Err -> Err
    end.

-spec replay_wal(wal(), pstate() | ?empty_schema, disk_log:continuation() | start) ->
    pstate() | ?empty_schema.
replay_wal(Log, Schema0, Cont0) ->
    case disk_log:chunk(Log, Cont0, ?replay_chunk_size) of
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

-spec ensure_site_schema(pstate() | ?empty_schema) -> pstate().
ensure_site_schema(?empty_schema) ->
    Site = binary:encode_hex(crypto:strong_rand_bytes(8)),
    ?tp(notice, "Initializing durable storage for the first time", #{site => binary_to_list(Site)}),
    {ok, Pstate} = safe_add_l(?log_current, [#sop_init{id = Site}], ?empty_schema),
    Pstate;
ensure_site_schema(Pstate = #{ver := _}) ->
    Pstate.

-doc """
A safe way to permanently change the schema.

This function does it in three steps:

1. Verify that sequence of commands is valid.
2. Append the commands to the WAL.
3. Return the mutated schema.
""".
-spec safe_add_l(wal(), [schema_op()], pstate() | ?empty_schema) ->
    {ok, pstate() | ?empty_schema} | {error, _}.
safe_add_l(WAL, Commands, Pstate0) ->
    try pure_mutate_l(Commands, Pstate0) of
        {ok, Pstate = #{ver := _, schema := #{site := _}}} ->
            ok = disk_log:log_terms(WAL, Commands),
            ok = disk_log:sync(WAL),
            {ok, Pstate};
        {error, _} = Err ->
            Err
    catch
        EC:Err:Stack ->
            ?tp(warning, ds_schema_command_crash, #{
                EC => Err, stacktrace => Stack, commands => Commands, s => Pstate0
            }),
            {error, unknown}
    end.

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
lookup_db_schema(DB, #s{sch = #{schema := #{dbs := DBs}}}) ->
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

-spec compress_and_migrate_pstate(Attempt) -> ok when
    Attempt :: pos_integer().
compress_and_migrate_pstate(Attempt) when Attempt < 3 ->
    %% Compressed and migrated schema will be dumped to a temporary
    %% log. Once everything's complete, it will replace the current
    %% schema dump:
    New = schema_file() ++ ".NEW",
    HasCurrent = filelib:is_file(schema_file()),
    HasNew = filelib:is_file(New),
    case {HasCurrent, HasNew} of
        {false, false} ->
            %% This is a new deployment:
            ok;
        {true, false} ->
            %% Normal situation:
            ok = open_log(read_only, ?log_old, schema_file()),
            ok = open_log(read_write, ?log_new, New),
            Pstate = perform_migration(restore_from_wal(?log_old)),
            case dump(Pstate, ?log_new) of
                ok ->
                    ok = disk_log:close(?log_old),
                    ok = disk_log:close(?log_new),
                    %% At this point we are certain that .NEW log has
                    %% complete data. Old log can be discarded:
                    file:rename(New, schema_file());
                Wrong ->
                    ?tp(
                        critical,
                        "Failed to read or migrate old durable storage schema",
                        #{
                            old_schema => Pstate,
                            result => Wrong
                        }
                    ),
                    %% Migration went wrong.
                    _ = disk_log:close(?log_old),
                    _ = disk_log:close(?log_new),
                    exit(failed_to_migrate_schema)
            end;
        {true, true} ->
            %% There's a NEW log from the previous migration attempt
            %% that was aborted. Discard the it (since it may be
            %% incomplete or otherwise broken) and migrate again:
            ?tp(debug, emqx_dsch_discard_new_schema, #{file => New}),
            ok = file:rename(New, file_backup(New)),
            compress_and_migrate_pstate(Attempt + 1);
        {false, true} ->
            %% This shouldn't really happen (rename failed?). But make NEW log current
            %% and attempt again (back it up first):
            Bak = file_backup(New),
            _ = file:copy(New, Bak),
            ?tp(warning, "Restoring schema from NEW file", #{file => New, backup => Bak}),
            ok = file:rename(New, schema_file()),
            compress_and_migrate_pstate(Attempt + 1)
    end;
compress_and_migrate_pstate(Attempt) ->
    %% Prevent infinite loop and fail:
    ?tp(critical, "Too many attempts to migrate durable storage schema. Exiting.", #{
        attempts => Attempt
    }),
    exit(failed_to_migrate_schema).

-spec perform_migration(#{ver := pos_integer(), _ => _}) -> pstate().
perform_migration(Pstate = #{ver := 1}) ->
    Pstate.

open_log(Mode, Name, File) ->
    case
        disk_log:open([
            {mode, Mode},
            {name, Name},
            {file, File},
            {repair, true},
            {type, halt}
        ])
    of
        {ok, _} ->
            ok;
        {repaired, _, _, _} = Result ->
            ?tp(warning, "Durable storage schema repaired", #{result => Result, file => File}),
            ok;
        Other ->
            Other
    end.

-spec file_backup(file:filename()) -> file:filename().
file_backup(Filename) ->
    binary_to_list(
        iolist_to_binary(
            io_lib:format("~s.BAK.~p", [Filename, os:system_time(millisecond)])
        )
    ).

-spec safe_call_backend(module(), atom(), list(), Ret) -> {ok, Ret} | error.
safe_call_backend(Mod, Fun, Args, Default) ->
    try
        Mod:Fun(Args)
    catch
        error:undef ->
            {ok, Default};
        EC:Err:Stack ->
            ?tp(error, dsch_backend_callback_error, #{
                mod => Mod, 'fun' => Fun, args => Args, EC => Err, stack => Stack
            }),
            error
    end.
