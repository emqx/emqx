%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dsch).
-moduledoc """
# Durable storage schema manager

This module implements a node-local persistent storage for the database schemas,
as well as a tracking mechanism for open databases and cluster state.
It is designed as an alternative to the Mnesia schema.

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

    %% DB API:
    ensure_db_schema/2,
    get_db_schema/1,
    drop_db_schema/1,
    update_db_schema/2,

    open_db/2,
    close_db/1,
    update_db_config/2,
    get_db_runtime/1,
    db_gvars/1,
    gvar_set/4,
    gvar_unset/3,
    gvar_get/3,
    gvar_set/5,
    gvar_get/4,
    gvar_unset/4,
    gvar_unset_all/3
]).

%% behavior callbacks:
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

%% internal exports:
-export([
    start_link/0
]).

-export_type([
    site/0,
    cluster/0,
    peer_state/0,
    schema/0,
    db_schema/0,

    db_runtime/0,
    db_runtime_config/0,

    dbshard/0,
    human_readable/0
]).

-include("emqx_ds.hrl").
-include("emqx_dsch.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("classy/include/classy.hrl").

-elvis([{elvis_style, no_single_clause_case, disable}]).

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

-type schema() :: #{
    site := site(),
    cluster := cluster() | singleton,
    peers := #{site() => peer_state()},
    dbs := #{emqx_ds:db() => db_schema()}
}.

%%--------------------------------------------------------------------------------
%% Table keys and values
%%--------------------------------------------------------------------------------

%% -record(db_k, {cluster :: classy:cluster_id(), db :: emqx_ds:db()}).
%% -record(shard_k, {cluster :: classy:cluster_id(), db :: emqx_ds:db(), shard :: emqx_ds:shard()}).
-record(dcache, {k, v}).
-define(migrated, migrated).

%%--------------------------------------------------------------------------------
%% Server state and misc. types
%%--------------------------------------------------------------------------------

-define(ptab, emqx_dsch_schema_tab).
-define(dcache, emqx_dsch_dist_cache).

-type dbshard() :: {emqx_ds:db(), emqx_ds:shard()}.

-type db_runtime_config() :: #{
    db_group => emqx_ds:db_group(),
    atom() => _
}.

-type db_runtime() :: #{
    cbm := module(),
    gvars := ets:tid(),
    runtime := db_runtime_config()
}.

%% Calls:
-record(call_register_backend, {alias :: atom(), cbm :: module()}).
-record(call_ensure_db_schema, {
    db :: emqx_ds:db(), backend :: emqx_ds:backend(), schema :: db_schema()
}).
-record(call_open_db, {db :: emqx_ds:db(), conf :: db_runtime_config()}).
-record(call_close_db, {db :: emqx_ds:db()}).
-record(call_drop_db, {db :: emqx_ds:db()}).
-record(call_update_db_schema, {
    db :: emqx_ds:db(), backend :: emqx_ds:backend(), schema :: db_schema()
}).
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
    %% Backend registrations:
    backends = #{} :: #{atom() => bs()},
    %% Transient DB and shard configuration:
    open_dbs = #{} :: #{emqx_ds:db() => dbs()}
}).
-type s() :: #s{}.

-type human_readable() :: string().

%%--------------------------------------------------------------------------------
%% Backend callbacks
%%--------------------------------------------------------------------------------

-doc """
Return human-readable information about the DB useful for the operator.
""".
-callback db_info(emqx_ds:db()) -> {ok, human_readable()} | undefined.

-doc """
This is called when runtime config changes.
""".
-callback handle_db_config_change(emqx_ds:db(), db_runtime_config()) -> ok.

%%================================================================================
%% API functions
%%================================================================================

-spec this_site() -> binary().
this_site() ->
    classy_node:maybe_site().

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
    case classy:node_of_site(Site) of
        {ok, Node} ->
            get_site_schema(Node, Timeout);
        undefined ->
            {error, down}
    end;
get_site_schema(Node, Timeout) when is_atom(Node) ->
    case emqx_dsch_proto_v1:get_site_schemas([Node], Timeout) of
        [{ok, _} = Ret] ->
            Ret;
        [Other] ->
            %% TODO: better error reason
            {error, Other}
    end.

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
    gen_server:call(?SERVER, #call_register_backend{alias = Alias, cbm = CBM}, infinity).

-spec get_backend_cbm(emqx_ds:backend()) -> {ok, module()} | {error, _}.
get_backend_cbm(Backend) ->
    case persistent_term:get(?dsch_pt_backends, #{}) of
        #{Backend := #bs{cbm = Mod}} ->
            {ok, Mod};
        #{} ->
            {error, {no_such_backend, Backend}}
    end.

-doc """
If database schema wasn't present before, create schema it (equal to the
second argument of the function).

If database schema is present and backend matches the supplied one,
return the original schema.

Return an error otherwise.
""".
-spec ensure_db_schema(emqx_ds:db(), db_schema()) -> {ok, IsNew, db_schema()} | {error, _} when
    IsNew :: boolean().
ensure_db_schema(DB, Schema = #{backend := Backend}) when is_atom(Backend) ->
    gen_server:call(?SERVER, #call_ensure_db_schema{db = DB, backend = Backend, schema = Schema}).

-doc """
Update DB schema.
Backend will be notified via a pending command `change_schema`.
""".
-spec update_db_schema(emqx_ds:db(), db_schema()) -> ok | {error, _}.
update_db_schema(DB, NewSchema = #{backend := Backend}) when is_atom(Backend) ->
    %% TODO: first check that schema change operation isn't already pending.
    gen_server:call(?SERVER, #call_update_db_schema{db = DB, backend = Backend, schema = NewSchema}).

-spec drop_db_schema(emqx_ds:db()) -> ok | {error, _}.
drop_db_schema(DB) ->
    gen_server:call(?SERVER, #call_drop_db{db = DB}).

-spec open_db(emqx_ds:db(), db_runtime_config()) -> ok | {error, _}.
open_db(DB, RuntimeConfig) ->
    gen_server:call(?SERVER, #call_open_db{db = DB, conf = RuntimeConfig}).

-spec close_db(emqx_ds:db()) -> ok.
close_db(DB) ->
    gen_server:call(?SERVER, #call_close_db{db = DB}).

-doc """
Update runtime configuration of an open DB.
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

-spec gvar_set(emqx_ds:db(), atom(), _Key, _Val) -> ok.
gvar_set(DB, Scope, Key, Val) when Scope =/= '_' ->
    true = ets:insert(db_gvars(DB), {{db, Scope, Key}, Val}),
    ok.

-spec gvar_unset(emqx_ds:db(), atom(), _Key) -> ok.
gvar_unset(DB, Scope, Key) ->
    true = ets:delete(db_gvars(DB), {db, Scope, Key}),
    ok.

-spec gvar_get(emqx_ds:db(), atom(), _Key) -> {ok, _Value} | undefined.
gvar_get(DB, Scope, Key) ->
    case ets:lookup(db_gvars(DB), {db, Scope, Key}) of
        [{_, Val}] ->
            {ok, Val};
        [] ->
            undefined
    end.

-spec gvar_set(emqx_ds:db(), emqx_ds:shard(), atom(), _Key, _Val) -> ok.
gvar_set(DB, Shard, Scope, Key, Val) when Scope =/= '_' ->
    true = ets:insert(db_gvars(DB), {{shard, Shard, Scope, Key}, Val}),
    ok.

-spec gvar_unset(emqx_ds:db(), emqx_ds:shard(), atom(), _Key) -> ok.
gvar_unset(DB, Shard, Scope, Key) ->
    true = ets:delete(db_gvars(DB), {shard, Shard, Scope, Key}),
    ok.

-spec gvar_get(emqx_ds:db(), emqx_ds:shard(), atom(), _Key) -> {ok, _Value} | undefined.
gvar_get(DB, Shard, Scope, Key) ->
    case ets:lookup(db_gvars(DB), {shard, Shard, Scope, Key}) of
        [{_, Val}] ->
            {ok, Val};
        [] ->
            undefined
    end.

-doc """
Helper function that deletes all gvars that belong to the given shard.

When `Scope = '_'` this function will delete variable from all scopes.
""".
-spec gvar_unset_all(emqx_ds:db(), emqx_ds:shard(), atom()) -> ok.
gvar_unset_all(DB, Shard, Scope) ->
    Pattern = {{shard, Shard, Scope, '_'}, '_'},
    true = ets:match_delete(db_gvars(DB), Pattern),
    ok.

%%================================================================================
%% Internal exports
%%================================================================================

-spec start_link() -> {ok, pid()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%================================================================================
%% behavior callbacks
%%================================================================================

init(_) ->
    process_flag(trap_exit, true),
    ok = classy_table:open(?ptab, #{
        ets => [ordered_set, {read_concurrency, true}],
        on_update => fun on_update/2
    }),
    Pattern = #dcache{k = {'$1', '_'}, _ = '_'},
    %% Mria table is used as a distributed cache. It's not the source
    %% of truth:
    ok = mria:create_table(
        ?dcache,
        [
            {merge_table, true},
            {storage, ram_copies},
            {type, ordered_set},
            {node_pattern, [Pattern]},
            {rlog_shard, dsch_mria_shard}
        ]
    ),
    mria:wait_for_tables([?dcache]),
    maybe_migrate(),
    S = #s{},
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
handle_call(#call_update_db_schema{db = DB, backend = NewBackend, schema = NewSchema}, _From, S) ->
    do_update_db_schema(DB, NewBackend, NewSchema, S);
handle_call(#call_drop_db{db = DB}, _From, S) ->
    do_drop_db(DB, S);
handle_call(#call_register_backend{alias = Alias, cbm = CBM}, _From, S) ->
    do_register_backend(Alias, CBM, S);
handle_call(Call, From, S) ->
    ?tp(error, emqx_dsch_unkown_call, #{from => From, call => Call, state => S}),
    {reply, {error, unknown_call}, S}.

handle_cast(Cast, S) ->
    ?tp(error, emqx_dsch_unkown_cast, #{call => Cast, state => S}),
    {noreply, S}.

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

terminate(Reason, S0 = #s{open_dbs = DBs}) ->
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
    classy_table:close(?ptab),
    ok.

%%================================================================================
%% Internal functions
%%================================================================================

-spec maybe_migrate() -> ok.
maybe_migrate() ->
    case classy_table:lookup(?ptab, ?migrated) of
        [_] ->
            ok;
        [] ->
            ?tp(warning, "Migrating data to classy", #{}),

            Schema = emqx_dsch_migrate:read_old(),
            ?tp(warning, "TODO restored schema", #{schema => Schema})
    end.

-spec do_open_db(emqx_ds:db(), db_runtime_config(), s()) -> {ok, s()} | {error, _}.
do_open_db(DB, RuntimeConf, S0 = #s{open_dbs = DBs}) ->
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
            open_dbs = DBs#{
                DB => #dbs{rtconf = RuntimeConf, gvars = GVars}
            }
        },
        set_db_runtime(DB, CBM, GVars, RuntimeConf),
        {ok, S}
    end.

-spec do_update_db_config(emqx_ds:db(), db_runtime_config(), s()) -> {ok, s()} | {error, _}.
do_update_db_config(DB, NewConf, S0 = #s{open_dbs = DBs}) ->
    maybe
        #{DB := DBstate0 = #dbs{gvars = GVars}} ?= DBs,
        {ok, DBSchema} ?= lookup_db_schema(DB, S0),
        #{backend := Backend} = DBSchema,
        {ok, CBM} ?= lookup_backend_cbm(Backend, S0),
        DBstate = DBstate0#dbs{rtconf = NewConf},
        S = S0#s{
            open_dbs = DBs#{DB := DBstate}
        },
        set_db_runtime(DB, CBM, GVars, NewConf),
        %% Notify backend:
        try
            _ = CBM:handle_db_config_change(DB, NewConf)
        catch
            EC:Err:Stack ->
                ?tp(
                    warning,
                    emqx_dsch_handle_update_config_crash,
                    #{db => DB, conf => NewConf, EC => Err, stack => Stack}
                )
        end,
        {ok, S}
    else
        #{} ->
            {error, {database_is_not_open, DB}}
    end.

-spec do_close_db(emqx_ds:db(), s()) -> s().
do_close_db(DB, S = #s{open_dbs = DBs}) ->
    case DBs of
        #{DB := #dbs{gvars = GVars}} ->
            erase_db_consts(DB),
            ets:delete(GVars),
            S#s{
                open_dbs = maps:remove(DB, DBs)
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

-spec do_ensure_db_schema(emqx_ds:db(), emqx_ds:backend(), db_schema(), s()) ->
    {reply, {ok, boolean(), db_schema()} | {error, _}, s()}.
do_ensure_db_schema(DB, Backend, NewDBSchema, S0) ->
    maybe
        %% Handle creation path:
        {error, no_db_schema} ?= lookup_db_schema(DB, S0),
        {ok, _} ?= lookup_backend_cbm(Backend, S0),
        {ok, S} ?= set_db_schema(DB, NewDBSchema, S0),
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

-spec do_update_db_schema(emqx_ds:db(), emqx_ds:backend(), db_schema(), s()) ->
    {reply, ok | {error, _}, s()}.
do_update_db_schema(DB, NewBackend, NewDBSchema, S0) ->
    maybe
        {ok, OldDBSchema} ?= lookup_db_schema(DB, S0),
        #{backend := OldBackend} = OldDBSchema,
        true ?= OldBackend =:= NewBackend orelse {error, backend_cannot_be_changed},
        {ok, S1} ?= set_db_schema(DB, NewDBSchema, S0),
        %% {ok, S} ?=
        %%     do_add_pending(
        %%         {db, DB},
        %%         change_schema,
        %%         #{
        %%             old => OldDBSchema,
        %%             new => NewDBSchema,
        %%             originator => this_site()
        %%         },
        %%         S1
        %%     ),
        S = S1,
        {reply, ok, S}
    else
        Err ->
            {reply, Err, S0}
    end.

-spec do_drop_db(emqx_ds:db(), s()) -> {reply, ok | {error, _}, s()}.
do_drop_db(DB, S0 = #s{open_dbs = OpenDBs}) ->
    maybe
        {ok, _} ?= lookup_db_schema(DB, S0),
        false ?= maps:is_key(DB, OpenDBs),
        {ok, S} ?= del_db_schema(DB, S0),
        {reply, ok, S}
    else
        true ->
            {reply, {error, database_is_currently_open}, S0};
        {error, _} = Err ->
            {reply, Err, S0}
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
lookup_db_schema(DB, _) ->
    case classy_table:lookup(?ptab, DB) of
        [DBSchema] ->
            {ok, DBSchema};
        [] ->
            {error, no_db_schema}
    end.

-spec set_db_schema(emqx_ds:db(), db_schema(), s()) -> {ok, s()}.
set_db_schema(DB, Schema, S) ->
    classy_table:write(?ptab, DB, Schema),
    {ok, S}.

-spec del_db_schema(emqx_ds:db(), s()) -> {ok, s()}.
del_db_schema(DB, S) ->
    classy_table:delete(?ptab, DB),
    {ok, S}.

-spec lookup_backend_cbm(emqx_ds:backend(), s()) -> {ok, module()} | {error, _}.
lookup_backend_cbm(Backend, #s{backends = Backends}) ->
    case Backends of
        #{Backend := #bs{cbm = CBM}} ->
            {ok, CBM};
        #{} ->
            {error, {no_such_backend, Backend}}
    end.

on_update(_, Op) ->
    %% Update the distributed cache:
    case Op of
        open ->
            mria:clear_table(?dcache);
        close ->
            mria:clear_table(?dcache);
        {w, K, V} ->
            mria:dirty_write(?dcache, #dcache{k = {node(), K}, v = V});
        {d, K} ->
            mria:dirty_delete(?dcache, {node(), K})
    end.
