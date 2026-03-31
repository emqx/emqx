%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_dsch_migrate).
-moduledoc """
This module migrates data from pre-6.3.0 release
""".

%% API:
-export([restore/0]).

%% behavior callbacks:
-export([]).

%% internal exports:
-export([]).

-export_type([]).

-include("emqx_ds.hrl").
-include("emqx_dsch.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-ifdef(TEST).
-define(replay_chunk_size, 2).
-else.
-define(replay_chunk_size, 1000).
-endif.

-define(log_current, emqx_ds_dbschema_current).

-define(schema, "ds_schema").

-doc """
Name of the disk log, as in `disk_log:open([{name, Name}, ...])`
""".
-type wal() :: term().

%% Schema operations:
%%    Create a new site and schema:
-record(sop_init, {
    ver = 1 :: 1, id :: emqx_dsch:site(), next_pending_id :: emqx_dsch:pending_id()
}).
%%%   Operation with the cluster
-record(sop_set_cluster, {cluster :: emqx_dsch:cluster() | singleton}).
-record(sop_set_peer, {peer :: emqx_dsch:site(), state :: emqx_dsch:peer_state()}).
-record(sop_delete_peer, {peer :: emqx_dsch:site()}).
%%    Set DB schema:
-record(sop_set_db, {db :: emqx_ds:db(), schema :: emqx_dsch:db_schema()}).
-record(sop_drop_db, {db :: emqx_ds:db()}).
%%    Pending actions:
-record(sop_add_pending, {id = new :: emqx_dsch:pending_id() | new, p :: emqx_dsch:pending()}).
-record(sop_del_pending, {id :: emqx_dsch:pending_id()}).

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

-doc """
Persistent state of the node including some private data. (OLD)
""".
-type pstate() :: #{
    ver := 1,
    pending_ctr := emqx_dsch:pending_id(),
    pending := #{emqx_dsch:pending_id() => emqx_dsch:pending()},
    schema := emqx_dsch:schema()
}.

%%================================================================================
%% API functions
%%================================================================================

-doc """
Open `current` log and apply all entries contained there to the empty
state, thus re-creating the state before shutdown of the node.

If the log is empty, then initialize the schema by creating a new
random site ID.
""".
-spec restore() -> pstate() | ?empty_schema.
restore() ->
    File = schema_file(),
    case filelib:is_file(File) of
        true ->
            %% FIXME: close log
            case open_log(read_write, ?log_current, File) of
                ok ->
                    restore_from_wal(?log_current);
                {error, Reason} ->
                    ?tp(critical, "Failed to read durable storage schema", #{
                        reason => Reason, file => File
                    }),
                    exit(badschema)
            end;
        false ->
            ?empty_schema
    end.

%%================================================================================
%% Internal functions
%%================================================================================

-doc """
Return location of the node's old (pre-6.3.0) schema file.
(For debugging and troubleshooting).
""".
-spec schema_file() -> file:filename().
schema_file() ->
    filename:join(emqx_ds_storage_layer:base_dir(), ?schema).

-doc """
Re-create the schema state by reading the WAL.

WAL should be opened using `disk_log:open(...)`.
""".
-spec restore_from_wal(wal()) -> pstate() | ?empty_schema.
restore_from_wal(Log) ->
    replay_wal(Log, ?empty_schema, start).

-spec pure_mutate_l([schema_op()], pstate() | ?empty_schema) ->
    {ok, pstate() | ?empty_schema} | {error, _}.
pure_mutate_l([], S) ->
    {ok, S};
pure_mutate_l([Command | L], S0) ->
    case pure_mutate(Command, S0) of
        {ok, S} -> pure_mutate_l(L, S);
        {error, _} = Err -> Err
    end.

-spec with_schema(atom(), pstate(), Fun) -> {ok, pstate()} | {error, Err} when
    Fun :: fun((A) -> {ok, A} | {error, Err}).
with_schema(Key, Pstate = #{schema := Schema}, Fun) ->
    #{Key := Val0} = Schema,
    Val = Fun(Val0),
    {ok, Pstate#{schema := Schema#{Key := Val}}}.

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

-doc """
A pure function that mutates the state record accoring to a command.

Note: this function has to maintain backward-compatibility.
""".
-spec pure_mutate(schema_op(), pstate() | ?empty_schema) ->
    {ok, pstate() | ?empty_schema} | {error, _}.
pure_mutate(Command, ?empty_schema) ->
    %% Only one command is allowed in the empty state:
    case Command of
        #sop_init{ver = Ver, id = Site, next_pending_id = PendingCtr} ->
            {ok, new_empty_pstate(Ver, Site, PendingCtr)};
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
pure_mutate(
    #sop_add_pending{id = MaybeId, p = Pending}, S0 = #{pending := Pend0, pending_ctr := NextId}
) ->
    case MaybeId of
        new ->
            Id = NextId,
            S1 = S0#{pending_ctr := NextId + 1};
        Id ->
            S1 = S0
    end,
    S = S1#{pending := Pend0#{Id => Pending}},
    {ok, S};
pure_mutate(#sop_del_pending{id = Id}, S0 = #{pending := Pend0}) ->
    S = S0#{pending := maps:remove(Id, Pend0)},
    {ok, S};
pure_mutate(Cmd, _S) ->
    {error, {unknown_comand, Cmd}}.

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

-spec new_empty_pstate(1, emqx_dsch:site(), emqx_dsch:pending_id()) -> pstate().
new_empty_pstate(Ver, Site, PendingCtr) ->
    #{
        ver => Ver,
        pending_ctr => PendingCtr,
        pending => #{},
        schema => #{
            site => Site,
            cluster => singleton,
            peers => #{},
            dbs => #{}
        }
    }.
