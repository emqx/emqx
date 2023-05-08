%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ee_schema_registry).

-behaviour(gen_server).
-behaviour(emqx_config_handler).

-include("emqx_ee_schema_registry.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/0,

    get_serde/1,

    add_schema/2,
    get_schema/1,
    delete_schema/1,
    list_schemas/0
]).

%% `gen_server' API
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_continue/2,
    terminate/2
]).

%% `emqx_config_handler' API
-export([post_config_update/5]).

-type schema() :: #{
    type := serde_type(),
    source := binary(),
    description => binary()
}.

%%-------------------------------------------------------------------------------------------------
%% API
%%-------------------------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec get_serde(schema_name()) -> {ok, serde_map()} | {error, not_found}.
get_serde(SchemaName) ->
    case ets:lookup(?SERDE_TAB, to_bin(SchemaName)) of
        [] ->
            {error, not_found};
        [Serde] ->
            {ok, serde_to_map(Serde)}
    end.

-spec get_schema(schema_name()) -> {ok, map()} | {error, not_found}.
get_schema(SchemaName) ->
    case emqx_config:get([?CONF_KEY_ROOT, schemas, SchemaName], undefined) of
        undefined ->
            {error, not_found};
        Config ->
            {ok, Config}
    end.

-spec add_schema(schema_name(), schema()) -> ok | {error, term()}.
add_schema(Name, Schema) ->
    RawSchema = emqx_utils_maps:binary_key_map(Schema),
    Res = emqx_conf:update(
        [?CONF_KEY_ROOT, schemas, Name],
        RawSchema,
        #{override_to => cluster}
    ),
    case Res of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

-spec delete_schema(schema_name()) -> ok | {error, term()}.
delete_schema(Name) ->
    Res = emqx_conf:remove(
        [?CONF_KEY_ROOT, schemas, Name],
        #{override_to => cluster}
    ),
    case Res of
        {ok, _} ->
            ok;
        Error ->
            Error
    end.

-spec list_schemas() -> #{schema_name() => schema()}.
list_schemas() ->
    emqx_config:get([?CONF_KEY_ROOT, schemas], #{}).

%%-------------------------------------------------------------------------------------------------
%% `emqx_config_handler' API
%%-------------------------------------------------------------------------------------------------

post_config_update(
    [?CONF_KEY_ROOT, schemas] = _Path,
    _Cmd,
    NewConf = #{schemas := NewSchemas},
    OldConf = #{},
    _AppEnvs
) ->
    OldSchemas = maps:get(schemas, OldConf, #{}),
    #{
        added := Added,
        changed := Changed0,
        removed := Removed
    } = emqx_utils_maps:diff_maps(NewSchemas, OldSchemas),
    Changed = maps:map(fun(_N, {_Old, New}) -> New end, Changed0),
    RemovedNames = maps:keys(Removed),
    case RemovedNames of
        [] ->
            ok;
        _ ->
            async_delete_serdes(RemovedNames)
    end,
    SchemasToBuild = maps:to_list(maps:merge(Changed, Added)),
    case build_serdes(SchemasToBuild) of
        ok ->
            {ok, NewConf};
        {error, Reason, SerdesToRollback} ->
            lists:foreach(fun ensure_serde_absent/1, SerdesToRollback),
            {error, Reason}
    end;
post_config_update(_Path, _Cmd, NewConf, _OldConf, _AppEnvs) ->
    {ok, NewConf}.

%%-------------------------------------------------------------------------------------------------
%% `gen_server' API
%%-------------------------------------------------------------------------------------------------

init(_) ->
    process_flag(trap_exit, true),
    create_tables(),
    Schemas = emqx_conf:get([?CONF_KEY_ROOT, schemas], #{}),
    State = #{},
    {ok, State, {continue, {build_serdes, Schemas}}}.

handle_continue({build_serdes, Schemas}, State) ->
    do_build_serdes(Schemas),
    {noreply, State}.

handle_call(_Call, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_cast({delete_serdes, Names}, State) ->
    lists:foreach(fun ensure_serde_absent/1, Names),
    ?tp(schema_registry_serdes_deleted, #{}),
    {noreply, State};
handle_cast({build_serdes, Schemas}, State) ->
    do_build_serdes(Schemas),
    {noreply, State};
handle_cast(_Cast, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%-------------------------------------------------------------------------------------------------
%% Internal fns
%%-------------------------------------------------------------------------------------------------

create_tables() ->
    ok = mria:create_table(?SERDE_TAB, [
        {type, ordered_set},
        {rlog_shard, ?SCHEMA_REGISTRY_SHARD},
        {storage, ram_copies},
        {record_name, serde},
        {attributes, record_info(fields, serde)}
    ]),
    ok = mria:create_table(?PROTOBUF_CACHE_TAB, [
        {type, set},
        {rlog_shard, ?SCHEMA_REGISTRY_SHARD},
        {storage, rocksdb_copies},
        {record_name, protobuf_cache},
        {attributes, record_info(fields, protobuf_cache)}
    ]),
    ok = mria:wait_for_tables([?SERDE_TAB, ?PROTOBUF_CACHE_TAB]),
    ok.

do_build_serdes(Schemas) ->
    %% TODO: use some kind of mutex to make each core build a
    %% different serde to avoid duplicate work.  Maybe ekka_locker?
    maps:foreach(fun do_build_serde/2, Schemas),
    ?tp(schema_registry_serdes_built, #{}).

build_serdes(Serdes) ->
    build_serdes(Serdes, []).

build_serdes([{Name, Params} | Rest], Acc0) ->
    Acc = [Name | Acc0],
    case do_build_serde(Name, Params) of
        ok ->
            build_serdes(Rest, Acc);
        {error, Error} ->
            {error, Error, Acc}
    end;
build_serdes([], _Acc) ->
    ok.

do_build_serde(Name0, #{type := Type, source := Source}) ->
    try
        Name = to_bin(Name0),
        {Serializer, Deserializer, Destructor} =
            emqx_ee_schema_registry_serde:make_serde(Type, Name, Source),
        Serde = #serde{
            name = Name,
            serializer = Serializer,
            deserializer = Deserializer,
            destructor = Destructor
        },
        ok = mria:dirty_write(?SERDE_TAB, Serde),
        ok
    catch
        Kind:Error:Stacktrace ->
            ?SLOG(
                error,
                #{
                    msg => "error_building_serde",
                    name => Name0,
                    type => Type,
                    kind => Kind,
                    error => Error,
                    stacktrace => Stacktrace
                }
            ),
            {error, Error}
    end.

ensure_serde_absent(Name) ->
    case get_serde(Name) of
        {ok, #{destructor := Destructor}} ->
            Destructor(),
            ok = mria:dirty_delete(?SERDE_TAB, to_bin(Name));
        {error, not_found} ->
            ok
    end.

async_delete_serdes(Names) ->
    gen_server:cast(?MODULE, {delete_serdes, Names}).

to_bin(A) when is_atom(A) -> atom_to_binary(A);
to_bin(B) when is_binary(B) -> B.

-spec serde_to_map(serde()) -> serde_map().
serde_to_map(#serde{} = Serde) ->
    #{
        name => Serde#serde.name,
        serializer => Serde#serde.serializer,
        deserializer => Serde#serde.deserializer,
        destructor => Serde#serde.destructor
    }.
