%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_schema_registry).

-behaviour(gen_server).
-behaviour(emqx_config_handler).
-behaviour(emqx_config_backup).

-include("emqx_schema_registry.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    start_link/0,
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

%% Data backup
-export([
    import_config/1
]).

%% for testing
-export([
    get_serde/1
]).

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

-spec get_serde(schema_name()) -> {ok, serde()} | {error, not_found}.
get_serde(SchemaName) ->
    case ets:lookup(?SERDE_TAB, to_bin(SchemaName)) of
        [] ->
            {error, not_found};
        [Serde] ->
            {ok, Serde}
    end.

-spec get_schema(schema_name()) -> {ok, map()} | {error, not_found}.
get_schema(SchemaName) ->
    case
        emqx_config:get(
            [?CONF_KEY_ROOT, schemas, schema_name_bin_to_atom(SchemaName)], undefined
        )
    of
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
%% remove
post_config_update(
    [?CONF_KEY_ROOT, schemas, Name],
    '$remove',
    _NewSchemas,
    _OldSchemas,
    _AppEnvs
) ->
    async_delete_serdes([Name]),
    ok;
%% add or update
post_config_update(
    [?CONF_KEY_ROOT, schemas, NewName],
    _Cmd,
    NewSchemas,
    %% undefined or OldSchemas
    _,
    _AppEnvs
) ->
    case build_serdes([{NewName, NewSchemas}]) of
        ok ->
            {ok, #{NewName => NewSchemas}};
        {error, Reason, SerdesToRollback} ->
            lists:foreach(fun ensure_serde_absent/1, SerdesToRollback),
            {error, Reason}
    end;
post_config_update(?CONF_KEY_PATH, _Cmd, NewConf = #{schemas := NewSchemas}, OldConf, _AppEnvs) ->
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
%% Data backup
%%-------------------------------------------------------------------------------------------------

import_config(#{<<"schema_registry">> := #{<<"schemas">> := Schemas} = SchemaRegConf}) ->
    OldSchemas = emqx:get_raw_config([?CONF_KEY_ROOT, schemas], #{}),
    SchemaRegConf1 = SchemaRegConf#{<<"schemas">> => maps:merge(OldSchemas, Schemas)},
    case emqx_conf:update(?CONF_KEY_PATH, SchemaRegConf1, #{override_to => cluster}) of
        {ok, #{raw_config := #{<<"schemas">> := NewRawSchemas}}} ->
            Changed = maps:get(changed, emqx_utils_maps:diff_maps(NewRawSchemas, OldSchemas)),
            ChangedPaths = [[?CONF_KEY_ROOT, schemas, Name] || Name <- maps:keys(Changed)],
            {ok, #{root_key => ?CONF_KEY_ROOT, changed => ChangedPaths}};
        Error ->
            {error, #{root_key => ?CONF_KEY_ROOT, reason => Error}}
    end;
import_config(_RawConf) ->
    {ok, #{root_key => ?CONF_KEY_ROOT, changed => []}}.

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
    ok = emqx_utils_ets:new(?SERDE_TAB, [public, ordered_set, {keypos, #serde.name}]),
    %% have to create the table for jesse_database otherwise the on-demand table will disappear
    %% when the caller process dies
    ok = emqx_utils_ets:new(jesse_ets, [public, ordered_set]),
    ok = mria:create_table(?PROTOBUF_CACHE_TAB, [
        {type, set},
        {rlog_shard, ?SCHEMA_REGISTRY_SHARD},
        {storage, rocksdb_copies},
        {record_name, protobuf_cache},
        {attributes, record_info(fields, protobuf_cache)}
    ]),
    ok = mria:wait_for_tables([?PROTOBUF_CACHE_TAB]),
    ok.

do_build_serdes(Schemas) ->
    %% We build a special serde for the Sparkplug B payload. This serde is used
    %% by the rule engine functions sparkplug_decode/1 and sparkplug_encode/1.
    ok = maybe_build_sparkplug_b_serde(),
    %% TODO: use some kind of mutex to make each core build a
    %% different serde to avoid duplicate work.  Maybe ekka_locker?
    maps:foreach(fun do_build_serde/2, Schemas),
    ?tp(schema_registry_serdes_built, #{}).

maybe_build_sparkplug_b_serde() ->
    case get_schema(?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME) of
        {error, not_found} ->
            do_build_serde(
                ?EMQX_SCHEMA_REGISTRY_SPARKPLUGB_SCHEMA_NAME,
                #{
                    type => protobuf,
                    source => get_schema_source(<<"sparkplug_b.proto">>)
                }
            );
        {ok, _} ->
            ok
    end.

get_schema_source(Filename) ->
    {ok, App} = application:get_application(),
    FilePath =
        case code:priv_dir(App) of
            {error, bad_name} ->
                erlang:error(
                    {error, <<"Could not find data directory (priv) for Schema Registry">>}
                );
            Dir ->
                filename:join(Dir, Filename)
        end,
    case file:read_file(FilePath) of
        {ok, Content} ->
            Content;
        {error, Reason} ->
            erlang:error({error, Reason})
    end.

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

do_build_serde(Name, Serde) when not is_binary(Name) ->
    do_build_serde(to_bin(Name), Serde);
do_build_serde(Name, #{type := Type, source := Source}) ->
    try
        Serde = emqx_schema_registry_serde:make_serde(Type, Name, Source),
        true = ets:insert(?SERDE_TAB, Serde),
        ok
    catch
        Kind:Error:Stacktrace ->
            ?SLOG(
                error,
                #{
                    msg => "error_building_serde",
                    name => Name,
                    type => Type,
                    kind => Kind,
                    error => Error,
                    stacktrace => Stacktrace
                }
            ),
            {error, Error}
    end.

ensure_serde_absent(Name) when not is_binary(Name) ->
    ensure_serde_absent(to_bin(Name));
ensure_serde_absent(Name) ->
    case get_serde(Name) of
        {ok, Serde} ->
            ok = emqx_schema_registry_serde:destroy(Serde),
            _ = ets:delete(?SERDE_TAB, Name),
            ok;
        {error, not_found} ->
            ok
    end.

async_delete_serdes(Names) ->
    gen_server:cast(?MODULE, {delete_serdes, Names}).

to_bin(A) when is_atom(A) -> atom_to_binary(A);
to_bin(B) when is_binary(B) -> B.

schema_name_bin_to_atom(Bin) when size(Bin) > 255 ->
    erlang:throw(
        iolist_to_binary(
            io_lib:format(
                "Name is is too long."
                " Please provide a shorter name (<= 255 bytes)."
                " The name that is too long: \"~s\"",
                [Bin]
            )
        )
    );
schema_name_bin_to_atom(Bin) ->
    binary_to_atom(Bin, utf8).
