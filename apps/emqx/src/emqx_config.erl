%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_config).

-compile({no_auto_import, [get/0, get/1, put/2, erase/1]}).
-elvis([{elvis_style, god_modules, disable}]).
-include("logger.hrl").
-include("emqx.hrl").
-include("emqx_schema.hrl").
-include("emqx_config.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("hocon/include/hocon.hrl").
-include("emqx_config.hrl").

-export([
    init_load/1,
    init_load/2,
    read_override_conf/1,
    has_deprecated_file/0,
    delete_override_conf_files/0,
    check_config/2,
    fill_defaults/1,
    fill_defaults/2,
    fill_defaults/3,
    save_configs/5,
    save_to_app_env/1,
    save_to_config_map/2,
    save_to_override_conf/3,
    config_files/0,
    include_dirs/0
]).
-export([merge_envs/2]).

-export([
    get_root/1,
    get_root_raw/1,
    get_root_namespaced/2,
    get_root_raw_namespaced/2
]).

-export([get_default_value/1]).

-export([
    get/1,
    get/2,
    find/1,
    find_raw/1,
    put/1,
    put/2,
    force_put/2,
    force_put/3,
    erase/1
]).

-export([
    get_raw/1,
    get_raw/2,
    put_raw/1,
    put_raw/2
]).

-export([
    save_schema_mod_and_names/1,
    get_schema_mod/0,
    get_schema_mod/1,
    get_root_names/0
]).

-export([
    get_zone_conf/2,
    get_zone_conf/3,
    put_zone_conf/3
]).

-export([
    get_listener_conf/3,
    get_listener_conf/4,
    put_listener_conf/4,
    find_listener_conf/3
]).

-export([
    add_handlers/0,
    remove_handlers/0
]).

-export([create_tables/0]).
-export([ensure_atom_conf_path/2]).
-export([load_config_files/2]).
-export([upgrade_raw_conf/2]).

%% Namespaced configs
-export([
    add_allowed_namespaced_config_root/1,
    get_namespaced/2,
    get_namespaced/3,
    get_raw_namespaced/2,
    get_raw_namespaced/3,
    get_all_namespaces_containing/1,
    get_all_raw_namespaced_configs/0,
    get_all_roots_from_namespace/1,
    get_root_from_all_namespaces/1,
    get_root_from_all_namespaces/2,
    get_raw_root_from_all_namespaces/1,
    save_configs_namespaced/6,
    save_configs_namespaced_tx/5,
    get_all_namespace_config_errors/0,
    get_namespace_config_errors/1,
    clear_all_invalid_namespaced_configs/0,
    erase_namespaced_configs/1,
    namespaced_config_allowed_roots/0
]).

-ifdef(TEST).
-export([erase_all/0, backup_and_write/2, cluster_hocon_file/0, base_hocon_file/0]).
-export([
    seed_defaults_for_all_roots_namespaced/2,
    seed_defaults_for_all_roots_namespaced/3
]).
-endif.

-include("logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-define(CONF, conf).
-define(RAW_CONF, raw_conf).
-define(NS_CONF(NS), {ns_conf, NS}).
-define(PERSIS_SCHEMA_MODS, {?MODULE, schema_mods}).
-define(PERSIS_KEY(TYPE, ROOT), {?MODULE, TYPE, ROOT}).
-define(ZONE_CONF_PATH(ZONE, PATH), [zones, ZONE | PATH]).
-define(LISTENER_CONF_PATH(TYPE, LISTENER, PATH), [listeners, TYPE, LISTENER | PATH]).
-define(INVALID_NS_CONF_PT_KEY(NS), {?MODULE, {corrupt_ns_conf, NS}}).
-define(ALLOWED_NS_ROOT_KEYS_PT_KEY, {?MODULE, allowed_ns_root_key}).

-export_type([
    namespace/0,
    maybe_namespace/0,
    update_request/0,
    raw_config/0,
    config/0,
    app_envs/0,
    update_opts/0,
    cluster_rpc_opts/0,
    update_cmd/0,
    update_args/0,
    update_error/0,
    update_result/0,
    runtime_config_key_path/0
]).

-define(CONFIG_TAB, emqx_config).
-record(?CONFIG_TAB, {
    %% {Namespace, RootKey}
    root_key,
    raw_value,
    extra = #{}
}).

-type update_request() :: term().
-type update_cmd() :: {update, update_request()} | remove.
-type update_opts() :: #{
    %% rawconf_with_defaults:
    %%   fill the default values into the `raw_config` field of the return value
    %%   defaults to `false`
    rawconf_with_defaults => boolean(),
    %% persistent:
    %%   save the updated config to the emqx_override.conf file
    %%   defaults to `true`
    persistent => boolean(),
    override_to => local | cluster,
    lazy_evaluator => fun((function()) -> term()),
    namespace => binary()
}.
-type update_args() :: {update_cmd(), Opts :: update_opts()}.
-type update_stage() :: pre_config_update | post_config_update.
-type update_error() :: {update_stage(), module(), term()} | {save_configs, term()} | term().
-type update_result() :: #{
    config => emqx_config:config(),
    raw_config => emqx_config:raw_config(),
    post_config_update => #{module() => any()}
}.
-type cluster_rpc_opts() :: #{kind => ?KIND_INITIATE | ?KIND_REPLICATE}.

-type namespace() :: binary().
-type maybe_namespace() :: ?global_ns | namespace().

%% raw_config() is the config that is NOT parsed and translated by hocon schema
-type raw_config() :: #{binary() => term()} | list() | undefined.
%% config() is the config that is parsed and translated by hocon schema
-type config() :: #{atom() => term()} | list() | undefined.
-type app_envs() :: [proplists:property()].

-type runtime_config_key_path() :: [atom()].

-spec create_tables() -> [atom()].
create_tables() ->
    ok = mria:create_table(?CONFIG_TAB, [
        {type, ordered_set},
        {rlog_shard, ?COMMON_SHARD},
        {storage, disc_copies},
        {record_name, ?CONFIG_TAB},
        {attributes, record_info(fields, ?CONFIG_TAB)}
    ]),
    Tables = [?CONFIG_TAB],
    ok = mria:wait_for_tables(Tables),
    Tables.

%% @doc For the given path, get root value enclosed in a single-key map.
-spec get_root(emqx_utils_maps:config_key_path()) -> map().
get_root([RootName | _]) ->
    #{RootName => do_get(?CONF, [RootName], #{})}.

get_root_namespaced([RootName | _], Namespace) ->
    #{RootName => do_get(?NS_CONF(Namespace), [RootName], #{})}.

get_all_namespaces_containing(RootKey0) ->
    RootKey = bin(RootKey0),
    MatchHead = erlang:make_tuple(
        record_info(size, ?CONFIG_TAB),
        '_',
        [{#?CONFIG_TAB.root_key, {'$1', RootKey}}]
    ),
    MS = [{MatchHead, [], ['$1']}],
    lists:usort(mnesia:dirty_select(?CONFIG_TAB, MS)).

-spec get_all_raw_namespaced_configs() -> #{namespace() => #{binary() => term()}}.
get_all_raw_namespaced_configs() ->
    MatchHead = erlang:make_tuple(record_info(size, ?CONFIG_TAB), '_'),
    lists:foldl(
        fun(#?CONFIG_TAB{root_key = {Namespace, RootKey}, raw_value = Config}, Acc) ->
            maps:update_with(
                Namespace,
                fun(NSConfigs) -> NSConfigs#{RootKey => Config} end,
                #{RootKey => Config},
                Acc
            )
        end,
        #{},
        mnesia:dirty_match_object(?CONFIG_TAB, MatchHead)
    ).

get_all_roots_from_namespace(Namespace) when is_binary(Namespace) ->
    MatchHead = erlang:make_tuple(
        record_info(size, ?CONFIG_TAB),
        '_',
        [{#?CONFIG_TAB.root_key, {Namespace, '_'}}]
    ),
    RootConfigs = mnesia:dirty_match_object(?CONFIG_TAB, MatchHead),
    lists:foldl(
        fun(#?CONFIG_TAB{root_key = {_, RootKey}, raw_value = Config}, Acc) ->
            Acc#{RootKey => Config}
        end,
        #{},
        RootConfigs
    ).

get_root_from_all_namespaces(RootKey) ->
    do_get_root_from_all_namespaces(RootKey, error).

get_root_from_all_namespaces(RootKey, Default) ->
    do_get_root_from_all_namespaces(RootKey, {value, Default}).

do_get_root_from_all_namespaces(RootKey0, DefaultAction) ->
    RootKey = bin(RootKey0),
    MS = erlang:make_tuple(
        record_info(size, ?CONFIG_TAB),
        '_',
        [{#?CONFIG_TAB.root_key, {'_', RootKey}}]
    ),
    Recs = mnesia:dirty_match_object(?CONFIG_TAB, MS),
    lists:foldl(
        fun(#?CONFIG_TAB{root_key = {Namespace, _RootKey}}, Acc) ->
            Config =
                case DefaultAction of
                    error ->
                        get_namespaced([RootKey0], Namespace);
                    {value, DefaultValue} ->
                        get_namespaced([RootKey0], Namespace, DefaultValue)
                end,
            Acc#{Namespace => Config}
        end,
        #{},
        Recs
    ).

get_raw_root_from_all_namespaces(RootKey0) ->
    RootKey = bin(RootKey0),
    MS = erlang:make_tuple(
        record_info(size, ?CONFIG_TAB),
        '_',
        [{#?CONFIG_TAB.root_key, {'_', RootKey}}]
    ),
    Recs = mnesia:dirty_match_object(?CONFIG_TAB, MS),
    lists:foldl(
        fun(#?CONFIG_TAB{root_key = {Namespace, _RootKey}, raw_value = RawConfig}, Acc) ->
            Acc#{Namespace => RawConfig}
        end,
        #{},
        Recs
    ).

%% @doc For the given path, get raw root value enclosed in a single-key map.
%% key is ensured to be binary.
get_root_raw([RootName | _]) ->
    #{bin(RootName) => get_raw([RootName], #{})}.

get_root_raw_namespaced([RootName | _], Namespace) ->
    #{bin(RootName) => do_get_raw_namespaced([RootName], Namespace, {value, #{}})}.

do_get_raw_namespaced([Root | KeyRest] = KeyPath, Namespace, DefaultAction) ->
    RootBin = bin(Root),
    case mnesia:dirty_read(?CONFIG_TAB, {Namespace, RootBin}) of
        [#?CONFIG_TAB{raw_value = #{} = RawConfig}] when DefaultAction == error ->
            deep_get_namespaced(RootBin, KeyRest, RawConfig);
        [#?CONFIG_TAB{raw_value = #{} = RawConfig}] ->
            {value, Default} = DefaultAction,
            emqx_utils_maps:deep_get(KeyRest, RawConfig, Default);
        _ ->
            case DefaultAction of
                error ->
                    error({config_not_found, KeyPath});
                {value, Default} ->
                    Default
            end
    end.

deep_get_namespaced(Root, KeyPath, Config) ->
    try
        emqx_utils_maps:deep_get(KeyPath, Config)
    catch
        error:{config_not_found, _} ->
            error({config_not_found, [Root | KeyPath]})
    end.

%% @doc Get a config value for the given path.
%% The path should at least include root config name.
-spec get(runtime_config_key_path()) -> term().
get(KeyPath) -> do_get(?CONF, KeyPath).

-spec get(runtime_config_key_path(), term()) -> term().
get(KeyPath, Default) -> do_get(?CONF, KeyPath, Default).

get_namespaced(KeyPath, Namespace) when is_binary(Namespace) ->
    do_get(?NS_CONF(Namespace), KeyPath).

get_namespaced(KeyPath, Namespace, Default) when is_binary(Namespace) ->
    do_get(?NS_CONF(Namespace), KeyPath, Default).

-spec find(runtime_config_key_path()) ->
    {ok, term()} | {not_found, emqx_utils_maps:config_key_path(), term()}.
find([]) ->
    case do_get(?CONF, [], ?CONFIG_NOT_FOUND_MAGIC) of
        ?CONFIG_NOT_FOUND_MAGIC -> {not_found, []};
        Res -> {ok, Res}
    end;
find(AtomKeyPath) ->
    emqx_utils_maps:deep_find(AtomKeyPath, get_root(AtomKeyPath)).

-spec find_raw(emqx_utils_maps:config_key_path()) ->
    {ok, term()} | {not_found, emqx_utils_maps:config_key_path(), term()}.
find_raw([]) ->
    case do_get_raw([], ?CONFIG_NOT_FOUND_MAGIC) of
        ?CONFIG_NOT_FOUND_MAGIC -> {not_found, []};
        Res -> {ok, Res}
    end;
find_raw(KeyPath) ->
    emqx_utils_maps:deep_find([bin(Key) || Key <- KeyPath], get_root_raw(KeyPath)).

-spec get_zone_conf(atom(), emqx_utils_maps:config_key_path()) -> term().
get_zone_conf(Zone, KeyPath) ->
    ?MODULE:get(?ZONE_CONF_PATH(Zone, KeyPath)).

-spec get_zone_conf(atom(), emqx_utils_maps:config_key_path(), term()) -> term().
get_zone_conf(Zone, KeyPath, Default) ->
    ?MODULE:get(?ZONE_CONF_PATH(Zone, KeyPath), Default).

-spec put_zone_conf(atom(), emqx_utils_maps:config_key_path(), term()) -> ok.
put_zone_conf(Zone, KeyPath, Conf) ->
    ?MODULE:put(?ZONE_CONF_PATH(Zone, KeyPath), Conf).

-spec get_listener_conf(atom(), atom(), emqx_utils_maps:config_key_path()) -> term().
get_listener_conf(Type, Listener, KeyPath) ->
    ?MODULE:get(?LISTENER_CONF_PATH(Type, Listener, KeyPath)).

-spec get_listener_conf(atom(), atom(), emqx_utils_maps:config_key_path(), term()) -> term().
get_listener_conf(Type, Listener, KeyPath, Default) ->
    ?MODULE:get(?LISTENER_CONF_PATH(Type, Listener, KeyPath), Default).

-spec put_listener_conf(atom(), atom(), emqx_utils_maps:config_key_path(), term()) -> ok.
put_listener_conf(Type, Listener, KeyPath, Conf) ->
    ?MODULE:put(?LISTENER_CONF_PATH(Type, Listener, KeyPath), Conf).

-spec find_listener_conf(atom(), atom(), emqx_utils_maps:config_key_path()) ->
    {ok, term()} | {not_found, emqx_utils_maps:config_key_path(), term()}.
find_listener_conf(Type, Listener, KeyPath) ->
    find(?LISTENER_CONF_PATH(Type, Listener, KeyPath)).

-spec put(map()) -> ok.
put(Config) ->
    put_with_order(Config).

put1(Config) ->
    maps:fold(
        fun(RootName, RootValue, _) ->
            ?MODULE:put([atom(RootName)], RootValue)
        end,
        ok,
        Config
    ).

erase(RootName) ->
    persistent_term:erase(?PERSIS_KEY(?CONF, atom(RootName))),
    persistent_term:erase(?PERSIS_KEY(?RAW_CONF, bin(RootName))),
    ok.

-spec put(emqx_utils_maps:config_key_path(), term()) -> ok.
put(KeyPath, Config) ->
    Putter = fun(_Path, Map, Value) ->
        maybe_update_zone(KeyPath, Map, Value)
    end,
    do_put(?CONF, Putter, KeyPath, Config).

%% Puts value into configuration even if path doesn't exist
%% For paths of non-existing atoms use force_put(KeyPath, Config, unsafe)
-spec force_put(emqx_utils_maps:config_key_path(), term()) -> ok.
force_put(KeyPath, Config) ->
    force_put(KeyPath, Config, safe).

-spec force_put(emqx_utils_maps:config_key_path(), term(), safe | unsafe) -> ok.
force_put(KeyPath0, Config, Safety) ->
    KeyPath =
        case Safety of
            safe -> KeyPath0;
            unsafe -> [unsafe_atom(Key) || Key <- KeyPath0]
        end,
    Putter = fun(Path, Map, Value) ->
        emqx_utils_maps:deep_force_put(Path, Map, Value)
    end,
    do_put(?CONF, Putter, KeyPath, Config).

-spec get_default_value(emqx_utils_maps:config_key_path()) -> {ok, term()} | {error, term()}.
get_default_value([RootName | _] = KeyPath) ->
    BinKeyPath = [bin(Key) || Key <- KeyPath],
    case find_raw([RootName]) of
        {ok, RawConf} ->
            RawConf1 = emqx_utils_maps:deep_remove(BinKeyPath, #{bin(RootName) => RawConf}),
            try fill_defaults(get_schema_mod(RootName), RawConf1, #{}) of
                FullConf ->
                    case emqx_utils_maps:deep_find(BinKeyPath, FullConf) of
                        {not_found, _, _} -> {error, no_default_value};
                        {ok, Val} -> {ok, Val}
                    end
            catch
                error:Reason -> {error, Reason}
            end;
        {not_found, _, _} ->
            {error, {rootname_not_found, RootName}}
    end.

-spec get_raw(emqx_utils_maps:config_key_path()) -> term().
get_raw([Root | _] = KeyPath) when is_binary(Root) -> do_get_raw(KeyPath);
get_raw([Root | T]) -> get_raw([bin(Root) | T]);
get_raw([]) -> do_get_raw([]).

-spec get_raw(emqx_utils_maps:config_key_path(), term()) -> term().
get_raw([Root | _] = KeyPath, Default) when is_binary(Root) -> do_get_raw(KeyPath, Default);
get_raw([Root | T], Default) -> get_raw([bin(Root) | T], Default);
get_raw([], Default) -> do_get_raw([], Default).

get_raw_namespaced([], Namespace) ->
    get_all_roots_from_namespace(Namespace);
get_raw_namespaced([_Root | _] = KeyPath0, Namespace) ->
    KeyPath = lists:map(fun bin/1, KeyPath0),
    do_get_raw_namespaced(KeyPath, Namespace, error).

get_raw_namespaced([], Namespace, _Default) ->
    get_all_roots_from_namespace(Namespace);
get_raw_namespaced([_Root | _] = KeyPath0, Namespace, Default) ->
    KeyPath = lists:map(fun bin/1, KeyPath0),
    do_get_raw_namespaced(KeyPath, Namespace, {value, Default}).

-spec put_raw(map()) -> ok.
put_raw(Config) ->
    maps:fold(
        fun(RootName, RootV, _) ->
            ?MODULE:put_raw([bin(RootName)], RootV)
        end,
        ok,
        hocon_maps:ensure_plain(Config)
    ).

-spec put_raw(emqx_utils_maps:config_key_path(), term()) -> ok.
put_raw(KeyPath0, Config) ->
    KeyPath = [bin(K) || K <- KeyPath0],
    Putter = fun(Path, Map, Value) ->
        emqx_utils_maps:deep_force_put(Path, Map, Value)
    end,
    do_put(?RAW_CONF, Putter, KeyPath, Config).

-spec put_namespaced(binary(), map()) -> ok.
put_namespaced(Namespace, Config) when is_binary(Namespace) ->
    maps:fold(
        fun(RootName, RootValue, _) ->
            put_namespaced(Namespace, [atom(RootName)], RootValue)
        end,
        ok,
        Config
    ).

-spec put_namespaced(binary(), emqx_utils_maps:config_key_path(), term()) -> ok.
put_namespaced(Namespace, KeyPath, Config) when is_binary(Namespace) ->
    [_RootName | KeyPathRest] = KeyPath,
    Putter = fun(_Path, RootValue, Value) ->
        emqx_utils_maps:deep_put(KeyPathRest, RootValue, Value)
    end,
    do_put(?NS_CONF(Namespace), Putter, KeyPath, Config).

%%============================================================================
%% Load/Update configs From/To files
%%============================================================================
init_load(SchemaMod) ->
    init_load(SchemaMod, config_files()).

%% @doc Initial load of the given config files.
%% NOTE: The order of the files is significant, configs from files ordered
%% in the rear of the list overrides prior values.
-spec init_load(module(), [string()] | binary() | hocon:config()) -> ok.
init_load(SchemaMod, Conf) when is_list(Conf) orelse is_binary(Conf) ->
    ok = save_schema_mod_and_names(SchemaMod),
    HasDeprecatedFile = has_deprecated_file(),
    RawConf0 = load_config_files(HasDeprecatedFile, Conf),
    warning_deprecated_root_key(RawConf0),
    RawConf1 =
        case HasDeprecatedFile of
            true ->
                overlay_v0(SchemaMod, RawConf0);
            false ->
                overlay_v1(SchemaMod, RawConf0)
        end,
    RawConf2 = upgrade_raw_conf(SchemaMod, RawConf1),
    RawConf3 = fill_defaults_for_all_roots(SchemaMod, RawConf2),
    %% check configs against the schema
    {AppEnvs, CheckedConf} = check_config(SchemaMod, RawConf3, #{}),
    save_to_app_env(AppEnvs),
    ok = save_to_config_map(CheckedConf, RawConf3),
    maybe_init_default_zone(),
    load_namespaced_configs(),
    ok.

upgrade_raw_conf(SchemaMod, RawConf) ->
    case erlang:function_exported(SchemaMod, upgrade_raw_conf, 1) of
        true ->
            %% TODO make it a schema module behaviour in hocon_schema
            apply(SchemaMod, upgrade_raw_conf, [RawConf]);
        false ->
            RawConf
    end.

%% Merge environment variable overrides on top, then merge with overrides.
overlay_v0(SchemaMod, RawConf) when is_map(RawConf) ->
    RawConfWithEnvs = merge_envs(SchemaMod, RawConf),
    Overrides = read_override_confs(),
    hocon:deep_merge(RawConfWithEnvs, Overrides).

%% Merge environment variable overrides on top.
overlay_v1(SchemaMod, RawConf) when is_map(RawConf) ->
    merge_envs(SchemaMod, RawConf).

%% @doc Read merged cluster + local overrides.
read_override_confs() ->
    ClusterOverrides = read_override_conf(#{override_to => cluster}),
    LocalOverrides = read_override_conf(#{override_to => local}),
    hocon:deep_merge(ClusterOverrides, LocalOverrides).

%% keep the raw and non-raw conf has the same keys to make update raw conf easier.
fill_defaults_for_all_roots(SchemaMod, RawConf0) ->
    RootSchemas = hocon_schema:roots(SchemaMod),
    %% the roots which are missing from the loaded configs
    MissingRoots = lists:filtermap(
        fun({BinName, Sc}) ->
            case maps:is_key(BinName, RawConf0) orelse is_already_loaded(BinName) of
                true -> false;
                false -> {true, Sc}
            end
        end,
        RootSchemas
    ),
    RawConf = lists:foldl(
        fun({RootName, Schema}, Acc) ->
            Acc#{bin(RootName) => seed_default(Schema)}
        end,
        RawConf0,
        MissingRoots
    ),
    fill_defaults(RawConf).

-ifdef(TEST).
seed_defaults_for_all_roots_namespaced(SchemaMod, Namespace) when is_binary(Namespace) ->
    seed_defaults_for_all_roots_namespaced(SchemaMod, Namespace, _ClusterRPCOpts = #{}).

seed_defaults_for_all_roots_namespaced(SchemaMod, Namespace, _ClusterRPCOpts) when
    is_binary(Namespace)
->
    RootSchemas = hocon_schema:roots(SchemaMod),
    AllowedNSRoots = namespaced_config_allowed_roots(),
    RawConf0 = lists:foldl(
        fun({BinRootName, {_RootName, Schema}}, Acc) ->
            case maps:get(BinRootName, AllowedNSRoots, false) of
                true ->
                    Acc#{BinRootName => seed_default(Schema)};
                false ->
                    Acc
            end
        end,
        #{},
        RootSchemas
    ),
    RawConf = fill_defaults(RawConf0),
    {_AppEnvs, CheckedConf} = check_config(SchemaMod, RawConf, #{}),
    Opts = #{},
    {atomic, ok} = mria:transaction(?COMMON_SHARD, fun() ->
        lists:foreach(
            fun(RootKeyAtom) ->
                RootKeyBin = atom_to_binary(RootKeyAtom, utf8),
                %% Checked conf may contain root keys that are not allowed.
                maybe
                    {ok, OneRawConf} ?= maps:find(RootKeyBin, RawConf),
                    OneCheckedConf = maps:get(RootKeyAtom, CheckedConf),
                    save_configs_namespaced_tx(
                        Namespace, RootKeyBin, OneCheckedConf, OneRawConf, Opts
                    )
                end
            end,
            maps:keys(CheckedConf)
        )
    end),
    put_namespaced(Namespace, CheckedConf),
    ok.

-endif.

erase_namespaced_configs(Namespace) when is_binary(Namespace) ->
    MS = erlang:make_tuple(
        record_info(size, ?CONFIG_TAB),
        '_',
        [{#?CONFIG_TAB.root_key, {Namespace, '_'}}]
    ),
    _ = mria:match_delete(?CONFIG_TAB, MS),
    lists:foreach(
        fun(RootName) ->
            persistent_term:erase(?PERSIS_KEY(?NS_CONF(Namespace), atom(RootName)))
        end,
        get_root_names()
    ),
    ok.

load_namespaced_configs() ->
    %% Ensure tables are ready when loading.
    _ = emqx_config:create_tables(),
    AllowedNSRoots = maps:keys(namespaced_config_allowed_roots()),
    RootKeysToSchemaMods = get_schema_mod(),
    lists:foreach(
        fun({Namespace, RootKeyBin}) ->
            case RootKeysToSchemaMods of
                #{RootKeyBin := SchemaMod} ->
                    do_load_namespaced_config(SchemaMod, AllowedNSRoots, Namespace, RootKeyBin);
                #{} ->
                    ?SLOG(error, #{
                        msg => "loaded_unknown_root_key_in_namespace_config",
                        namespace => Namespace,
                        root_key => RootKeyBin
                    })
            end
        end,
        mnesia:dirty_all_keys(?CONFIG_TAB)
    ).

do_load_namespaced_config(SchemaMod, AllowedNSRoots, Namespace, RootKeyBin) ->
    Key = {Namespace, RootKeyBin},
    [#?CONFIG_TAB{raw_value = RawConf0}] = mnesia:dirty_read(?CONFIG_TAB, Key),
    RootKeyAtom = atom(RootKeyBin),
    RawConf = #{RootKeyBin => RawConf0},
    case check_config_namespaced(SchemaMod, RawConf, AllowedNSRoots) of
        {ok, #{} = CheckedRoot} ->
            CheckedConf = maps:get(RootKeyAtom, CheckedRoot, #{}),
            clear_invalid_namespaced_config(Namespace, RootKeyBin),
            put_namespaced(Namespace, [RootKeyAtom], CheckedConf);
        {error, HoconErrors} ->
            mark_namespaced_config_invalid(Namespace, RootKeyBin, HoconErrors)
    end.

mark_namespaced_config_invalid(Namespace, RootKeyBin, HoconErrors) when is_binary(Namespace) ->
    PrevErrors = persistent_term:get(?INVALID_NS_CONF_PT_KEY(Namespace), #{}),
    Errors = PrevErrors#{RootKeyBin => HoconErrors},
    persistent_term:put(?INVALID_NS_CONF_PT_KEY(Namespace), Errors).

get_all_namespace_config_errors() ->
    [{Namespace, Errors} || {?INVALID_NS_CONF_PT_KEY(Namespace), Errors} <- persistent_term:get()].

get_namespace_config_errors(Namespace) when is_binary(Namespace) ->
    persistent_term:get(?INVALID_NS_CONF_PT_KEY(Namespace), undefined).

clear_invalid_namespaced_config(Namespace, RootKeyBin) when is_binary(Namespace) ->
    case get_namespace_config_errors(Namespace) of
        undefined ->
            ok;
        #{} = Errors0 ->
            Errors = maps:remove(RootKeyBin, Errors0),
            case map_size(Errors) == 0 of
                true ->
                    persistent_term:erase(?INVALID_NS_CONF_PT_KEY(Namespace)),
                    emqx_corrupt_namespace_config_checker:clear(Namespace);
                false ->
                    persistent_term:put(?INVALID_NS_CONF_PT_KEY(Namespace), Errors),
                    emqx_corrupt_namespace_config_checker:check(Namespace)
            end
    end.

clear_all_invalid_namespaced_configs() ->
    lists:foreach(
        fun
            ({?INVALID_NS_CONF_PT_KEY(_Namespace) = Key, _V}) ->
                persistent_term:erase(Key);
            (_) ->
                ok
        end,
        persistent_term:get()
    ).

%% So far, this can only return true when testing.
%% e.g. when testing an app, we need to load its config first
%% then start emqx_conf application which will load the
%% possibly empty config again (then filled with defaults).
-ifdef(TEST).
is_already_loaded(Name) ->
    ?MODULE:get_raw([Name], #{}) =/= #{}.
-else.
is_already_loaded(_) ->
    false.
-endif.

%% if a root is not found in the raw conf, fill it with default values.
seed_default(Schema) ->
    case hocon_schema:field_schema(Schema, default) of
        undefined ->
            %% so far all roots without a default value are objects
            #{};
        Value ->
            Value
    end.

load_config_files(HasDeprecatedFile, Conf) ->
    IncDirs = include_dirs(),
    case do_parse_hocon(HasDeprecatedFile, Conf, IncDirs) of
        {ok, HoconMap} ->
            HoconMap;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_load_config_file",
                reason => Reason,
                pwd => file:get_cwd(),
                include_dirs => IncDirs,
                config_file => Conf
            }),
            error(failed_to_load_config_file)
    end.

do_parse_hocon(true, Conf, IncDirs) ->
    Opts = #{format => map, include_dirs => IncDirs},
    case is_binary(Conf) of
        true -> hocon:binary(Conf, Opts);
        false -> hocon:files(Conf, Opts)
    end;
do_parse_hocon(false, Conf, IncDirs) ->
    Opts = #{format => map, include_dirs => IncDirs},
    case is_binary(Conf) of
        true ->
            %% only used in test
            hocon:binary(Conf, Opts);
        false ->
            BaseHocon = base_hocon_file(),
            ClusterFile = cluster_hocon_file(),
            hocon:files([BaseHocon, ClusterFile | Conf], Opts)
    end.

include_dirs() ->
    [filename:join(emqx:data_dir(), "configs")].

merge_envs(SchemaMod, RawConf) ->
    Opts = #{
        required => false,
        format => map,
        apply_override_envs => true
    },
    hocon_tconf:merge_env_overrides(SchemaMod, RawConf, all, Opts).

-spec check_config(hocon_schema:schema(), raw_config()) -> {AppEnvs, CheckedConf} when
    AppEnvs :: app_envs(), CheckedConf :: config().
check_config(SchemaMod, RawConf) ->
    check_config(SchemaMod, RawConf, #{}).

check_config(SchemaMod, RawConf, Opts0) ->
    try
        do_check_config(SchemaMod, RawConf, Opts0)
    catch
        throw:Errors:Stacktrace ->
            {error, Reason} = emqx_hocon:compact_errors(Errors, Stacktrace),
            erlang:raise(throw, Reason, Stacktrace)
    end.

do_check_config(SchemaMod, RawConf, Opts0) ->
    Opts1 = #{
        return_plain => true,
        format => map
    },
    Opts = maps:merge(Opts0, Opts1),
    {AppEnvs, CheckedConf} =
        hocon_tconf:map_translate(SchemaMod, RawConf, Opts),
    {AppEnvs, unsafe_atom_checked_hocon_key_map(CheckedConf)}.

check_config_namespaced(SchemaMod, RawConf, AllowedNSRoots) ->
    Opts = #{return_plain => true, format => map, required => false},
    Roots0 = [R || {R, _} <- hocon_schema:roots(SchemaMod)],
    Roots = lists:filter(fun(R) -> lists:member(R, AllowedNSRoots) end, Roots0),
    try hocon_tconf:check_plain(SchemaMod, RawConf, Opts, Roots) of
        CheckedConf ->
            {ok, unsafe_atom_checked_hocon_key_map(CheckedConf)}
    catch
        throw:{SchemaMod, Errors} ->
            {error, Errors}
    end.

fill_defaults(RawConf) ->
    fill_defaults(RawConf, #{}).

-spec fill_defaults(raw_config(), hocon_tconf:opts()) -> map().
fill_defaults(RawConf, Opts) ->
    RootNames = get_root_names(),
    maps:fold(
        fun(Key, Conf, Acc) ->
            SubMap = #{Key => Conf},
            WithDefaults =
                case lists:member(Key, RootNames) of
                    true -> fill_defaults(get_schema_mod(Key), SubMap, Opts);
                    false -> SubMap
                end,
            maps:merge(Acc, WithDefaults)
        end,
        #{},
        RawConf
    ).

-spec fill_defaults(module(), raw_config(), hocon_tconf:opts()) -> map().
fill_defaults(SchemaMod, RawConf = #{<<"durable_storage">> := Ds}, Opts) ->
    %% FIXME: kludge to prevent `emqx_config' module from filling in
    %% the default values for backends and layouts. These records are
    %% inside unions, and adding default values there will add
    %% incompatible fields.
    RawConf1 = maps:remove(<<"durable_storage">>, RawConf),
    Conf = fill_defaults(SchemaMod, RawConf1, Opts),
    Conf#{<<"durable_storage">> => Ds};
fill_defaults(SchemaMod, RawConf, Opts0) ->
    Opts = maps:merge(#{required => false, make_serializable => true}, Opts0),
    hocon_tconf:check_plain(
        SchemaMod,
        RawConf,
        Opts,
        root_names_from_conf(RawConf)
    ).

%% @doc Only for test cleanups.
%% Delete override config files.
-spec delete_override_conf_files() -> ok.
delete_override_conf_files() ->
    F1 = deprecated_conf_file(#{override_to => local}),
    F2 = deprecated_conf_file(#{override_to => cluster}),
    F3 = cluster_hocon_file(),
    ok = ensure_file_deleted(F1),
    ok = ensure_file_deleted(F2),
    ok = ensure_file_deleted(F3).

ensure_file_deleted(F) ->
    case file:delete(F) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, Reason} -> error({F, Reason})
    end.

-spec read_override_conf(map()) -> raw_config().
read_override_conf(#{} = Opts) ->
    Files =
        case has_deprecated_file() of
            true -> [deprecated_conf_file(Opts)];
            false -> [base_hocon_file(), cluster_hocon_file()]
        end,
    load_hocon_files(Files, map).

%% @doc Return `true' if this node is upgraded from older version which used cluster-override.conf for
%% cluster-wide config persistence.
has_deprecated_file() ->
    DeprecatedFile = deprecated_conf_file(#{override_to => cluster}),
    filelib:is_regular(DeprecatedFile).

deprecated_conf_file(Opts) when is_map(Opts) ->
    Key =
        case maps:get(override_to, Opts, cluster) of
            local -> local_override_conf_file;
            cluster -> cluster_override_conf_file
        end,
    application:get_env(emqx, Key, undefined);
deprecated_conf_file(Which) when is_atom(Which) ->
    application:get_env(emqx, Which, undefined).

base_hocon_file() ->
    emqx:etc_file("base.hocon").

%% The newer version cluster-wide config persistence file.
cluster_hocon_file() ->
    application:get_env(emqx, cluster_hocon_file, undefined).

-spec save_schema_mod_and_names(module()) -> ok.
save_schema_mod_and_names(SchemaMod) ->
    RootNames = hocon_schema:root_names(SchemaMod),
    OldMods = get_schema_mod(),
    OldNames = get_root_names(),
    %% map from root name to schema module name
    NewMods = maps:from_list([{Name, SchemaMod} || Name <- RootNames]),
    persistent_term:put(?PERSIS_SCHEMA_MODS, #{
        mods => maps:merge(OldMods, NewMods),
        names => lists:usort(OldNames ++ RootNames)
    }).

namespaced_config_allowed_roots() ->
    Roots = persistent_term:get(?ALLOWED_NS_ROOT_KEYS_PT_KEY, []),
    maps:from_keys(Roots, true).

add_allowed_namespaced_config_root(RootKeyBins) when is_list(RootKeyBins) ->
    lists:foreach(fun add_allowed_namespaced_config_root/1, RootKeyBins);
add_allowed_namespaced_config_root(RootKeyBin) when is_binary(RootKeyBin) ->
    Roots0 = persistent_term:get(?ALLOWED_NS_ROOT_KEYS_PT_KEY, []),
    Roots = [RootKeyBin | Roots0 -- [RootKeyBin]],
    persistent_term:put(?ALLOWED_NS_ROOT_KEYS_PT_KEY, Roots).

-ifdef(TEST).
erase_all() ->
    Names = get_root_names(),
    lists:foreach(fun erase/1, Names),
    persistent_term:erase(?PERSIS_SCHEMA_MODS),
    persistent_term:erase(?ALLOWED_NS_ROOT_KEYS_PT_KEY),
    try mnesia:table_info(?CONFIG_TAB, attributes) of
        _ ->
            Namespaces0 =
                mnesia:dirty_select(
                    ?CONFIG_TAB,
                    [{#?CONFIG_TAB{root_key = {'$1', '_'}, _ = '_'}, [], ['$1']}]
                ),
            lists:foreach(
                fun erase_namespaced_configs/1,
                lists:usort(Namespaces0)
            )
    catch
        exit:{aborted, {no_exists, _, _}} ->
            ok
    end,
    ok.
-endif.

-spec get_schema_mod() -> #{binary() => atom()}.
get_schema_mod() ->
    maps:get(mods, persistent_term:get(?PERSIS_SCHEMA_MODS, #{mods => #{}})).

-spec get_schema_mod(atom() | binary()) -> module().
get_schema_mod(RootName) ->
    maps:get(bin(RootName), get_schema_mod()).

-spec get_root_names() -> [binary()].
get_root_names() ->
    maps:get(names, persistent_term:get(?PERSIS_SCHEMA_MODS, #{names => []})).

-spec save_configs(
    app_envs(), config(), raw_config(), raw_config(), update_opts()
) -> ok.

save_configs(AppEnvs, Conf, RawConf, OverrideConf, Opts) ->
    %% We first try to save to files, because saving to files is more error prone
    %% than saving into memory.
    HasDeprecatedFile = has_deprecated_file(),
    ok = save_to_override_conf(HasDeprecatedFile, OverrideConf, Opts),
    save_to_app_env(AppEnvs),
    save_to_config_map(Conf, RawConf).

save_configs_namespaced(Namespace, RootKeyAtom, Conf, RawConf, ClusterRPCOpts, Opts) when
    is_binary(Namespace)
->
    RootKeyBin = bin(RootKeyAtom),
    maybe
        ?KIND_INITIATE ?= maps:get(kind, ClusterRPCOpts, ?KIND_INITIATE),
        ?tp("emqx_config_save_configs_namespaced_transaction", #{}),
        {atomic, ok} = mria:transaction(?COMMON_SHARD, fun() ->
            ?MODULE:save_configs_namespaced_tx(Namespace, RootKeyBin, Conf, RawConf, Opts)
        end),
        ok
    end,
    put_namespaced(Namespace, #{RootKeyAtom => Conf}),
    clear_invalid_namespaced_config(Namespace, RootKeyBin),
    ok.

save_configs_namespaced_tx(Namespace, RootKeyBin, _Conf, RawConf, _Opts) when
    is_binary(Namespace),
    is_binary(RootKeyBin)
->
    Key = {Namespace, RootKeyBin},
    Rec =
        case mnesia:read(?CONFIG_TAB, Key, write) of
            [] ->
                #?CONFIG_TAB{
                    root_key = Key,
                    raw_value = RawConf
                };
            [Rec0] ->
                Rec0#?CONFIG_TAB{
                    raw_value = RawConf
                }
        end,
    ok = mnesia:write(?CONFIG_TAB, Rec, write).

%% we ignore kernel app env,
%% because the old app env will be used in emqx_config_logger:post_config_update/5
-define(IGNORE_APPS, [kernel]).

-spec save_to_app_env([tuple()]) -> ok.
save_to_app_env(AppEnvs0) ->
    AppEnvs = lists:filter(fun({App, _}) -> not lists:member(App, ?IGNORE_APPS) end, AppEnvs0),
    application:set_env(AppEnvs).

-spec save_to_config_map(config(), raw_config()) -> ok.
save_to_config_map(Conf, RawConf) ->
    ?MODULE:put(Conf),
    ?MODULE:put_raw(RawConf).

-spec save_to_override_conf(boolean(), raw_config(), update_opts()) -> ok | {error, term()}.
save_to_override_conf(_HasDeprecatedFile, undefined, _) ->
    ok;
save_to_override_conf(true = _HasDeprecatedFile, RawConf, Opts) ->
    case deprecated_conf_file(Opts) of
        undefined ->
            ok;
        FileName ->
            backup_and_write(FileName, generate_hocon_content(RawConf, Opts))
    end;
save_to_override_conf(false = _HasDeprecatedFile, RawConf, Opts) ->
    case cluster_hocon_file() of
        undefined ->
            ok;
        FileName ->
            backup_and_write(FileName, generate_hocon_content(RawConf, Opts))
    end.

generate_hocon_content(RawConf, Opts) ->
    [
        cluster_dot_hocon_header(),
        hocon_pp:do(RawConf, Opts)
    ].

cluster_dot_hocon_header() ->
    [
        "# This file is generated. Do not edit.\n",
        "# The configs are results of online config changes from UI/API/CLI.\n",
        "# To persist configs in this file, copy the content to etc/base.hocon.\n"
    ].

backup_and_write(Path, Content) ->
    emqx_config_backup_manager:backup_and_write(Path, Content).

add_handlers() ->
    ok = emqx_config_logger:add_handler(),
    emqx_sys_mon:add_handler(),
    emqx_persistent_message:add_handler(),
    ok.

remove_handlers() ->
    ok = emqx_config_logger:remove_handler(),
    emqx_sys_mon:remove_handler(),
    ok.

load_hocon_files(FileNames, LoadType) ->
    Opts = #{include_dirs => include_dirs(), format => LoadType},
    case hocon:files(FileNames, Opts) of
        {ok, Raw0} ->
            Raw0;
        {error, Reason} ->
            throw(#{
                msg => failed_to_load_conf,
                reason => Reason,
                files => FileNames
            })
    end.

do_get_raw(Path) ->
    do_get(?RAW_CONF, Path).

do_get_raw(Path, Default) ->
    do_get(?RAW_CONF, Path, Default).

do_get(Type, KeyPath) ->
    case do_get(Type, KeyPath, ?CONFIG_NOT_FOUND_MAGIC) of
        ?CONFIG_NOT_FOUND_MAGIC -> error({config_not_found, KeyPath});
        Res -> Res
    end.

do_get(Type, [], Default) ->
    AllConf = lists:foldl(
        fun
            ({?PERSIS_KEY(Type0, RootName), Conf}, AccIn) when Type0 == Type ->
                AccIn#{conf_key(Type0, RootName) => Conf};
            (_, AccIn) ->
                AccIn
        end,
        #{},
        persistent_term:get()
    ),
    case AllConf =:= #{} of
        true -> Default;
        false -> AllConf
    end;
do_get(Type, [RootName], Default) ->
    persistent_term:get(?PERSIS_KEY(Type, RootName), Default);
do_get(Type, [RootName | KeyPath], Default) ->
    RootV = persistent_term:get(?PERSIS_KEY(Type, RootName), #{}),
    do_deep_get(Type, KeyPath, RootV, Default).

do_put(Type, Putter, [], DeepValue) ->
    maps:fold(
        fun(RootName, Value, _Res) ->
            do_put(Type, Putter, [RootName], Value)
        end,
        ok,
        DeepValue
    );
do_put(Type, Putter, [RootName | KeyPath], DeepValue) ->
    OldValue = do_get(Type, [RootName], #{}),
    NewValue = do_deep_put(Type, Putter, KeyPath, OldValue, DeepValue),
    Key = ?PERSIS_KEY(Type, RootName),
    persistent_term:put(Key, NewValue),
    put_config_post_change_actions(Key, OldValue, NewValue),
    ok.

do_deep_get(?CONF, AtomKeyPath, Map, Default) ->
    emqx_utils_maps:deep_get(AtomKeyPath, Map, Default);
do_deep_get(?RAW_CONF, KeyPath, Map, Default) ->
    emqx_utils_maps:deep_get([bin(Key) || Key <- KeyPath], Map, Default);
do_deep_get(?NS_CONF(_Namespace), AtomKeyPath, Map, Default) ->
    emqx_utils_maps:deep_get(AtomKeyPath, Map, Default).

do_deep_put(?CONF, Putter, KeyPath, Map, Value) ->
    AtomKeyPath = ensure_atom_conf_path(KeyPath, {raise_error, {not_found, KeyPath}}),
    Putter(AtomKeyPath, Map, Value);
do_deep_put(?RAW_CONF, Putter, KeyPath, Map, Value) ->
    Putter([bin(Key) || Key <- KeyPath], Map, Value);
do_deep_put(?NS_CONF(_Namespace), Putter, KeyPath, Map, Value) ->
    AtomKeyPath = ensure_atom_conf_path(KeyPath, {raise_error, {not_found, KeyPath}}),
    Putter(AtomKeyPath, Map, Value).

root_names_from_conf(RawConf) ->
    Keys = maps:keys(RawConf),
    [Name || Name <- get_root_names(), lists:member(Name, Keys)].

unsafe_atom(Bin) when is_binary(Bin) ->
    binary_to_atom(Bin, utf8);
unsafe_atom(Str) when is_list(Str) ->
    list_to_atom(Str);
unsafe_atom(Atom) when is_atom(Atom) ->
    Atom.

atom(Bin) when is_binary(Bin) ->
    binary_to_existing_atom(Bin, utf8);
atom(Str) when is_list(Str) ->
    list_to_existing_atom(Str);
atom(Atom) when is_atom(Atom) ->
    Atom.

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

warning_deprecated_root_key(RawConf) ->
    case maps:keys(RawConf) -- get_root_names() of
        [] ->
            ok;
        Keys ->
            Unknowns = string:join([binary_to_list(K) || K <- Keys], ","),
            ?tp(unknown_config_keys, #{unknown_config_keys => Unknowns}),
            ?SLOG(
                warning,
                #{
                    msg => "config_key_not_recognized",
                    unknown_config_keys => Unknowns
                }
            )
    end.

conf_key(?CONF, RootName) ->
    atom(RootName);
conf_key(?RAW_CONF, RootName) ->
    bin(RootName);
conf_key(?NS_CONF(_Namespace), RootName) ->
    atom(RootName).

ensure_atom_conf_path(Path, OnFail) ->
    case lists:all(fun erlang:is_atom/1, Path) of
        true ->
            %% Do not try to build new atom PATH if it already is.
            Path;
        _ ->
            to_atom_conf_path(Path, OnFail)
    end.

to_atom_conf_path(Path, OnFail) ->
    try
        [atom(Key) || Key <- Path]
    catch
        error:badarg ->
            case OnFail of
                {raise_error, Err} ->
                    error(Err);
                {return, V} ->
                    V
            end
    end.

%% @doc Init zones under root `zones'
%% 1. ensure one `default' zone as it is referenced by listeners.
%%   if default zone is unset, clone all default values from `GlobalDefaults'
%%   if default zone is set, values are merged with `GlobalDefaults'
%% 2. For any user defined zones, merge with `GlobalDefaults'
%%
%% note1, this should be called as post action after emqx_config terms (zones, and GlobalDefaults)
%%       are written in the PV storage during emqx config loading/initialization.
-spec maybe_init_default_zone() -> skip | ok.
maybe_init_default_zone() ->
    case emqx_config:get([zones], ?CONFIG_NOT_FOUND_MAGIC) of
        ?CONFIG_NOT_FOUND_MAGIC ->
            skip;
        Zones0 when is_map(Zones0) ->
            Zones =
                case Zones0 of
                    #{default := _DefaultZone} = Z1 ->
                        Z1;
                    Z2 ->
                        Z2#{default => #{}}
                end,
            NewZones = merge_zones_with_global_defaults(Zones),
            ?MODULE:put([zones], NewZones)
    end.

-spec merge_zones_with_global_defaults(map()) -> map().
merge_zones_with_global_defaults(ZonesConfig) ->
    Defaults = zone_global_defaults(),
    maps:map(
        fun(_ZoneName, ZoneVal) ->
            emqx_utils_maps:deep_merge(Defaults, ZoneVal)
        end,
        ZonesConfig
    ).

%% @doc Update zones
%%    when 1) zone updates, return *new* zones
%%    when 2) zone global config updates, write to PT directly.
%% Zone global defaults are always presented in the configmap (PT) when updating zone
-spec maybe_update_zone(runtime_config_key_path(), RootValue :: map(), Val :: term()) ->
    NewZoneVal :: map().
maybe_update_zone([zones | T], ZonesValue, Value) ->
    %% note, do not write to PT, return *New value* instead
    NewZonesValue0 = emqx_utils_maps:deep_put(T, ZonesValue, Value),
    NewZonesValue1 = emqx_utils_maps:deep_merge(
        #{default => zone_global_defaults()}, NewZonesValue0
    ),
    merge_zones_with_global_defaults(NewZonesValue1);
maybe_update_zone([RootName | T], RootValue, Value) when is_atom(RootName) ->
    NewRootValue = emqx_utils_maps:deep_put(T, RootValue, Value),
    case is_zone_root(RootName) of
        false ->
            skip;
        true ->
            %% When updates on global default roots.
            ExistingZones = ?MODULE:get([zones], #{}),
            RootNameBin = atom_to_binary(RootName),
            NewZones = maps:map(
                fun(ZoneName, ZoneVal) ->
                    BinPath = [<<"zones">>, atom_to_binary(ZoneName), RootNameBin],
                    case
                        %% look for user defined value from RAWCONF
                        ?MODULE:get_raw(
                            BinPath,
                            ?CONFIG_NOT_FOUND_MAGIC
                        )
                    of
                        ?CONFIG_NOT_FOUND_MAGIC ->
                            ZoneVal#{RootName => NewRootValue};
                        RawUserZoneRoot ->
                            UserDefinedValues = rawconf_to_conf(
                                emqx_schema, BinPath, RawUserZoneRoot
                            ),
                            ZoneVal#{
                                RootName :=
                                    emqx_utils_maps:deep_merge(
                                        NewRootValue,
                                        UserDefinedValues
                                    )
                            }
                    end
                end,
                ExistingZones
            ),
            ZonesKey = ?PERSIS_KEY(?CONF, zones),
            persistent_term:put(ZonesKey, NewZones),
            put_config_post_change_actions(ZonesKey, ExistingZones, NewZones)
    end,
    NewRootValue.

zone_global_defaults() ->
    maps:from_list([{K, ?MODULE:get([K])} || K <- zone_roots()]).

-spec is_zone_root(atom) -> boolean().
is_zone_root(Name) ->
    lists:member(Name, zone_roots()).

-spec zone_roots() -> [atom()].
zone_roots() ->
    emqx_zone_schema:roots().

%%%
%%% @doc During init, ensure order of puts that zone is put after the other global defaults.
%%%
put_with_order(#{zones := _Zones} = Conf) ->
    put1(maps:without([zones], Conf)),
    put1(maps:with([zones], Conf));
put_with_order(Conf) ->
    put1(Conf).

%%
%% @doc Helper function that converts raw conf val to runtime conf val
%%      with the types info from schema module
-spec rawconf_to_conf(module(), RawPath :: [binary()], RawValue :: term()) -> term().
rawconf_to_conf(SchemaModule, RawPath, RawValue) ->
    {_, RawUserDefinedValues} =
        check_config(
            SchemaModule,
            emqx_utils_maps:deep_put(RawPath, #{}, RawValue)
        ),
    AtomPath = to_atom_conf_path(RawPath, {raise_error, maybe_update_zone_error}),
    emqx_utils_maps:deep_get(AtomPath, RawUserDefinedValues).

% When the global zone changes, all zones are updated with the new global zone configuration.
% However, since the global zone contains too many configuration keys,
% we decided not to implement an emqx_config_handler callback to handle global zone changes.
put_config_post_change_actions(?PERSIS_KEY(?CONF, zones), OldZones, NewZones) ->
    _ = emqx_config_zones:post_update(OldZones, NewZones),
    ok;
put_config_post_change_actions(_Key, _OldValue, _NewValue) ->
    ok.

config_files() ->
    application:get_env(emqx, config_files, []).

unsafe_atom_checked_hocon_key_map(Map) ->
    do_unsafe_atom_checked_hocon_key_map(Map).

do_unsafe_atom_checked_hocon_key_map(Map) when is_map(Map) ->
    maps:fold(
        fun
            (?COMPUTED = K, V, Acc) ->
                %% Do not enter computed values
                Acc#{K => V};
            (K, V, Acc) when is_atom(K) ->
                Acc#{K => do_unsafe_atom_checked_hocon_key_map(V)};
            (K, V, Acc) when is_binary(K) ->
                Acc#{binary_to_atom(K, utf8) => do_unsafe_atom_checked_hocon_key_map(V)}
        end,
        #{},
        Map
    );
do_unsafe_atom_checked_hocon_key_map(List) when is_list(List) ->
    lists:map(fun do_unsafe_atom_checked_hocon_key_map/1, List);
do_unsafe_atom_checked_hocon_key_map(X) ->
    X.
