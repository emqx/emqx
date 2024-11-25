%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_config).

-compile({no_auto_import, [get/0, get/1, put/2, erase/1]}).
-elvis([{elvis_style, god_modules, disable}]).
-include("logger.hrl").
-include("emqx.hrl").
-include("emqx_schema.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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
    get_root_raw/1
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

-export([ensure_atom_conf_path/2]).
-export([load_config_files/2]).
-export([upgrade_raw_conf/2]).

-ifdef(TEST).
-export([erase_all/0, backup_and_write/2, cluster_hocon_file/0, base_hocon_file/0]).
-endif.

-include("logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-define(CONF, conf).
-define(RAW_CONF, raw_conf).
-define(PERSIS_SCHEMA_MODS, {?MODULE, schema_mods}).
-define(PERSIS_KEY(TYPE, ROOT), {?MODULE, TYPE, ROOT}).
-define(ZONE_CONF_PATH(ZONE, PATH), [zones, ZONE | PATH]).
-define(LISTENER_CONF_PATH(TYPE, LISTENER, PATH), [listeners, TYPE, LISTENER | PATH]).

-define(MAX_KEEP_BACKUP_CONFIGS, 10).

-export_type([
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
    lazy_evaluator => fun((function()) -> term())
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

%% raw_config() is the config that is NOT parsed and translated by hocon schema
-type raw_config() :: #{binary() => term()} | list() | undefined.
%% config() is the config that is parsed and translated by hocon schema
-type config() :: #{atom() => term()} | list() | undefined.
-type app_envs() :: [proplists:property()].

-type runtime_config_key_path() :: [atom()].

%% @doc For the given path, get root value enclosed in a single-key map.
-spec get_root(emqx_utils_maps:config_key_path()) -> map().
get_root([RootName | _]) ->
    #{RootName => do_get(?CONF, [RootName], #{})}.

%% @doc For the given path, get raw root value enclosed in a single-key map.
%% key is ensured to be binary.
get_root_raw([RootName | _]) ->
    #{bin(RootName) => get_raw([RootName], #{})}.

%% @doc Get a config value for the given path.
%% The path should at least include root config name.
-spec get(runtime_config_key_path()) -> term().
get(KeyPath) -> do_get(?CONF, KeyPath).

-spec get(runtime_config_key_path(), term()) -> term().
get(KeyPath, Default) -> do_get(?CONF, KeyPath, Default).

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
    {AppEnvs, emqx_utils_maps:unsafe_atom_key_map(CheckedConf)}.

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

-ifdef(TEST).
erase_all() ->
    Names = get_root_names(),
    lists:foreach(fun erase/1, Names),
    persistent_term:erase(?PERSIS_SCHEMA_MODS).
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

%% @private This is the same human-readable timestamp format as
%% hocon-cli generated app.<time>.config file name.
now_time() ->
    Ts = os:system_time(millisecond),
    {{Y, M, D}, {HH, MM, SS}} = calendar:system_time_to_local_time(Ts, millisecond),
    Res = io_lib:format(
        "~0p.~2..0b.~2..0b.~2..0b.~2..0b.~2..0b.~3..0b",
        [Y, M, D, HH, MM, SS, Ts rem 1000]
    ),
    lists:flatten(Res).

%% @private Backup the current config to a file with a timestamp suffix and
%% then save the new config to the config file.
backup_and_write(Path, Content) ->
    %% this may fail, but we don't care
    %% e.g. read-only file system
    _ = filelib:ensure_dir(Path),
    TmpFile = Path ++ ".tmp",
    case file:write_file(TmpFile, Content) of
        ok ->
            backup_and_replace(Path, TmpFile);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_save_conf_file",
                hint =>
                    "The updated cluster config is not saved on this node, please check the file system.",
                filename => TmpFile,
                reason => Reason
            }),
            %% e.g. read-only, it's not the end of the world
            ok
    end.

backup_and_replace(Path, TmpPath) ->
    Backup = Path ++ "." ++ now_time() ++ ".bak",
    case file:rename(Path, Backup) of
        ok ->
            ok = file:rename(TmpPath, Path),
            ok = prune_backup_files(Path);
        {error, enoent} ->
            %% not created yet
            ok = file:rename(TmpPath, Path);
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "failed_to_backup_conf_file",
                filename => Backup,
                reason => Reason
            }),
            ok
    end.

prune_backup_files(Path) ->
    Files0 = filelib:wildcard(Path ++ ".*"),
    Re = "\\.[0-9]{4}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{2}\\.[0-9]{3}\\.bak$",
    Files = lists:filter(fun(F) -> re:run(F, Re) =/= nomatch end, Files0),
    Sorted = lists:reverse(lists:sort(Files)),
    {_Keeps, Deletes} = lists:split(min(?MAX_KEEP_BACKUP_CONFIGS, length(Sorted)), Sorted),
    lists:foreach(
        fun(F) ->
            case file:delete(F) of
                ok ->
                    ok;
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "failed_to_delete_backup_conf_file",
                        filename => F,
                        reason => Reason
                    }),
                    ok
            end
        end,
        Deletes
    ).

add_handlers() ->
    ok = emqx_config_logger:add_handler(),
    ok = emqx_config_zones:add_handler(),
    emqx_sys_mon:add_handler(),
    emqx_persistent_message:add_handler(),
    ok.

remove_handlers() ->
    ok = emqx_config_logger:remove_handler(),
    ok = emqx_config_zones:remove_handler(),
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
    put_config_post_change_actions(Key, NewValue),
    ok.

do_deep_get(?CONF, AtomKeyPath, Map, Default) ->
    emqx_utils_maps:deep_get(AtomKeyPath, Map, Default);
do_deep_get(?RAW_CONF, KeyPath, Map, Default) ->
    emqx_utils_maps:deep_get([bin(Key) || Key <- KeyPath], Map, Default).

do_deep_put(?CONF, Putter, KeyPath, Map, Value) ->
    AtomKeyPath = ensure_atom_conf_path(KeyPath, {raise_error, {not_found, KeyPath}}),
    Putter(AtomKeyPath, Map, Value);
do_deep_put(?RAW_CONF, Putter, KeyPath, Map, Value) ->
    Putter([bin(Key) || Key <- KeyPath], Map, Value).

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
    bin(RootName).

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
            GLD = zone_global_defaults(),
            NewZones = maps:map(
                fun(_ZoneName, ZoneVal) ->
                    merge_with_global_defaults(GLD, ZoneVal)
                end,
                Zones
            ),
            ?MODULE:put([zones], NewZones)
    end.

-spec merge_with_global_defaults(map(), map()) -> map().
merge_with_global_defaults(GlobalDefaults, ZoneVal) ->
    emqx_utils_maps:deep_merge(GlobalDefaults, ZoneVal).

%% @doc Update zones
%%    when 1) zone updates, return *new* zones
%%    when 2) zone global config updates, write to PT directly.
%% Zone global defaults are always presented in the configmap (PT) when updating zone
-spec maybe_update_zone(runtime_config_key_path(), RootValue :: map(), Val :: term()) ->
    NewZoneVal :: map().
maybe_update_zone([zones | T], ZonesValue, Value) ->
    %% note, do not write to PT, return *New value* instead
    GLD = zone_global_defaults(),
    NewZonesValue0 = emqx_utils_maps:deep_put(T, ZonesValue, Value),
    NewZonesValue1 = emqx_utils_maps:deep_merge(#{default => GLD}, NewZonesValue0),
    maps:map(
        fun(_ZoneName, ZoneValue) ->
            merge_with_global_defaults(GLD, ZoneValue)
        end,
        NewZonesValue1
    );
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
            put_config_post_change_actions(ZonesKey, NewZones)
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

%% When the global zone change, the zones is updated with the new global zone.
%% The global zone's keys is too many,
%% so we don't choose to write a global zone change emqx_config_handler callback to hook
put_config_post_change_actions(?PERSIS_KEY(?CONF, zones), _Zones) ->
    emqx_flapping:update_config(),
    ok;
put_config_post_change_actions(_Key, _NewValue) ->
    ok.

config_files() ->
    application:get_env(emqx, config_files, []).
