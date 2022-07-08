%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([
    init_load/1,
    init_load/2,
    init_load/3,
    read_override_conf/1,
    delete_override_conf_files/0,
    check_config/2,
    fill_defaults/1,
    fill_defaults/2,
    fill_defaults/3,
    save_configs/5,
    save_to_app_env/1,
    save_to_config_map/2,
    save_to_override_conf/2
]).

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

-include("logger.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-define(CONF, conf).
-define(RAW_CONF, raw_conf).
-define(PERSIS_SCHEMA_MODS, {?MODULE, schema_mods}).
-define(PERSIS_KEY(TYPE, ROOT), {?MODULE, TYPE, ROOT}).
-define(ZONE_CONF_PATH(ZONE, PATH), [zones, ZONE | PATH]).
-define(LISTENER_CONF_PATH(TYPE, LISTENER, PATH), [listeners, TYPE, LISTENER | PATH]).

-export_type([
    update_request/0,
    raw_config/0,
    config/0,
    app_envs/0,
    update_opts/0,
    update_cmd/0,
    update_args/0,
    update_error/0,
    update_result/0
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
    override_to => local | cluster
}.
-type update_args() :: {update_cmd(), Opts :: update_opts()}.
-type update_stage() :: pre_config_update | post_config_update.
-type update_error() :: {update_stage(), module(), term()} | {save_configs, term()} | term().
-type update_result() :: #{
    config => emqx_config:config(),
    raw_config => emqx_config:raw_config(),
    post_config_update => #{module() => any()}
}.

%% raw_config() is the config that is NOT parsed and translated by hocon schema
-type raw_config() :: #{binary() => term()} | list() | undefined.
%% config() is the config that is parsed and translated by hocon schema
-type config() :: #{atom() => term()} | list() | undefined.
-type app_envs() :: [proplists:property()].

%% @doc For the given path, get root value enclosed in a single-key map.
-spec get_root(emqx_map_lib:config_key_path()) -> map().
get_root([RootName | _]) ->
    #{RootName => do_get(?CONF, [RootName], #{})}.

%% @doc For the given path, get raw root value enclosed in a single-key map.
%% key is ensured to be binary.
get_root_raw([RootName | _]) ->
    #{bin(RootName) => do_get_raw([RootName], #{})}.

%% @doc Get a config value for the given path.
%% The path should at least include root config name.
-spec get(emqx_map_lib:config_key_path()) -> term().
get(KeyPath) -> do_get(?CONF, KeyPath).

-spec get(emqx_map_lib:config_key_path(), term()) -> term().
get(KeyPath, Default) -> do_get(?CONF, KeyPath, Default).

-spec find(emqx_map_lib:config_key_path()) ->
    {ok, term()} | {not_found, emqx_map_lib:config_key_path(), term()}.
find([]) ->
    Ref = make_ref(),
    case do_get(?CONF, [], Ref) of
        Ref -> {not_found, []};
        Res -> {ok, Res}
    end;
find(KeyPath) ->
    atom_conf_path(
        KeyPath,
        fun(AtomKeyPath) -> emqx_map_lib:deep_find(AtomKeyPath, get_root(KeyPath)) end,
        {return, {not_found, KeyPath}}
    ).

-spec find_raw(emqx_map_lib:config_key_path()) ->
    {ok, term()} | {not_found, emqx_map_lib:config_key_path(), term()}.
find_raw([]) ->
    Ref = make_ref(),
    case do_get_raw([], Ref) of
        Ref -> {not_found, []};
        Res -> {ok, Res}
    end;
find_raw(KeyPath) ->
    emqx_map_lib:deep_find([bin(Key) || Key <- KeyPath], get_root_raw(KeyPath)).

-spec get_zone_conf(atom(), emqx_map_lib:config_key_path()) -> term().
get_zone_conf(Zone, KeyPath) ->
    case find(?ZONE_CONF_PATH(Zone, KeyPath)) of
        %% not found in zones, try to find the global config
        {not_found, _, _} ->
            ?MODULE:get(KeyPath);
        {ok, Value} ->
            Value
    end.

-spec get_zone_conf(atom(), emqx_map_lib:config_key_path(), term()) -> term().
get_zone_conf(Zone, KeyPath, Default) ->
    case find(?ZONE_CONF_PATH(Zone, KeyPath)) of
        %% not found in zones, try to find the global config
        {not_found, _, _} ->
            ?MODULE:get(KeyPath, Default);
        {ok, Value} ->
            Value
    end.

-spec put_zone_conf(atom(), emqx_map_lib:config_key_path(), term()) -> ok.
put_zone_conf(Zone, KeyPath, Conf) ->
    ?MODULE:put(?ZONE_CONF_PATH(Zone, KeyPath), Conf).

-spec get_listener_conf(atom(), atom(), emqx_map_lib:config_key_path()) -> term().
get_listener_conf(Type, Listener, KeyPath) ->
    ?MODULE:get(?LISTENER_CONF_PATH(Type, Listener, KeyPath)).

-spec get_listener_conf(atom(), atom(), emqx_map_lib:config_key_path(), term()) -> term().
get_listener_conf(Type, Listener, KeyPath, Default) ->
    ?MODULE:get(?LISTENER_CONF_PATH(Type, Listener, KeyPath), Default).

-spec put_listener_conf(atom(), atom(), emqx_map_lib:config_key_path(), term()) -> ok.
put_listener_conf(Type, Listener, KeyPath, Conf) ->
    ?MODULE:put(?LISTENER_CONF_PATH(Type, Listener, KeyPath), Conf).

-spec find_listener_conf(atom(), atom(), emqx_map_lib:config_key_path()) ->
    {ok, term()} | {not_found, emqx_map_lib:config_key_path(), term()}.
find_listener_conf(Type, Listener, KeyPath) ->
    find(?LISTENER_CONF_PATH(Type, Listener, KeyPath)).

-spec put(map()) -> ok.
put(Config) ->
    maps:fold(
        fun(RootName, RootValue, _) ->
            ?MODULE:put([RootName], RootValue)
        end,
        ok,
        Config
    ).

erase(RootName) ->
    persistent_term:erase(?PERSIS_KEY(?CONF, bin(RootName))),
    persistent_term:erase(?PERSIS_KEY(?RAW_CONF, bin(RootName))),
    ok.

-spec put(emqx_map_lib:config_key_path(), term()) -> ok.
put(KeyPath, Config) ->
    Putter = fun(Path, Map, Value) ->
        emqx_map_lib:deep_put(Path, Map, Value)
    end,
    do_put(?CONF, Putter, KeyPath, Config).

%% Puts value into configuration even if path doesn't exist
%% For paths of non-existing atoms use force_put(KeyPath, Config, unsafe)
-spec force_put(emqx_map_lib:config_key_path(), term()) -> ok.
force_put(KeyPath, Config) ->
    force_put(KeyPath, Config, safe).

-spec force_put(emqx_map_lib:config_key_path(), term(), safe | unsafe) -> ok.
force_put(KeyPath0, Config, Safety) ->
    KeyPath =
        case Safety of
            safe -> KeyPath0;
            unsafe -> [unsafe_atom(Key) || Key <- KeyPath0]
        end,
    Putter = fun(Path, Map, Value) ->
        emqx_map_lib:deep_force_put(Path, Map, Value)
    end,
    do_put(?CONF, Putter, KeyPath, Config).

-spec get_default_value(emqx_map_lib:config_key_path()) -> {ok, term()} | {error, term()}.
get_default_value([RootName | _] = KeyPath) ->
    BinKeyPath = [bin(Key) || Key <- KeyPath],
    case find_raw([RootName]) of
        {ok, RawConf} ->
            RawConf1 = emqx_map_lib:deep_remove(BinKeyPath, #{bin(RootName) => RawConf}),
            try fill_defaults(get_schema_mod(RootName), RawConf1, #{}) of
                FullConf ->
                    case emqx_map_lib:deep_find(BinKeyPath, FullConf) of
                        {not_found, _, _} -> {error, no_default_value};
                        {ok, Val} -> {ok, Val}
                    end
            catch
                error:Reason -> {error, Reason}
            end;
        {not_found, _, _} ->
            {error, {rootname_not_found, RootName}}
    end.

-spec get_raw(emqx_map_lib:config_key_path()) -> term().
get_raw(KeyPath) -> do_get_raw(KeyPath).

-spec get_raw(emqx_map_lib:config_key_path(), term()) -> term().
get_raw(KeyPath, Default) -> do_get_raw(KeyPath, Default).

-spec put_raw(map()) -> ok.
put_raw(Config) ->
    maps:fold(
        fun(RootName, RootV, _) ->
            ?MODULE:put_raw([RootName], RootV)
        end,
        ok,
        hocon_maps:ensure_plain(Config)
    ).

-spec put_raw(emqx_map_lib:config_key_path(), term()) -> ok.
put_raw(KeyPath, Config) ->
    Putter = fun(Path, Map, Value) ->
        emqx_map_lib:deep_force_put(Path, Map, Value)
    end,
    do_put(?RAW_CONF, Putter, KeyPath, Config).

%%============================================================================
%% Load/Update configs From/To files
%%============================================================================
init_load(SchemaMod) ->
    ConfFiles = application:get_env(emqx, config_files, []),
    init_load(SchemaMod, ConfFiles, #{raw_with_default => true}).

init_load(SchemaMod, Opts) when is_map(Opts) ->
    ConfFiles = application:get_env(emqx, config_files, []),
    init_load(SchemaMod, ConfFiles, Opts);
init_load(SchemaMod, ConfFiles) ->
    init_load(SchemaMod, ConfFiles, #{raw_with_default => false}).

%% @doc Initial load of the given config files.
%% NOTE: The order of the files is significant, configs from files ordered
%% in the rear of the list overrides prior values.
-spec init_load(module(), [string()] | binary() | hocon:config()) -> ok.
init_load(SchemaMod, Conf, Opts) when is_list(Conf) orelse is_binary(Conf) ->
    init_load(SchemaMod, parse_hocon(Conf), Opts);
init_load(SchemaMod, RawConf, Opts) when is_map(RawConf) ->
    ok = save_schema_mod_and_names(SchemaMod),
    %% Merge environment variable overrides on top
    RawConfWithEnvs = merge_envs(SchemaMod, RawConf),
    ClusterOverrides = read_override_conf(#{override_to => cluster}),
    LocalOverrides = read_override_conf(#{override_to => local}),
    Overrides = hocon:deep_merge(ClusterOverrides, LocalOverrides),
    RawConfWithOverrides = hocon:deep_merge(RawConfWithEnvs, Overrides),
    RootNames = get_root_names(),
    RawConfAll = raw_conf_with_default(SchemaMod, RootNames, RawConfWithOverrides, Opts),
    %% check configs against the schema
    {_AppEnvs, CheckedConf} = check_config(SchemaMod, RawConfAll, #{}),
    ok = save_to_config_map(CheckedConf, RawConfAll).

%% keep the raw and non-raw conf has the same keys to make update raw conf easier.
raw_conf_with_default(SchemaMod, RootNames, RawConf, #{raw_with_default := true}) ->
    Fun = fun(Name, Acc) ->
        case maps:is_key(Name, RawConf) of
            true ->
                Acc;
            false ->
                case lists:keyfind(Name, 1, hocon_schema:roots(SchemaMod)) of
                    false ->
                        Acc;
                    {_, {_, Schema}} ->
                        Acc#{Name => schema_default(Schema)}
                end
        end
    end,
    RawDefault = lists:foldl(Fun, #{}, RootNames),
    maps:merge(RawConf, fill_defaults(SchemaMod, RawDefault, #{}));
raw_conf_with_default(_SchemaMod, _RootNames, RawConf, _Opts) ->
    RawConf.

schema_default(Schema) ->
    case hocon_schema:field_schema(Schema, type) of
        ?ARRAY(_) ->
            [];
        ?LAZY(?ARRAY(_)) ->
            [];
        ?LAZY(?UNION(Unions)) ->
            case [A || ?ARRAY(A) <- Unions] of
                [_ | _] -> [];
                _ -> #{}
            end;
        _ ->
            #{}
    end.

parse_hocon(Conf) ->
    IncDirs = include_dirs(),
    case do_parse_hocon(Conf, IncDirs) of
        {ok, HoconMap} ->
            HoconMap;
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_load_hocon_conf",
                reason => Reason,
                pwd => file:get_cwd(),
                include_dirs => IncDirs,
                config_file => Conf
            }),
            error(failed_to_load_hocon_conf)
    end.

do_parse_hocon(Conf, IncDirs) ->
    Opts = #{format => map, include_dirs => IncDirs},
    case is_binary(Conf) of
        true -> hocon:binary(Conf, Opts);
        false -> hocon:files(Conf, Opts)
    end.

include_dirs() ->
    [filename:join(emqx:data_dir(), "configs")].

merge_envs(SchemaMod, RawConf) ->
    Opts = #{
        required => false,
        format => map,
        apply_override_envs => true,
        check_lazy => true
    },
    hocon_tconf:merge_env_overrides(SchemaMod, RawConf, all, Opts).

-spec check_config(hocon_schema:schema(), raw_config()) -> {AppEnvs, CheckedConf} when
    AppEnvs :: app_envs(), CheckedConf :: config().
check_config(SchemaMod, RawConf) ->
    check_config(SchemaMod, RawConf, #{}).

check_config(SchemaMod, RawConf, Opts0) ->
    Opts1 = #{
        return_plain => true,
        %% TODO: evil, remove, required should be declared in schema
        required => false,
        format => map
    },
    Opts = maps:merge(Opts0, Opts1),
    {AppEnvs, CheckedConf} =
        hocon_tconf:map_translate(SchemaMod, RawConf, Opts),
    {AppEnvs, emqx_map_lib:unsafe_atom_key_map(CheckedConf)}.

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
fill_defaults(SchemaMod, RawConf, Opts0) ->
    Opts = maps:merge(#{required => false, only_fill_defaults => true}, Opts0),
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
    F1 = override_conf_file(#{override_to => local}),
    F2 = override_conf_file(#{override_to => cluster}),
    ok = ensure_file_deleted(F1),
    ok = ensure_file_deleted(F2).

ensure_file_deleted(F) ->
    case file:delete(F) of
        ok -> ok;
        {error, enoent} -> ok;
        {error, Reason} -> error({F, Reason})
    end.

-spec read_override_conf(map()) -> raw_config().
read_override_conf(#{} = Opts) ->
    File = override_conf_file(Opts),
    load_hocon_file(File, map).

override_conf_file(Opts) when is_map(Opts) ->
    Key =
        case maps:get(override_to, Opts, local) of
            local -> local_override_conf_file;
            cluster -> cluster_override_conf_file
        end,
    application:get_env(emqx, Key, undefined);
override_conf_file(Which) when is_atom(Which) ->
    application:get_env(emqx, Which, undefined).

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

-spec get_schema_mod() -> #{binary() => atom()}.
get_schema_mod() ->
    maps:get(mods, persistent_term:get(?PERSIS_SCHEMA_MODS, #{mods => #{}})).

-spec get_schema_mod(atom() | binary()) -> module().
get_schema_mod(RootName) ->
    maps:get(bin(RootName), get_schema_mod()).

-spec get_root_names() -> [binary()].
get_root_names() ->
    maps:get(names, persistent_term:get(?PERSIS_SCHEMA_MODS, #{names => []})).

-spec save_configs(app_envs(), config(), raw_config(), raw_config(), update_opts()) -> ok.
save_configs(_AppEnvs, Conf, RawConf, OverrideConf, Opts) ->
    %% We first try to save to override.conf, because saving to files is more error prone
    %% than saving into memory.
    ok = save_to_override_conf(OverrideConf, Opts),
    %% We may need also support hot config update for the apps that use application envs.
    %% If that is the case uncomment the following line to update the configs to app env
    %save_to_app_env(_AppEnvs),
    save_to_config_map(Conf, RawConf).

-spec save_to_app_env([tuple()]) -> ok.
save_to_app_env(AppEnvs) ->
    lists:foreach(
        fun({AppName, Envs}) ->
            [application:set_env(AppName, Par, Val) || {Par, Val} <- Envs]
        end,
        AppEnvs
    ).

-spec save_to_config_map(config(), raw_config()) -> ok.
save_to_config_map(Conf, RawConf) ->
    ?MODULE:put(Conf),
    ?MODULE:put_raw(RawConf).

-spec save_to_override_conf(raw_config(), update_opts()) -> ok | {error, term()}.
save_to_override_conf(undefined, _) ->
    ok;
save_to_override_conf(RawConf, Opts) ->
    case override_conf_file(Opts) of
        undefined ->
            ok;
        FileName ->
            ok = filelib:ensure_dir(FileName),
            case file:write_file(FileName, hocon_pp:do(RawConf, #{})) of
                ok ->
                    ok;
                {error, Reason} ->
                    ?SLOG(error, #{
                        msg => "failed_to_write_override_file",
                        filename => FileName,
                        reason => Reason
                    }),
                    {error, Reason}
            end
    end.

add_handlers() ->
    ok = emqx_config_logger:add_handler(),
    ok.

remove_handlers() ->
    ok = emqx_config_logger:remove_handler(),
    ok.

load_hocon_file(FileName, LoadType) ->
    case filelib:is_regular(FileName) of
        true ->
            Opts = #{include_dirs => include_dirs(), format => LoadType},
            {ok, Raw0} = hocon:load(FileName, Opts),
            Raw0;
        false ->
            #{}
    end.

do_get_raw(Path) ->
    hocon_tconf:remove_env_meta(do_get(?RAW_CONF, Path)).

do_get_raw(Path, Default) ->
    hocon_tconf:remove_env_meta(do_get(?RAW_CONF, Path, Default)).

do_get(Type, KeyPath) ->
    Ref = make_ref(),
    Res = do_get(Type, KeyPath, Ref),
    case Res =:= Ref of
        true -> error({config_not_found, KeyPath});
        false -> Res
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
    persistent_term:get(?PERSIS_KEY(Type, bin(RootName)), Default);
do_get(Type, [RootName | KeyPath], Default) ->
    RootV = persistent_term:get(?PERSIS_KEY(Type, bin(RootName)), #{}),
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
    persistent_term:put(?PERSIS_KEY(Type, bin(RootName)), NewValue).

do_deep_get(?CONF, KeyPath, Map, Default) ->
    atom_conf_path(
        KeyPath,
        fun(AtomKeyPath) -> emqx_map_lib:deep_get(AtomKeyPath, Map, Default) end,
        {return, Default}
    );
do_deep_get(?RAW_CONF, KeyPath, Map, Default) ->
    emqx_map_lib:deep_get([bin(Key) || Key <- KeyPath], Map, Default).

do_deep_put(?CONF, Putter, KeyPath, Map, Value) ->
    atom_conf_path(
        KeyPath,
        fun(AtomKeyPath) -> Putter(AtomKeyPath, Map, Value) end,
        {raise_error, {not_found, KeyPath}}
    );
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

conf_key(?CONF, RootName) ->
    atom(RootName);
conf_key(?RAW_CONF, RootName) ->
    bin(RootName).

atom_conf_path(Path, ExpFun, OnFail) ->
    try [atom(Key) || Key <- Path] of
        AtomKeyPath -> ExpFun(AtomKeyPath)
    catch
        error:badarg ->
            case OnFail of
                {return, Val} ->
                    Val;
                {raise_error, Err} ->
                    error(Err)
            end
    end.
