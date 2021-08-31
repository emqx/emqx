%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-compile({no_auto_import, [get/0, get/1, put/2]}).

-export([ init_load/2
        , read_override_conf/0
        , check_config/2
        , fill_defaults/1
        , fill_defaults/2
        , save_configs/4
        , save_to_app_env/1
        , save_to_config_map/2
        , save_to_override_conf/1
        ]).

-export([ get_root/1
        , get_root_raw/1
        ]).

-export([ get_default_value/1
        ]).

-export([ get/1
        , get/2
        , find/1
        , find_raw/1
        , put/1
        , put/2
        ]).

-export([ get_raw/1
        , get_raw/2
        , put_raw/1
        , put_raw/2
        ]).

-export([ save_schema_mod_and_names/1
        , get_schema_mod/0
        , get_schema_mod/1
        , get_root_names/0
        ]).

-export([ get_zone_conf/2
        , get_zone_conf/3
        , put_zone_conf/3
        ]).

-export([ get_listener_conf/3
        , get_listener_conf/4
        , put_listener_conf/4
        , find_listener_conf/3
        ]).

-define(CONF, conf).
-define(RAW_CONF, raw_conf).
-define(PERSIS_SCHEMA_MODS, {?MODULE, schema_mods}).
-define(PERSIS_KEY(TYPE, ROOT), {?MODULE, TYPE, ROOT}).
-define(ZONE_CONF_PATH(ZONE, PATH), [zones, ZONE | PATH]).
-define(LISTENER_CONF_PATH(TYPE, LISTENER, PATH), [listeners, TYPE, LISTENER | PATH]).

-define(ATOM_CONF_PATH(PATH, EXP, EXP_ON_FAIL),
    try [atom(Key) || Key <- PATH] of
        AtomKeyPath -> EXP
    catch
        error:badarg -> EXP_ON_FAIL
    end).

-export_type([update_request/0, raw_config/0, config/0, app_envs/0,
              update_opts/0, update_cmd/0, update_args/0,
              update_error/0, update_result/0]).

-type update_request() :: term().
-type update_cmd() :: {update, update_request()} | remove.
-type update_opts() :: #{
        %% fill the default values into the rawconf map
        rawconf_with_defaults => boolean()
    }.
-type update_args() :: {update_cmd(), Opts :: update_opts()}.
-type update_stage() :: pre_config_update | post_config_update.
-type update_error() :: {update_stage(), module(), term()} | {save_configs, term()} | term().
-type update_result() :: #{
    config => emqx_config:config(),
    raw_config => emqx_config:raw_config(),
    post_config_update => #{module() => any()}
}.

%% raw_config() is the config that is NOT parsed and tranlated by hocon schema
-type raw_config() :: #{binary() => term()} | list() | undefined.
%% config() is the config that is parsed and tranlated by hocon schema
-type config() :: #{atom() => term()} | list() | undefined.
-type app_envs() :: [proplists:property()].

%% @doc For the given path, get root value enclosed in a single-key map.
-spec get_root(emqx_map_lib:config_key_path()) -> map().
get_root([RootName | _]) ->
    #{RootName => do_get(?CONF, [RootName], #{})}.

%% @doc For the given path, get raw root value enclosed in a single-key map.
%% key is ensured to be binary.
get_root_raw([RootName | _]) ->
    #{bin(RootName) => do_get(?RAW_CONF, [RootName], #{})}.

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
    Res = do_get(?CONF, [], Ref),
    case Res =:= Ref of
        true -> {not_found, []};
        false -> {ok, Res}
    end;
find(KeyPath) ->
    ?ATOM_CONF_PATH(KeyPath, emqx_map_lib:deep_find(AtomKeyPath, get_root(KeyPath)),
        {not_found, KeyPath}).

-spec find_raw(emqx_map_lib:config_key_path()) ->
    {ok, term()} | {not_found, emqx_map_lib:config_key_path(), term()}.
find_raw([]) ->
    Ref = make_ref(),
    Res = do_get(?RAW_CONF, [], Ref),
    case Res =:= Ref of
        true -> {not_found, []};
        false -> {ok, Res}
    end;
find_raw(KeyPath) ->
    emqx_map_lib:deep_find([bin(Key) || Key <- KeyPath], get_root_raw(KeyPath)).

-spec get_zone_conf(atom(), emqx_map_lib:config_key_path()) -> term().
get_zone_conf(Zone, KeyPath) ->
    case find(?ZONE_CONF_PATH(Zone, KeyPath)) of
        {not_found, _, _} -> %% not found in zones, try to find the global config
            ?MODULE:get(KeyPath);
        {ok, Value} -> Value
    end.

-spec get_zone_conf(atom(), emqx_map_lib:config_key_path(), term()) -> term().
get_zone_conf(Zone, KeyPath, Default) ->
    case find(?ZONE_CONF_PATH(Zone, KeyPath)) of
        {not_found, _, _} -> %% not found in zones, try to find the global config
            ?MODULE:get(KeyPath, Default);
        {ok, Value} -> Value
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
    maps:fold(fun(RootName, RootValue, _) ->
            ?MODULE:put([RootName], RootValue)
        end, ok, Config).

-spec put(emqx_map_lib:config_key_path(), term()) -> ok.
put(KeyPath, Config) -> do_put(?CONF, KeyPath, Config).

-spec get_default_value(emqx_map_lib:config_key_path()) -> {ok, term()} | {error, term()}.
get_default_value([RootName | _] = KeyPath) ->
    BinKeyPath = [bin(Key) || Key <- KeyPath],
    case find_raw([RootName]) of
        {ok, RawConf} ->
            RawConf1 = emqx_map_lib:deep_remove(BinKeyPath, #{bin(RootName) => RawConf}),
            try fill_defaults(get_schema_mod(RootName), RawConf1) of FullConf ->
                case emqx_map_lib:deep_find(BinKeyPath, FullConf) of
                    {not_found, _, _} -> {error, no_default_value};
                    {ok, Val} -> {ok, Val}
                end
            catch error : Reason -> {error, Reason}
            end;
        {not_found, _, _} ->
            {error, {rootname_not_found, RootName}}
    end.

-spec get_raw(emqx_map_lib:config_key_path()) -> term().
get_raw(KeyPath) -> do_get(?RAW_CONF, KeyPath).

-spec get_raw(emqx_map_lib:config_key_path(), term()) -> term().
get_raw(KeyPath, Default) -> do_get(?RAW_CONF, KeyPath, Default).

-spec put_raw(map()) -> ok.
put_raw(Config) ->
    maps:fold(fun(RootName, RootV, _) ->
            ?MODULE:put_raw([RootName], RootV)
        end, ok, hocon_schema:get_value([], Config)).

-spec put_raw(emqx_map_lib:config_key_path(), term()) -> ok.
put_raw(KeyPath, Config) -> do_put(?RAW_CONF, KeyPath, Config).

%%============================================================================
%% Load/Update configs From/To files
%%============================================================================

%% @doc Initial load of the given config files.
%% NOTE: The order of the files is significant, configs from files orderd
%% in the rear of the list overrides prior values.
-spec init_load(module(), [string()] | binary() | hocon:config()) -> ok.
init_load(SchemaMod, Conf) when is_list(Conf) orelse is_binary(Conf) ->
    ParseOptions = #{format => richmap},
    Parser = case is_binary(Conf) of
              true -> fun hocon:binary/2;
              false -> fun hocon:files/2
             end,
    case Parser(Conf, ParseOptions) of
        {ok, RawRichConf} ->
            init_load(SchemaMod, RawRichConf);
        {error, Reason} ->
            logger:error(#{msg => failed_to_load_hocon_conf,
                           reason => Reason
                          }),
            error(failed_to_load_hocon_conf)
    end;
init_load(SchemaMod, RawRichConf) when is_map(RawRichConf) ->
    %% check with richmap for line numbers in error reports (future enhancement)
    Opts = #{return_plain => true,
             nullable => true
            },
    %% this call throws exception in case of check failure
    {_AppEnvs, CheckedConf} = hocon_schema:map_translate(SchemaMod, RawRichConf, Opts),
    ok = save_schema_mod_and_names(SchemaMod),
    ok = save_to_config_map(emqx_map_lib:unsafe_atom_key_map(normalize_conf(CheckedConf)),
            normalize_conf(hocon_schema:richmap_to_map(RawRichConf))).

normalize_conf(Conf) ->
    maps:with(get_root_names(bin), Conf).

-spec check_config(module(), raw_config()) -> {AppEnvs, CheckedConf}
    when AppEnvs :: app_envs(), CheckedConf :: config().
check_config(SchemaMod, RawConf) ->
    Opts = #{return_plain => true,
             nullable => true,
             format => map
            },
    {AppEnvs, CheckedConf} =
        hocon_schema:map_translate(SchemaMod, RawConf, Opts),
    Conf = maps:with(maps:keys(RawConf), CheckedConf),
    {AppEnvs, emqx_map_lib:unsafe_atom_key_map(Conf)}.

-spec fill_defaults(raw_config()) -> map().
fill_defaults(RawConf) ->
    RootNames = get_root_names(bin),
    maps:fold(fun(Key, Conf, Acc) ->
            SubMap = #{Key => Conf},
            WithDefaults = case lists:member(Key, RootNames) of
                true -> fill_defaults(get_schema_mod(Key), SubMap);
                false -> SubMap
            end,
            maps:merge(Acc, WithDefaults)
        end, #{}, RawConf).

-spec fill_defaults(module(), raw_config()) -> map().
fill_defaults(SchemaMod, RawConf) ->
    hocon_schema:check_plain(SchemaMod, RawConf,
        #{nullable => true, no_conversion => true}, root_names_from_conf(RawConf)).

-spec read_override_conf() -> raw_config().
read_override_conf() ->
    load_hocon_file(emqx_override_conf_name(), map).

-spec save_schema_mod_and_names(module()) -> ok.
save_schema_mod_and_names(SchemaMod) ->
    RootNames = SchemaMod:structs(),
    OldMods = get_schema_mod(),
    OldNames = get_root_names(),
    NewMods = maps:from_list([{root_bin(Name), SchemaMod} || Name <- RootNames]),
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

get_root_names(bin) ->
    maps:keys(get_schema_mod()).

-spec save_configs(app_envs(), config(), raw_config(), raw_config()) -> ok | {error, term()}.
save_configs(_AppEnvs, Conf, RawConf, OverrideConf) ->
    %% We may need also support hot config update for the apps that use application envs.
    %% If that is the case uncomment the following line to update the configs to app env
    %save_to_app_env(AppEnvs),
    save_to_config_map(Conf, RawConf),
    %% TODO: merge RawConf to OverrideConf can be done here
    save_to_override_conf(OverrideConf).

-spec save_to_app_env([tuple()]) -> ok.
save_to_app_env(AppEnvs) ->
    lists:foreach(fun({AppName, Envs}) ->
            [application:set_env(AppName, Par, Val) || {Par, Val} <- Envs]
        end, AppEnvs).

-spec save_to_config_map(config(), raw_config()) -> ok.
save_to_config_map(Conf, RawConf) ->
    ?MODULE:put(Conf),
    ?MODULE:put_raw(RawConf).

-spec save_to_override_conf(raw_config()) -> ok | {error, term()}.
save_to_override_conf(RawConf) ->
    FileName = emqx_override_conf_name(),
    ok = filelib:ensure_dir(FileName),
    case file:write_file(FileName, jsx:prettify(jsx:encode(RawConf))) of
        ok -> ok;
        {error, Reason} ->
            logger:error("write to ~s failed, ~p", [FileName, Reason]),
            {error, Reason}
    end.

load_hocon_file(FileName, LoadType) ->
    case filelib:is_regular(FileName) of
        true ->
            {ok, Raw0} = hocon:load(FileName, #{format => LoadType}),
            Raw0;
        false -> #{}
    end.

emqx_override_conf_name() ->
    application:get_env(emqx, override_conf_file, "emqx_override.conf").

do_get(Type, KeyPath) ->
    Ref = make_ref(),
    Res = do_get(Type, KeyPath, Ref),
    case Res =:= Ref of
        true -> error({config_not_found, KeyPath});
        false -> Res
    end.

do_get(Type, [], Default) ->
    AllConf = lists:foldl(fun
            ({?PERSIS_KEY(Type0, RootName), Conf}, AccIn) when Type0 == Type ->
                AccIn#{conf_key(Type0, RootName) => Conf};
            (_, AccIn) -> AccIn
        end, #{}, persistent_term:get()),
    case map_size(AllConf) == 0 of
        true -> Default;
        false -> AllConf
    end;
do_get(Type, [RootName], Default) ->
    persistent_term:get(?PERSIS_KEY(Type, bin(RootName)), Default);
do_get(Type, [RootName | KeyPath], Default) ->
    RootV = persistent_term:get(?PERSIS_KEY(Type, bin(RootName)), #{}),
    do_deep_get(Type, KeyPath, RootV, Default).

do_put(Type, [], DeepValue) ->
    maps:fold(fun(RootName, Value, _Res) ->
            do_put(Type, [RootName], Value)
        end, ok, DeepValue);
do_put(Type, [RootName | KeyPath], DeepValue) ->
    OldValue = do_get(Type, [RootName], #{}),
    NewValue = do_deep_put(Type, KeyPath, OldValue, DeepValue),
    persistent_term:put(?PERSIS_KEY(Type, bin(RootName)), NewValue).

do_deep_get(?CONF, KeyPath, Map, Default) ->
    ?ATOM_CONF_PATH(KeyPath, emqx_map_lib:deep_get(AtomKeyPath, Map, Default),
        Default);
do_deep_get(?RAW_CONF, KeyPath, Map, Default) ->
    emqx_map_lib:deep_get([bin(Key) || Key <- KeyPath], Map, Default).

do_deep_put(?CONF, KeyPath, Map, Value) ->
    ?ATOM_CONF_PATH(KeyPath, emqx_map_lib:deep_put(AtomKeyPath, Map, Value),
        error({not_found, KeyPath}));
do_deep_put(?RAW_CONF, KeyPath, Map, Value) ->
    emqx_map_lib:deep_put([bin(Key) || Key <- KeyPath], Map, Value).

root_names_from_conf(RawConf) ->
    Keys = maps:keys(RawConf),
    StrNames = [str(K) || K <- Keys],
    AtomNames = lists:foldl(fun(K, Acc) ->
            try [atom(K) | Acc]
            catch error:badarg -> Acc
            end
        end, [], Keys),
    PossibleNames = StrNames ++ AtomNames,
    [Name || Name <- get_root_names(), lists:member(Name, PossibleNames)].

atom(Bin) when is_binary(Bin) ->
    binary_to_existing_atom(Bin, latin1);
atom(Str) when is_list(Str) ->
    list_to_existing_atom(Str);
atom(Atom) when is_atom(Atom) ->
    Atom.

str(Bin) when is_binary(Bin) ->
    binary_to_list(Bin);
str(Str) when is_list(Str) ->
    Str;
str(Atom) when is_atom(Atom) ->
    atom_to_list(Atom).

bin(Bin) when is_binary(Bin) -> Bin;
bin(Str) when is_list(Str) -> list_to_binary(Str);
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

conf_key(?CONF, RootName) ->
    atom(RootName);
conf_key(?RAW_CONF, RootName) ->
    bin(RootName).

root_bin({array, Bin}) -> bin(Bin);
root_bin(Bin) -> bin(Bin).
