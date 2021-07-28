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
        , save_configs/2
        , save_to_app_env/1
        , save_to_emqx_config/2
        , save_to_override_conf/1
        , to_richmap/1
        , to_plainmap/1
        ]).

-export([get_root/1,
         get_root_raw/1]).

-export([ get/1
        , get/2
        , find/1
        , put/1
        , put/2
        ]).

-export([ get_zone_conf/2
        , get_zone_conf/3
        , put_zone_conf/3
        , find_zone_conf/2
        ]).

-export([ get_listener_conf/3
        , get_listener_conf/4
        , put_listener_conf/4
        , find_listener_conf/3
        ]).

-export([ update/2
        , remove/1
        ]).

%% raw configs is the config that is now parsed and tranlated by hocon schema
-export([ get_raw/1
        , get_raw/2
        , put_raw/1
        , put_raw/2
        ]).

-define(CONF, fun(ROOT) -> {?MODULE, bin(ROOT)} end).
-define(RAW_CONF, fun(ROOT) -> {?MODULE, raw, bin(ROOT)} end).
-define(ZONE_CONF_PATH(ZONE, PATH), [zones, ZONE | PATH]).
-define(LISTENER_CONF_PATH(ZONE, LISTENER, PATH), [zones, ZONE, listeners, LISTENER | PATH]).

-export_type([update_request/0, raw_config/0, config/0]).
-type update_request() :: term().
-type raw_config() :: #{binary() => term()} | undefined.
-type config() :: #{atom() => term()} | undefined.

%% @doc For the given path, get root value enclosed in a single-key map.
-spec get_root(emqx_map_lib:config_key_path()) -> map().
get_root([RootName | _]) ->
    #{RootName => do_get(?CONF, [RootName])}.

%% @doc For the given path, get raw root value enclosed in a single-key map.
%% key is ensured to be binary.
get_root_raw([RootName | _]) ->
    #{bin(RootName) => do_get(?RAW_CONF, [RootName])}.

%% @doc Get a config value for the given path.
%% The path should at least include root config name.
-spec get(emqx_map_lib:config_key_path()) -> term().
get(KeyPath) -> do_get(?CONF, KeyPath).

-spec get(emqx_map_lib:config_key_path(), term()) -> term().
get(KeyPath, Default) -> do_get(?CONF, KeyPath, Default).

-spec find(emqx_map_lib:config_key_path()) ->
    {ok, term()} | {not_found, emqx_map_lib:config_key_path(), term()}.
find(KeyPath) ->
    emqx_map_lib:deep_find(KeyPath, get_root(KeyPath)).

-spec get_zone_conf(atom(), emqx_map_lib:config_key_path()) -> term().
get_zone_conf(Zone, KeyPath) ->
    ?MODULE:get(?ZONE_CONF_PATH(Zone, KeyPath)).

-spec get_zone_conf(atom(), emqx_map_lib:config_key_path(), term()) -> term().
get_zone_conf(Zone, KeyPath, Default) ->
    ?MODULE:get(?ZONE_CONF_PATH(Zone, KeyPath), Default).

-spec put_zone_conf(atom(), emqx_map_lib:config_key_path(), term()) -> ok.
put_zone_conf(Zone, KeyPath, Conf) ->
    ?MODULE:put(?ZONE_CONF_PATH(Zone, KeyPath), Conf).

-spec find_zone_conf(atom(), emqx_map_lib:config_key_path()) ->
    {ok, term()} | {not_found, emqx_map_lib:config_key_path(), term()}.
find_zone_conf(Zone, KeyPath) ->
    find(?ZONE_CONF_PATH(Zone, KeyPath)).

-spec get_listener_conf(atom(), atom(), emqx_map_lib:config_key_path()) -> term().
get_listener_conf(Zone, Listener, KeyPath) ->
    ?MODULE:get(?LISTENER_CONF_PATH(Zone, Listener, KeyPath)).

-spec get_listener_conf(atom(), atom(), emqx_map_lib:config_key_path(), term()) -> term().
get_listener_conf(Zone, Listener, KeyPath, Default) ->
    ?MODULE:get(?LISTENER_CONF_PATH(Zone, Listener, KeyPath), Default).

-spec put_listener_conf(atom(), atom(), emqx_map_lib:config_key_path(), term()) -> ok.
put_listener_conf(Zone, Listener, KeyPath, Conf) ->
    ?MODULE:put(?LISTENER_CONF_PATH(Zone, Listener, KeyPath), Conf).

-spec find_listener_conf(atom(), atom(), emqx_map_lib:config_key_path()) ->
    {ok, term()} | {not_found, emqx_map_lib:config_key_path(), term()}.
find_listener_conf(Zone, Listener, KeyPath) ->
    find(?LISTENER_CONF_PATH(Zone, Listener, KeyPath)).

-spec put(map()) -> ok.
put(Config) ->
    maps:fold(fun(RootName, RootValue, _) ->
                      ?MODULE:put([RootName], RootValue)
              end, [], Config).

-spec put(emqx_map_lib:config_key_path(), term()) -> ok.
put(KeyPath, Config) -> do_put(?CONF, KeyPath, Config).

-spec update(emqx_map_lib:config_key_path(), update_request()) ->
    ok | {error, term()}.
update(ConfKeyPath, UpdateReq) ->
    emqx_config_handler:update_config(ConfKeyPath, UpdateReq).

-spec remove(emqx_map_lib:config_key_path()) -> ok | {error, term()}.
remove(ConfKeyPath) ->
    emqx_config_handler:remove_config(ConfKeyPath).

-spec get_raw(emqx_map_lib:config_key_path()) -> term().
get_raw(KeyPath) -> do_get(?RAW_CONF, KeyPath).

-spec get_raw(emqx_map_lib:config_key_path(), term()) -> term().
get_raw(KeyPath, Default) -> do_get(?RAW_CONF, KeyPath, Default).

-spec put_raw(map()) -> ok.
put_raw(Config) ->
    maps:fold(fun(RootName, RootV, _) ->
                      ?MODULE:put_raw([RootName], RootV)
              end, [], hocon_schema:get_value([], Config)).

-spec put_raw(emqx_map_lib:config_key_path(), term()) -> ok.
put_raw(KeyPath, Config) -> do_put(?RAW_CONF, KeyPath, Config).

%%============================================================================
%% Load/Update configs From/To files
%%============================================================================

%% @doc Initial load of the given config files.
%% NOTE: The order of the files is significant, configs from files orderd
%% in the rear of the list overrides prior values.
-spec init_load(module(), [string()]) -> ok.
init_load(SchemaModule, ConfFiles) ->
    RawRichConf = lists:foldl(fun(ConfFile, Acc) ->
        Raw = load_hocon_file(ConfFile, richmap),
        emqx_map_lib:deep_merge(Acc, Raw)
    end, #{}, ConfFiles),
    {_MappedEnvs, RichConf} = hocon_schema:map_translate(SchemaModule, RawRichConf, #{}),
    ok = save_to_emqx_config(to_plainmap(RichConf), to_plainmap(RawRichConf)).

-spec read_override_conf() -> raw_config().
read_override_conf() ->
    load_hocon_file(emqx_override_conf_name(), map).

-spec save_configs(raw_config(), raw_config()) -> ok | {error, term()}.
save_configs(RawConf, OverrideConf) ->
    {_MappedEnvs, RichConf} = hocon_schema:map_translate(emqx_schema, to_richmap(RawConf), #{}),
    %% We may need also support hot config update for the apps that use application envs.
    %% If that is the case uncomment the following line to update the configs to application env
    %save_to_app_env(_MappedEnvs),
    Conf = maps:with(maps:keys(RawConf), to_plainmap(RichConf)),
    save_to_emqx_config(Conf, RawConf),
    save_to_override_conf(OverrideConf).

-spec save_to_app_env([tuple()]) -> ok.
save_to_app_env(AppEnvs) ->
    lists:foreach(fun({AppName, Envs}) ->
            [application:set_env(AppName, Par, Val) || {Par, Val} <- Envs]
        end, AppEnvs).

-spec save_to_emqx_config(config(), raw_config()) -> ok.
save_to_emqx_config(Conf, RawConf) ->
    ?MODULE:put(emqx_map_lib:unsafe_atom_key_map(Conf)),
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
    filename:join([?MODULE:get([node, data_dir]), "emqx_override.conf"]).

to_richmap(Map) ->
    {ok, RichMap} = hocon:binary(jsx:encode(Map), #{format => richmap}),
    RichMap.

to_plainmap(RichMap) ->
    hocon_schema:richmap_to_map(RichMap).

bin(Bin) when is_binary(Bin) -> Bin;
bin(Atom) when is_atom(Atom) -> atom_to_binary(Atom, utf8).

do_get(PtKey, [RootName | KeyPath]) ->
    RootV = persistent_term:get(PtKey(RootName), #{}),
    emqx_map_lib:deep_get(KeyPath, RootV).

do_get(PtKey, [RootName | KeyPath], Default) ->
    RootV = persistent_term:get(PtKey(RootName), #{}),
    emqx_map_lib:deep_get(KeyPath, RootV, Default).

do_put(PtKey, [RootName | KeyPath], DeepValue) ->
    OldValue = do_get(PtKey, [RootName]),
    NewValue = emqx_map_lib:deep_put(KeyPath, OldValue, DeepValue),
    persistent_term:put(PtKey(RootName), NewValue).
