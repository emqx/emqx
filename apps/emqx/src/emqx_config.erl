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

-compile({no_auto_import, [get/0, get/1]}).

-export([ get/0
        , get/1
        , get/2
        , find/1
        , put/1
        , put/2
        ]).

-export([ get_listener_conf/3
        , get_listener_conf/4
        , put_listener_conf/4
        , find_listener_conf/3
        ]).

-export([ update_config/2
        ]).

%% raw configs is the config that is now parsed and tranlated by hocon schema
-export([ get_raw/0
        , get_raw/1
        , get_raw/2
        , put_raw/1
        , put_raw/2
        ]).

-define(CONF, ?MODULE).
-define(RAW_CONF, {?MODULE, raw}).

-export_type([update_request/0, raw_config/0, config/0]).
-type update_request() :: term().
-type raw_config() :: #{binary() => term()} | undefined.
-type config() :: #{atom() => term()} | undefined.

-spec get() -> map().
get() ->
    persistent_term:get(?CONF, #{}).

-spec get(emqx_map_lib:config_key_path()) -> term().
get(KeyPath) ->
    emqx_map_lib:deep_get(KeyPath, get()).

-spec get(emqx_map_lib:config_key_path(), term()) -> term().
get(KeyPath, Default) ->
    emqx_map_lib:deep_get(KeyPath, get(), Default).

-spec find(emqx_map_lib:config_key_path()) ->
    {ok, term()} | {not_found, emqx_map_lib:config_key_path(), term()}.
find(KeyPath) ->
    emqx_map_lib:deep_find(KeyPath, get()).

-spec get_listener_conf(atom(), atom(), emqx_map_lib:config_key_path()) -> term().
get_listener_conf(Zone, Listener, KeyPath) ->
    case find_listener_conf(Zone, Listener, KeyPath) of
        {not_found, SubKeyPath, Data} -> error({not_found, SubKeyPath, Data});
        {ok, Data} -> Data
    end.

-spec get_listener_conf(atom(), atom(), emqx_map_lib:config_key_path(), term()) -> term().
get_listener_conf(Zone, Listener, KeyPath, Default) ->
    case find_listener_conf(Zone, Listener, KeyPath) of
        {not_found, _, _} -> Default;
        {ok, Data} -> Data
    end.

-spec put_listener_conf(atom(), atom(), emqx_map_lib:config_key_path(), term()) -> ok.
put_listener_conf(Zone, Listener, KeyPath, Conf) ->
    ?MODULE:put([zones, Zone, listeners, Listener | KeyPath], Conf).

-spec find_listener_conf(atom(), atom(), emqx_map_lib:config_key_path()) ->
    {ok, term()} | {not_found, emqx_map_lib:config_key_path(), term()}.
find_listener_conf(Zone, Listener, KeyPath) ->
    %% the configs in listener is prior to the ones in the zone
    case find([zones, Zone, listeners, Listener | KeyPath]) of
        {not_found, _, _} -> find([zones, Zone | KeyPath]);
        {ok, Data} -> {ok, Data}
    end.

-spec put(map()) -> ok.
put(Config) ->
    persistent_term:put(?CONF, Config).

-spec put(emqx_map_lib:config_key_path(), term()) -> ok.
put(KeyPath, Config) ->
    put(emqx_map_lib:deep_put(KeyPath, get(), Config)).

-spec update_config(emqx_map_lib:config_key_path(), update_request()) ->
    ok | {error, term()}.
update_config(ConfKeyPath, UpdateReq) ->
    emqx_config_handler:update_config(ConfKeyPath, UpdateReq, get_raw()).

-spec get_raw() -> map().
get_raw() ->
    persistent_term:get(?RAW_CONF, #{}).

-spec get_raw(emqx_map_lib:config_key_path()) -> term().
get_raw(KeyPath) ->
    emqx_map_lib:deep_get(KeyPath, get_raw()).

-spec get_raw(emqx_map_lib:config_key_path(), term()) -> term().
get_raw(KeyPath, Default) ->
    emqx_map_lib:deep_get(KeyPath, get_raw(), Default).

-spec put_raw(map()) -> ok.
put_raw(Config) ->
    persistent_term:put(?RAW_CONF, Config).

-spec put_raw(emqx_map_lib:config_key_path(), term()) -> ok.
put_raw(KeyPath, Config) ->
    put_raw(emqx_map_lib:deep_put(KeyPath, get_raw(), Config)).
