%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_gateway).

-behaviour(emqx_config_handler).

-include("include/emqx_gateway.hrl").

%% callbacks for emqx_config_handler
-export([ pre_config_update/2
        , post_config_update/4
        ]).

%% APIs
-export([ registered_gateway/0
        , load/2
        , unload/1
        , lookup/1
        , update/2
        , start/1
        , stop/1
        , list/0
        ]).

-export([update_rawconf/2]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

-spec registered_gateway() ->
    [{gateway_name(), emqx_gateway_registry:descriptor()}].
registered_gateway() ->
    emqx_gateway_registry:list().

%%--------------------------------------------------------------------
%% Gateway Instace APIs

-spec list() -> [gateway()].
list() ->
    emqx_gateway_sup:list_gateway_insta().

-spec load(gateway_name(), emqx_config:config())
    -> {ok, pid()}
     | {error, any()}.
load(Name, Config) ->
    Gateway = #{ name => Name
               , descr => undefined
               , config => Config
               },
    emqx_gateway_sup:load_gateway(Gateway).

-spec unload(gateway_name()) -> ok | {error, not_found}.
unload(Name) ->
    emqx_gateway_sup:unload_gateway(Name).

-spec lookup(gateway_name()) -> gateway() | undefined.
lookup(Name) ->
    emqx_gateway_sup:lookup_gateway(Name).

-spec update(gateway_name(), emqx_config:config()) -> ok | {error, any()}.
update(Name, Config) ->
    emqx_gateway_sup:update_gateway(Name, Config).

-spec start(gateway_name()) -> ok | {error, any()}.
start(Name) ->
    emqx_gateway_sup:start_gateway_insta(Name).

-spec stop(gateway_name()) -> ok | {error, any()}.
stop(Name) ->
    emqx_gateway_sup:stop_gateway_insta(Name).

-spec update_rawconf(binary(), emqx_config:raw_config())
    -> ok
     | {error, any()}.
update_rawconf(RawName, RawConfDiff) ->
    case emqx:update_config([gateway], {RawName, RawConfDiff}) of
        {ok, _Result} -> ok;
        {error, Reason} -> {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Config Handler

-spec pre_config_update(emqx_config:update_request(), emqx_config:raw_config()) ->
    {ok, emqx_config:update_request()} | {error, term()}.
pre_config_update({RawName, RawConfDiff}, RawConf) ->
    {ok, emqx_map_lib:deep_merge(RawConf, #{RawName => RawConfDiff})}.

-spec post_config_update(emqx_config:update_request(), emqx_config:config(),
                         emqx_config:config(), emqx_config:app_envs())
    -> ok | {ok, Result::any()} | {error, Reason::term()}.
post_config_update({RawName, _}, NewConfig, OldConfig, _AppEnvs) ->
    GwName = binary_to_existing_atom(RawName),
    SubConf = maps:get(GwName, NewConfig),
    case maps:get(GwName, OldConfig, undefined) of
        undefined ->
            emqx_gateway:load(GwName, SubConf);
        _ ->
            emqx_gateway:update(GwName, SubConf)
    end.

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

