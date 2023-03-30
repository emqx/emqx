%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc File Transfer configuration management module

-module(emqx_ft_conf).

-behaviour(emqx_config_handler).

%% Accessors
-export([storage/0]).
-export([gc_interval/1]).
-export([segments_ttl/1]).

%% Load/Unload
-export([
    load/0,
    unload/0
]).

%% callbacks for emqx_config_handler
-export([
    pre_config_update/3,
    post_config_update/5
]).

-type milliseconds() :: non_neg_integer().
-type seconds() :: non_neg_integer().

%%--------------------------------------------------------------------
%% Accessors
%%--------------------------------------------------------------------

-spec storage() -> _Storage | disabled.
storage() ->
    emqx_config:get([file_transfer, storage], disabled).

-spec gc_interval(_Storage) -> milliseconds().
gc_interval(_Storage) ->
    Conf = assert_storage(local),
    emqx_map_lib:deep_get([segments, gc, interval], Conf).

-spec segments_ttl(_Storage) -> {_Min :: seconds(), _Max :: seconds()}.
segments_ttl(_Storage) ->
    Conf = assert_storage(local),
    {
        emqx_map_lib:deep_get([segments, gc, minimum_segments_ttl], Conf),
        emqx_map_lib:deep_get([segments, gc, maximum_segments_ttl], Conf)
    }.

assert_storage(Type) ->
    case storage() of
        Conf = #{type := Type} ->
            Conf;
        Conf ->
            error({inapplicable, Conf})
    end.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec load() -> ok.
load() ->
    ok = emqx_ft_storage_exporter:update_exporter(
        undefined,
        emqx_config:get([file_transfer, storage])
    ),
    emqx_conf:add_handler([file_transfer], ?MODULE).

-spec unload() -> ok.
unload() ->
    emqx_conf:remove_handler([file_transfer]).

%%--------------------------------------------------------------------
%% emqx_config_handler callbacks
%%--------------------------------------------------------------------

-spec pre_config_update(list(atom()), emqx_config:update_request(), emqx_config:raw_config()) ->
    {ok, emqx_config:update_request()} | {error, term()}.
pre_config_update(_, Req, _Config) ->
    {ok, Req}.

-spec post_config_update(
    list(atom()),
    emqx_config:update_request(),
    emqx_config:config(),
    emqx_config:config(),
    emqx_config:app_envs()
) ->
    ok | {ok, Result :: any()} | {error, Reason :: term()}.
post_config_update(_Path, _Req, NewConfig, OldConfig, _AppEnvs) ->
    OldStorageConfig = maps:get(storage, OldConfig, undefined),
    NewStorageConfig = maps:get(storage, NewConfig, undefined),
    emqx_ft_storage_exporter:update_exporter(OldStorageConfig, NewStorageConfig).
