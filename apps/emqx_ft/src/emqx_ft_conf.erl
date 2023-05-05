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

-include_lib("emqx/include/logger.hrl").

%% Accessors
-export([enabled/0]).
-export([storage/0]).
-export([gc_interval/1]).
-export([segments_ttl/1]).
-export([init_timeout/0]).
-export([store_segment_timeout/0]).
-export([assemble_timeout/0]).

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

-spec enabled() -> boolean().
enabled() ->
    emqx_config:get([file_transfer, enable], false).

-spec storage() -> emqx_config:config().
storage() ->
    emqx_config:get([file_transfer, storage]).

-spec gc_interval(emqx_ft_storage_fs:storage()) ->
    emqx_maybe:t(milliseconds()).
gc_interval(Storage) ->
    emqx_utils_maps:deep_get([segments, gc, interval], Storage, undefined).

-spec segments_ttl(emqx_ft_storage_fs:storage()) ->
    emqx_maybe:t({_Min :: seconds(), _Max :: seconds()}).
segments_ttl(Storage) ->
    Min = emqx_utils_maps:deep_get([segments, gc, minimum_segments_ttl], Storage, undefined),
    Max = emqx_utils_maps:deep_get([segments, gc, maximum_segments_ttl], Storage, undefined),
    case is_integer(Min) andalso is_integer(Max) of
        true ->
            {Min, Max};
        false ->
            undefined
    end.

init_timeout() ->
    emqx_config:get([file_transfer, init_timeout]).

assemble_timeout() ->
    emqx_config:get([file_transfer, assemble_timeout]).

store_segment_timeout() ->
    emqx_config:get([file_transfer, store_segment_timeout]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec load() -> ok.
load() ->
    ok = maybe_start(),
    emqx_conf:add_handler([file_transfer], ?MODULE).

-spec unload() -> ok.
unload() ->
    ok = stop(),
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
post_config_update([file_transfer | _], _Req, NewConfig, OldConfig, _AppEnvs) ->
    on_config_update(OldConfig, NewConfig).

on_config_update(#{enable := false}, #{enable := false}) ->
    ok;
on_config_update(#{enable := true, storage := OldStorage}, #{enable := false}) ->
    ok = emqx_ft_storage:on_config_update(OldStorage, undefined),
    ok = emqx_ft:unhook();
on_config_update(#{enable := false}, #{enable := true, storage := NewStorage}) ->
    ok = emqx_ft_storage:on_config_update(undefined, NewStorage),
    ok = emqx_ft:hook();
on_config_update(#{enable := true, storage := OldStorage}, #{enable := true, storage := NewStorage}) ->
    ok = emqx_ft_storage:on_config_update(OldStorage, NewStorage).

maybe_start() ->
    case emqx_config:get([file_transfer]) of
        #{enable := true, storage := Storage} ->
            ok = emqx_ft_storage:on_config_update(undefined, Storage),
            ok = emqx_ft:hook();
        _ ->
            ok
    end.

stop() ->
    ok = emqx_ft:unhook(),
    ok = emqx_ft_storage:on_config_update(storage(), undefined).
