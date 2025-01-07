%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-behaviour(emqx_config_backup).

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
    unload/0,
    update/1,
    get_raw/0
]).

%% callbacks for emqx_config_handler
-export([
    pre_config_update/3,
    post_config_update/5
]).

%% callbacks for emqx_config_backup
-export([
    import_config/1
]).

-type update_request() :: emqx_config:config().

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
    ok = emqx_conf:remove_handler([file_transfer]),
    maybe_stop().

get_raw() ->
    emqx:get_raw_config([file_transfer], #{}).

-spec update(emqx_config:config()) -> {ok, emqx_config:update_result()} | {error, term()}.
update(Config) ->
    emqx_conf:update([file_transfer], Config, #{override_to => cluster}).

%%----------------------------------------------------------------------------------------
%% Data backup
%%----------------------------------------------------------------------------------------

import_config(#{<<"file_transfer">> := FTConf}) ->
    OldFTConf = emqx:get_raw_config([file_transfer], #{}),
    NewFTConf = maps:merge(OldFTConf, FTConf),
    case emqx_conf:update([file_transfer], NewFTConf, #{override_to => cluster}) of
        {ok, #{raw_config := NewRawConf}} ->
            Changed = maps:get(changed, emqx_utils_maps:diff_maps(NewRawConf, FTConf)),
            ChangedPaths = [[file_transfer, K] || K <- maps:keys(Changed)],
            {ok, #{root_key => file_transfer, changed => ChangedPaths}};
        Error ->
            {error, #{root_key => file_transfer, reason => Error}}
    end;
import_config(_) ->
    {ok, #{root_key => file_transfer, changed => []}}.

%%--------------------------------------------------------------------
%% emqx_config_handler callbacks
%%--------------------------------------------------------------------

-spec pre_config_update(list(atom()), update_request(), emqx_config:raw_config()) ->
    {ok, emqx_config:update_request()} | {error, term()}.
pre_config_update([file_transfer | _], NewConfig, OldConfig) ->
    propagate_config_update(
        fun emqx_ft_storage_exporter_s3:pre_config_update/3,
        [<<"storage">>, <<"local">>, <<"exporter">>, <<"s3">>],
        NewConfig,
        OldConfig
    ).

-spec post_config_update(
    list(atom()),
    update_request(),
    emqx_config:config(),
    emqx_config:config(),
    emqx_config:app_envs()
) ->
    ok | {ok, Result :: any()} | {error, Reason :: term()}.
post_config_update([file_transfer | _], _Req, NewConfig, OldConfig, _AppEnvs) ->
    PropResult = propagate_config_update(
        fun emqx_ft_storage_exporter_s3:post_config_update/3,
        [storage, local, exporter, s3],
        NewConfig,
        OldConfig
    ),
    case PropResult of
        ok ->
            on_config_update(OldConfig, NewConfig);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

propagate_config_update(Fun, ConfKey, NewConfig, OldConfig) ->
    NewSubConf = emqx_utils_maps:deep_get(ConfKey, NewConfig, undefined),
    OldSubConf = emqx_utils_maps:deep_get(ConfKey, OldConfig, undefined),
    case Fun(ConfKey, NewSubConf, OldSubConf) of
        ok ->
            ok;
        {ok, undefined} ->
            {ok, NewConfig};
        {ok, NewSubConfUpdate} ->
            {ok, emqx_utils_maps:deep_put(ConfKey, NewConfig, NewSubConfUpdate)};
        {error, Reason} ->
            {error, Reason}
    end.

on_config_update(#{enable := false}, #{enable := false}) ->
    ok;
on_config_update(#{enable := true, storage := OldStorage}, #{enable := false}) ->
    ok = stop(OldStorage);
on_config_update(#{enable := false}, #{enable := true, storage := NewStorage}) ->
    ok = start(NewStorage);
on_config_update(#{enable := true, storage := OldStorage}, #{enable := true, storage := NewStorage}) ->
    ok = emqx_ft_storage:update_config(OldStorage, NewStorage).

maybe_start() ->
    case emqx_config:get([file_transfer]) of
        #{enable := true, storage := Storage} ->
            start(Storage);
        _ ->
            ok
    end.

maybe_stop() ->
    case emqx_config:get([file_transfer]) of
        #{enable := true, storage := Storage} ->
            stop(Storage);
        _ ->
            ok
    end.

start(Storage) ->
    ok = emqx_ft_storage:update_config(undefined, Storage),
    ok = emqx_ft:hook().

stop(Storage) ->
    ok = emqx_ft:unhook(),
    ok = emqx_ft_storage:update_config(Storage, undefined).
