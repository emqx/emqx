%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_app).

-behaviour(application).

-export([
    start/2,
    prep_stop/1,
    stop/1,
    get_description/0,
    get_release/0,
    set_init_config_load_done/0,
    get_init_config_load_done/0,
    set_init_tnx_id/1,
    get_init_tnx_id/0
]).

-include("emqx.hrl").
-include("logger.hrl").

-define(APP, emqx).

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_Type, _Args) ->
    ok = maybe_load_config(),
    ok = emqx_persistent_session:init_db_backend(),
    ok = maybe_start_quicer(),
    ok = emqx_bpapi:start(),
    wait_boot_shards(),
    ok = emqx_alarm_handler:load(),
    {ok, Sup} = emqx_sup:start_link(),
    ok = maybe_start_listeners(),
    emqx_config:add_handlers(),
    register(emqx, self()),
    {ok, Sup}.

prep_stop(_State) ->
    ok = emqx_alarm_handler:unload(),
    emqx_config:remove_handlers(),
    emqx_boot:is_enabled(listeners) andalso
        emqx_listeners:stop().

stop(_State) -> ok.

wait_boot_shards() ->
    ok = mria_rlog:wait_for_shards(?BOOT_SHARDS, infinity).

%% @doc Call this function to make emqx boot without loading config,
%% in case we want to delegate the config load to a higher level app
%% which manages emqx app.
set_init_config_load_done() ->
    application:set_env(emqx, init_config_load_done, true).

get_init_config_load_done() ->
    application:get_env(emqx, init_config_load_done, false).

set_init_tnx_id(TnxId) ->
    application:set_env(emqx, cluster_rpc_init_tnx_id, TnxId).

get_init_tnx_id() ->
    application:get_env(emqx, cluster_rpc_init_tnx_id, -1).

maybe_load_config() ->
    case get_init_config_load_done() of
        true -> ok;
        false -> emqx_config:init_load(emqx_schema)
    end.

maybe_start_listeners() ->
    case emqx_boot:is_enabled(listeners) of
        true ->
            ok = emqx_listeners:start();
        false ->
            ok
    end.

maybe_start_quicer() ->
    case is_quicer_app_present() andalso is_quic_listener_configured() of
        true ->
            {ok, _} = application:ensure_all_started(quicer),
            ok;
        false ->
            ok
    end.

is_quicer_app_present() ->
    case application:load(quicer) of
        ok ->
            true;
        {error, {already_loaded, _}} ->
            true;
        _ ->
            ?SLOG(info, #{msg => "quicer_app_not_found"}),
            false
    end.

is_quic_listener_configured() ->
    maps:is_key(quic, emqx:get_config([listeners])).

get_description() -> emqx_release:description().

get_release() ->
    emqx_release:version().
