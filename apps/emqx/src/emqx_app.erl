%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([ start/2
        , prep_stop/1
        , stop/1
        , get_description/0
        , get_release/0
        , set_init_config_load_done/0
        , get_init_config_load_done/0
        , set_override_conf_file/1
        ]).

-include("emqx.hrl").
-include("emqx_release.hrl").
-include("logger.hrl").

-define(APP, emqx).

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_Type, _Args) ->
    ok = maybe_load_config(),
    ok = maybe_start_quicer(),
    ensure_ekka_started(),
    {ok, Sup} = emqx_sup:start_link(),
    ok = maybe_start_listeners(),
    ok = emqx_alarm_handler:load(),
    register(emqx, self()),
    {ok, Sup}.

prep_stop(_State) ->
    ok = emqx_alarm_handler:unload(),
    emqx_boot:is_enabled(listeners)
      andalso emqx_listeners:stop().

stop(_State) -> ok.

ensure_ekka_started() ->
    ekka:start(),
    ok = ekka_rlog:wait_for_shards(?BOOT_SHARDS, infinity).

%% @doc Call this function to make emqx boot without loading config,
%% in case we want to delegate the config load to a higher level app
%% which manages emqx app.
set_init_config_load_done() ->
    application:set_env(emqx, init_config_load_done, true).

get_init_config_load_done() ->
    application:get_env(emqx, init_config_load_done, false).

%% @doc This API is mostly for testing.
%% The override config file is typically located in the 'data' dir when
%% it is a emqx release, but emqx app should not have to know where the
%% 'data' dir is located.
set_override_conf_file(File) ->
    application:set_env(emqx, override_conf_file, File).

maybe_load_config() ->
    case get_init_config_load_done() of
        true ->
            ok;
        false ->
            %% the app env 'config_files' should be set before emqx get started.
            ConfFiles = application:get_env(emqx, config_files, []),
            emqx_config:init_load(emqx_schema, ConfFiles)
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
        true -> {ok, _} = application:ensure_all_started(quicer), ok;
        false -> ok
    end.

is_quicer_app_present() ->
    case application:load(quicer) of
        ok -> true;
        {error, {already_loaded, _}} -> true;
        _ ->
            ?SLOG(info, #{msg => "quicer_app_not_found"}),
            false
    end.

is_quic_listener_configured() ->
    emqx_listeners:has_enabled_listener_conf_by_type(quic).

get_description() ->
    {ok, Descr0} = application:get_key(?APP, description),
    case os:getenv("EMQX_DESCRIPTION") of
        false -> Descr0;
        "" -> Descr0;
        Str -> string:strip(Str, both, $\n)
    end.

get_release() ->
    case lists:keyfind(emqx_vsn, 1, ?MODULE:module_info(compile)) of
        false ->    %% For TEST build or depedency build.
            release_in_macro();
        {_, Vsn} -> %% For emqx release build
            VsnStr = release_in_macro(),
            case string:str(Vsn, VsnStr) of
                1 -> ok;
                _ ->
                    erlang:error(#{ reason => version_mismatch
                                  , source => VsnStr
                                  , built_for => Vsn
                                  })
            end,
            Vsn
    end.

release_in_macro() ->
    element(2, ?EMQX_RELEASE).
