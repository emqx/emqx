%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_app).

-behaviour(application).

-export([
    start/2,
    prep_stop/1,
    stop/1,
    get_description/0,
    get_release/0,
    set_config_loader/1,
    get_config_loader/0,
    unset_config_loaded/0,
    init_load_done/0
]).

-include("logger.hrl").

%%--------------------------------------------------------------------
%% Application callbacks
%%--------------------------------------------------------------------

start(_Type, _Args) ->
    _ = emqx_config:create_tables(),
    ok = maybe_load_config(),
    ok = maybe_start_quicer(),
    ok = emqx_alarm_handler:load(),
    {ok, Sup} = emqx_sup:start_link(),
    ok = emqx_limiter:init(),
    ok = maybe_start_listeners(),
    emqx_config:add_handlers(),
    register(emqx, self()),
    {ok, Sup}.

prep_stop(_State) ->
    ok = emqx_alarm_handler:unload(),
    emqx_config:remove_handlers(),
    emqx_boot:is_enabled(listeners) andalso
        emqx_listeners:stop().

stop(_State) ->
    ok.

-define(CONFIG_LOADER, config_loader).
-define(DEFAULT_LOADER, emqx).
%% @doc Call this function to make emqx boot without loading config,
%% in case we want to delegate the config load to a higher level app
%% which manages emqx app.
set_config_loader(Module) when is_atom(Module) ->
    application:set_env(emqx, ?CONFIG_LOADER, Module).

get_config_loader() ->
    application:get_env(emqx, ?CONFIG_LOADER, ?DEFAULT_LOADER).

unset_config_loaded() ->
    application:unset_env(emqx, ?CONFIG_LOADER).

init_load_done() ->
    get_config_loader() =/= ?DEFAULT_LOADER.

maybe_load_config() ->
    case get_config_loader() of
        emqx ->
            emqx_config:init_load(emqx_schema);
        Module ->
            ?SLOG(debug, #{
                msg => "skip_init_config_load",
                reason => "Some application has set another config loader",
                loader => Module
            })
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
