%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_dashboard_listener).

-behaviour(emqx_config_handler).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([add_handler/0, remove_handler/0]).
-export([pre_config_update/3, post_config_update/5]).
-export([regenerate_minirest_dispatch/0]).
-export([delay_job/1]).

-behaviour(gen_server).

-export([start_link/0, is_ready/1]).

-export([
    init/1,
    handle_continue/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2,
    code_change/3
]).

is_ready(Timeout) ->
    try
        ready =:= gen_server:call(?MODULE, is_ready, Timeout)
    catch
        exit:{timeout, _} ->
            false
    end.

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(trap_exit, true),
    ok = add_handler(),
    {ok, undefined, {continue, regenerate_dispatch}}.

handle_continue(regenerate_dispatch, _State) ->
    %% initialize the swagger dispatches
    ready = regenerate_minirest_dispatch(),
    {noreply, ready, hibernate}.

handle_call(is_ready, _From, State) ->
    {reply, State, State, hibernate};
handle_call(_Request, _From, State) ->
    {reply, ok, State, hibernate}.

handle_cast(_Request, State) ->
    {noreply, State, hibernate}.

handle_info(regenerate, _State) ->
    NewState = regenerate_minirest_dispatch(),
    {noreply, NewState, hibernate};
handle_info({update_listeners, OldListeners, NewListeners}, _State) ->
    ok = emqx_dashboard:stop_listeners(OldListeners),
    ok = emqx_dashboard:start_listeners(NewListeners),
    NewState = regenerate_minirest_dispatch(),
    {noreply, NewState, hibernate};
handle_info(_Info, State) ->
    {noreply, State, hibernate}.

terminate(_Reason, _State) ->
    ok = remove_handler(),
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% generate dispatch is very slow, takes about 1s.
regenerate_minirest_dispatch() ->
    %% optvar:read waits for the var to be set
    Names = emqx_dashboard:wait_for_listeners(),
    {Time, ok} = timer:tc(fun() -> do_regenerate_minirest_dispatch(Names) end),
    Lang = emqx:get_config([dashboard, i18n_lang]),
    ?tp(info, regenerate_minirest_dispatch, #{
        elapsed => erlang:convert_time_unit(Time, microsecond, millisecond),
        listeners => Names,
        i18n_lang => Lang
    }),
    ready.

do_regenerate_minirest_dispatch(Names) ->
    lists:foreach(
        fun(Name) ->
            ok = minirest:update_dispatch(Name)
        end,
        Names
    ).

add_handler() ->
    Roots = emqx_dashboard_schema:roots(),
    ok = emqx_config_handler:add_handler(Roots, ?MODULE),
    ok.

remove_handler() ->
    Roots = emqx_dashboard_schema:roots(),
    ok = emqx_config_handler:remove_handler(Roots),
    ok.

pre_config_update(_Path, {change_i18n_lang, NewLang}, RawConf) ->
    %% e.g. emqx_conf:update([dashboard], {change_i18n_lang, zh}, #{}).
    %% TODO: check if there is such a language (all languages are cached in emqx_dashboard_desc_cache)
    Update = #{<<"i18n_lang">> => NewLang},
    NewConf = emqx_utils_maps:deep_merge(RawConf, Update),
    {ok, NewConf};
pre_config_update(_Path, UpdateConf0, RawConf) ->
    UpdateConf = remove_sensitive_data(UpdateConf0),
    NewConf = emqx_utils_maps:deep_merge(RawConf, UpdateConf),
    ensure_ssl_cert(NewConf).

-define(SENSITIVE_PASSWORD, <<"******">>).

remove_sensitive_data(Conf0) ->
    Conf1 =
        case Conf0 of
            #{<<"default_password">> := ?SENSITIVE_PASSWORD} ->
                maps:remove(<<"default_password">>, Conf0);
            _ ->
                Conf0
        end,
    case Conf1 of
        #{<<"listeners">> := #{<<"https">> := #{<<"password">> := ?SENSITIVE_PASSWORD}}} ->
            emqx_utils_maps:deep_remove([<<"listeners">>, <<"https">>, <<"password">>], Conf1);
        _ ->
            Conf1
    end.

post_config_update(_, {change_i18n_lang, _}, _NewConf, _OldConf, _AppEnvs) ->
    delay_job(regenerate);
post_config_update(_, _Req, NewConf, OldConf, _AppEnvs) ->
    SwaggerSupport = diff_swagger_support(NewConf, OldConf),
    OldHttp = get_listener(http, OldConf),
    OldHttps = get_listener(https, OldConf),
    NewHttp = get_listener(http, NewConf),
    NewHttps = get_listener(https, NewConf),
    {StopHttp, StartHttp} = diff_listeners(http, OldHttp, NewHttp, SwaggerSupport),
    {StopHttps, StartHttps} = diff_listeners(https, OldHttps, NewHttps, SwaggerSupport),
    Stop = maps:merge(StopHttp, StopHttps),
    Start = maps:merge(StartHttp, StartHttps),
    delay_job({update_listeners, Stop, Start}).

%% in post_config_update, the config is not yet persisted to persistent_term
%% so we need to delegate the listener update to the gen_server a bit later
delay_job(Msg) ->
    _ = erlang:send_after(500, ?MODULE, Msg),
    ok.

get_listener(Type, Conf) ->
    emqx_utils_maps:deep_get([listeners, Type], Conf, undefined).

diff_swagger_support(NewConf, OldConf) ->
    maps:get(swagger_support, NewConf, true) =:=
        maps:get(swagger_support, OldConf, true).

diff_listeners(_, undefined, undefined, _) -> {#{}, #{}};
diff_listeners(_, Listener, Listener, true) -> {#{}, #{}};
diff_listeners(Type, undefined, Start, _) -> {#{}, #{Type => Start}};
diff_listeners(Type, Stop, undefined, _) -> {#{Type => Stop}, #{}};
diff_listeners(Type, Listener, Listener, false) -> {#{Type => Listener}, #{Type => Listener}};
diff_listeners(Type, Stop, Start, _) -> {#{Type => Stop}, #{Type => Start}}.

-define(DIR, <<"dashboard">>).

ensure_ssl_cert(#{<<"listeners">> := #{<<"https">> := #{<<"bind">> := Bind} = Https0}} = Conf0) when
    Bind =/= 0
->
    Https1 = emqx_dashboard_schema:https_converter(Https0, #{}),
    Conf1 = emqx_utils_maps:deep_put([<<"listeners">>, <<"https">>], Conf0, Https1),
    Ssl = maps:get(<<"ssl_options">>, Https1, undefined),
    Opts = #{required_keys => [[<<"keyfile">>], [<<"certfile">>]]},
    case emqx_tls_lib:ensure_ssl_files_in_mutable_certs_dir(?DIR, Ssl, Opts) of
        {ok, undefined} ->
            {error, <<"ssl_cert_not_found">>};
        {ok, NewSsl} ->
            Keys = [<<"listeners">>, <<"https">>, <<"ssl_options">>],
            {ok, emqx_utils_maps:deep_put(Keys, Conf1, NewSsl)};
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "bad_ssl_config"}),
            {error, Reason}
    end;
ensure_ssl_cert(Conf) ->
    {ok, Conf}.
