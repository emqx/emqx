%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/logger.hrl").
-behaviour(emqx_config_handler).

%% API
-export([add_handler/0, remove_handler/0]).
-export([pre_config_update/3, post_config_update/5]).
-export([regenerate_minirest_dispatch/0]).

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
    ready =:= gen_server:call(?MODULE, is_ready, Timeout).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

init([]) ->
    erlang:process_flag(trap_exit, true),
    ok = add_handler(),
    {ok, undefined, {continue, regenerate_dispatch}}.

handle_continue(regenerate_dispatch, _State) ->
    NewState = regenerate_minirest_dispatch(),
    {noreply, NewState, hibernate}.

handle_call(is_ready, _From, retry) ->
    NewState = regenerate_minirest_dispatch(),
    {reply, NewState, NewState, hibernate};
handle_call(is_ready, _From, State) ->
    {reply, State, State, hibernate};
handle_call(_Request, _From, State) ->
    {reply, ok, State, hibernate}.

handle_cast(_Request, State) ->
    {noreply, State, hibernate}.

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

%% generate dispatch is very slow.
regenerate_minirest_dispatch() ->
    try
        emqx_dashboard:init_i18n(),
        lists:foreach(
            fun(Listener) ->
                minirest:update_dispatch(element(1, Listener))
            end,
            emqx_dashboard:list_listeners()
        ),
        ready
    catch
        T:E:S ->
            ?SLOG(error, #{
                msg => "regenerate_minirest_dispatch_failed",
                reason => E,
                type => T,
                stacktrace => S
            }),
            retry
    after
        emqx_dashboard:clear_i18n()
    end.

add_handler() ->
    Roots = emqx_dashboard_schema:roots(),
    ok = emqx_config_handler:add_handler(Roots, ?MODULE),
    ok.

remove_handler() ->
    Roots = emqx_dashboard_schema:roots(),
    ok = emqx_config_handler:remove_handler(Roots),
    ok.

pre_config_update(_Path, UpdateConf0, RawConf) ->
    UpdateConf = remove_sensitive_data(UpdateConf0),
    NewConf = emqx_map_lib:deep_merge(RawConf, UpdateConf),
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
            emqx_map_lib:deep_remove([<<"listeners">>, <<"https">>, <<"password">>], Conf1);
        _ ->
            Conf1
    end.

post_config_update(_, _Req, NewConf, OldConf, _AppEnvs) ->
    OldHttp = get_listener(http, OldConf),
    OldHttps = get_listener(https, OldConf),
    NewHttp = get_listener(http, NewConf),
    NewHttps = get_listener(https, NewConf),
    {StopHttp, StartHttp} = diff_listeners(http, OldHttp, NewHttp),
    {StopHttps, StartHttps} = diff_listeners(https, OldHttps, NewHttps),
    Stop = maps:merge(StopHttp, StopHttps),
    Start = maps:merge(StartHttp, StartHttps),
    _ = erlang:send_after(500, ?MODULE, {update_listeners, Stop, Start}),
    ok.

get_listener(Type, Conf) ->
    emqx_map_lib:deep_get([listeners, Type], Conf, undefined).

diff_listeners(_, Listener, Listener) -> {#{}, #{}};
diff_listeners(Type, undefined, Start) -> {#{}, #{Type => Start}};
diff_listeners(Type, Stop, undefined) -> {#{Type => Stop}, #{}};
diff_listeners(Type, Stop, Start) -> {#{Type => Stop}, #{Type => Start}}.

-define(DIR, <<"dashboard">>).

ensure_ssl_cert(#{<<"listeners">> := #{<<"https">> := #{<<"enable">> := true}}} = Conf) ->
    Https = emqx_map_lib:deep_get([<<"listeners">>, <<"https">>], Conf, undefined),
    Opts = #{required_keys => [<<"keyfile">>, <<"certfile">>, <<"cacertfile">>]},
    case emqx_tls_lib:ensure_ssl_files(?DIR, Https, Opts) of
        {ok, undefined} ->
            {error, <<"ssl_cert_not_found">>};
        {ok, NewHttps} ->
            {ok, emqx_map_lib:deep_merge(Conf, #{<<"listeners">> => #{<<"https">> => NewHttps}})};
        {error, Reason} ->
            ?SLOG(error, Reason#{msg => "bad_ssl_config"}),
            {error, Reason}
    end;
ensure_ssl_cert(Conf) ->
    {ok, Conf}.
