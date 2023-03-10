%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%
%% @doc EMQX CRL cache.
%%--------------------------------------------------------------------

-module(emqx_crl_cache).

%% API
-export([
    start_link/0,
    start_link/1,
    refresh/1,
    evict/1,
    refresh_config/0,
    refresh_config/1
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% internal exports
-export([http_get/2]).

-behaviour(gen_server).

-include("logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(HTTP_TIMEOUT, timer:seconds(15)).
-define(RETRY_TIMEOUT, 5_000).
-ifdef(TEST).
-define(MIN_REFRESH_PERIOD, timer:seconds(5)).
-else.
-define(MIN_REFRESH_PERIOD, timer:minutes(1)).
-endif.
-define(DEFAULT_REFRESH_INTERVAL, timer:minutes(15)).

-record(state, {
    refresh_timers = #{} :: #{binary() => timer:tref()},
    refresh_interval = timer:minutes(15) :: timer:time(),
    http_timeout = ?HTTP_TIMEOUT :: timer:time(),
    %% for future use
    extra = #{} :: map()
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    Config = gather_config(),
    start_link(Config).

start_link(Config = #{urls := _, refresh_interval := _, http_timeout := _}) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

refresh(URL) ->
    gen_server:cast(?MODULE, {refresh, URL}).

evict(URL) ->
    gen_server:cast(?MODULE, {evict, URL}).

%% to pick up changes from the config
-spec refresh_config() -> ok.
refresh_config() ->
    gen_server:cast(?MODULE, refresh_config).

-spec refresh_config(map()) -> ok.
refresh_config(Config) ->
    gen_server:cast(?MODULE, {refresh_config, Config}).

%%--------------------------------------------------------------------
%% gen_server behaviour
%%--------------------------------------------------------------------

init(Config) ->
    #{
        urls := URLs,
        refresh_interval := RefreshIntervalMS,
        http_timeout := HTTPTimeoutMS
    } = Config,
    State = lists:foldl(
        fun(URL, Acc) -> ensure_timer(URL, Acc, 0) end,
        #state{
            refresh_interval = RefreshIntervalMS,
            http_timeout = HTTPTimeoutMS
        },
        URLs
    ),
    {ok, State}.

handle_call(Call, _From, State) ->
    {reply, {error, {bad_call, Call}}, State}.

handle_cast({evict, URL}, State0 = #state{refresh_timers = RefreshTimers0}) ->
    ssl_crl_cache:delete(URL),
    MTimer = maps:get(URL, RefreshTimers0, undefined),
    emqx_misc:cancel_timer(MTimer),
    RefreshTimers = maps:without([URL], RefreshTimers0),
    State = State0#state{refresh_timers = RefreshTimers},
    ?tp(
        crl_cache_evict,
        #{url => URL}
    ),
    {noreply, State};
handle_cast({refresh, URL}, State0) ->
    case do_http_fetch_and_cache(URL, State0#state.http_timeout) of
        {error, Error} ->
            ?tp(crl_refresh_failure, #{error => Error, url => URL}),
            ?SLOG(error, #{
                msg => "failed_to_fetch_crl_response",
                url => URL,
                error => Error
            }),
            {noreply, ensure_timer(URL, State0, ?RETRY_TIMEOUT)};
        {ok, _CRLs} ->
            ?SLOG(debug, #{
                msg => "fetched_crl_response",
                url => URL
            }),
            {noreply, ensure_timer(URL, State0)}
    end;
handle_cast(refresh_config, State0) ->
    handle_refresh_config(_ReceivedConfig = undefined, State0);
handle_cast({refresh_config, ReceivedConfig}, State0) ->
    handle_refresh_config(ReceivedConfig, State0);
handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(
    {timeout, TRef, {refresh, URL}},
    State = #state{
        refresh_timers = RefreshTimers,
        http_timeout = HTTPTimeoutMS
    }
) ->
    case maps:get(URL, RefreshTimers, undefined) of
        TRef ->
            ?tp(debug, crl_refresh_timer, #{url => URL}),
            case do_http_fetch_and_cache(URL, HTTPTimeoutMS) of
                {error, Error} ->
                    ?SLOG(error, #{
                        msg => "failed_to_fetch_crl_response",
                        url => URL,
                        error => Error
                    }),
                    {noreply, ensure_timer(URL, State, ?RETRY_TIMEOUT)};
                {ok, _CRLs} ->
                    ?SLOG(debug, #{
                        msg => "fetched_crl_response",
                        url => URL
                    }),
                    ?tp(crl_refresh_timer_done, #{url => URL}),
                    {noreply, ensure_timer(URL, State)}
            end;
        _ ->
            {noreply, State}
    end;
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------

http_get(URL, HTTPTimeout) ->
    httpc:request(
        get,
        {URL, [{"connection", "close"}]},
        [{timeout, HTTPTimeout}],
        [{body_format, binary}]
    ).

do_http_fetch_and_cache(URL, HTTPTimeoutMS) ->
    ?tp(crl_http_fetch, #{crl_url => URL}),
    Resp = ?MODULE:http_get(URL, HTTPTimeoutMS),
    case Resp of
        {ok, {{_, 200, _}, _, Body}} ->
            case parse_crls(Body) of
                error ->
                    {error, invalid_crl};
                CRLs ->
                    %% Note: must ensure it's a string and not a
                    %% binary because that's what the ssl manager uses
                    %% when doing lookups.
                    ssl_crl_cache:insert(to_string(URL), {der, CRLs}),
                    ?tp(crl_cache_insert, #{url => URL, crls => CRLs}),
                    {ok, CRLs}
            end;
        {ok, {{_, Code, _}, _, Body}} ->
            {error, {bad_response, #{code => Code, body => Body}}};
        {error, Error} ->
            {error, {http_error, Error}}
    end.

parse_crls(Bin) ->
    try
        [CRL || {'CertificateList', CRL, not_encrypted} <- public_key:pem_decode(Bin)]
    catch
        _:_ ->
            error
    end.

ensure_timer(URL, State = #state{refresh_interval = Timeout}) ->
    ensure_timer(URL, State, Timeout).

ensure_timer(URL, State = #state{refresh_timers = RefreshTimers0}, Timeout) ->
    ?tp(crl_cache_ensure_timer, #{url => URL, timeout => Timeout}),
    MTimer = maps:get(URL, RefreshTimers0, undefined),
    emqx_misc:cancel_timer(MTimer),
    RefreshTimers = RefreshTimers0#{
        URL => emqx_misc:start_timer(
            Timeout,
            {refresh, URL}
        )
    },
    State#state{refresh_timers = RefreshTimers}.

collect_urls(TLSListeners) ->
    URLs = [
        URL
     || {_Name, #{
            ssl_options :=
                #{
                    crl :=
                        #{
                            enable_crl_check := true,
                            cache_urls := CacheURLs
                        }
                }
        }} <-
            maps:to_list(TLSListeners),
        URL <- CacheURLs
    ],
    lists:usort(URLs).

-spec gather_config() ->
    #{
        urls := [binary()],
        refresh_interval := timer:time(),
        http_timeout := timer:time()
    }.
gather_config() ->
    TLSListeners = emqx_config:get([listeners, ssl], #{}),
    URLs = collect_urls(TLSListeners),
    %% TODO: add a config handler to refresh the config when those
    %% globals change?
    RefreshIntervalMS0 = emqx_config:get([crl_cache, refresh_interval], ?DEFAULT_REFRESH_INTERVAL),
    MinimumRefreshInverval = ?MIN_REFRESH_PERIOD,
    RefreshIntervalMS = max(RefreshIntervalMS0, MinimumRefreshInverval),
    HTTPTimeoutMS = emqx_config:get([crl_cache, http_timeout], ?HTTP_TIMEOUT),
    #{
        urls => URLs,
        refresh_interval => RefreshIntervalMS,
        http_timeout => HTTPTimeoutMS
    }.

handle_refresh_config(_ReceivedConfig = undefined, State0) ->
    #{
        urls := URLs,
        http_timeout := HTTPTimeoutMS,
        refresh_interval := RefreshIntervalMS
    } = gather_config(),
    do_handle_refresh_config(URLs, HTTPTimeoutMS, RefreshIntervalMS, State0);
handle_refresh_config(_ReceivedConfig = #{ssl_options := #{crl := #{cache_urls := URLs}}}, State0) ->
    #{
        http_timeout := HTTPTimeoutMS,
        refresh_interval := RefreshIntervalMS
    } = gather_config(),
    do_handle_refresh_config(URLs, HTTPTimeoutMS, RefreshIntervalMS, State0);
handle_refresh_config(_ReceivedConfig, State0) ->
    {noreply, State0}.

do_handle_refresh_config(URLs, HTTPTimeoutMS, RefreshIntervalMS, State0) ->
    State = lists:foldl(
        fun(URL, Acc) -> ensure_timer(URL, Acc, 0) end,
        State0#state{
            refresh_interval = RefreshIntervalMS,
            http_timeout = HTTPTimeoutMS
        },
        URLs
    ),
    ?tp(crl_cache_refresh_config, #{
        refresh_interval => RefreshIntervalMS,
        http_timeout => HTTPTimeoutMS,
        urls => URLs
    }),
    {noreply, State}.

to_string(B) when is_binary(B) ->
    binary_to_list(B);
to_string(L) when is_list(L) ->
    L.
