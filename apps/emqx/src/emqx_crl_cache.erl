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
    register_der_crls/2,
    refresh/1,
    evict/1
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
-define(DEFAULT_CACHE_CAPACITY, 100).

-record(state, {
    refresh_timers = #{} :: #{binary() => timer:tref()},
    refresh_interval = timer:minutes(15) :: timer:time(),
    http_timeout = ?HTTP_TIMEOUT :: timer:time(),
    %% keeps track of URLs by insertion time
    insertion_times = gb_trees:empty() :: gb_trees:tree(timer:time(), url()),
    %% the set of cached URLs, for testing if an URL is already
    %% registered.
    cached_urls = sets:new([{version, 2}]) :: sets:set(url()),
    cache_capacity = 100 :: pos_integer(),
    %% for future use
    extra = #{} :: map()
}).
-type url() :: uri_string:uri_string().
-type state() :: #state{}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    Config = gather_config(),
    start_link(Config).

start_link(Config = #{cache_capacity := _, refresh_interval := _, http_timeout := _}) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Config, []).

-spec refresh(url()) -> ok.
refresh(URL) ->
    gen_server:cast(?MODULE, {refresh, URL}).

-spec evict(url()) -> ok.
evict(URL) ->
    gen_server:cast(?MODULE, {evict, URL}).

%% Adds CRLs in DER format to the cache and register them for periodic
%% refresh.
-spec register_der_crls(url(), [public_key:der_encoded()]) -> ok.
register_der_crls(URL, CRLs) when is_list(CRLs) ->
    gen_server:cast(?MODULE, {register_der_crls, URL, CRLs}).

%%--------------------------------------------------------------------
%% gen_server behaviour
%%--------------------------------------------------------------------

init(Config) ->
    #{
        cache_capacity := CacheCapacity,
        refresh_interval := RefreshIntervalMS,
        http_timeout := HTTPTimeoutMS
    } = Config,
    State = #state{
        cache_capacity = CacheCapacity,
        refresh_interval = RefreshIntervalMS,
        http_timeout = HTTPTimeoutMS
    },
    {ok, State}.

handle_call(Call, _From, State) ->
    {reply, {error, {bad_call, Call}}, State}.

handle_cast({evict, URL}, State0 = #state{refresh_timers = RefreshTimers0}) ->
    emqx_ssl_crl_cache:delete(URL),
    MTimer = maps:get(URL, RefreshTimers0, undefined),
    emqx_utils:cancel_timer(MTimer),
    RefreshTimers = maps:without([URL], RefreshTimers0),
    State = State0#state{refresh_timers = RefreshTimers},
    ?tp(
        crl_cache_evict,
        #{url => URL}
    ),
    {noreply, State};
handle_cast({register_der_crls, URL, CRLs}, State0) ->
    handle_register_der_crls(State0, URL, CRLs);
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
                    ?tp(debug, crl_refresh_timer_done, #{url => URL}),
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
                    emqx_ssl_crl_cache:insert(to_string(URL), {der, CRLs}),
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
    emqx_utils:cancel_timer(MTimer),
    RefreshTimers = RefreshTimers0#{
        URL => emqx_utils:start_timer(
            Timeout,
            {refresh, URL}
        )
    },
    State#state{refresh_timers = RefreshTimers}.

-spec gather_config() ->
    #{
        cache_capacity := pos_integer(),
        refresh_interval := timer:time(),
        http_timeout := timer:time()
    }.
gather_config() ->
    %% TODO: add a config handler to refresh the config when those
    %% globals change?
    CacheCapacity = emqx_config:get([crl_cache, capacity], ?DEFAULT_CACHE_CAPACITY),
    RefreshIntervalMS0 = emqx_config:get([crl_cache, refresh_interval], ?DEFAULT_REFRESH_INTERVAL),
    MinimumRefreshInverval = ?MIN_REFRESH_PERIOD,
    RefreshIntervalMS = max(RefreshIntervalMS0, MinimumRefreshInverval),
    HTTPTimeoutMS = emqx_config:get([crl_cache, http_timeout], ?HTTP_TIMEOUT),
    #{
        cache_capacity => CacheCapacity,
        refresh_interval => RefreshIntervalMS,
        http_timeout => HTTPTimeoutMS
    }.

-spec handle_register_der_crls(state(), url(), [public_key:der_encoded()]) -> {noreply, state()}.
handle_register_der_crls(State0, URL0, CRLs) ->
    #state{cached_urls = CachedURLs0} = State0,
    URL = to_string(URL0),
    case sets:is_element(URL, CachedURLs0) of
        true ->
            {noreply, State0};
        false ->
            emqx_ssl_crl_cache:insert(URL, {der, CRLs}),
            ?tp(debug, new_crl_url_inserted, #{url => URL}),
            State1 = do_register_url(State0, URL),
            State2 = handle_cache_overflow(State1),
            State = ensure_timer(URL, State2),
            {noreply, State}
    end.

-spec do_register_url(state(), url()) -> state().
do_register_url(State0, URL) ->
    #state{
        cached_urls = CachedURLs0,
        insertion_times = InsertionTimes0
    } = State0,
    Now = erlang:monotonic_time(),
    CachedURLs = sets:add_element(URL, CachedURLs0),
    InsertionTimes = gb_trees:enter(Now, URL, InsertionTimes0),
    State0#state{
        cached_urls = CachedURLs,
        insertion_times = InsertionTimes
    }.

-spec handle_cache_overflow(state()) -> state().
handle_cache_overflow(State0) ->
    #state{
        cached_urls = CachedURLs0,
        insertion_times = InsertionTimes0,
        cache_capacity = CacheCapacity,
        refresh_timers = RefreshTimers0
    } = State0,
    case sets:size(CachedURLs0) > CacheCapacity of
        false ->
            State0;
        true ->
            {_Time, OldestURL, InsertionTimes} = gb_trees:take_smallest(InsertionTimes0),
            emqx_ssl_crl_cache:delete(OldestURL),
            MTimer = maps:get(OldestURL, RefreshTimers0, undefined),
            emqx_utils:cancel_timer(MTimer),
            RefreshTimers = maps:remove(OldestURL, RefreshTimers0),
            CachedURLs = sets:del_element(OldestURL, CachedURLs0),
            ?tp(debug, crl_cache_overflow, #{oldest_url => OldestURL}),
            State0#state{
                insertion_times = InsertionTimes,
                cached_urls = CachedURLs,
                refresh_timers = RefreshTimers
            }
    end.

to_string(B) when is_binary(B) ->
    binary_to_list(B);
to_string(L) when is_list(L) ->
    L.
