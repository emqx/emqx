%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    register_der_crls/2,
    refresh/1,
    evict/1,
    update_config/1,
    info/0
]).

%% gen_server callbacks
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([post_config_update/5]).

%% internal exports
-export([http_get/2]).

-behaviour(gen_server).
-behaviour(emqx_config_handler).

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
-define(CONF_KEY_PATH, [crl_cache]).

-type duration() :: non_neg_integer().

-record(state, {
    refresh_timers = #{} :: #{binary() => reference()},
    refresh_interval = timer:minutes(15) :: duration(),
    http_timeout = ?HTTP_TIMEOUT :: duration(),
    %% keeps track of URLs by insertion time
    insertion_times = gb_trees:empty() :: gb_trees:tree(duration(), url()),
    %% the set of cached URLs, for testing if an URL is already
    %% registered.
    cached_urls = sets:new([{version, 2}]) :: sets:set(url()),
    cache_capacity = 100 :: pos_integer(),
    %% for future use
    extra = #{} :: map()
}).
-type url() :: string().
-type state() :: #state{}.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

post_config_update(?CONF_KEY_PATH, _Req, Conf, Conf, _AppEnvs) -> ok;
post_config_update(?CONF_KEY_PATH, _Req, NewConf, _OldConf, _AppEnvs) -> update_config(NewConf).

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

-spec refresh(url()) -> ok.
refresh(URL) ->
    gen_server:cast(?MODULE, {refresh, URL}).

-spec evict(url()) -> ok.
evict(URL) ->
    gen_server:cast(?MODULE, {evict, URL}).

-spec update_config(map()) -> ok.
update_config(Conf) ->
    gen_server:cast(?MODULE, {update_config, Conf}).

%% Adds CRLs in DER format to the cache and register them for periodic
%% refresh.
-spec register_der_crls(url(), [public_key:der_encoded()]) -> ok.
register_der_crls(URL, CRLs) when is_list(CRLs) ->
    gen_server:cast(?MODULE, {register_der_crls, URL, CRLs}).

-spec info() -> #{atom() => _}.
info() ->
    [state | State] = tuple_to_list(sys:get_state(?MODULE)),
    maps:from_list(lists:zip(record_info(fields, state), State)).

%%--------------------------------------------------------------------
%% gen_server behaviour
%%--------------------------------------------------------------------

init([]) ->
    erlang:process_flag(trap_exit, true),
    ok = emqx_config_handler:add_handler(?CONF_KEY_PATH, ?MODULE),
    Conf = emqx:get_config(
        ?CONF_KEY_PATH,
        #{
            capacity => ?DEFAULT_CACHE_CAPACITY,
            refresh_interval => ?DEFAULT_REFRESH_INTERVAL,
            http_timeout => ?HTTP_TIMEOUT
        }
    ),
    {ok, update_state_config(Conf, #state{})}.

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
handle_cast({update_config, Conf}, State0) ->
    {noreply, update_state_config(Conf, State0)};
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

terminate(_, _) ->
    emqx_config_handler:remove_handler(?CONF_KEY_PATH).

%%--------------------------------------------------------------------
%% internal functions
%%--------------------------------------------------------------------

update_state_config(Conf, State) ->
    #{
        capacity := CacheCapacity,
        refresh_interval := RefreshIntervalMS,
        http_timeout := HTTPTimeoutMS
    } = gather_config(Conf),
    State#state{
        cache_capacity = CacheCapacity,
        refresh_interval = RefreshIntervalMS,
        http_timeout = HTTPTimeoutMS
    }.

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
                    %% when doing lookup.
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

gather_config(Conf) ->
    RefreshIntervalMS0 = maps:get(refresh_interval, Conf),
    MinimumRefreshInterval = ?MIN_REFRESH_PERIOD,
    RefreshIntervalMS = max(RefreshIntervalMS0, MinimumRefreshInterval),
    Conf#{refresh_interval => RefreshIntervalMS}.

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
