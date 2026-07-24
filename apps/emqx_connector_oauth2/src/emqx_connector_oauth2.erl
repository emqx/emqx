%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQX Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_connector_oauth2).

-behaviour(gen_server).

%% API
-export([
    start_link/0,
    register/2,
    get_token/1,
    unregister/1,
    clear_cache/0,
    clear_cache/1
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%% Invoked through the module name from fetch workers so hot upgrades use the
%% current implementation.
-export([fetch_token/1]).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("jose/include/jose_jwt.hrl").
-include("emqx_connector_oauth2_tables.hrl").

-define(REGISTERED, registered).
-define(TIMERS, timers).
-define(INFLIGHT, inflight).

%% Refresh at 75% of the token lifetime, like the Kafka bridge token cache.
-define(REFRESH_FRACTION, 0.75).
%% When `expires_in` is missing and the token is not a JWT, assume a short
%% lifetime so the token is refreshed frequently.
-define(DEFAULT_EXPIRY_MS, timer:seconds(15)).
%% How long to cache a failed fetch to avoid stampeding the token endpoint.
-define(CACHE_FAILURES_FOR, timer:seconds(1)).
%% Retry interval after a failed refresh.
-define(RETRY_INTERVAL, timer:seconds(5)).
%% Minimum refresh delay.
-define(MIN_REFRESH_MS, 1_000).

-define(KEY(ResourceId), ResourceId).
-define(TOKEN_ROW(KEY, Deadline, Result), {KEY, Deadline, Result}).

-record(register, {resource_id, params}).
-record(fetch, {resource_id}).
-record(unregister, {resource_id}).
-record(refresh, {resource_id}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, _Opts = #{}, []).

%% Registers an OAuth2 client-credentials configuration for a connector
%% instance.  The first token is fetched lazily on the first `get_token/1'
%% call (or by the periodic refresh timer once it has been scheduled).
%%
%% Only plain data (the fetch parameters) is stored in the GenServer state;
%% the refresh logic lives in the named `fetch_token/1' function so that a
%% hot code upgrade of this module picks up the new logic instead of running a
%% closure captured from the old version.
-spec register(term(), map()) -> ok.
register(ResourceId, Oauth2Config) ->
    Params = make_fetch_params(Oauth2Config),
    call(#register{resource_id = ResourceId, params = Params}).

%% Returns a valid access token for the given connector instance.
%% Reads the ETS cache first; on a miss (or expiry) it asks the GenServer to
%% coordinate a fresh token fetch.  Callers for the same resource share one
%% fetch, while fetches for different resources run concurrently.
-spec get_token(term()) -> {ok, binary()} | {error, term()}.
get_token(ResourceId) ->
    case get_cached(ResourceId) of
        {ok, Response} ->
            Response;
        error ->
            try
                call(#fetch{resource_id = ResourceId})
            catch
                exit:Reason ->
                    {error, {oauth2_manager_unavailable, Reason}}
            end
    end.

%% Removes the cached token and cancels the refresh timer for a connector
%% instance.  Called from the connector `on_stop'.
-spec unregister(term()) -> ok.
unregister(ResourceId) ->
    %% Delete the supervisor-owned persistent entries first.  If the manager
    %% is restarting, its next incarnation will not restore this registration.
    clear_cache(ResourceId),
    delete_registration(ResourceId),
    try
        call(#unregister{resource_id = ResourceId})
    catch
        exit:Reason ->
            ?tp(warning, "oauth2_unregister_manager_unavailable", #{
                resource_id => ResourceId, reason => Reason
            }),
            ok
    end.

%% For debug/test/manual ops
clear_cache() ->
    try ets:delete_all_objects(?OAUTH2_TOKEN_TAB) of
        _ -> ok
    catch
        error:badarg -> ok
    end.

clear_cache(ResourceId) ->
    try ets:delete(?OAUTH2_TOKEN_TAB, ?KEY(ResourceId)) of
        _ -> ok
    catch
        error:badarg -> ok
    end.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    Registered = load_registered(),
    State0 = #{
        ?REGISTERED => Registered,
        ?TIMERS => #{},
        ?INFLIGHT => #{}
    },
    State = restore_refresh_timers(Registered, State0),
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_call(
    #register{resource_id = ResourceId, params = Params}, _From, State0
) ->
    #{?REGISTERED := Registered0} = State0,
    case maps:get(ResourceId, Registered0, undefined) of
        Params ->
            {reply, ok, State0};
        _OldParams ->
            State1 = cancel_inflight(ResourceId, oauth2_config_changed, State0),
            State2 = clear_refresh_timer(ResourceId, State1),
            clear_cache(ResourceId),
            store_registration(ResourceId, Params),
            #{?REGISTERED := Registered1} = State2,
            Registered = Registered1#{ResourceId => Params},
            State = State2#{?REGISTERED := Registered},
            {reply, ok, State}
    end;
handle_call(#fetch{resource_id = ResourceId}, From, State0) ->
    %% Another call might've just stored a token.
    case get_cached(ResourceId) of
        {ok, Response} ->
            {reply, Response, State0};
        error ->
            fetch_or_enqueue(ResourceId, From, State0)
    end;
handle_call(#unregister{resource_id = ResourceId}, _From, State0) ->
    State = handle_unregister(ResourceId, State0),
    {reply, ok, State};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(
    {timeout, TRef, #refresh{resource_id = ResourceId}},
    #{?TIMERS := Timers0} = State0
) when map_get(ResourceId, Timers0) =:= TRef ->
    Timers = maps:remove(ResourceId, Timers0),
    State1 = State0#{?TIMERS := Timers},
    State = handle_refresh(ResourceId, State1),
    {noreply, State};
handle_info(
    {?MODULE, fetch_result, ResourceId, FetchRef, Result},
    State0
) ->
    State = handle_fetch_result(ResourceId, FetchRef, Result, State0),
    {noreply, State};
handle_info({'DOWN', MonitorRef, process, _Pid, Reason}, State0) ->
    State = handle_fetch_down(MonitorRef, Reason, State0),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

call(Call) ->
    gen_server:call(?MODULE, Call, infinity).

get_cached(ResourceId) ->
    NowMS = now_ms(),
    try
        case ets:lookup(?OAUTH2_TOKEN_TAB, ?KEY(ResourceId)) of
            [?TOKEN_ROW(?KEY(ResourceId), Deadline, Response)] when Deadline > NowMS ->
                {ok, Response};
            _ ->
                error
        end
    catch
        error:badarg ->
            error
    end.

handle_unregister(ResourceId, State0) ->
    clear_cache(ResourceId),
    delete_registration(ResourceId),
    State1 = cancel_inflight(ResourceId, oauth2_not_registered, State0),
    State2 = maps:update_with(
        ?REGISTERED,
        fun(R) -> maps:remove(ResourceId, R) end,
        State1
    ),
    clear_refresh_timer(ResourceId, State2).

handle_refresh(ResourceId, #{?REGISTERED := Registered} = State0) when
    not is_map_key(ResourceId, Registered)
->
    %% The connector was stopped; nothing to do.
    State0;
handle_refresh(ResourceId, #{?REGISTERED := Registered} = State0) ->
    ?tp(info, "oauth2_token_refreshing", #{resource_id => ResourceId}),
    Params = maps:get(ResourceId, Registered),
    start_fetch_if_needed(ResourceId, Params, refresh, State0).

fetch_or_enqueue(ResourceId, From, #{?REGISTERED := Registered} = State0) ->
    case maps:get(ResourceId, Registered, undefined) of
        undefined ->
            {reply, {error, oauth2_not_registered}, State0};
        Params ->
            State = enqueue_or_start_fetch(ResourceId, Params, From, State0),
            {noreply, State}
    end.

enqueue_or_start_fetch(ResourceId, Params, From, #{?INFLIGHT := Inflight0} = State0) ->
    case maps:get(ResourceId, Inflight0, undefined) of
        undefined ->
            start_fetch(ResourceId, Params, demand, [From], State0);
        Fetch0 ->
            Froms = maps:get(froms, Fetch0),
            Fetch = Fetch0#{froms := [From | Froms]},
            State0#{?INFLIGHT := Inflight0#{ResourceId := Fetch}}
    end.

start_fetch_if_needed(ResourceId, Params, Purpose, #{?INFLIGHT := Inflight} = State0) ->
    case maps:is_key(ResourceId, Inflight) of
        true ->
            State0;
        false ->
            start_fetch(ResourceId, Params, Purpose, [], State0)
    end.

start_fetch(ResourceId, Params, Purpose, Froms, #{?INFLIGHT := Inflight0} = State0) ->
    Parent = self(),
    FetchRef = make_ref(),
    {Pid, MonitorRef} = spawn_monitor(fun() ->
        Result = do_fetch_token(Params),
        Parent ! {?MODULE, fetch_result, ResourceId, FetchRef, Result}
    end),
    Fetch = #{
        ref => FetchRef,
        pid => Pid,
        monitor_ref => MonitorRef,
        purpose => Purpose,
        froms => Froms
    },
    State0#{?INFLIGHT := Inflight0#{ResourceId => Fetch}}.

handle_fetch_result(ResourceId, FetchRef, Result, #{?INFLIGHT := Inflight0} = State0) ->
    case maps:get(ResourceId, Inflight0, undefined) of
        #{ref := FetchRef, monitor_ref := MonitorRef, purpose := Purpose, froms := Froms} ->
            _ = erlang:demonitor(MonitorRef, [flush]),
            Inflight = maps:remove(ResourceId, Inflight0),
            State1 = State0#{?INFLIGHT := Inflight},
            complete_fetch(ResourceId, Purpose, Froms, Result, State1);
        _StaleOrUnknown ->
            State0
    end.

handle_fetch_down(MonitorRef, Reason, State0) ->
    case find_inflight_by_monitor(MonitorRef, State0) of
        error ->
            State0;
        {ok, ResourceId, #{purpose := Purpose, froms := Froms}} ->
            #{?INFLIGHT := Inflight0} = State0,
            Inflight = maps:remove(ResourceId, Inflight0),
            State1 = State0#{?INFLIGHT := Inflight},
            complete_fetch(
                ResourceId,
                Purpose,
                Froms,
                {error, {fetch_worker_down, Reason}},
                State1
            )
    end.

find_inflight_by_monitor(MonitorRef, #{?INFLIGHT := Inflight}) ->
    maps:fold(
        fun
            (ResourceId, #{monitor_ref := Ref} = Fetch, error) when Ref =:= MonitorRef ->
                {ok, ResourceId, Fetch};
            (_ResourceId, _Fetch, Acc) ->
                Acc
        end,
        error,
        Inflight
    ).

complete_fetch(ResourceId, _Purpose, Froms, {ok, ExpiryMS, Token}, State0) ->
    ?tp(info, "oauth2_token_refreshed", #{resource_id => ResourceId, expiry_ms => ExpiryMS}),
    State = store_token_and_schedule_refresh(ExpiryMS, ResourceId, {ok, Token}, State0),
    reply_all(Froms, {ok, Token}),
    State;
complete_fetch(ResourceId, Purpose, Froms, {error, Reason}, State0) ->
    ?tp(warning, "oauth2_token_refresh_failed", #{resource_id => ResourceId, reason => Reason}),
    case Froms of
        [] ->
            ok;
        [_ | _] ->
            Deadline = now_ms() + ?CACHE_FAILURES_FOR,
            store_row(ResourceId, Deadline, {error, Reason})
    end,
    reply_all(Froms, {error, Reason}),
    case Purpose of
        refresh -> ensure_refresh_timer(ResourceId, ?RETRY_INTERVAL, State0);
        demand -> State0
    end.

reply_all(Froms, Response) ->
    lists:foreach(fun(From) -> gen_server:reply(From, Response) end, Froms).

cancel_inflight(ResourceId, Reason, #{?INFLIGHT := Inflight0} = State0) ->
    case maps:take(ResourceId, Inflight0) of
        error ->
            State0;
        {#{pid := Pid, monitor_ref := MonitorRef, froms := Froms}, Inflight} ->
            _ = erlang:demonitor(MonitorRef, [flush]),
            exit(Pid, kill),
            reply_all(Froms, {error, Reason}),
            State0#{?INFLIGHT := Inflight}
    end.

load_registered() ->
    try maps:from_list(ets:tab2list(?OAUTH2_REGISTRY_TAB)) of
        Registered -> Registered
    catch
        error:badarg -> #{}
    end.

store_registration(ResourceId, Params) ->
    true = ets:insert(?OAUTH2_REGISTRY_TAB, {ResourceId, Params}),
    ok.

delete_registration(ResourceId) ->
    try ets:delete(?OAUTH2_REGISTRY_TAB, ResourceId) of
        true -> ok
    catch
        error:badarg -> ok
    end.

restore_refresh_timers(Registered, State0) ->
    maps:fold(
        fun(ResourceId, _Params, State) ->
            case get_cached(ResourceId) of
                {ok, {ok, _Token}} ->
                    %% The worker may have restarted after losing its timer state.
                    %% Refresh promptly so proactive renewal resumes.
                    ensure_refresh_timer(ResourceId, ?MIN_REFRESH_MS, State);
                _ ->
                    State
            end
        end,
        State0,
        Registered
    ).

do_fetch_token(Params) ->
    try
        ?MODULE:fetch_token(Params)
    catch
        Kind:Reason:Stacktrace ->
            ?tp(error, "oauth2_token_fetch_exception", #{
                kind => Kind, reason => Reason, stacktrace => Stacktrace
            }),
            {error, {Kind, Reason}}
    end.

store_token_and_schedule_refresh(ExpiryMS, ResourceId, Response, State0) ->
    Deadline = now_ms() + ExpiryMS,
    store_row(ResourceId, Deadline, Response),
    RefreshTime = max(?MIN_REFRESH_MS, ceil(ExpiryMS * ?REFRESH_FRACTION)),
    ?tp("oauth2_token_success", #{
        resource_id => ResourceId, expiry_ms => ExpiryMS, refresh_after => RefreshTime
    }),
    ensure_refresh_timer(ResourceId, RefreshTime, State0).

store_row(ResourceId, Deadline, Response) ->
    try
        true = ets:insert(?OAUTH2_TOKEN_TAB, ?TOKEN_ROW(?KEY(ResourceId), Deadline, Response))
    catch
        error:badarg ->
            ok
    end.

ensure_refresh_timer(ResourceId, Time, State0) ->
    %% Always replace any existing timer.  A retry timer may have been
    %% scheduled after a refresh failure; if a successful fetch then happens
    %% (e.g. via `get_token/1') before that retry fires, the timer must be
    %% rescheduled based on the new token's expiry, otherwise the stale retry
    %% fires an unnecessary extra fetch shortly after the success.
    State1 = clear_refresh_timer(ResourceId, State0),
    #{?TIMERS := Timers0} = State1,
    TRef = emqx_utils:start_timer(Time, #refresh{resource_id = ResourceId}),
    State1#{?TIMERS := Timers0#{ResourceId => TRef}}.

clear_refresh_timer(ResourceId, #{?TIMERS := Timers0} = State0) ->
    case maps:take(ResourceId, Timers0) of
        error ->
            State0;
        {TRef, Timers} ->
            emqx_utils:cancel_timer(TRef),
            State0#{?TIMERS := Timers}
    end.

now_ms() ->
    erlang:monotonic_time(millisecond).

%%------------------------------------------------------------------------------
%% Token endpoint interaction
%%------------------------------------------------------------------------------

%% Extracts the plain-data parameters needed to fetch a token from the OAuth2
%% config.  These parameters are the only thing stored in the GenServer state;
%% `fetch_token/1' is the fixed, named function that consumes them.  The
%% `client_secret' is kept in its `emqx_secret'-wrapped (redacted) form: that
%% closure is owned by the `emqx_secret' module, not by this one, so a hot code
%% upgrade of `emqx_connector_oauth2' does not freeze it, and the secret stays
%% out of crash reports.
-spec make_fetch_params(map()) -> map().
make_fetch_params(Oauth2Config) ->
    Timeout = maps:get(timeout, Oauth2Config, 5_000),
    #{
        token_endpoint => maps:get(token_endpoint, Oauth2Config),
        client_id => maps:get(client_id, Oauth2Config),
        client_secret => maps:get(client_secret, Oauth2Config),
        scope => maps:get(scope, Oauth2Config, undefined),
        timeout => Timeout,
        connect_timeout => maps:get(connect_timeout, Oauth2Config, Timeout),
        ssl => maps:get(ssl, Oauth2Config, #{})
    }.

%% Fixed, named function that exchanges the client credentials for an access
%% token using the `client_credentials` grant.  Called as
%% `?MODULE:fetch_token/1' (dynamic dispatch) so a hot code upgrade runs the
%% new implementation against the parameters stored in state.
-spec fetch_token(map()) -> {ok, non_neg_integer(), binary()} | {error, term()}.
fetch_token(Params) ->
    #{
        token_endpoint := Endpoint,
        client_id := ClientId,
        client_secret := Secret,
        scope := Scope,
        timeout := Timeout,
        connect_timeout := ConnectTimeout,
        ssl := SSL
    } = Params,
    BodyParams = lists:flatten([
        {"grant_type", "client_credentials"},
        {"client_id", str(ClientId)},
        {"client_secret", emqx_secret:unwrap(Secret)},
        [{"scope", str(Scope)} || Scope =/= undefined]
    ]),
    Body = uri_string:compose_query(BodyParams),
    Resp = do_request(#{
        uri => Endpoint,
        body => Body,
        timeout => Timeout,
        connect_timeout => ConnectTimeout,
        ssl => SSL
    }),
    case Resp of
        {ok, {{_, 200, _}, _, RespBody}} ->
            case emqx_utils_json:safe_decode(RespBody) of
                {ok, Response} ->
                    parse_token_response(Response);
                {error, Reason} ->
                    {error, {bad_token_response, Reason}}
            end;
        {ok, {{_, Status, _}, _Headers, BadResp}} ->
            {error, {token_endpoint_error, Status, oauth_error(BadResp)}};
        {error, Reason} ->
            {error, {failed_to_fetch_token, Reason}}
    end.

do_request(#{
    uri := URI,
    body := Body,
    timeout := Timeout,
    connect_timeout := ConnectTimeout,
    ssl := SSL
}) ->
    httpc:request(
        post,
        {str(URI), _Headers = [], "application/x-www-form-urlencoded", Body},
        http_options(URI, SSL, Timeout, ConnectTimeout),
        [{body_format, binary}]
    ).

oauth_error(Body) ->
    case emqx_utils_json:safe_decode(Body) of
        {ok, #{<<"error">> := Error}} when is_binary(Error), byte_size(Error) =< 128 ->
            Error;
        _ ->
            undefined
    end.

http_options(URI, SSL, Timeout, ConnectTimeout) ->
    Opts = [
        {timeout, Timeout},
        {connect_timeout, ConnectTimeout}
    ],
    case emqx_utils_uri:parse(URI) of
        #{scheme := <<"https">>} ->
            [{ssl, emqx_tls_lib:to_client_opts(SSL#{enable => true})} | Opts];
        _ ->
            Opts
    end.

parse_token_response(#{<<"access_token">> := Token} = Response) when is_binary(Token) ->
    case valid_token_type(maps:get(<<"token_type">>, Response, <<"Bearer">>)) of
        true ->
            parse_token_expiry(Response, Token);
        false ->
            {error, {bad_token_response, unsupported_token_type}}
    end;
parse_token_response(#{<<"access_token">> := _InvalidToken}) ->
    {error, {bad_token_response, invalid_access_token}};
parse_token_response(_Response) ->
    {error, {bad_token_response, missing_access_token}}.

parse_token_expiry(#{<<"expires_in">> := ExpiryS}, Token) when
    is_integer(ExpiryS), ExpiryS > 0
->
    ExpiryMS = max(1_000, erlang:convert_time_unit(ExpiryS, second, millisecond)),
    {ok, ExpiryMS, Token};
parse_token_expiry(#{<<"expires_in">> := _InvalidExpiry}, _Token) ->
    {error, {bad_token_response, invalid_expires_in}};
parse_token_expiry(_Response, Token) ->
    {ok, get_expiry_ms(Token), Token}.

valid_token_type(TokenType) when is_binary(TokenType) ->
    string:equal(TokenType, <<"Bearer">>, true) orelse
        string:equal(TokenType, <<"JWT">>, true);
valid_token_type(_TokenType) ->
    false.

get_expiry_ms(Token) ->
    try
        case jose_jwt:peek(Token) of
            #jose_jwt{fields = #{<<"exp">> := ExpS}} ->
                ExpMS = erlang:convert_time_unit(ExpS, second, millisecond),
                max(1_000, ExpMS - wall_clock_ms());
            _ ->
                ?DEFAULT_EXPIRY_MS
        end
    catch
        _:_ ->
            ?DEFAULT_EXPIRY_MS
    end.

str(X) -> emqx_utils_conv:str(X).

wall_clock_ms() -> erlang:system_time(millisecond).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

separate_connect_timeout_test() ->
    Params = make_fetch_params(#{
        token_endpoint => <<"http://127.0.0.1/token">>,
        client_id => <<"client">>,
        client_secret => emqx_secret:wrap(<<"secret">>),
        timeout => 10_000,
        connect_timeout => 2_000
    }),
    ?assertEqual(10_000, maps:get(timeout, Params)),
    ?assertEqual(2_000, maps:get(connect_timeout, Params)),
    ?assertEqual(
        [{timeout, 10_000}, {connect_timeout, 2_000}],
        http_options(<<"http://127.0.0.1/token">>, #{}, 10_000, 2_000)
    ).

connect_timeout_defaults_to_request_timeout_test() ->
    Params = make_fetch_params(#{
        token_endpoint => <<"http://127.0.0.1/token">>,
        client_id => <<"client">>,
        client_secret => emqx_secret:wrap(<<"secret">>),
        timeout => 10_000
    }),
    ?assertEqual(10_000, maps:get(connect_timeout, Params)).

-endif.
