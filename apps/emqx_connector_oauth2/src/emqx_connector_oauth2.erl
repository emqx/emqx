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

%% Internal exports (for mocking/tests).  `fetch_token/1' is also exported
%% because it is invoked via `?MODULE:fetch_token/1' (dynamic dispatch) so a
%% hot code upgrade runs the new implementation.
-export([do_request/1, fetch_token/1]).

-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("jose/include/jose_jwt.hrl").
-include("emqx_connector_oauth2_tables.hrl").

-define(REGISTERED, registered).
-define(TIMERS, timers).

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
%% Reads the ETS cache first; on a miss (or expiry) it synchronously asks the
%% GenServer to fetch a fresh token.
-spec get_token(term()) -> {ok, binary()} | {error, term()}.
get_token(ResourceId) ->
    case get_cached(ResourceId) of
        {ok, Response} ->
            Response;
        error ->
            call(#fetch{resource_id = ResourceId})
    end.

%% Removes the cached token and cancels the refresh timer for a connector
%% instance.  Called from the connector `on_stop'.
-spec unregister(term()) -> ok.
unregister(ResourceId) ->
    call(#unregister{resource_id = ResourceId}).

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
    State = #{
        ?REGISTERED => #{},
        ?TIMERS => #{}
    },
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_call(
    #register{resource_id = ResourceId, params = Params}, _From, State0
) ->
    State1 = State0#{?REGISTERED => (maps:get(?REGISTERED, State0))#{ResourceId => Params}},
    {reply, ok, State1};
handle_call(#fetch{resource_id = ResourceId}, _From, State0) ->
    %% Another call might've just stored a token.
    case get_cached(ResourceId) of
        {ok, Response} ->
            {reply, Response, State0};
        error ->
            {Response, State} = do_fetch_and_store(ResourceId, State0),
            {reply, Response, State}
    end;
handle_call(#unregister{resource_id = ResourceId}, _From, State0) ->
    State = handle_unregister(ResourceId, State0),
    {reply, ok, State};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(
    {timeout, _TRef, #refresh{resource_id = ResourceId}}, #{?TIMERS := Timers0} = State0
) when
    is_map_key(ResourceId, Timers0)
->
    Timers = maps:remove(ResourceId, Timers0),
    State1 = State0#{?TIMERS := Timers},
    State = handle_refresh(ResourceId, State1),
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
    State1 = maps:update_with(
        ?REGISTERED,
        fun(R) -> maps:remove(ResourceId, R) end,
        State0
    ),
    clear_refresh_timer(ResourceId, State1).

handle_refresh(ResourceId, #{?REGISTERED := Registered} = State0) when
    not is_map_key(ResourceId, Registered)
->
    %% The connector was stopped; nothing to do.
    State0;
handle_refresh(ResourceId, #{?REGISTERED := Registered} = State0) ->
    ?tp(info, "oauth2_token_refreshing", #{resource_id => ResourceId}),
    Params = maps:get(ResourceId, Registered),
    case do_fetch_token(Params) of
        {ok, ExpiryMS, Token} ->
            ?tp(info, "oauth2_token_refreshed", #{resource_id => ResourceId, expiry_ms => ExpiryMS}),
            store_token_and_schedule_refresh(ExpiryMS, ResourceId, {ok, Token}, State0);
        {error, Reason} ->
            ?tp(warning, "oauth2_token_refresh_failed", #{
                resource_id => ResourceId, reason => Reason
            }),
            ensure_refresh_timer(ResourceId, ?RETRY_INTERVAL, State0)
    end.

do_fetch_and_store(ResourceId, #{?REGISTERED := Registered} = State0) ->
    case maps:get(ResourceId, Registered, undefined) of
        undefined ->
            %% Not registered (shouldn't happen for a live connector).
            Response = {error, oauth2_not_registered},
            Deadline = now_ms() + ?CACHE_FAILURES_FOR,
            store_row(ResourceId, Deadline, Response),
            {Response, State0};
        Params ->
            case do_fetch_token(Params) of
                {ok, ExpiryMS, Token} ->
                    State = store_token_and_schedule_refresh(
                        ExpiryMS, ResourceId, {ok, Token}, State0
                    ),
                    {{ok, Token}, State};
                {error, Reason} ->
                    Deadline = now_ms() + ?CACHE_FAILURES_FOR,
                    store_row(ResourceId, Deadline, {error, Reason}),
                    {{error, Reason}, State0}
            end
    end.

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
    erlang:system_time(millisecond).

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
    #{
        token_endpoint => maps:get(token_endpoint, Oauth2Config),
        client_id => maps:get(client_id, Oauth2Config),
        client_secret => maps:get(client_secret, Oauth2Config),
        scope => maps:get(scope, Oauth2Config, undefined),
        timeout => maps:get(timeout, Oauth2Config, 5_000)
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
        timeout := Timeout
    } = Params,
    BodyParams = lists:flatten([
        {"grant_type", "client_credentials"},
        {"client_id", str(ClientId)},
        {"client_secret", emqx_secret:unwrap(Secret)},
        [{"scope", str(Scope)} || Scope =/= undefined]
    ]),
    Body = uri_string:compose_query(BodyParams),
    Resp = ?MODULE:do_request(#{
        uri => Endpoint,
        body => Body,
        timeout => Timeout
    }),
    case Resp of
        {ok, {{_, 200, _}, _, RespBody}} ->
            case emqx_utils_json:safe_decode(RespBody) of
                {ok, #{<<"access_token">> := Token, <<"expires_in">> := ExpiryS}} ->
                    ExpiryMS = max(1_000, erlang:convert_time_unit(ExpiryS, second, millisecond)),
                    {ok, ExpiryMS, Token};
                {ok, #{<<"access_token">> := Token}} ->
                    {ok, get_expiry_ms(Token), Token};
                {ok, BadResp} ->
                    {error, {bad_token_response, BadResp}};
                {error, Reason} ->
                    {error, {bad_token_response, Reason}}
            end;
        {ok, {{_, Status, _}, Headers, BadResp}} ->
            {error, {bad_token_response, #{status => Status, headers => Headers, body => BadResp}}};
        {error, Reason} ->
            {error, {failed_to_fetch_token, Reason}}
    end.

%% Only exposed for mocking/tests.
do_request(#{uri := URI, body := Body, timeout := Timeout}) ->
    httpc:request(
        post,
        {str(URI), _Headers = [], "application/x-www-form-urlencoded", Body},
        [
            {timeout, Timeout},
            {connect_timeout, Timeout}
        ],
        [{body_format, binary}]
    ).

get_expiry_ms(Token) ->
    try
        %% `jose_jwt:peek' may not be available in all builds; fall back to a
        %% short default lifetime when the token is not a JWT or is malformed.
        case code:ensure_loaded(jose_jwt) of
            {module, _} ->
                case jose_jwt:peek(Token) of
                    #jose_jwt{fields = #{<<"exp">> := ExpS}} ->
                        ExpMS = erlang:convert_time_unit(ExpS, second, millisecond),
                        max(1_000, ExpMS - now_ms());
                    _ ->
                        ?DEFAULT_EXPIRY_MS
                end;
            _ ->
                ?DEFAULT_EXPIRY_MS
        end
    catch
        _:_ ->
            ?DEFAULT_EXPIRY_MS
    end.

str(X) -> emqx_utils_conv:str(X).
