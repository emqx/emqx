%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kafka_token_cache).

-behaviour(gen_server).

%% API
-export([
    start_link/0,

    create_tables/0,
    get_or_refresh/2,
    get_or_refresh/3,
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

-include("emqx_bridge_kafka_internal.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(registered, registered).
-define(timers, timers).

-define(KEY(CLIENT_ID), CLIENT_ID).
-define(TOKEN_ROW(KEY, DEADLINE, TOKEN), {KEY, DEADLINE, TOKEN}).

%% Calls/casts/infos
-record(register_refresh, {client_id, refresh_fn, opts}).
-record(unregister, {client_id}).
-record(refresh, {client_id}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, _Opts = #{}, []).

create_tables() ->
    _ = emqx_utils_ets:new(?TOKEN_RESP_TAB, [public]),
    ok.

get_or_refresh(ClientId, RefreshFn) ->
    get_or_refresh(ClientId, RefreshFn, _Opts = #{}).

get_or_refresh(ClientId, RefreshFn, Opts) ->
    case get_cached(ClientId) of
        {ok, Response} ->
            Response;
        error ->
            call(#register_refresh{client_id = ClientId, refresh_fn = RefreshFn, opts = Opts})
    end.

unregister(ClientId) ->
    call(#unregister{client_id = ClientId}).

%% For debug/test/manual ops
clear_cache() ->
    true = ets:delete_all_objects(?TOKEN_RESP_TAB),
    ok.

%% For debug/test/manual ops
clear_cache(ClientId) ->
    true = ets:delete(?TOKEN_RESP_TAB, ?KEY(ClientId)),
    ok.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    State = #{
        ?registered => #{},
        ?timers => #{}
    },
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_call(
    #register_refresh{client_id = ClientId, refresh_fn = RefreshFn, opts = Opts}, _From, State0
) ->
    {Reply, State} = handle_register_refresh(ClientId, RefreshFn, Opts, State0),
    {reply, Reply, State};
handle_call(#unregister{client_id = ClientId}, _From, State0) ->
    State = handle_unregister(ClientId, State0),
    {reply, ok, State};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({timeout, _TRef, #refresh{client_id = ClientId}}, #{?timers := Timers0} = State0) when
    is_map_key(ClientId, Timers0)
->
    Timers = maps:remove(ClientId, Timers0),
    State1 = State0#{?timers := Timers},
    State = handle_refresh(ClientId, State1),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

call(Call) ->
    gen_server:call(?MODULE, Call, infinity).

get_cached(ClientId) ->
    NowMS = now_ms(),
    case ets:lookup(?TOKEN_RESP_TAB, ?KEY(ClientId)) of
        [?TOKEN_ROW(?KEY(ClientId), Deadline, Response)] when Deadline > NowMS ->
            {ok, Response};
        _ ->
            error
    end.

handle_register_refresh(ClientId, RefreshFn, Opts, State0) ->
    %% Might've been just stored by a previous call.
    case get_cached(ClientId) of
        {ok, Response} ->
            %% Store provided refresh fn, as it could be an updated version.
            State = maps:update_with(
                ?registered,
                fun(R) -> R#{ClientId => {RefreshFn, Opts}} end,
                State0
            ),
            {Response, State};
        error ->
            do_handle_register_refresh(ClientId, RefreshFn, Opts, State0)
    end.

do_handle_register_refresh(ClientId, RefreshFn, Opts, State0) ->
    case do_fetch_token(ClientId, RefreshFn) of
        {ok, ExpiryMS, Token} ->
            State1 = store_token_and_schedule_refresh(ExpiryMS, ClientId, Token, State0),
            State = maps:update_with(
                ?registered,
                fun(R) -> R#{ClientId => {RefreshFn, Opts}} end,
                State1
            ),
            {{ok, Token}, State};
        {error, Reason} ->
            CacheFailuresFor = maps:get(cache_failures_for, Opts, timer:seconds(1)),
            Deadline = now_ms() + CacheFailuresFor,
            _ = ets:insert(?TOKEN_RESP_TAB, ?TOKEN_ROW(?KEY(ClientId), Deadline, {error, Reason})),
            {{error, Reason}, State0}
    end.

do_fetch_token(ClientId, RefreshFn) ->
    try
        RefreshFn()
    catch
        Kind:Reason:Stacktrace ->
            ?tp(error, "kafka_token_refresh_exception", #{
                client_id => ClientId,
                kind => Kind,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, {Kind, Reason}}
    end.

handle_unregister(ClientId, State0) ->
    _ = ets:delete(?TOKEN_RESP_TAB, ?KEY(ClientId)),
    State1 = maps:update_with(?registered, fun(R) -> maps:remove(ClientId, R) end, State0),
    clear_refresh_timer(ClientId, State1).

handle_refresh(ClientId, #{?registered := Registered} = State0) when
    not is_map_key(ClientId, Registered)
->
    %% Shouldn't be possible
    ?tp(error, "kafka_token_refresh_unknown_clientid", #{clientid => ClientId}),
    State0;
handle_refresh(ClientId, #{?registered := Registered} = State0) ->
    {RefreshFn, Opts} = maps:get(ClientId, Registered),
    ?tp(info, "kafka_token_refreshing", #{client_id => ClientId}),
    case do_fetch_token(ClientId, RefreshFn) of
        {ok, ExpiryMS, Token} ->
            ?tp(info, "kafka_token_refreshed", #{client_id => ClientId, expiry_ms => ExpiryMS}),
            store_token_and_schedule_refresh(ExpiryMS, ClientId, Token, State0);
        {error, Reason} ->
            ?tp(warning, "kafka_token_refresh_failed", #{client_id => ClientId, reason => Reason}),
            RetryTime = maps:get(retry_interval, Opts, timer:seconds(5)),
            ensure_refresh_timer(ClientId, RetryTime, State0)
    end.

store_token_and_schedule_refresh(ExpiryMS, ClientId, Token, State0) ->
    Deadline = now_ms() + ExpiryMS,
    _ = ets:insert(?TOKEN_RESP_TAB, ?TOKEN_ROW(?KEY(ClientId), Deadline, {ok, Token})),
    %% Tokens typically last for 1 hour
    RefreshTime = ceil(ExpiryMS * 0.75),
    ?tp("kafka_token_success", #{
        client_id => ClientId,
        expiry_ms => ExpiryMS,
        refresh_after => RefreshTime
    }),
    ensure_refresh_timer(ClientId, RefreshTime, State0).

ensure_refresh_timer(ClientId, _Time, #{?timers := Timers} = State) when
    is_map_key(ClientId, Timers)
->
    State;
ensure_refresh_timer(ClientId, Time, #{?timers := Timers0} = State0) ->
    TRef = emqx_utils:start_timer(Time, #refresh{client_id = ClientId}),
    Timers = Timers0#{ClientId => TRef},
    State0#{?timers := Timers}.

clear_refresh_timer(ClientId, #{?timers := Timers0} = State0) ->
    case maps:take(ClientId, Timers0) of
        error ->
            State0;
        {TRef, Timers} ->
            emqx_utils:cancel_timer(TRef),
            State0#{?timers := Timers}
    end.

now_ms() ->
    erlang:system_time(millisecond).
