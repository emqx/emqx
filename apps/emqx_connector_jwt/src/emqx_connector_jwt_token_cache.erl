%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_connector_jwt_token_cache).

-moduledoc """
This `gen_server` serves as a refresher of tokens are simple to obtain with a single
request.

- `get_or_refresh` is called with a caller identifier, the table where tokens are stored,
  and a supplied refresh function that fetches the token if needed.  If the token is
  absent or expired, a fresh one is fetched, cached and returned.  By calling this, it
  also registers the refresh function associated with the caller id, so that this token is
  refreshed automatically at about 3/4 of its expiry time.

- `unregister` is called when the caller is terminating and wants to clear up its
  resources.  It prevents further refreshes and removes the token from the table
  associated with the caller id.
""".

-behaviour(gen_server).

%% API
-export([
    start_link/1,
    start_link/2,

    create_tables/1,
    get_or_refresh/4,
    get_or_refresh/5,
    unregister/2,
    clear_cache/1,
    clear_cache/2
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include_lib("snabbkaffe/include/trace.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(registered, registered).
-define(table, table).
-define(timers, timers).

-define(KEY(RES_ID), RES_ID).
-define(TOKEN_ROW(KEY, DEADLINE, TOKEN), {KEY, DEADLINE, TOKEN}).

%% Calls/casts/infos
-record(register_refresh, {res_id, refresh_fn, opts}).
-record(unregister, {res_id}).
-record(refresh, {res_id}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

start_link(#{} = Opts) ->
    gen_server:start_link(?MODULE, Opts, []).

start_link(ServerName, #{} = Opts) ->
    gen_server:start_link(ServerName, ?MODULE, Opts, []).

-doc """
Creates the table used by the refresher process.

It's best called by a supervisor so that a process restart does not drop all stored
tokens.
""".
create_tables(TabName) ->
    _ = emqx_utils_ets:new(TabName, [ordered_set, public]),
    ok.

get_or_refresh(ServerRef, Tab, ResId, RefreshFn) ->
    get_or_refresh(ServerRef, Tab, ResId, RefreshFn, _Opts = #{}).

get_or_refresh(ServerRef, Tab, ResId, RefreshFn, Opts) ->
    case get_cached(Tab, ResId) of
        {ok, Response} ->
            Response;
        error ->
            call(ServerRef, #register_refresh{res_id = ResId, refresh_fn = RefreshFn, opts = Opts})
    end.

unregister(ServerRef, ResId) ->
    call(ServerRef, #unregister{res_id = ResId}).

%% For debug/test/manual ops
clear_cache(Tab) ->
    true = ets:delete_all_objects(Tab),
    ok.

%% For debug/test/manual ops
clear_cache(Tab, ResId) ->
    true = ets:delete(Tab, ?KEY(ResId)),
    ok.

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(Opts) ->
    #{table := Tab} = Opts,
    State = #{
        ?table => Tab,
        ?registered => #{},
        ?timers => #{}
    },
    {ok, State}.

terminate(_Reason, _State) ->
    ok.

handle_call(
    #register_refresh{res_id = ResId, refresh_fn = RefreshFn, opts = Opts}, _From, State0
) ->
    {Reply, State} = handle_register_refresh(ResId, RefreshFn, Opts, State0),
    {reply, Reply, State};
handle_call(#unregister{res_id = ResId}, _From, State0) ->
    State = handle_unregister(ResId, State0),
    {reply, ok, State};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info({timeout, _TRef, #refresh{res_id = ResId}}, #{?timers := Timers0} = State0) when
    is_map_key(ResId, Timers0)
->
    Timers = maps:remove(ResId, Timers0),
    State1 = State0#{?timers := Timers},
    State = handle_refresh(ResId, State1),
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

call(ServerRef, Call) ->
    gen_server:call(ServerRef, Call, infinity).

get_cached(Tab, ResId) ->
    NowMS = now_ms(),
    case ets:lookup(Tab, ?KEY(ResId)) of
        [?TOKEN_ROW(?KEY(ResId), Deadline, Response)] when Deadline > NowMS ->
            {ok, Response};
        _ ->
            error
    end.

handle_register_refresh(ResId, RefreshFn, Opts, State0) ->
    #{?table := Tab} = State0,
    %% Might've been just stored by a previous call.
    case get_cached(Tab, ResId) of
        {ok, Response} ->
            %% Store provided refresh fn, as it could be an updated version.
            State = maps:update_with(
                ?registered,
                fun(R) -> R#{ResId => {RefreshFn, Opts}} end,
                State0
            ),
            {Response, State};
        error ->
            do_handle_register_refresh(ResId, RefreshFn, Opts, State0)
    end.

do_handle_register_refresh(ResId, RefreshFn, Opts, State0) ->
    #{?table := Tab} = State0,
    case do_fetch_token(ResId, RefreshFn) of
        {ok, ExpiryMS, Token} ->
            State1 = store_token_and_schedule_refresh(ExpiryMS, ResId, Token, State0),
            State = maps:update_with(
                ?registered,
                fun(R) -> R#{ResId => {RefreshFn, Opts}} end,
                State1
            ),
            {{ok, Token}, State};
        {error, Reason} ->
            CacheFailuresFor = maps:get(cache_failures_for, Opts, timer:seconds(1)),
            Deadline = now_ms() + CacheFailuresFor,
            _ = ets:insert(Tab, ?TOKEN_ROW(?KEY(ResId), Deadline, {error, Reason})),
            {{error, Reason}, State0}
    end.

do_fetch_token(ResId, RefreshFn) ->
    try
        call_refresh_fn(RefreshFn)
    catch
        Kind:Reason:Stacktrace ->
            ?tp(error, "token_refresh_exception", #{
                res_id => ResId,
                kind => Kind,
                reason => Reason,
                stacktrace => Stacktrace
            }),
            {error, {Kind, Reason}}
    end.

call_refresh_fn(RefreshFn) when is_function(RefreshFn, 0) ->
    RefreshFn();
call_refresh_fn({M, F, A}) ->
    apply(M, F, A).

handle_unregister(ResId, State0) ->
    #{?table := Tab} = State0,
    _ = ets:delete(Tab, ?KEY(ResId)),
    State1 = maps:update_with(?registered, fun(R) -> maps:remove(ResId, R) end, State0),
    clear_refresh_timer(ResId, State1).

handle_refresh(ResId, #{?registered := Registered} = State0) when
    not is_map_key(ResId, Registered)
->
    %% Shouldn't be possible
    ?tp(error, "token_refresh_unknown_clientid", #{clientid => ResId}),
    State0;
handle_refresh(ResId, #{?registered := Registered} = State0) ->
    {RefreshFn, Opts} = maps:get(ResId, Registered),
    ?tp(info, "token_refreshing", #{res_id => ResId}),
    case do_fetch_token(ResId, RefreshFn) of
        {ok, ExpiryMS, Token} ->
            ?tp(info, "token_refreshed", #{res_id => ResId, expiry_ms => ExpiryMS}),
            store_token_and_schedule_refresh(ExpiryMS, ResId, Token, State0);
        {error, Reason} ->
            ?tp(warning, "token_refresh_failed", #{res_id => ResId, reason => Reason}),
            RetryTime = maps:get(retry_interval, Opts, timer:seconds(5)),
            ensure_refresh_timer(ResId, RetryTime, State0)
    end.

store_token_and_schedule_refresh(ExpiryMS, ResId, Token, State0) ->
    #{?table := Tab} = State0,
    Deadline = now_ms() + ExpiryMS,
    _ = ets:insert(Tab, ?TOKEN_ROW(?KEY(ResId), Deadline, {ok, Token})),
    %% Tokens typically last for 1 hour
    RefreshTime = ceil(ExpiryMS * 0.75),
    ?tp("token_success", #{
        res_id => ResId,
        expiry_ms => ExpiryMS,
        refresh_after => RefreshTime
    }),
    ensure_refresh_timer(ResId, RefreshTime, State0).

ensure_refresh_timer(ResId, _Time, #{?timers := Timers} = State) when
    is_map_key(ResId, Timers)
->
    State;
ensure_refresh_timer(ResId, Time, #{?timers := Timers0} = State0) ->
    TRef = emqx_utils:start_timer(Time, #refresh{res_id = ResId}),
    Timers = Timers0#{ResId => TRef},
    State0#{?timers := Timers}.

clear_refresh_timer(ResId, #{?timers := Timers0} = State0) ->
    case maps:take(ResId, Timers0) of
        error ->
            State0;
        {TRef, Timers} ->
            emqx_utils:cancel_timer(TRef),
            State0#{?timers := Timers}
    end.

now_ms() ->
    erlang:system_time(millisecond).
