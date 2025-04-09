%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_login_lock).

-include("emqx_dashboard.hrl").

-include_lib("emqx/include/logger.hrl").

-behaviour(gen_server).

-export([
    register_unsuccessful_login/1,
    verify/1,
    reset/1,
    is_login_locked_error/1
]).

-export([
    create_tables/0,
    start_link/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

%% For testing/debugging
-export([
    cleanup_all/0,
    info/0
]).

-type time_s() :: non_neg_integer().
-type time_us() :: non_neg_integer().
-type username() :: binary().

-record(login_lock, {
    username :: username() | '_',
    locked_until :: time_s() | '_'
}).

%% NOTE
%% We ignore the fact that we may lose some attempts
%% if they happen in the same microsecond.
-record(login_attempts, {
    key :: {username(), time_us()} | {'_', '_'} | '_',
    extra :: map() | '_'
}).

-define(TAB_ATTEMPTS, login_attempts).
-define(TAB_LOCK, login_lock).

-ifdef(TEST).
-define(CLEANUP_INTERVAL, 100).
-else.
-define(CLEANUP_INTERVAL, timer:minutes(30)).
-endif.

-define(ETS_CHUNK_SIZE, 100).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec register_unsuccessful_login(username()) -> ok.
register_unsuccessful_login(Username) ->
    NowUs = now_us(),
    Key = {Username, NowUs},
    Attempt = #login_attempts{key = Key, extra = #{}},
    max_attempts_reached(Username, NowUs) andalso lock(Username, NowUs),
    ok = mria:dirty_write(?TAB_ATTEMPTS, Attempt).

-spec verify(username()) -> ok | {error, login_locked}.
verify(Username) ->
    Now = erlang:system_time(second),
    case mnesia:dirty_read(?TAB_LOCK, Username) of
        [#login_lock{locked_until = LockedUntil}] when LockedUntil > Now ->
            {error, login_locked};
        _ ->
            ok
    end.

-spec is_login_locked_error(term()) -> boolean().
is_login_locked_error(login_locked) ->
    true;
is_login_locked_error(_) ->
    false.

-spec reset(username()) -> ok.
reset(Username) ->
    cleanup(Username).

-spec info() ->
    #{
        failed_attempt_records := non_neg_integer(),
        lock_records := non_neg_integer()
    }.
info() ->
    #{
        failed_attempt_records => mnesia:table_info(?TAB_ATTEMPTS, size),
        lock_records => mnesia:table_info(?TAB_LOCK, size)
    }.

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

create_tables() ->
    ok = mria:create_table(?TAB_ATTEMPTS, [
        {type, ordered_set},
        {rlog_shard, ?DASHBOARD_SHARD},
        {storage, disc_copies},
        {record_name, login_attempts},
        {attributes, record_info(fields, login_attempts)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    ok = mria:create_table(?TAB_LOCK, [
        {type, set},
        {rlog_shard, ?DASHBOARD_SHARD},
        {storage, disc_copies},
        {record_name, login_lock},
        {attributes, record_info(fields, login_lock)},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    [?TAB_ATTEMPTS, ?TAB_LOCK].

start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([]) ->
    _ = erlang:send_after(?CLEANUP_INTERVAL, self(), cleanup),
    {ok, #{}, hibernate}.

handle_call(Request, _From, State) ->
    ?SLOG(warning, #{
        msg => emqx_dashboard_login_lock_unexpected_call,
        request => Request
    }),
    {reply, error, State, hibernate}.

handle_cast(Request, State) ->
    ?SLOG(warning, #{
        msg => emqx_dashboard_login_lock_unexpected_cast,
        request => Request
    }),
    {noreply, State, hibernate}.

handle_info(cleanup, State) ->
    ok = cleanup_all(now_us()),
    _ = erlang:send_after(?CLEANUP_INTERVAL, self(), cleanup),
    {noreply, State, hibernate};
handle_info(Info, State) ->
    ?SLOG(warning, #{
        msg => emqx_dashboard_login_lock_unexpected_info,
        info => Info
    }),
    {noreply, State, hibernate}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

max_attempts() ->
    emqx_config:get([dashboard, unsuccessful_login_max_attempts], 5).

lock_duration_s() ->
    emqx_config:get([dashboard, unsuccessful_login_lock_duration], 600).

interval_s() ->
    emqx_config:get([dashboard, unsuccessful_login_interval], 300).

max_attempts_reached(Username, NowUs) ->
    MinTimeUs = NowUs - erlang:convert_time_unit(interval_s(), second, microsecond),
    MaxCount = max_attempts() - 1,
    max_attempts_reached(Username, MinTimeUs, MaxCount, {Username, infinity}, 0).

max_attempts_reached(_Username, _MinTimeUs, MaxCount, _CurrentKey, Count) when Count >= MaxCount ->
    true;
max_attempts_reached(_Username, MinTimeUs, _MaxCount, {_, TimeUs}, _Count) when
    TimeUs < MinTimeUs
->
    false;
max_attempts_reached(Username, MinTimeUs, MaxCount, CurrentKey, Count) ->
    case mnesia:dirty_prev(?TAB_ATTEMPTS, CurrentKey) of
        '$end_of_table' ->
            false;
        {Username, _TimeUs} = NewKey ->
            max_attempts_reached(Username, MinTimeUs, MaxCount, NewKey, Count + 1);
        {_OtherUsername, _TimeUs} ->
            false
    end.

lock(Username, NowUs) ->
    LockedUntil = lock_duration_s() + erlang:convert_time_unit(NowUs, microsecond, second),
    ok = mria:dirty_write(?TAB_LOCK, #login_lock{username = Username, locked_until = LockedUntil}).

cleanup_all() ->
    {atomic, ok} = mria:clear_table(?TAB_ATTEMPTS),
    {atomic, ok} = mria:clear_table(?TAB_LOCK),
    ok.

cleanup_all(NowUs) ->
    cleanup_attempts(NowUs),
    cleanup_locks(NowUs).

cleanup_attempts(NowUs) ->
    MaxAttemptTimeUs = NowUs - erlang:convert_time_unit(interval_s(), second, microsecond),
    Stream0 = ets_stream(?TAB_ATTEMPTS, #login_attempts{_ = '_'}),
    Stream1 = emqx_utils_stream:filter(
        fun(#login_attempts{key = {_, TimeUs}}) ->
            TimeUs < MaxAttemptTimeUs
        end,
        Stream0
    ),
    emqx_utils_stream:foreach(
        fun(#login_attempts{key = Key}) ->
            mria:dirty_delete(?TAB_ATTEMPTS, Key)
        end,
        Stream1
    ).

cleanup_locks(NowUs) ->
    MaxLockTimeS = erlang:convert_time_unit(NowUs, microsecond, second),
    Stream0 = ets_stream(?TAB_LOCK, #login_lock{_ = '_'}),
    Stream1 = emqx_utils_stream:filter(
        fun(#login_lock{locked_until = LockedUntil}) ->
            LockedUntil < MaxLockTimeS
        end,
        Stream0
    ),
    emqx_utils_stream:foreach(
        fun(#login_lock{username = Username}) ->
            mria:dirty_delete(?TAB_LOCK, Username)
        end,
        Stream1
    ).

cleanup(Username) ->
    _ = mria:dirty_delete(?TAB_LOCK, Username),
    Stream = ets_stream(?TAB_ATTEMPTS, #login_attempts{key = {Username, '_'}, _ = '_'}),
    emqx_utils_stream:foreach(
        fun(#login_attempts{key = Key}) ->
            mria:dirty_delete(?TAB_ATTEMPTS, Key)
        end,
        Stream
    ).

ets_stream(Tab, MatchSpec) ->
    emqx_utils_stream:ets(
        fun
            (undefined) -> ets:match_object(Tab, MatchSpec, ?ETS_CHUNK_SIZE);
            (Cont) -> ets:match_object(Cont)
        end
    ).

now_us() ->
    erlang:system_time(microsecond).
