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
    cleanup_all/1
]).

-type time_us() :: non_neg_integer().
-type username() :: binary().

%% NOTE
%% We ignore the fact that we may lose some attempts
%% if they happen in the same microsecond.
-record(login_attempts, {
    key :: {username(), time_us() | '$1'},
    extra :: map() | '_'
}).

-define(TAB_ATTEMPTS, login_attempts).

-ifdef(TEST).
-define(CLEANUP_INTERVAL, 100).
-else.
-define(CLEANUP_INTERVAL, timer:minutes(30)).
-endif.

-define(CLEANUP_CHUNK_SIZE, 100).

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
    case emqx_dashboard_admin:get_login_lock(Username) of
        {ok, LockedUntil} when LockedUntil > Now ->
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

%%--------------------------------------------------------------------
%% Internal API
%%--------------------------------------------------------------------

create_tables() ->
    ok = mria:create_table(?TAB_ATTEMPTS, [
        {type, ordered_set},
        {rlog_shard, ?DASHBOARD_SHARD},
        {storage, rocksdb_copies},
        {record_name, login_attempts},
        {attributes, record_info(fields, login_attempts)}
    ]),
    [?TAB_ATTEMPTS].

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
    {ok, _} = emqx_dashboard_admin:set_login_lock(Username, LockedUntil).

cleanup_all() ->
    {atomic, ok} = mria:clear_table(?TAB_ATTEMPTS),
    ok.

cleanup_all(NowUs) ->
    cleanup_attempts(NowUs).

cleanup_attempts(NowUs) ->
    MaxAttemptTimeUs = NowUs - erlang:convert_time_unit(interval_s(), second, microsecond),
    cleanup_attempts(MaxAttemptTimeUs, mnesia:dirty_first(?TAB_ATTEMPTS), []).

cleanup_attempts(_MaxAttemptTimeUs, '$end_of_table', []) ->
    ok;
cleanup_attempts(_MaxAttemptTimeUs, '$end_of_table', KeysAcc) ->
    ok = delete_attempts(KeysAcc);
cleanup_attempts(MaxAttemptTimeUs, Key, KeysAcc) when length(KeysAcc) >= ?CLEANUP_CHUNK_SIZE ->
    ok = delete_attempts(KeysAcc),
    cleanup_attempts(MaxAttemptTimeUs, Key, []);
cleanup_attempts(MaxAttemptTimeUs, {_, TimeUs} = Key, KeysAcc0) when TimeUs < MaxAttemptTimeUs ->
    cleanup_attempts(MaxAttemptTimeUs, mnesia:dirty_next(?TAB_ATTEMPTS, Key), [Key | KeysAcc0]);
cleanup_attempts(MaxAttemptTimeUs, Key, KeysAcc) ->
    cleanup_attempts(MaxAttemptTimeUs, mnesia:dirty_next(?TAB_ATTEMPTS, Key), KeysAcc).

cleanup(Username) ->
    _ = emqx_dashboard_admin:clear_login_lock(Username),
    Keys = user_attempt_keys(Username),
    delete_attempts(Keys).

user_attempt_keys(Username) ->
    mnesia:dirty_select(?TAB_ATTEMPTS, [
        {
            #login_attempts{key = {Username, '$1'}, _ = '_'},
            [],
            [{{Username, '$1'}}]
        }
    ]).

delete_attempts(Keys) ->
    _ = mria:async_dirty(?DASHBOARD_SHARD, fun() ->
        lists:foreach(
            fun(Key) ->
                _ = mria:dirty_delete(?TAB_ATTEMPTS, Key)
            end,
            Keys
        )
    end),
    ok.

now_us() ->
    erlang:system_time(microsecond).
