%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_stats).

-moduledoc """
Owns the ETS table that tracks per-username quota exceeded statistics.

Each row stores the node-local count of rejected authentication attempts for a username
and the timestamp of the most recent rejection. The server also centralizes throttled
warning logging for quota exceeded events.
""".

-behaviour(gen_server).

-export([
    start_link/0,
    record_quota_exceeded/1,
    get/1,
    reset/0
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include("emqx_username_quota.hrl").

-define(SERVER, ?MODULE).
-define(STATS_TAB, emqx_username_quota_stats).
-define(LOG_INTERVAL_MS, 60_000).

-record(?STATS_TAB, {
    username :: binary(),
    quota_exceeded_count = 0 :: non_neg_integer(),
    last_quota_exceeded_at = 0 :: integer()
}).

-doc """
Start the stats server and create the ETS table it owns.
""".
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-doc """
Record one quota exceeded event for Username.
""".
record_quota_exceeded(Username) when is_binary(Username) ->
    gen_server:cast(?SERVER, {record_quota_exceeded, Username}).

-doc """
Return the stored quota exceeded statistics for Username.
""".
-spec get(binary()) ->
    {ok, #{
        username := binary(),
        quota_exceeded_count := pos_integer(),
        last_quota_exceeded_at := pos_integer()
    }}
    | {error, not_found | not_ready}.
get(Username) when is_binary(Username) ->
    try ets:lookup(?STATS_TAB, Username) of
        [
            #?STATS_TAB{
                quota_exceeded_count = Count,
                last_quota_exceeded_at = LastQuotaExceededAt
            }
        ] ->
            {ok, #{
                username => Username,
                quota_exceeded_count => Count,
                last_quota_exceeded_at => LastQuotaExceededAt
            }};
        [] ->
            {error, not_found}
    catch
        _:_ ->
            {error, not_ready}
    end.

-doc """
Clear all recorded quota exceeded statistics.
""".
reset() ->
    case whereis(?SERVER) of
        undefined ->
            ok;
        _Pid ->
            gen_server:call(?SERVER, reset)
    end.

init([]) ->
    ok = emqx_utils_ets:new(?STATS_TAB, [
        named_table, ordered_set, public, {keypos, #?STATS_TAB.username}
    ]),
    {ok, #{}}.

handle_call(reset, _From, State) ->
    true = ets:delete_all_objects(?STATS_TAB),
    {reply, ok, State};
handle_call(_Request, _From, State) ->
    {reply, ignored, State}.

handle_cast({record_quota_exceeded, Username}, State) ->
    Now = erlang:system_time(millisecond),
    PrevLastQuotaExceededAt =
        case ets:lookup(?STATS_TAB, Username) of
            [#?STATS_TAB{last_quota_exceeded_at = Ts}] -> Ts;
            [] -> 0
        end,
    NewCount = ets:update_counter(
        ?STATS_TAB,
        Username,
        {#?STATS_TAB.quota_exceeded_count, 1},
        #?STATS_TAB{
            username = Username,
            quota_exceeded_count = 0,
            last_quota_exceeded_at = Now
        }
    ),
    true = ets:update_element(
        ?STATS_TAB,
        Username,
        {#?STATS_TAB.last_quota_exceeded_at, Now}
    ),
    maybe_log_quota_exceeded(Username, NewCount, PrevLastQuotaExceededAt, Now),
    {noreply, State};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

maybe_log_quota_exceeded(Username, Count, PrevLastQuotaExceededAt, Now) ->
    case should_log_quota_exceeded(Count, PrevLastQuotaExceededAt, Now) of
        true ->
            ?LOG(warning, #{
                msg => "username_quota_exceeded",
                username => Username,
                node_local_count => Count
            });
        false ->
            ok
    end.

should_log_quota_exceeded(Count, PrevLastQuotaExceededAt, Now) ->
    PrevLastQuotaExceededAt =:= 0 orelse
        Now - PrevLastQuotaExceededAt > ?LOG_INTERVAL_MS orelse
        Count rem 100 =:= 0.
